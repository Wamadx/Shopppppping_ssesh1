use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

const INTERNED_BIT: u64 = 1u64 << 63;
const INTERNED_TAG_SHIFT: u32 = 55;
const INTERNED_LEN_SHIFT: u32 = 47;
const INTERNED_LEN_MASK: u64 = 0xFF;
const INTERNED_PTR_MASK: u64 = (1u64 << 47) - 1;
const NUM_INTERN_SHARDS: usize = 256;
const INTERN_ARENA_CHUNK: usize = 256 * 1024;
const INTERN_CACHE_SLOTS: usize = 4096;

pub const EID_ARENA_MAX_BYTES: usize = 1 << 28; // 256 MiB per worker; (off >> 2) fits in 26 bits

/// Worker-local byte arena for non-inline (`>9` bytes or non-ASCII) `event_id`s.
/// Eliminates global atomic interning and pointer chasing for `event_id`:
/// a 33-bit hash, worker index (`0..15`), and 4-byte-aligned offset live inside `Key(u64)`.
pub struct EidArena {
    buf: Vec<u8>,
}

impl EidArena {
    pub fn with_capacity(cap_bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap_bytes.min(EID_ARENA_MAX_BYTES)),
        }
    }

    #[inline(always)]
    pub fn encode(&mut self, s: &[u8], worker_id: usize) -> Option<Key> {
        let len = s.len();
        if (1..=9).contains(&len) {
            #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
            {
                if let Some(k) = unsafe { Key::pack_bmi2(s) } {
                    return Some(k);
                }
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
            {
                if let Some(k) = Key::pack_scalar(s) {
                    return Some(k);
                }
            }
        }
        self.encode_long(s, worker_id)
    }

    #[cold]
    #[inline(never)]
    fn encode_long(&mut self, s: &[u8], worker_id: usize) -> Option<Key> {
        if worker_id >= 16 || s.len() > u16::MAX as usize {
            return None;
        }
        let aligned = (self.buf.len() + 3) & !3;
        let need = aligned.checked_add(2)?.checked_add(s.len())?;
        if need > EID_ARENA_MAX_BYTES {
            return None;
        }
        if aligned > self.buf.len() {
            self.buf.resize(aligned, 0);
        }
        let h = hash_bytes(s);
        let h33 = (h >> 31) & 0x1_FFFF_FFFF;
        self.buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
        self.buf.extend_from_slice(s);
        Some(Key(
            INTERNED_BIT | (h33 << 30) | ((worker_id as u64) << 26) | ((aligned as u64) >> 2),
        ))
    }
}

#[inline(always)]
fn hash_word(h: u64, word: u64) -> u64 {
    (h.rotate_left(5) ^ word).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95)
}

/// Fast, deterministic hash used once for the shard, table, and local cache.
#[inline]
fn hash_bytes(mut s: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64 ^ s.len() as u64;
    while s.len() >= 8 {
        let w = u64::from_le(unsafe { (s.as_ptr() as *const u64).read_unaligned() });
        h = hash_word(h, w);
        s = &s[8..];
    }
    if s.len() >= 4 {
        let w = u32::from_le(unsafe { (s.as_ptr() as *const u32).read_unaligned() }) as u64;
        h = hash_word(h, w);
        s = &s[4..];
    }
    if s.len() >= 2 {
        let w = u16::from_le(unsafe { (s.as_ptr() as *const u16).read_unaligned() }) as u64;
        h = hash_word(h, w);
        s = &s[2..];
    }
    if let Some(&b) = s.first() {
        h = hash_word(h, b as u64);
    }
    let r = (h as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (r as u64) ^ (r >> 64) as u64
}

#[derive(Default)]
struct InternArena {
    // Each Vec retains its allocation; its length stays zero while offset tracks
    // initialized bytes. Moving the Vec never moves the allocation itself.
    chunks: Vec<Vec<u8>>,
    offset: usize,
}

impl Drop for InternArena {
    fn drop(&mut self) {
        // Keys can outlive the worker that allocated their bytes (notably the
        // links workers), so thread-local arenas are intentionally leaked.
        for chunk in self.chunks.drain(..) {
            std::mem::forget(chunk);
        }
    }
}

impl InternArena {
    fn alloc(&mut self, s: &[u8]) -> &'static [u8] {
        let need_chunk = match self.chunks.last() {
            Some(chunk) => chunk.capacity() - self.offset < s.len(),
            None => true,
        };
        if need_chunk {
            self.chunks
                .push(Vec::with_capacity(INTERN_ARENA_CHUNK.max(s.len())));
            self.offset = 0;
        }
        let chunk = self.chunks.last_mut().unwrap();
        let start = self.offset;
        unsafe {
            let ptr = chunk.as_mut_ptr().add(start);
            std::ptr::copy_nonoverlapping(s.as_ptr(), ptr, s.len());
            self.offset = start + s.len();
            // INTERNERS and its arena allocations live until process exit. No
            // later allocation writes this range or reallocates this chunk.
            std::slice::from_raw_parts(ptr, s.len())
        }
    }
}

#[derive(Clone, Copy, Default)]
struct InternSlot {
    hash: u64,
    idx_plus_one: u32,
}

#[derive(Default)]
struct Interner {
    slots: Vec<InternSlot>,
    slices: Vec<&'static [u8]>,
    arena: InternArena,
}

impl Interner {
    fn lookup(&self, s: &[u8], hash: u64) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut pos = (hash as usize) & mask;
        loop {
            let slot = unsafe { *self.slots.get_unchecked(pos) };
            if slot.idx_plus_one == 0 {
                return None;
            }
            let idx = slot.idx_plus_one - 1;
            if slot.hash == hash && self.slices[idx as usize] == s {
                return Some(idx);
            }
            pos = (pos + 1) & mask;
        }
    }

    fn grow(&mut self) {
        let cap = if self.slots.is_empty() {
            1024
        } else {
            self.slots.len() * 2
        };
        let mut slots = vec![InternSlot::default(); cap];
        let mask = cap - 1;
        for slot in &self.slots {
            if slot.idx_plus_one != 0 {
                let mut pos = (slot.hash as usize) & mask;
                while slots[pos].idx_plus_one != 0 {
                    pos = (pos + 1) & mask;
                }
                slots[pos] = *slot;
            }
        }
        self.slots = slots;
    }

    fn intern(&mut self, s: &[u8], hash: u64, sh: usize) -> Key {
        if let Some(idx) = self.lookup(s, hash) {
            return Key::encode_interned(sh, idx, self.slices[idx as usize], hash);
        }
        if self.slots.is_empty() || (self.slices.len() + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mask = self.slots.len() - 1;
        let mut pos = (hash as usize) & mask;
        while self.slots[pos].idx_plus_one != 0 {
            pos = (pos + 1) & mask;
        }
        let idx = u32::try_from(self.slices.len()).expect("too many interned IDs");
        let stored = self.arena.alloc(s);
        self.slices.push(stored);
        self.slots[pos] = InternSlot {
            hash,
            idx_plus_one: idx + 1,
        };
        Key::encode_interned(sh, idx, stored, hash)
    }
}

static INTERNERS: OnceLock<[Mutex<Interner>; NUM_INTERN_SHARDS]> = OnceLock::new();

fn interners() -> &'static [Mutex<Interner>; NUM_INTERN_SHARDS] {
    INTERNERS.get_or_init(|| std::array::from_fn(|_| Mutex::new(Interner::default())))
}

// Input parsing is the only phase that creates IDs. Keep the common path entirely
// lock-free; the legacy mutex tables remain as an exact overflow fallback.
const FAST_INTERN_SLOTS: usize = 1 << 13;
const FAST_INTERN_LIMIT: usize = FAST_INTERN_SLOTS * 3 / 4;

#[repr(align(64))]
struct FastInterner {
    slots: Vec<AtomicU64>,
    next: AtomicUsize,
    active: AtomicUsize,
    closed: AtomicBool,
}

unsafe impl Sync for FastInterner {}

impl FastInterner {
    fn new() -> Self {
        let layout = std::alloc::Layout::array::<AtomicU64>(FAST_INTERN_SLOTS).unwrap();
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) as *mut AtomicU64 };
        assert!(!ptr.is_null());
        let slots = unsafe { Vec::from_raw_parts(ptr, FAST_INTERN_SLOTS, FAST_INTERN_SLOTS) };
        Self {
            slots,
            next: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            closed: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn lookup(&self, s: &[u8], hash: u64) -> Option<Key> {
        let len = s.len();
        if len == 0 || len > INTERNED_LEN_MASK as usize {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut pos = (hash as usize) & mask;
        let tag_len = (((hash >> 48) & 0xFF) << INTERNED_TAG_SHIFT)
            | ((len as u64) << INTERNED_LEN_SHIFT);
        let tag_len_mask =
            (0xFFu64 << INTERNED_TAG_SHIFT) | (INTERNED_LEN_MASK << INTERNED_LEN_SHIFT);
        loop {
            let v = unsafe { self.slots.get_unchecked(pos) }.load(Ordering::Acquire);
            if v == 0 {
                return None;
            }
            if (v & tag_len_mask) == tag_len {
                let ptr = (v & INTERNED_PTR_MASK) as *const u8;
                let stored = unsafe { std::slice::from_raw_parts(ptr, len) };
                if stored == s {
                    return Some(Key(v));
                }
            }
            pos = (pos + 1) & mask;
        }
    }

    // None means that this shard closed at its load limit. The caller drains
    // active inserts before crossing to the legacy table so the two tables can
    // never return different canonical keys for the same bytes.
    fn intern(&self, s: &[u8], hash: u64) -> Option<Key> {
        let len = s.len();
        if len == 0 || len > INTERNED_LEN_MASK as usize || self.closed.load(Ordering::Acquire) {
            return None;
        }
        self.active.fetch_add(1, Ordering::Acquire);
        if self.closed.load(Ordering::Acquire) {
            self.active.fetch_sub(1, Ordering::Release);
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        if idx >= FAST_INTERN_LIMIT {
            self.closed.store(true, Ordering::Release);
            self.active.fetch_sub(1, Ordering::Release);
            return None;
        }

        let stored = INTERN_ARENA.with(|arena| arena.borrow_mut().alloc(s));
        let Some(wanted_key) = Key::encode_direct(stored, hash) else {
            self.closed.store(true, Ordering::Release);
            self.active.fetch_sub(1, Ordering::Release);
            return None;
        };
        let wanted = wanted_key.0;
        let tag_len_mask =
            (0xFFu64 << INTERNED_TAG_SHIFT) | (INTERNED_LEN_MASK << INTERNED_LEN_SHIFT);
        let tag_len = wanted & tag_len_mask;
        let mask = self.slots.len() - 1;
        let mut pos = (hash as usize) & mask;
        let key = loop {
            let slot = unsafe { self.slots.get_unchecked(pos) };
            let mut v = slot.load(Ordering::Acquire);
            if v == 0 {
                match slot.compare_exchange_weak(0, wanted, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break wanted_key,
                    Err(actual) => v = actual,
                }
            }
            if (v & tag_len_mask) == tag_len {
                let ptr = (v & INTERNED_PTR_MASK) as *const u8;
                let other = unsafe { std::slice::from_raw_parts(ptr, len) };
                if other == s {
                    break Key(v);
                }
            }
            pos = (pos + 1) & mask;
        };
        self.active.fetch_sub(1, Ordering::Release);
        Some(key)
    }

    fn fallback(&self, s: &[u8], hash: u64, sh: usize) -> Key {
        while self.active.load(Ordering::Acquire) != 0 {
            std::hint::spin_loop();
        }
        self.lookup(s, hash)
            .unwrap_or_else(|| interners()[sh].lock().unwrap().intern(s, hash, sh))
    }
}

static FAST_INTERNERS: OnceLock<[FastInterner; NUM_INTERN_SHARDS]> = OnceLock::new();

fn fast_interners() -> &'static [FastInterner; NUM_INTERN_SHARDS] {
    FAST_INTERNERS.get_or_init(|| std::array::from_fn(|_| FastInterner::new()))
}

thread_local! {
    static INTERN_ARENA: RefCell<InternArena> = const { RefCell::new(InternArena { chunks: Vec::new(), offset: 0 }) };
}

#[derive(Clone, Copy)]
struct InternCacheEntry {
    hash: u64,
    key: Key,
}

thread_local! {
    static INTERN_CACHE: RefCell<[InternCacheEntry; INTERN_CACHE_SLOTS]> = const {
        RefCell::new([InternCacheEntry { hash: 0, key: Key::EMPTY }; INTERN_CACHE_SLOTS])
    };
}

/// 8-byte order-preserving ID key.
/// - Bit 63 == 0: 1..=9 ASCII bytes in 0x01..=0x7F packed 7 bits/char MSB-first.
///   Integer `<` equals byte-wise lexicographical `<`.
/// - Bit 63 == 1: interned. Normally `bits 48..62` = length and `bits 0..46` =
///   the stable pointer, so byte access needs no lock. The supported x86_64
///   targets use user pointers below 2^47; this is checked at insertion.
/// - Zero length bits select the legacy `(shard << 32) | idx` representation.
///   Empty/oversized strings and unusual high pointers retain that safe path.
/// - `Key(0)` is `EMPTY` sentinel (never produced by `from_bytes`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Key(pub u64);

impl Key {
    pub const EMPTY: Self = Self(0);

    #[inline(always)]
    pub fn from_bytes(s: &[u8]) -> Self {
        Self::from_bytes_impl(s, 0)
    }

    /// Keeps stable bytes for comparisons but does not canonicalize identity.
    /// Only for fields such as click_id that are ordered, never hashed/routed.
    #[inline(always)]
    pub fn from_bytes_unordered(s: &[u8]) -> Self {
        Self::from_bytes_impl(s, 1)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    #[inline(always)]
    fn pack_scalar(s: &[u8]) -> Option<Self> {
        let mut acc = 0u64;
        let mut shift = 56i32;
        for &b in s {
            if b == 0 || b >= 0x80 {
                return None;
            }
            acc |= (b as u64) << shift;
            shift -= 7;
        }
        Some(Self(acc))
    }

    #[inline(always)]
    fn from_bytes_impl(s: &[u8], kind: u8) -> Self {
        let len = s.len();
        if (1..=9).contains(&len) {
            #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
            {
                if let Some(k) = unsafe { Self::pack_bmi2(s) } {
                    return k;
                }
                return Self::id_slow(s, kind);
            }
            #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
            {
                if let Some(k) = Self::pack_scalar(s) {
                    return k;
                }
                return Self::id_slow(s, kind);
            }
        }
        Self::id_slow(s, kind)
    }

    #[inline(always)]
    fn id_slow(s: &[u8], kind: u8) -> Self {
        if kind != 1 {
            return Self::intern_slow(s, hash_bytes(s));
        }
        let stored = INTERN_ARENA.with(|arena| arena.borrow_mut().alloc(s));
        if let Some(k) = Self::encode_direct(stored, 0) {
            k
        } else {
            Self::intern_slow(s, hash_bytes(s))
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "bmi2")]
    #[inline]
    unsafe fn pack_bmi2(s: &[u8]) -> Option<Self> {
        let len = s.len();
        let k = len.min(8);
        let word: u64 = if k == 8 {
            u64::from_le_bytes(unsafe { *(s.as_ptr() as *const [u8; 8]) })
        } else if k >= 4 {
            let lo = u32::from_le_bytes(unsafe { *(s.as_ptr() as *const [u8; 4]) }) as u64;
            let hi =
                u32::from_le_bytes(unsafe { *(s.as_ptr().add(k - 4) as *const [u8; 4]) }) as u64;
            let combined = lo | (hi << ((k - 4) * 8));
            combined & ((!0u64) >> ((8 - k) * 8))
        } else {
            let mut w = s[0] as u64;
            if k >= 2 {
                w |= (s[1] as u64) << 8;
            }
            if k == 3 {
                w |= (s[2] as u64) << 16;
            }
            w
        };

        let pad = (!0u64).checked_shl((k * 8) as u32).unwrap_or(0) & 0x0101_0101_0101_0101;
        let cw = word | pad;
        let bad = (word & 0x8080_8080_8080_8080)
            | (cw.wrapping_sub(0x0101_0101_0101_0101) & 0x8080_8080_8080_8080);
        if bad != 0 {
            return None;
        }
        let mut acc = core::arch::x86_64::_pext_u64(word.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F) << 7;
        if len == 9 {
            let b8 = s[8];
            if b8 == 0 || b8 >= 0x80 {
                return None;
            }
            acc |= b8 as u64;
        }
        Some(Self(acc))
    }

    #[inline(always)]
    fn encode_direct(s: &'static [u8], hash: u64) -> Option<Self> {
        let ptr = s.as_ptr() as usize as u64;
        let len = s.len() as u64;
        if len != 0 && len <= INTERNED_LEN_MASK && ptr <= INTERNED_PTR_MASK {
            let tag8 = (hash >> 48) & 0xFF;
            Some(Self(
                INTERNED_BIT | (tag8 << INTERNED_TAG_SHIFT) | (len << INTERNED_LEN_SHIFT) | ptr,
            ))
        } else {
            None
        }
    }

    #[inline]
    fn encode_interned(sh: usize, idx: u32, s: &'static [u8], hash: u64) -> Self {
        if let Some(k) = Self::encode_direct(s, hash) {
            k
        } else {
            Self(INTERNED_BIT | ((sh as u64) << 32) | (idx as u64))
        }
    }

    #[inline]
    fn intern_slow(s: &[u8], hash: u64) -> Self {
        let cache_slot = ((hash >> 24) as usize) & (INTERN_CACHE_SLOTS - 1);
        let cached = INTERN_CACHE.with(|cache| {
            let entry = cache.borrow()[cache_slot];
            if entry.hash == hash && entry.key != Self::EMPTY && entry.key.long_bytes() == s {
                Some(entry.key)
            } else {
                None
            }
        });
        if let Some(key) = cached {
            return key;
        }

        let sh = ((hash >> 56) as usize) & (NUM_INTERN_SHARDS - 1);
        let fast = &fast_interners()[sh];
        let key = if let Some(key) = fast.lookup(s, hash) {
            key
        } else {
            fast.intern(s, hash)
                .unwrap_or_else(|| fast.fallback(s, hash, sh))
        };
        INTERN_CACHE.with(|cache| {
            cache.borrow_mut()[cache_slot] = InternCacheEntry { hash, key };
        });
        key
    }

    #[inline(always)]
    pub fn unpack_inline(self, buf: &mut [u8; 9]) -> usize {
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        {
            if self.0 == 0 {
                return 0;
            }
            let be = unsafe {
                core::arch::x86_64::_pdep_u64(self.0 >> 7, 0x7F7F_7F7F_7F7F_7F7F)
            };
            buf[..8].copy_from_slice(&be.to_be_bytes());
            let b8 = (self.0 & 0x7F) as u8;
            buf[8] = b8;
            if b8 != 0 {
                9
            } else {
                8 - (be.trailing_zeros() >> 3) as usize
            }
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        {
            let mut v = self.0;
            let mut len = 0usize;
            while len < 9 {
                let b = ((v >> 56) & 0x7f) as u8;
                if b == 0 {
                    break;
                }
                buf[len] = b;
                len += 1;
                v <<= 7;
            }
            len
        }
    }

    #[inline(always)]
    fn long_bytes(self) -> &'static [u8] {
        let len = ((self.0 >> INTERNED_LEN_SHIFT) & INTERNED_LEN_MASK) as usize;
        if len != 0 {
            let ptr = (self.0 & INTERNED_PTR_MASK) as *const u8;
            // The interned allocation is leaked, immutable, and outlives every Key.
            return unsafe { std::slice::from_raw_parts(ptr, len) };
        }
        let sh = ((self.0 >> 32) as usize) & (NUM_INTERN_SHARDS - 1);
        let idx = (self.0 & 0xFFFF_FFFF) as usize;
        interners()[sh].lock().unwrap().slices[idx]
    }

    #[inline]
    pub fn prefix_bytes(self, out: &mut [u8; 8]) -> usize {
        if (self.0 & INTERNED_BIT) == 0 {
            let mut buf = [0u8; 9];
            let len = self.unpack_inline(&mut buf).min(8);
            out[..len].copy_from_slice(&buf[..len]);
            len
        } else {
            let s = self.long_bytes();
            let len = s.len().min(8);
            out[..len].copy_from_slice(&s[..len]);
            len
        }
    }

    #[inline]
    pub fn write_parquet_bytes(self, out: &mut Vec<u8>) {
        if (self.0 & INTERNED_BIT) == 0 {
            let mut buf = [0u8; 9];
            let len = self.unpack_inline(&mut buf);
            out.extend_from_slice(&(len as u32).to_le_bytes());
            out.extend_from_slice(&buf[..len]);
        } else {
            let s = self.long_bytes();
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s);
        }
    }

    #[inline(always)]
    pub fn hash64(self) -> u64 {
        let x = self.0 ^ (self.0 >> 32);
        let r = (x as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (r as u64) ^ ((r >> 64) as u64)
    }

    #[inline(always)]
    pub fn eid_hash64(self) -> u64 {
        if (self.0 & INTERNED_BIT) == 0 {
            self.hash64()
        } else {
            let h33 = (self.0 >> 30) & 0x1_FFFF_FFFF;
            let x = h33 ^ (h33 >> 17);
            let r = (x as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            (r as u64) ^ ((r >> 64) as u64)
        }
    }

    #[inline(always)]
    pub fn eid_eq(self, other: Self, arenas: &[EidArena]) -> bool {
        if (self.0 & other.0 & INTERNED_BIT) == 0 {
            return self.0 == other.0;
        }
        if (self.0 >> 30) != (other.0 >> 30) {
            return false;
        }
        if self.0 == other.0 {
            return true;
        }
        unsafe { self.eid_slice(arenas) == other.eid_slice(arenas) }
    }

    #[inline(always)]
    unsafe fn eid_slice<'a>(self, arenas: &'a [EidArena]) -> &'a [u8] {
        let w = ((self.0 >> 26) & 0xF) as usize;
        let off = ((self.0 & 0x03FF_FFFF) as usize) << 2;
        let ptr = unsafe { arenas.get_unchecked(w).buf.as_ptr().add(off) };
        let len = u16::from_le_bytes(unsafe { *(ptr as *const [u8; 2]) }) as usize;
        unsafe { std::slice::from_raw_parts(ptr.add(2), len) }
    }

    #[inline]
    pub fn cmp_bytes(&self, other: &Self) -> std::cmp::Ordering {
        if self == other {
            return std::cmp::Ordering::Equal;
        }
        if ((self.0 | other.0) & INTERNED_BIT) == 0 {
            self.0.cmp(&other.0)
        } else {
            self.cmp_bytes_slow(other)
        }
    }

    #[inline]
    fn cmp_bytes_slow(&self, other: &Self) -> std::cmp::Ordering {
        if (self.0 & other.0 & INTERNED_BIT) != 0 {
            let a = self.long_bytes();
            let b = other.long_bytes();
            if a.len() >= 8 && b.len() >= 8 {
                // Long generated IDs almost always differ in their first eight
                // bytes. Avoid constructing two slices into memcmp on every
                // comparison in the link rank and click tie-break sorts.
                let aw = u64::from_le(unsafe { (a.as_ptr() as *const u64).read_unaligned() })
                    .swap_bytes();
                let bw = u64::from_le(unsafe { (b.as_ptr() as *const u64).read_unaligned() })
                    .swap_bytes();
                if aw != bw {
                    return aw.cmp(&bw);
                }
            }
            return a.cmp(b);
        }
        let mut a_buf = [0u8; 9];
        let mut b_buf = [0u8; 9];
        let a_slice: &[u8] = if (self.0 & INTERNED_BIT) == 0 {
            let n = self.unpack_inline(&mut a_buf);
            &a_buf[..n]
        } else {
            self.long_bytes()
        };
        let b_slice: &[u8] = if (other.0 & INTERNED_BIT) == 0 {
            let n = other.unpack_inline(&mut b_buf);
            &b_buf[..n]
        } else {
            other.long_bytes()
        };
        a_slice.cmp(b_slice)
    }
}

#[inline(always)]
fn validate_and_unquote<'a, const N: usize>(
    buf: &'a [u8],
    starts: &[usize; N],
    ends: &[usize; N],
    q_cnt: u32,
    out: &mut [&'a [u8]; N],
) -> bool {
    let ptr = buf.as_ptr();
    if q_cnt == 0 {
        for i in 0..N {
            let s = starts[i];
            let e = ends[i];
            out[i] = unsafe { std::slice::from_raw_parts(ptr.add(s), e - s) };
        }
        return true;
    }
    if (q_cnt & 1) != 0 {
        return false;
    }
    let mut wrapped = 0u32;
    for i in 0..N {
        let s = starts[i];
        let e = ends[i];
        let len = e - s;
        let is_q = if len >= 2 {
            unsafe { (*ptr.add(s) == b'"') & (*ptr.add(e - 1) == b'"') }
        } else {
            false
        };
        let d = is_q as usize;
        wrapped += (d as u32) << 1;
        out[i] = unsafe { std::slice::from_raw_parts(ptr.add(s + d), len - (d << 1)) };
    }
    wrapped == q_cnt
}

/// Scans `buf[start..scan_len]` (must have >=64 slack bytes after `scan_len` and `buf[scan_len-1] == '\n'`)
/// processing every valid N-field line whose first byte is in `[start, own_end)` in a single SIMD pass.
#[inline]
pub fn for_each_record_simd<const N: usize>(
    buf: &[u8],
    start: usize,
    own_end: usize,
    scan_len: usize,
    mut cb: impl FnMut([&[u8]; N]),
) {
    if start >= own_end || scan_len <= start {
        return;
    }
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        unsafe {
            for_each_record_avx2::<N>(buf, start, own_end, scan_len, &mut cb);
        }
        return;
    }
    for_each_record_scalar::<N>(&buf[start..scan_len], own_end - start, &mut cb);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn for_each_record_avx2<const N: usize>(
    buf: &[u8],
    start: usize,
    own_end: usize,
    scan_len: usize,
    cb: &mut impl FnMut([&[u8]; N]),
) {
    use core::arch::x86_64::*;

    debug_assert!(buf.len() >= scan_len + 64);
    let ptr = buf.as_ptr();
    let nl_v = _mm256_set1_epi8(b'\n' as i8);
    let cm_v = _mm256_set1_epi8(b',' as i8);
    let qt_v = _mm256_set1_epi8(b'"' as i8);

    let mut cur_fstart = start;
    let mut f_starts = [0usize; N];
    let mut f_ends = [0usize; N];
    let mut fidx = 0usize;
    let mut too_many = false;
    let mut q_cnt = 0u32;
    let mut fields = [&[][..]; N];

        let mut blk = start;
        while blk < scan_len {
            let p = unsafe { ptr.add(blk) };
            unsafe {
                _mm_prefetch(p.add(256) as *const i8, _MM_HINT_T0);
            }
            let v0 = unsafe { _mm256_loadu_si256(p as *const __m256i) };
            let v1 = unsafe { _mm256_loadu_si256(p.add(32) as *const __m256i) };

            let mut nl = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, nl_v)) as u32) as u64)
                | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, nl_v)) as u32) as u64) << 32);
            let mut cm = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, cm_v)) as u32) as u64)
                | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, cm_v)) as u32) as u64) << 32);
            let mut qt = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, qt_v)) as u32) as u64)
                | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, qt_v)) as u32) as u64) << 32);

            if blk + 64 > scan_len {
                let rem = scan_len - blk;
                let keep = (1u64 << rem) - 1;
                nl &= keep;
                cm &= keep;
                qt &= keep;
            }

            if nl == 0 && fidx == 0 && !too_many && q_cnt == 0 && blk + 64 < scan_len {
                let blk1 = blk + 64;
                let p1 = unsafe { ptr.add(blk1) };
                let v2 = unsafe { _mm256_loadu_si256(p1 as *const __m256i) };
                let v3 = unsafe { _mm256_loadu_si256(p1.add(32) as *const __m256i) };

                let mut nl1 = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, nl_v)) as u32) as u64)
                    | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v3, nl_v)) as u32) as u64) << 32);
                let mut cm1 = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, cm_v)) as u32) as u64)
                    | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v3, cm_v)) as u32) as u64) << 32);
                let mut qt1 = ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v2, qt_v)) as u32) as u64)
                    | (((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v3, qt_v)) as u32) as u64) << 32);

                if blk1 + 64 > scan_len {
                    let rem = scan_len - blk1;
                    let keep = (1u64 << rem) - 1;
                    nl1 &= keep;
                    cm1 &= keep;
                    qt1 &= keep;
                }

                if nl1 != 0 {
                    let nl_bit = nl1.trailing_zeros();
                    let nl_off = blk1 + nl_bit as usize;
                    let before_nl1 = (1u64 << nl_bit) - 1;
                    let mut line_cm0 = cm;
                    let mut line_cm1 = cm1 & before_nl1;
                    let line_qt1 = qt1 & before_nl1;
                    if (line_cm0.count_ones() + line_cm1.count_ones()) as usize == N - 1 {
                        let total_q = qt.count_ones() + line_qt1.count_ones();
                        let mut j = 0usize;
                        while line_cm0 != 0 {
                            let cb_bit = line_cm0.trailing_zeros();
                            let coff = blk + cb_bit as usize;
                            f_starts[j] = cur_fstart;
                            f_ends[j] = coff;
                            j += 1;
                            cur_fstart = coff + 1;
                            line_cm0 &= line_cm0 - 1;
                        }
                        while line_cm1 != 0 {
                            let cb_bit = line_cm1.trailing_zeros();
                            let coff = blk1 + cb_bit as usize;
                            f_starts[j] = cur_fstart;
                            f_ends[j] = coff;
                            j += 1;
                            cur_fstart = coff + 1;
                            line_cm1 &= line_cm1 - 1;
                        }
                        f_starts[N - 1] = cur_fstart;
                        f_ends[N - 1] = nl_off;
                        if validate_and_unquote::<N>(buf, &f_starts, &f_ends, total_q, &mut fields) {
                            cb(fields);
                        }
                    }
                    let next_line = nl_off + 1;
                    if next_line >= own_end {
                        return;
                    }
                    cur_fstart = next_line;
                    blk = next_line;
                    continue;
                }
            }

            let had_nl = nl != 0;
            while nl != 0 {
                let nl_bit = nl.trailing_zeros();
                let nl_off = blk + nl_bit as usize;
                let before_nl = (1u64 << nl_bit) - 1;
                let mut line_cm = cm & before_nl;
                let line_qt = qt & before_nl;
                let line_commas = line_cm.count_ones() as usize;

                if fidx == 0 && !too_many && line_commas == N - 1 {
                    let total_q = q_cnt + line_qt.count_ones();
                    for j in 0..(N - 1) {
                        let cb_bit = line_cm.trailing_zeros();
                        let coff = blk + cb_bit as usize;
                        f_starts[j] = cur_fstart;
                        f_ends[j] = coff;
                        cur_fstart = coff + 1;
                        line_cm &= line_cm - 1;
                    }
                    f_starts[N - 1] = cur_fstart;
                    f_ends[N - 1] = nl_off;
                    if validate_and_unquote::<N>(buf, &f_starts, &f_ends, total_q, &mut fields) {
                        cb(fields);
                    }
                } else if !too_many && fidx + line_commas == N - 1 {
                    let total_q = q_cnt + line_qt.count_ones();
                    while line_cm != 0 {
                        let cb_bit = line_cm.trailing_zeros();
                        let coff = blk + cb_bit as usize;
                        f_starts[fidx] = cur_fstart;
                        f_ends[fidx] = coff;
                        fidx += 1;
                        cur_fstart = coff + 1;
                        line_cm &= line_cm - 1;
                    }
                    f_starts[fidx] = cur_fstart;
                    f_ends[fidx] = nl_off;
                    if validate_and_unquote::<N>(buf, &f_starts, &f_ends, total_q, &mut fields) {
                        cb(fields);
                    }
                }

                let through_nl = before_nl | (1u64 << nl_bit);
                nl &= !through_nl;
                cm &= !through_nl;
                qt &= !through_nl;

                let next_line = nl_off + 1;
                if next_line >= own_end {
                    return;
                }
                cur_fstart = next_line;
                fidx = 0;
                too_many = false;
                q_cnt = 0;
            }
            if had_nl {
                blk = cur_fstart;
                continue;
            }

            q_cnt += qt.count_ones();
            if !too_many {
                let rem_cm = cm.count_ones() as usize;
                if fidx + rem_cm >= N {
                    too_many = true;
                } else {
                    while cm != 0 {
                        let cb_bit = cm.trailing_zeros();
                        let coff = blk + cb_bit as usize;
                        f_starts[fidx] = cur_fstart;
                        f_ends[fidx] = coff;
                        fidx += 1;
                        cur_fstart = coff + 1;
                        cm &= cm - 1;
                    }
                }
            }
            blk += 64;
        }
}

fn for_each_record_scalar<const N: usize>(
    slice: &[u8],
    own_end_rel: usize,
    cb: &mut impl FnMut([&[u8]; N]),
) {
    let mut start = 0usize;
    let limit = own_end_rel.min(slice.len());
    for (i, &b) in slice.iter().enumerate() {
        if b == b'\n' {
            if let Some(fields) = parse_fields::<N>(&slice[start..i]) {
                cb(fields);
            }
            start = i + 1;
            if start >= limit {
                return;
            }
        }
    }
    if start < limit {
        if let Some(fields) = parse_fields::<N>(&slice[start..]) {
            cb(fields);
        }
    }
}

#[inline]
pub fn for_each_line(slice: &[u8], mut f: impl FnMut(&[u8])) {
    let mut start = 0usize;
    for (i, &b) in slice.iter().enumerate() {
        if b == b'\n' {
            f(&slice[start..i]);
            start = i + 1;
        }
    }
    if start < slice.len() {
        f(&slice[start..]);
    }
}

#[inline]
pub fn skip_header(data: &[u8]) -> &[u8] {
    match data.iter().position(|&b| b == b'\n') {
        Some(pos) => &data[pos + 1..],
        None => &[],
    }
}

/// Parses an exact N-field CSV line according to `rules.md`.
#[inline]
pub fn parse_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let has_quote = line.contains(&b'"');
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    let mut idx = 0usize;
    let len = line.len();

    if !has_quote {
        let mut i = 0usize;
        while i < len {
            if line[i] == b',' {
                if idx + 1 >= N {
                    return None;
                }
                out[idx] = &line[start..i];
                idx += 1;
                start = i + 1;
            }
            i += 1;
        }
        if idx + 1 != N {
            return None;
        }
        out[idx] = &line[start..len];
        return Some(out);
    }

    let mut i = 0usize;
    while i < len {
        if line[i] == b',' {
            if idx + 1 >= N {
                return None;
            }
            out[idx] = unquote_field(&line[start..i])?;
            idx += 1;
            start = i + 1;
        }
        i += 1;
    }
    if idx + 1 != N {
        return None;
    }
    out[idx] = unquote_field(&line[start..len])?;
    Some(out)
}

#[inline]
fn unquote_field(field: &[u8]) -> Option<&[u8]> {
    if field.first() == Some(&b'"') {
        if field.len() < 2 || field.last() != Some(&b'"') {
            return None;
        }
        let inner = &field[1..field.len() - 1];
        if inner.contains(&b'"') {
            return None;
        }
        Some(inner)
    } else {
        if field.contains(&b'"') {
            return None;
        }
        Some(field)
    }
}

#[inline]
fn parse_2(b0: u8, b1: u8) -> Option<i64> {
    if b0.is_ascii_digit() && b1.is_ascii_digit() {
        Some(((b0 - b'0') as i64) * 10 + ((b1 - b'0') as i64))
    } else {
        None
    }
}

#[inline]
fn parse_4(s: &[u8]) -> Option<i64> {
    if s[0].is_ascii_digit()
        && s[1].is_ascii_digit()
        && s[2].is_ascii_digit()
        && s[3].is_ascii_digit()
    {
        Some(
            ((s[0] - b'0') as i64) * 1000
                + ((s[1] - b'0') as i64) * 100
                + ((s[2] - b'0') as i64) * 10
                + ((s[3] - b'0') as i64),
        )
    } else {
        None
    }
}

#[inline(always)]
fn parse_8_digits_swar(bytes: &[u8]) -> Option<i64> {
    let w = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let sub30 = w.wrapping_sub(0x3030_3030_3030_3030);
    let over39 = w.wrapping_add(0x4646_4646_4646_4646);
    if ((w | sub30 | over39) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    let mut v = sub30;
    v = (v.wrapping_mul(2561) >> 8) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(6_553_601) >> 16) & 0x0000_FFFF_0000_FFFF;
    v = (v.wrapping_mul(42_949_672_960_001) >> 32) & 0xFFFF_FFFF;
    Some(v as i64)
}

#[inline(always)]
fn parse_hms_swar(s: &[u8]) -> Option<i64> {
    let w = u64::from_le_bytes(s[0..8].try_into().unwrap());
    // s = [h0, h1, ':', m0, m1, ':', s0, s1]
    // Mask out ':' positions (bytes 2 and 5) with '0' so all 8 bytes check cleanly as ASCII digits
    let d = (w & 0xFFFF_00FF_FF00_FFFF) | 0x0000_3000_0030_0000;
    let sub30 = d.wrapping_sub(0x3030_3030_3030_3030);
    let over39 = d.wrapping_add(0x4646_4646_4646_4646);
    if ((d | sub30 | over39) & 0x8080_8080_8080_8080) != 0 {
        return None;
    }
    let h = ((sub30 & 0x0F) * 10 + ((sub30 >> 8) & 0x0F)) as i64;
    let m = (((sub30 >> 24) & 0x0F) * 10 + ((sub30 >> 32) & 0x0F)) as i64;
    let sec = (((sub30 >> 48) & 0x0F) * 10 + ((sub30 >> 56) & 0x0F)) as i64;
    if h > 23 || m > 59 || sec > 59 {
        return None;
    }
    Some((h * 3_600 + m * 60 + sec) * 1_000)
}

#[inline]
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[derive(Clone, Copy)]
struct DateEntry {
    k0: u64,
    k1: u16,
    day_ms: i64, // i64::MIN = empty/invalid
}

pub struct DateCache {
    last_k0: u64,
    last_k1: u16,
    last_day_ms: i64,
    entries: [DateEntry; DATE_CACHE_SLOTS],
}

const DATE_CACHE_SLOTS: usize = 256;
const DATE_CACHE_SHIFT: u32 = 56; // 64 - log2(DATE_CACHE_SLOTS)

impl DateCache {
    pub fn new() -> Self {
        Self {
            last_k0: u64::MAX,
            last_k1: u16::MAX,
            last_day_ms: i64::MIN,
            entries: [DateEntry {
                k0: u64::MAX,
                k1: u16::MAX,
                day_ms: i64::MIN,
            }; DATE_CACHE_SLOTS],
        }
    }

    /// Epoch ms of the `YYYY-MM-DD` date in `s[0..10]`, memoised by its exact
    /// 10 bytes. Returns None for an invalid date (also memoised).
    #[inline(always)]
    fn cached_day_ms(&mut self, s: &[u8]) -> Option<i64> {
        let k0 = u64::from_ne_bytes(s[0..8].try_into().unwrap());
        let k1 = u16::from_ne_bytes(s[8..10].try_into().unwrap());
        let d_ms = if k0 == self.last_k0 && k1 == self.last_k1 {
            self.last_day_ms
        } else {
            let slot = ((k0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (k1 as u64)) >> DATE_CACHE_SHIFT)
                as usize;
            let ent = &self.entries[slot];
            let val = if ent.k0 == k0 && ent.k1 == k1 {
                ent.day_ms
            } else {
                let val = parse_date_10(&s[0..10]).unwrap_or(i64::MIN);
                self.entries[slot] = DateEntry {
                    k0,
                    k1,
                    day_ms: val,
                };
                val
            };
            self.last_k0 = k0;
            self.last_k1 = k1;
            self.last_day_ms = val;
            val
        };
        if d_ms == i64::MIN { None } else { Some(d_ms) }
    }

    #[inline(always)]
    pub fn parse_ts_with_day(&mut self, s: &[u8]) -> Option<(i64, i64)> {
        let len = s.len();
        if len == 20 && s[19] == b'Z' && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            let day_ms = self.cached_day_ms(s)?;
            let hms_ms = parse_hms_swar(&s[11..19])?;
            return Some((day_ms + hms_ms, day_ms));
        }
        let ts = self.parse_ts(s)?;
        Some((ts, ts - ts.rem_euclid(86_400_000)))
    }

    #[inline(always)]
    pub fn parse_ts_with_hint(&mut self, s: &[u8], hint_src: &[u8], hint_day_ms: i64) -> Option<i64> {
        if s.len() == 20
            && hint_src.len() == 20
            && s[19] == b'Z'
            && s[10] == b'T'
            && s[13] == b':'
            && s[16] == b':'
        {
            let k0 = u64::from_ne_bytes(s[0..8].try_into().unwrap());
            let hk0 = u64::from_ne_bytes(hint_src[0..8].try_into().unwrap());
            if k0 == hk0 {
                let k1 = u16::from_ne_bytes(s[8..10].try_into().unwrap());
                let hk1 = u16::from_ne_bytes(hint_src[8..10].try_into().unwrap());
                if k1 == hk1 {
                    return Some(hint_day_ms + parse_hms_swar(&s[11..19])?);
                }
            }
        }
        self.parse_ts(s)
    }

    #[inline(always)]
    pub fn parse_ts(&mut self, s: &[u8]) -> Option<i64> {
        let len = s.len();
        if len == 20 && s[19] == b'Z' && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            let day_ms = self.cached_day_ms(s)?;
            let hms_ms = parse_hms_swar(&s[11..19])?;
            return Some(day_ms + hms_ms);
        }
        // F2: same date/time layout plus a +HH:MM / -HH:MM offset (same checks as parse_ts).
        if len == 25
            && (s[19] == b'+' || s[19] == b'-')
            && s[22] == b':'
            && s[10] == b'T'
            && s[13] == b':'
            && s[16] == b':'
        {
            let day_ms = self.cached_day_ms(s)?;
            let hms_ms = parse_hms_swar(&s[11..19])?;
            let off_h = parse_2(s[20], s[21])?;
            let off_m = parse_2(s[23], s[24])?;
            if off_h > 23 || off_m > 59 {
                return None;
            }
            let off_ms = (off_h * 60 + off_m) * 60_000;
            let ms = day_ms + hms_ms;
            return Some(if s[19] == b'+' {
                ms - off_ms
            } else {
                ms + off_ms
            });
        }
        if len == 13 {
            if let (Some(hi8), Some(lo8)) = (
                parse_8_digits_swar(&s[0..8]),
                parse_8_digits_swar(&s[5..13]),
            ) {
                let lo5 = lo8 % 100_000;
                return Some(hi8 * 100_000 + lo5);
            }
        }
        parse_ts(s)
    }
}

#[inline]
fn parse_date_10(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let year = parse_4(&s[0..4])?;
    let month = parse_2(s[5], s[6])?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let day = parse_2(s[8], s[9])?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => return None,
    };
    if !(1..=max_day).contains(&day) {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400_000)
}

/// Parses timestamps in forms F1, F2, and F3 into epoch milliseconds UTC.
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let len = s.len();
    if len == 0 {
        return None;
    }

    if (len == 20 && s[19] == b'Z')
        || (len == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':')
    {
        if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let day_ms = parse_date_10(&s[0..10])?;
        let hms_ms = parse_hms_swar(&s[11..19])?;
        let mut ms = day_ms + hms_ms;
        if len == 25 {
            let off_h = parse_2(s[20], s[21])?;
            let off_m = parse_2(s[23], s[24])?;
            if off_h > 23 || off_m > 59 {
                return None;
            }
            let off_ms = (off_h * 60 + off_m) * 60_000;
            if s[19] == b'+' {
                ms -= off_ms;
            } else {
                ms += off_ms;
            }
        }
        return Some(ms);
    }

    if len == 13 {
        if let (Some(hi8), Some(lo8)) = (
            parse_8_digits_swar(&s[0..8]),
            parse_8_digits_swar(&s[5..13]),
        ) {
            return Some(hi8 * 100_000 + (lo8 % 100_000));
        }
        return None;
    }

    if s.iter().all(|&b| b.is_ascii_digit()) {
        let mut val: i64 = 0;
        for &b in s {
            val = val.saturating_mul(10).saturating_add((b - b'0') as i64);
        }
        return Some(val);
    }

    None
}

/// Parses purchase price_cents: non-empty ASCII digits, value in 1..=10_000_000.
#[inline]
pub fn parse_price_cents(s: &[u8]) -> Option<i32> {
    if s.is_empty() {
        return None;
    }
    let mut val: i64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        val = val * 10 + (b - b'0') as i64;
        if val > 10_000_000 {
            return None;
        }
    }
    if val >= 1 { Some(val as i32) } else { None }
}
