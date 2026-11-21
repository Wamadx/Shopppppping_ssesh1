mod csv_ts;
mod parquet;
mod snappy;
mod sys;

use std::cell::UnsafeCell;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use csv_ts::{
    DateCache, Key, for_each_line, for_each_record_simd, parse_fields, parse_price_cents,
    parse_ts, skip_header,
};
use parquet::{CampaignDailyRow, UserDailyWriter, write_campaign_daily};
use sys::{load_chunk_lines, plan_file_chunks};

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const SEVEN_DAYS_REL: u32 = SEVEN_DAYS_MS as u32;
const SESSION_GAP_REL: u32 = (30 * 60 * 1_000) as u32;

const NUM_DEDUP_SHARDS: usize = 512;
const DEDUP_SHIFT: u32 = 55; // 64 - 9
const NUM_PERSON_SHARDS: usize = 256;
const PERSON_SHIFT: u32 = 56; // 64 - 8

const BITFILTER_BITS_DEFAULT: u32 = 24; // 2 MiB

#[derive(Clone, Copy)]
struct Config {
    chunk_bytes: usize,
    use_bitfilter: bool,
    use_sortless: bool,
    use_prefetch: bool,
    use_dedup_bitmap: bool,
    use_wc: bool,
    timing: bool,
    links_legacy: bool,
    overlap: bool,
    bitfilter_bits: u32,
}

impl Config {
    fn from_env() -> Self {
        let env_bool = |name: &str, default: bool| -> bool {
            match env::var(name).as_deref() {
                Ok("0") | Ok("false") | Ok("FALSE") => false,
                Ok("1") | Ok("true") | Ok("TRUE") => true,
                _ => default,
            }
        };
        let chunk_kb = env::var("SHOP_CHUNK_KB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&k| k >= 64)
            .unwrap_or(256);
        Self {
            // SHOP_CHUNK_B (bytes, test-only) overrides the 64 KiB floor so tests can force many chunk boundaries.
            chunk_bytes: env::var("SHOP_CHUNK_B")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&b| b > 0)
                .unwrap_or(chunk_kb * 1024),
            use_bitfilter: env_bool("SHOP_BITFILTER", true),
            use_sortless: env_bool("SHOP_SORTLESS", false),
            use_prefetch: env_bool("SHOP_PREFETCH", false),
            use_dedup_bitmap: env_bool("SHOP_DEDUP_BITMAP", true),
            use_wc: env_bool("SHOP_WC", true),
            timing: env_bool("TIMING", false),
            links_legacy: env_bool("SHOP_LINKS_LEGACY", false),
            overlap: env_bool("SHOP_OVERLAP", true),
            bitfilter_bits: env::var("SHOP_BITFILTER_BITS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|&b| (16..=28).contains(&b))
                .unwrap_or(BITFILTER_BITS_DEFAULT),
        }
    }
}

#[inline(always)]
fn prefetch_ro<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

/// Sync wrapper over per-worker shard vector matrix (`worker_vecs[tid][sh]`) so Phase 2/3 can mutate
/// disjoint shard columns `sh` across threads without any `.append()` merging.
struct WorkerMatrix<T>(Vec<Vec<UnsafeCell<Vec<T>>>>);

unsafe impl<T: Send> Sync for WorkerMatrix<T> {}

impl<T> WorkerMatrix<T> {
    fn new(workers: Vec<Vec<Vec<T>>>) -> Self {
        Self(
            workers
                .into_iter()
                .map(|w| w.into_iter().map(UnsafeCell::new).collect())
                .collect(),
        )
    }

    #[inline(always)]
    fn num_workers(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    unsafe fn get_mut(&self, tid: usize, sh: usize) -> &mut Vec<T> {
        unsafe { &mut *self.0[tid][sh].get() }
    }

    #[inline(always)]
    fn shard_len(&self, sh: usize) -> usize {
        let mut total = 0usize;
        for tid in 0..self.0.len() {
            total += unsafe { (*self.0[tid][sh].get()).len() };
        }
        total
    }
}

/// Single-cache-line AoS open-addressing hash map (`key == 0` empty sentinel).
struct KeyMap<V: Copy + Default> {
    entries: Vec<(u64, V)>,
    mask: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    fn with_capacity(expected: usize) -> Self {
        let cap = (expected.saturating_mul(2).max(16)).next_power_of_two();
        Self {
            entries: vec![(0u64, V::default()); cap],
            mask: cap - 1,
        }
    }

    #[inline]
    fn insert_with_hash(&mut self, key: Key, h: u64, val: V) {
        let k = key.0;
        let mut idx = (h as usize) & self.mask;
        loop {
            let e = &mut self.entries[idx];
            if e.0 == 0 || e.0 == k {
                *e = (k, val);
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline]
    fn insert(&mut self, key: Key, val: V) {
        self.insert_with_hash(key, key.hash64(), val);
    }

    #[inline(always)]
    fn prefetch_with_hash(&self, h: u64) {
        let idx = (h as usize) & self.mask;
        prefetch_ro(self.entries.as_ptr().wrapping_add(idx));
    }

    #[inline(always)]
    fn get_with_hash(&self, key: Key, h: u64) -> Option<V> {
        let k = key.0;
        let mut idx = (h as usize) & self.mask;
        loop {
            let e = unsafe { *self.entries.get_unchecked(idx) };
            if e.0 == 0 {
                return None;
            }
            if e.0 == k {
                return Some(e.1);
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, key: Key) -> Option<V> {
        self.get_with_hash(key, key.hash64())
    }
}

struct LinkTable {
    /// 1 map (legacy) or 8 hash-sharded maps (sort path); shard = top 3 hash bits.
    maps: Vec<KeyMap<Key>>,
    non_canonical: Vec<u64>,
    use_bitfilter: bool,
    mask: usize,
}

impl LinkTable {
    fn empty() -> Self {
        Self {
            maps: vec![KeyMap::with_capacity(16)],
            non_canonical: Vec::new(),
            use_bitfilter: false,
            mask: 0,
        }
    }

    #[inline(always)]
    fn map_for(&self, h: u64) -> &KeyMap<Key> {
        if self.maps.len() == 1 {
            &self.maps[0]
        } else {
            unsafe { self.maps.get_unchecked((h >> 61) as usize) }
        }
    }

    #[inline(always)]
    fn prefetch(&self, uid: Key) {
        let h = uid.hash64();
        if self.use_bitfilter {
            let bit = (h as usize) & self.mask;
            prefetch_ro(self.non_canonical.as_ptr().wrapping_add(bit >> 6));
        } else {
            self.map_for(h).prefetch_with_hash(h);
        }
    }

    #[inline(always)]
    fn resolve(&self, uid: Key, cache: &mut [(Key, Key); 1024]) -> Key {
        let h = uid.hash64();
        if self.use_bitfilter {
            let bit = (h as usize) & self.mask;
            let word = unsafe { *self.non_canonical.get_unchecked(bit >> 6) };
            if ((word >> (bit & 63)) & 1) == 0 {
                return uid;
            }
        }
        let slot = ((h >> 54) as usize) & 1023;
        let entry = cache[slot];
        if entry.0 == uid {
            return entry.1;
        }
        let resolved = self.map_for(h).get_with_hash(uid, h).unwrap_or(uid);
        cache[slot] = (uid, resolved);
        resolved
    }
}

/// Parses unsigned decimal ID suffix without leading zeros (`123` -> `Some(123)`, `01` -> `None`).
#[inline(always)]
fn parse_dec_suffix(rest: &[u8]) -> Option<usize> {
    let len = rest.len();
    if len == 0 || len > 6 {
        return None;
    }
    if len > 1 && rest[0] == b'0' {
        return None;
    }
    let mut val = 0usize;
    let mut bad = 0u8;
    for &b in rest {
        let d = b.wrapping_sub(b'0');
        bad |= (d > 9) as u8;
        val = val * 10 + (d as usize);
    }
    if bad == 0 { Some(val) } else { None }
}

const FAST_P_CAP: usize = 131_072;
struct ProductCatalog {
    fast_p: Vec<u8>,
    fallback: KeyMap<u16>,
    has_fallback: bool,
}

impl ProductCatalog {
    #[inline(always)]
    fn get(&self, pid: &[u8], unknown_cat_id: u16) -> u16 {
        if pid.len() >= 2 && pid[0] == b'p' {
            if let Some(num) = parse_dec_suffix(&pid[1..]) {
                if num < self.fast_p.len() {
                    return unsafe { *self.fast_p.get_unchecked(num) as u16 };
                }
                if num < FAST_P_CAP {
                    return unknown_cat_id;
                }
            }
        }
        if self.has_fallback {
            return self.fallback.get(Key::from_bytes(pid)).unwrap_or(unknown_cat_id);
        }
        unknown_cat_id
    }
}

const FAST_C_CAP: usize = 16_384;
struct CampaignCatalog {
    fast_c: Vec<u16>,
    fallback: KeyMap<u16>,
    has_fallback: bool,
}

impl CampaignCatalog {
    #[inline(always)]
    fn get(&self, cid: &[u8]) -> Option<u16> {
        if cid.len() >= 3 && cid[0] == b'c' && cid[1] == b'_' {
            if let Some(num) = parse_dec_suffix(&cid[2..]) {
                if num < self.fast_c.len() {
                    let c = unsafe { *self.fast_c.get_unchecked(num) };
                    return if c != u16::MAX { Some(c) } else { None };
                }
                if num < FAST_C_CAP {
                    return None;
                }
            }
        }
        if self.has_fallback {
            return self.fallback.get(Key::from_bytes(cid));
        }
        None
    }
}

/// Single-array L2-resident dedup table: `slot = (tag32 << 32) | ((pos + 1) as u64)`, `0` = empty.
struct DedupTable {
    slots: Vec<u64>,
    mask: usize,
}

impl DedupTable {
    fn new(initial_pow2: usize) -> Self {
        let cap = initial_pow2.next_power_of_two().max(1024);
        Self {
            slots: vec![0; cap],
            mask: cap - 1,
        }
    }

    fn reset(&mut self, needed: usize) {
        let cap = needed.saturating_mul(2).max(1024).next_power_of_two();
        if cap > self.slots.len() {
            self.slots.resize(cap, 0);
        }
        self.slots[..cap].fill(0);
        self.mask = cap - 1;
    }

    #[inline(always)]
    fn prefetch(&self, eid: Key) {
        let idx = (eid.hash64() as usize) & self.mask;
        prefetch_ro(self.slots.as_ptr().wrapping_add(idx));
    }

    #[inline(always)]
    fn get_or_insert(&mut self, ev: RawEvent, kept: &[RawEvent]) -> Option<u32> {
        let h = ev.event_id.hash64();
        let tag = (h >> 23) & 0xFFFF_FFFF;
        let mut idx = (h as usize) & self.mask;
        loop {
            let slot_val = unsafe { *self.slots.get_unchecked(idx) };
            if slot_val == 0 {
                unsafe {
                    *self.slots.get_unchecked_mut(idx) = (tag << 32) | ((kept.len() as u64) + 1);
                }
                return None;
            }
            if (slot_val >> 32) == tag {
                let pos = ((slot_val as u32) - 1) as usize;
                if unsafe { kept.get_unchecked(pos) }.event_id == ev.event_id {
                    return Some(pos as u32);
                }
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

struct LocalPersonMap {
    entries: Vec<(u64, u32)>,
    stamps: Vec<u16>,
    epoch: u16,
    mask: usize,
}

impl LocalPersonMap {
    fn new(initial_pow2: usize) -> Self {
        let cap = initial_pow2.next_power_of_two().max(1024);
        Self {
            entries: vec![(0, 0); cap],
            stamps: vec![0; cap],
            epoch: 1,
            mask: cap - 1,
        }
    }

    fn reset(&mut self, needed: usize) {
        let cap = needed.saturating_mul(2).max(1024).next_power_of_two();
        if cap > self.entries.len() {
            self.entries.resize(cap, (0, 0));
            self.stamps.resize(cap, 0);
        }
        self.mask = cap - 1;
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamps.fill(0);
            self.epoch = 1;
        }
    }

    #[inline(always)]
    fn prefetch(&self, key: Key) {
        let idx = (key.hash64() as usize) & self.mask;
        prefetch_ro(self.stamps.as_ptr().wrapping_add(idx));
        prefetch_ro(self.entries.as_ptr().wrapping_add(idx));
    }

    #[inline(always)]
    fn get_or_insert(&mut self, key: Key, next_id: &mut u32) -> (u32, bool) {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if unsafe { *self.stamps.get_unchecked(idx) } != self.epoch {
                let id = *next_id;
                *next_id = id + 1;
                unsafe {
                    *self.stamps.get_unchecked_mut(idx) = self.epoch;
                    *self.entries.get_unchecked_mut(idx) = (k, id);
                }
                return (id, true);
            }
            let e = unsafe { *self.entries.get_unchecked(idx) };
            if e.0 == k {
                return (e.1, false);
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn get(&self, key: Key) -> Option<u32> {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if unsafe { *self.stamps.get_unchecked(idx) } != self.epoch {
                return None;
            }
            let e = unsafe { *self.entries.get_unchecked(idx) };
            if e.0 == k {
                return Some(e.1);
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

fn list_csvs(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir)? {
        let p = entry?.path();
        if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("csv") {
            files.push(p);
        }
    }
    files.sort();
    Ok(files)
}

fn parse_manifest(path: &Path) -> io::Result<(i64, i64)> {
    let s = fs::read(path)?;
    let ws = extract_json_str(&s, b"\"window_start\"")
        .and_then(parse_ts)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_start"))?;
    let we = extract_json_str(&s, b"\"window_end\"")
        .and_then(parse_ts)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_end"))?;
    Ok((ws, we))
}

fn extract_json_str<'a>(buf: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let pos = buf.windows(key.len()).position(|w| w == key)?;
    let rest = &buf[pos + key.len()..];
    let colon = rest.iter().position(|&b| b == b':')?;
    let rest = &rest[colon + 1..];
    let q1 = rest.iter().position(|&b| b == b'"')?;
    let after = &rest[q1 + 1..];
    let q2 = after.iter().position(|&b| b == b'"')?;
    Some(&after[..q2])
}

fn load_products(path: &Path) -> io::Result<(ProductCatalog, Vec<Box<[u8]>>, u16)> {
    let data = fs::read(path)?;
    let mut raw_cats: Vec<Vec<u8>> = vec![b"unknown".to_vec()];
    let mut raw_pairs: Vec<(&[u8], usize)> = Vec::with_capacity(65_536);

    let body = skip_header(&data);
    let mut start = 0usize;
    while start < body.len() {
        let end = body[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(body.len(), |r| start + r);
        let line = &body[start..end];
        start = end + 1;
        if let Some([pid, cat]) = parse_fields::<2>(line) {
            if !pid.is_empty() {
                let raw_cid = match raw_cats.iter().position(|c| c.as_slice() == cat) {
                    Some(idx) => idx,
                    None => {
                        let idx = raw_cats.len();
                        raw_cats.push(cat.to_vec());
                        idx
                    }
                };
                raw_pairs.push((pid, raw_cid));
            }
        }
    }

    assert!(raw_cats.len() <= 255);
    let mut order: Vec<usize> = (0..raw_cats.len()).collect();
    order.sort_by(|&a, &b| raw_cats[a].cmp(&raw_cats[b]));

    let mut categories: Vec<Box<[u8]>> = Vec::with_capacity(raw_cats.len());
    let mut remap = [0u16; 256];
    for (sorted_id, &orig_id) in order.iter().enumerate() {
        remap[orig_id] = sorted_id as u16;
        categories.push(raw_cats[orig_id].clone().into_boxed_slice());
    }
    let unknown_id = remap[0];

    let mut fast_items: Vec<(u32, u8)> = Vec::with_capacity(raw_pairs.len());
    let mut max_fast: isize = -1;
    let mut fallback_items: Vec<(Key, u16)> = Vec::new();
    for (pid, raw_cid) in raw_pairs {
        let cid = remap[raw_cid];
        if pid.len() >= 2 && pid[0] == b'p' {
            if let Some(num) = parse_dec_suffix(&pid[1..]) {
                if num < FAST_P_CAP {
                    fast_items.push((num as u32, cid as u8));
                    if (num as isize) > max_fast {
                        max_fast = num as isize;
                    }
                    continue;
                }
            }
        }
        fallback_items.push((Key::from_bytes(pid), cid));
    }

    let fast_len = if max_fast >= 0 {
        (max_fast as usize + 1).min(FAST_P_CAP)
    } else {
        0
    };
    let mut fast_p = vec![unknown_id as u8; fast_len];
    for (num, cid) in fast_items {
        fast_p[num as usize] = cid;
    }

    let has_fallback = !fallback_items.is_empty();
    let mut fallback = KeyMap::with_capacity(fallback_items.len().max(4));
    for (k, cid) in fallback_items {
        fallback.insert(k, cid);
    }

    Ok((
        ProductCatalog {
            fast_p,
            fallback,
            has_fallback,
        },
        categories,
        unknown_id,
    ))
}

fn load_campaigns(path: &Path) -> io::Result<(CampaignCatalog, Vec<(Key, Key)>)> {
    let data = fs::read(path)?;
    let mut list: Vec<(Key, Key)> = Vec::with_capacity(2048);
    let mut fast_items: Vec<(u32, u16)> = Vec::with_capacity(2048);
    let mut max_fast: isize = -1;
    let mut fallback_items: Vec<(Key, u16)> = Vec::new();

    for_each_line(skip_header(&data), |line| {
        if let Some([cid, ch]) = parse_fields::<2>(line) {
            if !cid.is_empty() {
                assert!(list.len() < u16::MAX as usize);
                let idx = list.len() as u16;
                let mut is_fast = false;
                if cid.len() >= 3 && cid[0] == b'c' && cid[1] == b'_' {
                    if let Some(num) = parse_dec_suffix(&cid[2..]) {
                        if num < FAST_C_CAP {
                            fast_items.push((num as u32, idx));
                            if (num as isize) > max_fast {
                                max_fast = num as isize;
                            }
                            is_fast = true;
                        }
                    }
                }
                if !is_fast {
                    fallback_items.push((Key::from_bytes(cid), idx));
                }
                list.push((Key::from_bytes(cid), Key::from_bytes(ch)));
            }
        }
    });

    let fast_len = if max_fast >= 0 {
        (max_fast as usize + 1).min(FAST_C_CAP)
    } else {
        0
    };
    let mut fast_c = vec![u16::MAX; fast_len];
    for (num, idx) in fast_items {
        fast_c[num as usize] = idx;
    }

    let has_fallback = !fallback_items.is_empty();
    let mut fallback = KeyMap::with_capacity(fallback_items.len().max(4));
    for (k, idx) in fallback_items {
        fallback.insert(k, idx);
    }

    assert!(list.len() < u16::MAX as usize);
    list.push((Key::from_bytes(b"organic"), Key::from_bytes(b"organic")));
    Ok((
        CampaignCatalog {
            fast_c,
            fallback,
            has_fallback,
        },
        list,
    ))
}

struct UnionFind {
    id_map: KeyMap<u32>,
    ids: Vec<Key>,
    hashes: Vec<u64>,
    parent: Vec<u32>,
    min_id: Vec<Key>,
}

impl UnionFind {
    fn new(expected_nodes: usize) -> Self {
        Self {
            id_map: KeyMap::with_capacity(expected_nodes.max(64)),
            ids: Vec::with_capacity(expected_nodes),
            hashes: Vec::with_capacity(expected_nodes),
            parent: Vec::with_capacity(expected_nodes),
            min_id: Vec::with_capacity(expected_nodes),
        }
    }

    #[inline(always)]
    fn intern(&mut self, s: Key) -> u32 {
        let h = s.hash64();
        if let Some(idx) = self.id_map.get_with_hash(s, h) {
            return idx;
        }
        let idx = self.ids.len() as u32;
        self.id_map.insert_with_hash(s, h, idx);
        self.ids.push(s);
        self.hashes.push(h);
        self.parent.push(idx);
        self.min_id.push(s);
        idx
    }

    #[inline(always)]
    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while self.parent[x as usize] != root {
            let next = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = next;
        }
        root
    }

    #[inline(always)]
    fn union(&mut self, a: u32, b: u32) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        let (keep, drop) = if ra < rb { (ra, rb) } else { (rb, ra) };
        self.parent[drop as usize] = keep;
        if self.min_id[drop as usize].cmp_bytes(&self.min_id[keep as usize])
            == std::cmp::Ordering::Less
        {
            self.min_id[keep as usize] = self.min_id[drop as usize];
        }
    }
}

fn load_links_legacy(
    dir: &Path,
    ncpu: usize,
    chunk_bytes: usize,
    use_bitfilter: bool,
    bitfilter_bits: u32,
    timing: bool,
) -> io::Result<LinkTable> {
    let t_parse = Instant::now();
    let paths = list_csvs(dir)?;
    let (files, chunks) = plan_file_chunks(&paths, chunk_bytes)?;
    if chunks.is_empty() {
        return Ok(LinkTable::empty());
    }

    let next_chunk = AtomicUsize::new(0);
    let pairs_per_worker: Vec<Vec<(Key, Key)>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu.min(chunks.len()).max(1));
        for _ in 0..ncpu.min(chunks.len()).max(1) {
            handles.push(s.spawn(|| {
                let mut local_pairs = Vec::new();
                let mut buf = Vec::new();
                loop {
                    let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if cidx >= chunks.len() {
                        break;
                    }
                    let ch = &chunks[cidx];
                    let (start, own_end, scan_len) =
                        load_chunk_lines(&files[ch.file_idx], ch, &mut buf)
                            .expect("failed to read link chunk");
                    for_each_record_simd::<2>(&buf, start, own_end, scan_len, |[a, b]| {
                        if !a.is_empty() && !b.is_empty() && a != b {
                            local_pairs.push((Key::from_bytes(a), Key::from_bytes(b)));
                        }
                    });
                }
                local_pairs
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(files);
    let ms_parse = t_parse.elapsed().as_secs_f64() * 1000.0;

    let t_union = Instant::now();
    let total_edges: usize = pairs_per_worker.iter().map(|v| v.len()).sum();
    let mut uf = UnionFind::new((total_edges * 2).max(64));
    for pv in &pairs_per_worker {
        for &(a, b) in pv {
            let ia = uf.intern(a);
            let ib = uf.intern(b);
            uf.union(ia, ib);
        }
    }
    drop(pairs_per_worker);
    let ms_union = t_union.elapsed().as_secs_f64() * 1000.0;

    let t_build = Instant::now();
    let mask = if use_bitfilter {
        (1usize << bitfilter_bits) - 1
    } else {
        0
    };
    let mut map = KeyMap::with_capacity(uf.ids.len().max(16));
    let mut non_canonical = if use_bitfilter {
        vec![0u64; (mask + 1) >> 6]
    } else {
        Vec::new()
    };

    for idx in 0..uf.ids.len() {
        let id = uf.ids[idx];
        let root = uf.find(idx as u32);
        let min_id = uf.min_id[root as usize];
        if min_id != id {
            let h = uf.hashes[idx];
            map.insert_with_hash(id, h, min_id);
            if use_bitfilter {
                let bit = (h as usize) & mask;
                non_canonical[bit >> 6] |= 1u64 << (bit & 63);
            }
        }
    }
    if timing {
        eprintln!(
            "LINKS_SUB parse={:.1} union={:.1} build={:.1} nodes={} edges={}",
            ms_parse,
            ms_union,
            t_build.elapsed().as_secs_f64() * 1000.0,
            uf.ids.len(),
            total_edges
        );
    }
    Ok(LinkTable {
        maps: vec![map],
        non_canonical,
        use_bitfilter,
        mask,
    })
}

// ---------------------------------------------------------------------------
// Sort-based link pipeline: parallel radix interning (rank = byte order),
// concurrent hook-to-smaller union-find, parallel sharded lookup-table build.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct EndRec {
    key: u64,
    refr: u32,
}

/// Shared write buffer: threads write provably disjoint elements (see callers).
struct SharedBuf<T>(UnsafeCell<Vec<T>>);
unsafe impl<T: Send> Sync for SharedBuf<T> {}

#[inline(always)]
fn range_of(n: usize, t: usize, tid: usize) -> (usize, usize) {
    (n * tid / t, n * (tid + 1) / t)
}

/// Parallel stable LSD radix sort of `recs` by `key` (8-bit digits, constant
/// digits skipped). Small inputs use a single-threaded unstable sort (equal
/// keys are deduplicated later, so within-key order is irrelevant).
fn radix_sort_recs(recs: &mut Vec<EndRec>, scratch: &mut Vec<EndRec>, ncpu: usize) {
    let n = recs.len();
    if n < 2 {
        return;
    }
    let t = ncpu.min((n / 16384).max(1));
    if t == 1 {
        recs.sort_unstable_by_key(|r| r.key);
        return;
    }
    let mut hist = vec![[0usize; 256]; t];
    let mut offs = vec![[0usize; 256]; t];
    let dst = SharedBuf(UnsafeCell::new(std::mem::take(scratch)));
    for pass in 0..8u32 {
        let shift = pass * 8;
        thread::scope(|s| {
            for (tid, h) in hist.iter_mut().enumerate() {
                let (lo, hi) = range_of(n, t, tid);
                let r = &*recs;
                s.spawn(move || {
                    h.fill(0);
                    for rec in &r[lo..hi] {
                        h[((rec.key >> shift) & 255) as usize] += 1;
                    }
                });
            }
        });
        let mut total = [0usize; 256];
        for h in &hist {
            for b in 0..256 {
                total[b] += h[b];
            }
        }
        let mut nz = 0;
        let mut run = 0usize;
        for b in 0..256 {
            if total[b] > 0 {
                nz += 1;
            }
            for tid in 0..t {
                offs[tid][b] = run;
                run += hist[tid][b];
            }
        }
        if nz <= 1 {
            continue; // constant digit: scatter would be the identity
        }
        thread::scope(|s| {
            for (tid, o) in offs.iter_mut().enumerate() {
                let (lo, hi) = range_of(n, t, tid);
                let r = &*recs;
                let d = &dst;
                s.spawn(move || {
                    let w = unsafe { &mut *d.0.get() };
                    let mut cur = *o;
                    for rec in &r[lo..hi] {
                        let b = ((rec.key >> shift) & 255) as usize;
                        unsafe { *w.get_unchecked_mut(cur[b]) = *rec };
                        cur[b] += 1;
                    }
                });
            }
        });
        let src = unsafe { &mut *dst.0.get() };
        std::mem::swap(recs, src);
    }
    *scratch = dst.0.into_inner();
}

/// Deduplicate sorted endpoint records; emit unique keys `u` (sorted by `key`)
/// and `halves[ref] = rank`. Two parallel scans with a prefix-sum barrier.
fn rank_recs(recs: &[EndRec], ncpu: usize) -> (Vec<u64>, Vec<u32>) {
    let n = recs.len();
    let t = ncpu.min((n / 16384).max(1));
    let mut counts = vec![0usize; t];
    thread::scope(|s| {
        for (tid, c) in counts.iter_mut().enumerate() {
            let (lo, hi) = range_of(n, t, tid);
            s.spawn(move || {
                let mut cnt = 0usize;
                for i in lo..hi {
                    if i == 0 || recs[i].key != recs[i - 1].key {
                        cnt += 1;
                    }
                }
                *c = cnt;
            });
        }
    });
    let mut offs = vec![0usize; t];
    let mut total = 0usize;
    for tid in 0..t {
        offs[tid] = total;
        total += counts[tid];
    }
    let ubuf = SharedBuf(UnsafeCell::new(vec![0u64; total]));
    let hbuf = SharedBuf(UnsafeCell::new(vec![0u32; n]));
    thread::scope(|s| {
        for tid in 0..t {
            let (lo, hi) = range_of(n, t, tid);
            let off = offs[tid];
            let u = &ubuf;
            let h = &hbuf;
            s.spawn(move || {
                let uw = unsafe { &mut *u.0.get() };
                let hw = unsafe { &mut *h.0.get() };
                let mut cur = off;
                for i in lo..hi {
                    let r = &recs[i];
                    if i == 0 || r.key != recs[i - 1].key {
                        uw[cur] = r.key;
                        cur += 1;
                    }
                    unsafe { *hw.get_unchecked_mut(r.refr as usize) = (cur - 1) as u32 };
                }
            });
        }
    });
    (ubuf.0.into_inner(), hbuf.0.into_inner())
}

/// Interned keys (>9 bytes / non-ASCII) are not order-preserving in `Key`.
/// Re-sort the interned suffix by bytes and merge it into the inline part so
/// rank order == byte order for every key; remap endpoint ranks accordingly.
fn fix_interned(u: &mut Vec<u64>, halves: &mut Vec<u32>, ncpu: usize) {
    const IB: u64 = 1u64 << 63;
    let n = u.len();
    if n == 0 || (u[n - 1] & IB) == 0 {
        return;
    }
    let p = u.partition_point(|&k| (k & IB) == 0);
    // Carry each interned key's ORIGINAL rank (p + position in the u64-sorted
    // suffix) through the byte sort; the merge must remap that original rank.
    let mut long: Vec<(Key, usize)> = u[p..]
        .iter()
        .enumerate()
        .map(|(idx, &k)| (Key(k), p + idx))
        .collect();
    long.sort_unstable_by(|a, b| a.0.cmp_bytes(&b.0));
    let mut nu = vec![0u64; n];
    let mut remap = vec![0u32; n];
    let (mut i, mut j, mut k) = (0usize, 0usize, 0usize);
    while i < p && j < long.len() {
        if Key(u[i]).cmp_bytes(&long[j].0) != std::cmp::Ordering::Greater {
            nu[k] = u[i];
            remap[i] = k as u32;
            i += 1;
        } else {
            nu[k] = long[j].0 .0;
            remap[long[j].1] = k as u32;
            j += 1;
        }
        k += 1;
    }
    while i < p {
        nu[k] = u[i];
        remap[i] = k as u32;
        i += 1;
        k += 1;
    }
    while j < long.len() {
        nu[k] = long[j].0 .0;
        remap[long[j].1] = k as u32;
        j += 1;
        k += 1;
    }
    *u = nu;
    let hn = halves.len();
    let t = ncpu.min((hn / 16384).max(1));
    let hbuf = SharedBuf(UnsafeCell::new(std::mem::take(halves)));
    thread::scope(|s| {
        for tid in 0..t {
            let (lo, hi) = range_of(hn, t, tid);
            let h = &hbuf;
            let rm = &remap;
            s.spawn(move || {
                let hw = unsafe { &mut *h.0.get() };
                for x in &mut hw[lo..hi] {
                    *x = rm[*x as usize];
                }
            });
        }
    });
    *halves = hbuf.0.into_inner();
}

/// Find with path halving via monotone CAS: parent values only ever decrease
/// and parent[x] <= x always holds, so the forest stays acyclic under races.
#[inline(always)]
fn cas_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(Ordering::Relaxed);
        if px == x {
            return x;
        }
        let gx = p[px as usize].load(Ordering::Relaxed);
        if gx == px {
            return px;
        }
        let _ = p[x as usize].compare_exchange(px, gx, Ordering::Relaxed, Ordering::Relaxed);
        x = gx;
    }
}

/// Concurrent union rounds: hook the larger root onto the smaller (CAS on the
/// root slot). Roots only decrease, so at convergence each component's root is
/// its minimum rank == byte-wise smallest ID, independent of thread schedule.
/// A round that observes different roots but lands no CAS witnessed another
/// thread's hook this round; hooks are bounded by n-1, so this terminates.
fn union_rounds(halves: &[u32], parent: &[AtomicU32], ncpu: usize) -> usize {
    let e = halves.len() / 2;
    let t = ncpu.min((e / 4096).max(1));
    let mut rounds = 0usize;
    loop {
        rounds += 1;
        let saw_diff: Vec<bool> = thread::scope(|s| {
            let mut hs = Vec::with_capacity(t);
            for tid in 0..t {
                let (lo, hi) = range_of(e, t, tid);
                let h = halves;
                let p = parent;
                hs.push(s.spawn(move || {
                    let mut diff = false;
                    for i in lo..hi {
                        let ra = cas_find(p, h[2 * i]);
                        let rb = cas_find(p, h[2 * i + 1]);
                        if ra != rb {
                            diff = true;
                            let (big, small) = if ra > rb { (ra, rb) } else { (rb, ra) };
                            let _ = p[big as usize].compare_exchange(
                                big,
                                small,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            );
                        }
                    }
                    diff
                }));
            }
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        if !saw_diff.into_iter().any(|d| d) {
            break;
        }
    }
    rounds
}

/// Parallel read-only flatten of the converged forest, bucketing non-canonical
/// entries by hash shard; then build the 8 shard maps in parallel and OR-merge
/// per-thread bit filters.
fn build_link_maps(
    u: &[u64],
    parent: &[AtomicU32],
    ncpu: usize,
    use_bitfilter: bool,
    mask: usize,
) -> (Vec<KeyMap<Key>>, Vec<u64>) {
    let n = u.len();
    let t = ncpu.min((n / 4096).max(1));
    let words = if use_bitfilter { (mask + 1) >> 6 } else { 0 };
    let results: Vec<(Vec<Vec<(u64, u64)>>, Vec<u64>)> = thread::scope(|s| {
        let mut hs = Vec::with_capacity(t);
        for tid in 0..t {
            let (lo, hi) = range_of(n, t, tid);
            let u = u;
            let p = parent;
            hs.push(s.spawn(move || {
                let mut buckets: Vec<Vec<(u64, u64)>> = (0..8).map(|_| Vec::new()).collect();
                let mut filt = vec![0u64; words];
                for r in lo..hi {
                    let mut root = r as u32;
                    loop {
                        let pr = p[root as usize].load(Ordering::Relaxed);
                        if pr == root {
                            break;
                        }
                        root = pr;
                    }
                    if root as usize != r {
                        let k = u[r];
                        let v = u[root as usize];
                        let h = Key(k).hash64();
                        buckets[(h >> 61) as usize].push((k, v));
                        if use_bitfilter {
                            let bit = (h as usize) & mask;
                            filt[bit >> 6] |= 1u64 << (bit & 63);
                        }
                    }
                }
                (buckets, filt)
            }));
        }
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let maps: Vec<KeyMap<Key>> = thread::scope(|s| {
        let mut hs = Vec::with_capacity(8);
        for sh in 0..8usize {
            let res = &results;
            hs.push(s.spawn(move || {
                let cnt: usize = res.iter().map(|(b, _)| b[sh].len()).sum();
                let mut m = KeyMap::with_capacity(cnt.max(16));
                for (b, _) in res {
                    for &(k, v) in &b[sh] {
                        m.insert_with_hash(Key(k), Key(k).hash64(), Key(v));
                    }
                }
                m
            }));
        }
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let non_canonical = if use_bitfilter {
        let mut out = vec![0u64; words];
        for (w, o) in out.iter_mut().enumerate() {
            let mut v = 0u64;
            for (_, f) in &results {
                v |= f[w];
            }
            *o = v;
        }
        out
    } else {
        Vec::new()
    };
    (maps, non_canonical)
}

fn load_links_sort(
    dir: &Path,
    ncpu: usize,
    chunk_bytes: usize,
    use_bitfilter: bool,
    bitfilter_bits: u32,
    timing: bool,
) -> io::Result<LinkTable> {
    let t_parse = Instant::now();
    let paths = list_csvs(dir)?;
    let (files, chunks) = plan_file_chunks(&paths, chunk_bytes)?;
    if chunks.is_empty() {
        return Ok(LinkTable::empty());
    }
    let next_chunk = AtomicUsize::new(0);
    let pairs_per_worker: Vec<Vec<(Key, Key)>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu.min(chunks.len()).max(1));
        for _ in 0..ncpu.min(chunks.len()).max(1) {
            handles.push(s.spawn(|| {
                let mut local_pairs = Vec::new();
                let mut buf = Vec::new();
                loop {
                    let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if cidx >= chunks.len() {
                        break;
                    }
                    let ch = &chunks[cidx];
                    let (start, own_end, scan_len) =
                        load_chunk_lines(&files[ch.file_idx], ch, &mut buf)
                            .expect("failed to read link chunk");
                    for_each_record_simd::<2>(&buf, start, own_end, scan_len, |[a, b]| {
                        if !a.is_empty() && !b.is_empty() && a != b {
                            local_pairs.push((Key::from_bytes(a), Key::from_bytes(b)));
                        }
                    });
                }
                local_pairs
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(files);
    let ms_parse = t_parse.elapsed().as_secs_f64() * 1000.0;

    let e: usize = pairs_per_worker.iter().map(|v| v.len()).sum();
    if e == 0 {
        return Ok(LinkTable::empty());
    }

    // Endpoint records (key, edge*2+side), filled in parallel at disjoint offsets.
    let t_rec = Instant::now();
    let mut woff = Vec::with_capacity(pairs_per_worker.len() + 1);
    let mut acc = 0usize;
    for pv in &pairs_per_worker {
        woff.push(acc);
        acc += pv.len();
    }
    woff.push(acc);
    let zero = EndRec { key: 0, refr: 0 };
    let recs = SharedBuf(UnsafeCell::new(vec![zero; 2 * e]));
    let mut scratch = vec![zero; 2 * e];
    thread::scope(|s| {
        let mut hs = Vec::with_capacity(pairs_per_worker.len());
        for (w, pv) in pairs_per_worker.iter().enumerate() {
            let base = woff[w];
            let r = &recs;
            hs.push(s.spawn(move || {
                let rw = unsafe { &mut *r.0.get() };
                for (j, &(a, b)) in pv.iter().enumerate() {
                    let ei = (base + j) as u32;
                    rw[2 * (base + j)] = EndRec { key: a.0, refr: 2 * ei };
                    rw[2 * (base + j) + 1] = EndRec { key: b.0, refr: 2 * ei + 1 };
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    });
    drop(pairs_per_worker);
    let mut recs = recs.0.into_inner();
    let ms_rec = t_rec.elapsed().as_secs_f64() * 1000.0;

    let t_sort = Instant::now();
    radix_sort_recs(&mut recs, &mut scratch, ncpu);
    drop(scratch);
    let ms_sort = t_sort.elapsed().as_secs_f64() * 1000.0;

    let t_rank = Instant::now();
    let (mut u, mut halves) = rank_recs(&recs, ncpu);
    drop(recs);
    fix_interned(&mut u, &mut halves, ncpu);
    let ms_rank = t_rank.elapsed().as_secs_f64() * 1000.0;

    let t_union = Instant::now();
    let parent: Vec<AtomicU32> = (0..u.len() as u32).map(AtomicU32::new).collect();
    let rounds = union_rounds(&halves, &parent, ncpu);
    drop(halves);
    let ms_union = t_union.elapsed().as_secs_f64() * 1000.0;

    let t_build = Instant::now();
    let mask = if use_bitfilter {
        (1usize << bitfilter_bits) - 1
    } else {
        0
    };
    let (maps, non_canonical) = build_link_maps(&u, &parent, ncpu, use_bitfilter, mask);
    let ms_build = t_build.elapsed().as_secs_f64() * 1000.0;
    if timing {
        eprintln!(
            "LINKS_SUB parse={:.1} recs={:.1} sort={:.1} rank={:.1} union={:.1} build={:.1} rounds={} nodes={} edges={}",
            ms_parse, ms_rec, ms_sort, ms_rank, ms_union, ms_build, rounds, u.len(), e
        );
    }
    Ok(LinkTable {
        maps,
        non_canonical,
        use_bitfilter,
        mask,
    })
}

fn load_links_dispatch(data_dir: &Path, ncpu: usize, cfg: Config) -> io::Result<LinkTable> {
    let dir = data_dir.join("links");
    if cfg.links_legacy {
        load_links_legacy(&dir, ncpu, cfg.chunk_bytes, cfg.use_bitfilter, cfg.bitfilter_bits, cfg.timing)
    } else {
        load_links_sort(&dir, ncpu, cfg.chunk_bytes, cfg.use_bitfilter, cfg.bitfilter_bits, cfg.timing)
    }
}

#[derive(Clone, Copy, Default)]
struct RawEvent {
    event_id: Key,
    user_id: Key,
    ingest_ts: i64,
    ts_rel: u32,    // u32::MAX means invalid
    price_cat: u32, // 0 = non-purchase, else ((cat_id + 1) << 24) | price_cents
}

#[derive(Clone, Copy, Default)]
struct PersonEvent {
    person_id: Key,
    packed_ev: u64, // ((ts_rel as u64) << 32) | (price_cat as u64)
}

#[derive(Clone, Copy, Default)]
struct PersonClick {
    person_id: Key,
    click_id: Key,
    ts_rel: u32,
    campaign_idx: u32,
}

#[derive(Clone, Copy)]
struct CompactClick {
    ts_rel: u32,
    campaign_idx: u32,
    click_id: Key,
}

#[derive(Clone, Copy, Default)]
struct CampaignAgg {
    sessions: i32,
    purchasing_sessions: i32,
    revenue_cents: i64,
    assisted_sessions: i32,
}

/// Software write-combining scatter, reusable for any record stream (P3b tickets/payloads).
///
/// API: `WcSink::<T, N>::new(num_shards, enabled)`, then per record
/// `sink.push(shard, record, &mut out)` where `out: &mut [Vec<T>]` holds one Vec per shard, and
/// exactly once per worker, before it returns, `sink.finish(&mut out)`.
/// Requirements: `N * size_of::<T>() == 64` for the streaming path (e.g. 16 B records -> N = 4,
/// 32 B -> N = 2; other sizes still work through plain copies).
///
/// Each full 64 B group is appended with two `_mm256_stream_si256` stores (no read-for-ownership)
/// when the destination is 64 B aligned (Vec storage from the allocator's slabs is), otherwise
/// with a plain copy. `finish` appends partial tails with plain stores and issues `_mm_sfence`,
/// so streamed data is globally visible before the worker's join. `enabled = false` (SHOP_WC=0)
/// pushes directly.
struct WcSink<T: Copy + Default, const N: usize> {
    stage: Vec<[T; N]>,
    cnt: Vec<u8>,
    on: bool,
}

impl<T: Copy + Default, const N: usize> WcSink<T, N> {
    fn new(shards: usize, on: bool) -> Self {
        Self { stage: vec![[T::default(); N]; shards], cnt: vec![0; shards], on }
    }

    #[inline(always)]
    fn push(&mut self, sh: usize, v: T, out: &mut [Vec<T>]) {
        if !self.on {
            unsafe { out.get_unchecked_mut(sh) }.push(v);
            return;
        }
        let c = unsafe { *self.cnt.get_unchecked(sh) } as usize;
        unsafe { *self.stage.get_unchecked_mut(sh).get_unchecked_mut(c) = v };
        if c + 1 == N {
            Self::flush_full(unsafe { self.stage.get_unchecked(sh) }, unsafe { out.get_unchecked_mut(sh) });
            unsafe { *self.cnt.get_unchecked_mut(sh) = 0 };
        } else {
            unsafe { *self.cnt.get_unchecked_mut(sh) = (c + 1) as u8 };
        }
    }

    #[inline(always)]
    fn flush_full(src: &[T; N], dst: &mut Vec<T>) {
        dst.reserve(N);
        let len = dst.len();
        let bytes = N * std::mem::size_of::<T>();
        let p = unsafe { dst.as_mut_ptr().add(len) } as *mut u8;
        #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
        {
            if bytes == 64 && (p as usize) & 63 == 0 {
                use std::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_stream_si256};
                let s = src.as_ptr() as *const u8;
                unsafe {
                    let a = _mm256_loadu_si256(s as *const __m256i);
                    let b = _mm256_loadu_si256(s.add(32) as *const __m256i);
                    _mm256_stream_si256(p as *mut __m256i, a);
                    _mm256_stream_si256(p.add(32) as *mut __m256i, b);
                    dst.set_len(len + N);
                }
                return;
            }
        }
        let _ = (bytes, p);
        dst.extend_from_slice(src);
    }

    fn finish(&mut self, out: &mut [Vec<T>]) {
        if !self.on {
            return;
        }
        for (sh, c) in self.cnt.iter_mut().enumerate() {
            if *c > 0 {
                out[sh].extend_from_slice(&self.stage[sh][..*c as usize]);
                *c = 0;
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

const _: () = assert!(std::mem::size_of::<RawEvent>() * 2 == 64);
const _: () = assert!(std::mem::size_of::<PersonEvent>() * 4 == 64);

fn read_events_parallel(
    paths: &[PathBuf],
    ncpu: usize,
    cfg: Config,
    base_ts: i64,
    window_start: i64,
    window_end: i64,
    products: &ProductCatalog,
    unknown_cat_id: u16,
) -> io::Result<WorkerMatrix<RawEvent>> {
    let (files, chunks) = plan_file_chunks(paths, cfg.chunk_bytes)?;
    if chunks.is_empty() {
        return Ok(WorkerMatrix::new(Vec::new()));
    }
    let total_bytes: u64 = files.iter().map(|f| f.len.saturating_sub(f.data_start)).sum();
    let per_shard_cap = ((total_bytes / 56) as usize / (ncpu * NUM_DEDUP_SHARDS)).max(64);
    let next_chunk = AtomicUsize::new(0);
    let workers: Vec<Vec<Vec<RawEvent>>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for _ in 0..ncpu {
            handles.push(s.spawn(|| {
                let mut out: Vec<Vec<RawEvent>> = (0..NUM_DEDUP_SHARDS)
                    .map(|_| Vec::with_capacity(per_shard_cap))
                    .collect();
                let mut wc: WcSink<RawEvent, 2> = WcSink::new(NUM_DEDUP_SHARDS, cfg.use_wc);
                let mut dc_ing = DateCache::new();
                let mut dc_ts = DateCache::new();
                let mut buf = Vec::new();
                loop {
                    let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if cidx >= chunks.len() {
                        break;
                    }
                    let ch = &chunks[cidx];
                    let (start, own_end, scan_len) =
                        load_chunk_lines(&files[ch.file_idx], ch, &mut buf)
                            .expect("failed to read event chunk");
                    for_each_record_simd::<7>(
                        &buf,
                        start,
                        own_end,
                        scan_len,
                        |[eid, uid, ts_b, ing_b, etype, pid, price_b]| {
                            if eid.is_empty() {
                                return;
                            }
                            let Some(ingest_ts) = dc_ing.parse_ts(ing_b) else {
                                return;
                            };

                            let mut ts_rel = u32::MAX;
                            let mut price_cat = 0u32;

                            if !uid.is_empty() && !pid.is_empty() {
                                match etype {
                                    b"view" | b"cart" => {
                                        if let Some(ts) = dc_ts.parse_ts(ts_b) {
                                            if ts >= window_start
                                                && ts < window_end
                                                && ingest_ts - ts <= DAY_MS
                                            {
                                                ts_rel = (ts - base_ts) as u32;
                                            }
                                        }
                                    }
                                    b"purchase" => {
                                        if let Some(p) = parse_price_cents(price_b) {
                                            if let Some(ts) = dc_ts.parse_ts(ts_b) {
                                                if ts >= window_start
                                                    && ts < window_end
                                                    && ingest_ts - ts <= DAY_MS
                                                {
                                                    ts_rel = (ts - base_ts) as u32;
                                                    let cid = products.get(pid, unknown_cat_id);
                                                    price_cat = (((cid as u32) + 1) << 24)
                                                        | (p as u32);
                                                }
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            let event_id = Key::from_bytes(eid);
                            let user_id = if ts_rel != u32::MAX {
                                Key::from_bytes(uid)
                            } else {
                                Key::EMPTY
                            };
                            let shard = (event_id.hash64() >> DEDUP_SHIFT) as usize;
                            wc.push(
                                shard,
                                RawEvent {
                                    event_id,
                                    user_id,
                                    ingest_ts,
                                    ts_rel,
                                    price_cat,
                                },
                                &mut out,
                            );
                        },
                    );
                }
                wc.finish(&mut out);
                out
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    Ok(WorkerMatrix::new(workers))
}

fn read_clicks_parallel(
    paths: &[PathBuf],
    ncpu: usize,
    cfg: Config,
    base_ts: i64,
    window_end: i64,
    campaigns: &CampaignCatalog,
    links: &LinkTable,
) -> io::Result<WorkerMatrix<PersonClick>> {
    let (files, chunks) = plan_file_chunks(paths, cfg.chunk_bytes)?;
    if chunks.is_empty() {
        return Ok(WorkerMatrix::new(Vec::new()));
    }
    let total_bytes: u64 = files.iter().map(|f| f.len.saturating_sub(f.data_start)).sum();
    let per_click_shard_cap = ((total_bytes / 42) as usize / (ncpu * NUM_PERSON_SHARDS)).max(32);
    let next_chunk = AtomicUsize::new(0);
    let workers: Vec<Vec<Vec<PersonClick>>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for _ in 0..ncpu {
            handles.push(s.spawn(|| {
                let mut out: Vec<Vec<PersonClick>> = (0..NUM_PERSON_SHARDS)
                    .map(|_| Vec::with_capacity(per_click_shard_cap))
                    .collect();
                let mut dc = DateCache::new();
                let mut link_cache = [(Key::EMPTY, Key::EMPTY); 1024];
                let mut buf = Vec::new();
                loop {
                    let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if cidx >= chunks.len() {
                        break;
                    }
                    let ch = &chunks[cidx];
                    let (start, own_end, scan_len) =
                        load_chunk_lines(&files[ch.file_idx], ch, &mut buf)
                            .expect("failed to read click chunk");
                    for_each_record_simd::<4>(
                        &buf,
                        start,
                        own_end,
                        scan_len,
                        |[clid, uid, camp_b, ts_b]| {
                            if clid.is_empty() || uid.is_empty() {
                                return;
                            }
                            let Some(campaign_idx) = campaigns.get(camp_b) else {
                                return;
                            };
                            let Some(ts) = dc.parse_ts(ts_b) else {
                                return;
                            };
                            if ts < base_ts || ts >= window_end {
                                return;
                            }
                            let raw_uid = Key::from_bytes(uid);
                            let person_id = links.resolve(raw_uid, &mut link_cache);
                            let click_id = Key::from_bytes(clid);
                            let shard = (person_id.hash64() >> PERSON_SHIFT) as usize;
                            out[shard].push(PersonClick {
                                person_id,
                                click_id,
                                ts_rel: (ts - base_ts) as u32,
                                campaign_idx: campaign_idx as u32,
                            });
                        },
                    );
                }
                out
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    Ok(WorkerMatrix::new(workers))
}

#[inline]
fn insertion_sort_u64(arr: &mut [u64]) {
    for i in 1..arr.len() {
        let key = arr[i];
        let mut j = i;
        while j > 0 && arr[j - 1] > key {
            arr[j] = arr[j - 1];
            j -= 1;
        }
        arr[j] = key;
    }
}

/// Sliding multiset of campaigns over a person's click window `[c_lo, c_hi)`.
/// `active` lists exactly the campaigns with a non-zero count, so an assisted session
/// costs O(distinct campaigns in window) instead of O(clicks in window).
struct AssistWindow {
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
}

impl AssistWindow {
    fn new(num_campaigns: usize) -> Self {
        Self { cnt: vec![0; num_campaigns], pos: vec![0; num_campaigns], active: Vec::with_capacity(64) }
    }

    #[inline(always)]
    fn enter(&mut self, ci: u32) {
        let c = &mut self.cnt[ci as usize];
        *c += 1;
        if *c == 1 {
            self.pos[ci as usize] = self.active.len() as u32;
            self.active.push(ci);
        }
    }

    #[inline(always)]
    fn leave(&mut self, ci: u32) {
        let c = &mut self.cnt[ci as usize];
        *c -= 1;
        if *c == 0 {
            let p = self.pos[ci as usize] as usize;
            let last = self.active.pop().unwrap_or(ci);
            if last != ci {
                self.active[p] = last;
                self.pos[last as usize] = p as u32;
            }
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        for &ci in &self.active {
            self.cnt[ci as usize] = 0;
        }
        self.active.clear();
    }
}

#[inline(always)]
fn credit_session(
    pclks: &[CompactClick],
    c_lo: &mut usize,
    c_hi: &mut usize,
    session_start_rel: u32,
    day_off: usize,
    sess_has_purchase: bool,
    sess_rev: i64,
    num_days: usize,
    organic_idx: usize,
    camp_agg: &mut [CampaignAgg],
    aw: &mut AssistWindow,
) {
    let min_click_rel = session_start_rel - SEVEN_DAYS_REL;
    while *c_hi < pclks.len() && pclks[*c_hi].ts_rel <= session_start_rel {
        aw.enter(pclks[*c_hi].campaign_idx);
        *c_hi += 1;
    }
    while *c_lo < *c_hi && pclks[*c_lo].ts_rel < min_click_rel {
        aw.leave(pclks[*c_lo].campaign_idx);
        *c_lo += 1;
    }

    if *c_lo == *c_hi {
        let cell = &mut camp_agg[organic_idx * num_days + day_off];
        cell.sessions += 1;
        if sess_has_purchase {
            cell.purchasing_sessions += 1;
            cell.revenue_cents += sess_rev;
        }
    } else {
        let cred_camp = pclks[*c_hi - 1].campaign_idx as usize;
        let cell = &mut camp_agg[cred_camp * num_days + day_off];
        cell.sessions += 1;
        if sess_has_purchase {
            cell.purchasing_sessions += 1;
            cell.revenue_cents += sess_rev;
        }

        // Every campaign with at least one click in [start - 7d, start] assists once.
        for &ci in &aw.active {
            camp_agg[ci as usize * num_days + day_off].assisted_sessions += 1;
        }
    }
}

#[inline]
fn finish_day(cat_rev: &mut [i64], dirty_cats: &mut Vec<u16>) -> u16 {
    if dirty_cats.is_empty() {
        return u16::MAX;
    }
    let mut best_cat = u16::MAX;
    let mut best_rev = -1i64;
    for &cid in dirty_cats.iter() {
        let rev = cat_rev[cid as usize];
        cat_rev[cid as usize] = 0;
        if rev > best_rev || (rev == best_rev && cid < best_cat) {
            best_rev = rev;
            best_cat = cid;
        }
    }
    dirty_cats.clear();
    best_cat
}

fn main() -> io::Result<()> {
    let t_total = Instant::now();
    let cfg = Config::from_env();

    let mut args = env::args_os().skip(1);
    let data_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())));
    let out_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()))
        });
    let ncpu = env::var("NCPU")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(4, usize::from));

    let (window_start, window_end) = parse_manifest(&data_dir.join("manifest.json"))?;
    let base_ts = window_start - SEVEN_DAYS_MS;
    assert!((window_end - base_ts) < (u32::MAX as i64));

    let min_day = window_start.div_euclid(DAY_MS) as i32;
    let max_day = (window_end - 1).div_euclid(DAY_MS) as i32;
    let num_days = (max_day - min_day + 1).max(1) as usize;
    let num_bins = if window_end > window_start {
        (((window_end - window_start) as u32 / SESSION_GAP_REL) as usize) + 2
    } else {
        2
    };

    let (products, categories, unknown_cat_id) = load_products(&data_dir.join("products.csv"))?;
    let (campaigns, campaign_list) = load_campaigns(&data_dir.join("campaigns.csv"))?;
    let organic_idx = campaign_list.len() - 1;

    let t0 = Instant::now();
    let (links, ms_links, event_shards, ms_events_io) = if cfg.overlap {
        // Links construction runs beside event parsing; dedup (the first links
        // consumer) only starts after both join. Output is schedule-independent.
        let (lres, eres) = thread::scope(|s| {
            let h = s.spawn(|| {
                let t = Instant::now();
                load_links_dispatch(&data_dir, ncpu, cfg)
                    .map(|l| (l, t.elapsed().as_secs_f64() * 1000.0))
            });
            let t1 = Instant::now();
            let r = (|| -> io::Result<WorkerMatrix<RawEvent>> {
                let event_files = list_csvs(&data_dir.join("events"))?;
                read_events_parallel(
                    &event_files,
                    ncpu,
                    cfg,
                    base_ts,
                    window_start,
                    window_end,
                    &products,
                    unknown_cat_id,
                )
            })();
            let ms_ev = t1.elapsed().as_secs_f64() * 1000.0;
            (h.join().unwrap(), r.map(|ev| (ev, ms_ev)))
        });
        let (l, ml) = lres?;
        let (ev, me) = eres?;
        (l, ml, ev, me)
    } else {
        let links = load_links_dispatch(&data_dir, ncpu, cfg)?;
        let ms_links = t0.elapsed().as_secs_f64() * 1000.0;
        let t1 = Instant::now();
        let event_files = list_csvs(&data_dir.join("events"))?;
        let event_shards = read_events_parallel(
            &event_files,
            ncpu,
            cfg,
            base_ts,
            window_start,
            window_end,
            &products,
            unknown_cat_id,
        )?;
        let ms_events_io = t1.elapsed().as_secs_f64() * 1000.0;
        (links, ms_links, event_shards, ms_events_io)
    };

    // Phase 2: In-place dedup across [0..ew][sh] -> 16-ahead AoS link prefetch -> scatter to 256 person shards
    let t2 = Instant::now();
    let ew = event_shards.num_workers();
    let total_raw_events: usize = if ew == 0 {
        0
    } else {
        (0..NUM_DEDUP_SHARDS)
            .map(|sh| event_shards.shard_len(sh))
            .sum()
    };
    let per_person_shard_cap = (total_raw_events / (ncpu * NUM_PERSON_SHARDS)).max(64);
    let next_shard = AtomicUsize::new(0);
    let pe_workers: Vec<Vec<Vec<PersonEvent>>> = if ew == 0 {
        Vec::new()
    } else {
        thread::scope(|s| {
            let mut handles = Vec::with_capacity(ncpu);
            for _ in 0..ncpu {
                handles.push(s.spawn(|| {
                    let mut table = DedupTable::new(1 << 14);
                    let mut kept: Vec<RawEvent> = Vec::new();
                    let mut out: Vec<Vec<PersonEvent>> = (0..NUM_PERSON_SHARDS)
                        .map(|_| Vec::with_capacity(per_person_shard_cap))
                        .collect();
                    let mut link_cache = [(Key::EMPTY, Key::EMPTY); 1024];
                    let mut pwc: WcSink<PersonEvent, 4> = WcSink::new(NUM_PERSON_SHARDS, cfg.use_wc);
                    let mut seen_bm: Vec<u64> = Vec::new();
                    let mut dup_bm: Vec<u64> = Vec::new();

                    loop {
                        let sh = next_shard.fetch_add(1, Ordering::Relaxed);
                        if sh >= NUM_DEDUP_SHARDS {
                            break;
                        }
                        let total_len = event_shards.shard_len(sh);
                        if total_len == 0 {
                            continue;
                        }
                        table.reset(total_len);
                        kept.clear();
                        if kept.capacity() < total_len {
                            kept.reserve(total_len - kept.capacity());
                        }

                        // Seen/dup bitmap prefilter: every copy of a repeated event_id sets the same
                        // bit, so an event whose bit was hit only once is unique and skips the exact
                        // table. Only the ~2-4% duplicated ids (plus bitmap collisions) are probed.
                        let bm_on = cfg.use_dedup_bitmap;
                        let bm_bits = if bm_on { (total_len * 16).next_power_of_two().max(1 << 12) } else { 0 };
                        let bm_mask = bm_bits.wrapping_sub(1);
                        if bm_on {
                            let words = bm_bits / 64;
                            seen_bm.clear();
                            seen_bm.resize(words, 0);
                            dup_bm.clear();
                            dup_bm.resize(words, 0);
                            for t in 0..ew {
                                let raw = unsafe { event_shards.get_mut(t, sh) };
                                for ev in raw.iter() {
                                    let b = ((ev.event_id.hash64() >> 8) as usize) & bm_mask;
                                    let (w, m) = (b >> 6, 1u64 << (b & 63));
                                    let sw = unsafe { seen_bm.get_unchecked_mut(w) };
                                    let hit = *sw & m;
                                    *sw |= m;
                                    unsafe { *dup_bm.get_unchecked_mut(w) |= hit };
                                }
                            }
                        }

                        for t in 0..ew {
                            let raw = unsafe { event_shards.get_mut(t, sh) };
                            let rlen = raw.len();
                            for i in 0..rlen {
                                if cfg.use_prefetch && i + 16 < rlen {
                                    table.prefetch(raw[i + 16].event_id);
                                }
                                let ev = raw[i];
                                if bm_on {
                                    let b = ((ev.event_id.hash64() >> 8) as usize) & bm_mask;
                                    if unsafe { *dup_bm.get_unchecked(b >> 6) } & (1u64 << (b & 63)) == 0 {
                                        if ev.ts_rel != u32::MAX {
                                            if i + 16 < rlen && raw[i + 16].ts_rel != u32::MAX {
                                                links.prefetch(raw[i + 16].user_id);
                                            }
                                            let person_id = links.resolve(ev.user_id, &mut link_cache);
                                            let psh = (person_id.hash64() >> PERSON_SHIFT) as usize;
                                            let packed_ev =
                                                ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
                                            pwc.push(psh, PersonEvent { person_id, packed_ev }, &mut out);
                                        }
                                        continue;
                                    }
                                }
                                if let Some(pos) = table.get_or_insert(ev, &kept) {
                                    let cur = &mut kept[pos as usize];
                                    if ev.ingest_ts < cur.ingest_ts {
                                        *cur = ev;
                                    }
                                } else {
                                    kept.push(ev);
                                }
                            }
                            *raw = Vec::new();
                        }

                        let klen = kept.len();
                        for i in 0..klen {
                            if i + 16 < klen && kept[i + 16].ts_rel != u32::MAX {
                                links.prefetch(kept[i + 16].user_id);
                            }
                            let ev = kept[i];
                            if ev.ts_rel != u32::MAX {
                                let person_id = links.resolve(ev.user_id, &mut link_cache);
                                let psh = (person_id.hash64() >> PERSON_SHIFT) as usize;
                                let packed_ev =
                                    ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
                                pwc.push(psh, PersonEvent { person_id, packed_ev }, &mut out);
                            }
                        }
                    }
                    pwc.finish(&mut out);
                    out
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };
    drop(event_shards);
    let person_event_shards = WorkerMatrix::new(pe_workers);
    let ms_dedup = t2.elapsed().as_secs_f64() * 1000.0;

    let t3 = Instant::now();
    let click_files = list_csvs(&data_dir.join("clicks"))?;
    let person_click_shards = read_clicks_parallel(
        &click_files,
        ncpu,
        cfg,
        base_ts,
        window_end,
        &campaigns,
        &links,
    )?;
    let ms_clicks = t3.elapsed().as_secs_f64() * 1000.0;

    fs::create_dir_all(out_dir.join("user_daily"))?;

    // Phase 3: Heaviest-first LPT scheduling -> O(N) binned/counting-sort sessionization -> direct columnar Parquet
    let t4 = Instant::now();
    let pew = person_event_shards.num_workers();
    let pcw = person_click_shards.num_workers();
    let mut shard_order: Vec<(usize, usize)> = (0..NUM_PERSON_SHARDS)
        .map(|sh| {
            let cnt = if pew > 0 {
                person_event_shards.shard_len(sh)
            } else {
                0
            };
            (sh, cnt)
        })
        .collect();
    shard_order.sort_unstable_by(|a, b| b.1.cmp(&a.1));

    let next_person_idx = AtomicUsize::new(0);
    let num_campaigns = campaign_list.len();
    let num_cats = categories.len();

    let camp_results: Vec<Vec<CampaignAgg>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for tid in 0..ncpu {
            let out_dir_ref = &out_dir;
            let cats_ref = &categories;
            let pe_ref = &person_event_shards;
            let pc_ref = &person_click_shards;
            let order_ref = &shard_order;
            let next_ref = &next_person_idx;
            handles.push(s.spawn(move || {
                let mut writer = UserDailyWriter::new(out_dir_ref, tid);
                let mut camp_agg = vec![CampaignAgg::default(); num_campaigns * num_days];
                let mut cat_rev = vec![0i64; num_cats];
                let mut dirty_cats: Vec<u16> = Vec::with_capacity(16);
                let mut assist_win = AssistWindow::new(num_campaigns);

                let mut pmap = LocalPersonMap::new(1 << 14);
                let mut p_ids: Vec<Key> = Vec::new();
                let mut ev_counts: Vec<u32> = Vec::new();
                let mut ev_lpids: Vec<u32> = Vec::new();
                let mut ev_offs: Vec<u32> = Vec::new();
                let mut bin_counts: Vec<u32> = vec![0; num_bins];
                let mut bin_offs: Vec<u32> = vec![0; num_bins + 1];
                let mut bin_buf: Vec<(u32, u64)> = Vec::new();
                let mut sorted_evs: Vec<u64> = Vec::new();

                let mut clk_counts: Vec<u32> = Vec::new();
                let mut clk_tmp: Vec<(u32, CompactClick)> = Vec::new();
                let mut clk_offs: Vec<u32> = Vec::new();
                let mut sorted_clks: Vec<CompactClick> = Vec::new();

                loop {
                    let order_idx = next_ref.fetch_add(1, Ordering::Relaxed);
                    if order_idx >= NUM_PERSON_SHARDS {
                        break;
                    }
                    let (sh, total_evs) = order_ref[order_idx];
                    if total_evs == 0 {
                        for t in 0..pcw {
                            unsafe {
                                *pc_ref.get_mut(t, sh) = Vec::new();
                            }
                        }
                        continue;
                    }

                    pmap.reset(total_evs);
                    p_ids.clear();
                    ev_counts.clear();
                    ev_lpids.clear();
                    if ev_lpids.capacity() < total_evs {
                        ev_lpids.reserve(total_evs - ev_lpids.capacity());
                    }
                    let mut next_pid = 0u32;

                    if cfg.use_sortless {
                        bin_counts.fill(0);
                        for t in 0..pew {
                            let evs = unsafe { pe_ref.get_mut(t, sh) };
                            let elen = evs.len();
                            for i in 0..elen {
                                if cfg.use_prefetch && i + 16 < elen {
                                    pmap.prefetch(evs[i + 16].person_id);
                                }
                                let ev = &evs[i];
                                let (lpid, is_new) =
                                    pmap.get_or_insert(ev.person_id, &mut next_pid);
                                if is_new {
                                    p_ids.push(ev.person_id);
                                    ev_counts.push(1);
                                } else {
                                    ev_counts[lpid as usize] += 1;
                                }
                                ev_lpids.push(lpid);
                                let ts_rel = (ev.packed_ev >> 32) as u32;
                                let b = ((ts_rel - SEVEN_DAYS_REL) / SESSION_GAP_REL) as usize;
                                bin_counts[b] += 1;
                            }
                        }

                        let mut bacc = 0u32;
                        for b in 0..num_bins {
                            bin_offs[b] = bacc;
                            bacc += bin_counts[b];
                            bin_counts[b] = bin_offs[b];
                        }
                        bin_offs[num_bins] = bacc;

                        bin_buf.resize(total_evs, (0, 0));
                        let mut k = 0usize;
                        for t in 0..pew {
                            let evs = unsafe { pe_ref.get_mut(t, sh) };
                            for ev in evs.iter() {
                                let lpid = ev_lpids[k];
                                k += 1;
                                let ts_rel = (ev.packed_ev >> 32) as u32;
                                let b = ((ts_rel - SEVEN_DAYS_REL) / SESSION_GAP_REL) as usize;
                                let dst = bin_counts[b] as usize;
                                bin_counts[b] = (dst + 1) as u32;
                                bin_buf[dst] = (lpid, ev.packed_ev);
                            }
                            *evs = Vec::new();
                        }

                        let num_people = p_ids.len();
                        ev_offs.resize(num_people + 1, 0);
                        let mut acc = 0u32;
                        for p in 0..num_people {
                            ev_offs[p] = acc;
                            acc += ev_counts[p];
                            ev_counts[p] = ev_offs[p];
                        }
                        ev_offs[num_people] = acc;

                        sorted_evs.resize(total_evs, 0);
                        for &(lpid, packed_ev) in &bin_buf[..total_evs] {
                            let dst = ev_counts[lpid as usize] as usize;
                            ev_counts[lpid as usize] = (dst + 1) as u32;
                            sorted_evs[dst] = packed_ev;
                        }
                    } else {
                        for t in 0..pew {
                            let evs = unsafe { pe_ref.get_mut(t, sh) };
                            let elen = evs.len();
                            for i in 0..elen {
                                if cfg.use_prefetch && i + 16 < elen {
                                    pmap.prefetch(evs[i + 16].person_id);
                                }
                                let ev = &evs[i];
                                let (lpid, is_new) =
                                    pmap.get_or_insert(ev.person_id, &mut next_pid);
                                if is_new {
                                    p_ids.push(ev.person_id);
                                    ev_counts.push(1);
                                } else {
                                    ev_counts[lpid as usize] += 1;
                                }
                                ev_lpids.push(lpid);
                            }
                        }

                        let num_people = p_ids.len();
                        ev_offs.resize(num_people + 1, 0);
                        let mut acc = 0u32;
                        for p in 0..num_people {
                            ev_offs[p] = acc;
                            acc += ev_counts[p];
                            ev_counts[p] = ev_offs[p];
                        }
                        ev_offs[num_people] = acc;

                        sorted_evs.resize(total_evs, 0);
                        let mut k = 0usize;
                        for t in 0..pew {
                            let evs = unsafe { pe_ref.get_mut(t, sh) };
                            for ev in evs.iter() {
                                let lpid = ev_lpids[k] as usize;
                                k += 1;
                                let dst = ev_counts[lpid] as usize;
                                ev_counts[lpid] = (dst + 1) as u32;
                                sorted_evs[dst] = ev.packed_ev;
                            }
                            *evs = Vec::new();
                        }
                    }

                    let num_people = p_ids.len();

                    // Filter and counting-sort clicks strictly for people with surviving events
                    clk_counts.clear();
                    clk_counts.resize(num_people, 0);
                    clk_tmp.clear();
                    for t in 0..pcw {
                        let clks = unsafe { pc_ref.get_mut(t, sh) };
                        let clen = clks.len();
                        for i in 0..clen {
                            if cfg.use_prefetch && i + 16 < clen {
                                pmap.prefetch(clks[i + 16].person_id);
                            }
                            let clk = &clks[i];
                            if let Some(lpid) = pmap.get(clk.person_id) {
                                clk_counts[lpid as usize] += 1;
                                clk_tmp.push((
                                    lpid,
                                    CompactClick {
                                        ts_rel: clk.ts_rel,
                                        campaign_idx: clk.campaign_idx,
                                        click_id: clk.click_id,
                                    },
                                ));
                            }
                        }
                        *clks = Vec::new();
                    }

                    clk_offs.resize(num_people + 1, 0);
                    let mut cacc = 0u32;
                    for p in 0..num_people {
                        clk_offs[p] = cacc;
                        cacc += clk_counts[p];
                        clk_counts[p] = clk_offs[p];
                    }
                    clk_offs[num_people] = cacc;

                    sorted_clks.resize(
                        clk_tmp.len(),
                        CompactClick {
                            ts_rel: 0,
                            campaign_idx: 0,
                            click_id: Key::EMPTY,
                        },
                    );
                    for &(lpid, cc) in &clk_tmp {
                        let dst = clk_counts[lpid as usize] as usize;
                        clk_counts[lpid as usize] = (dst + 1) as u32;
                        sorted_clks[dst] = cc;
                    }

                    for p in 0..num_people {
                        let pid = p_ids[p];
                        let pevs = &mut sorted_evs[ev_offs[p] as usize..ev_offs[p + 1] as usize];
                        let pclks =
                            &mut sorted_clks[clk_offs[p] as usize..clk_offs[p + 1] as usize];
                        if pclks.len() > 1 {
                            pclks.sort_unstable_by(|a, b| {
                                a.ts_rel
                                    .cmp(&b.ts_rel)
                                    .then_with(|| b.click_id.cmp_bytes(&a.click_id))
                            });
                        }

                        let mut c_lo = 0usize;
                        let mut c_hi = 0usize;
                        assist_win.reset();
                        let mut cur_day = i32::MIN;
                        let mut day_sessions = 0i32;
                        let mut day_events = 0i32;
                        let mut day_rev = 0i64;

                        if cfg.use_sortless {
                            let ee = pevs.len();
                            let mut idx = 0usize;
                            let mut has_active = false;
                            let mut session_start_rel = 0u32;
                            let mut last_ts_rel = 0u32;
                            let mut sess_day = 0i32;
                            let mut sess_events = 0i32;
                            let mut sess_rev = 0i64;
                            let mut sess_has_purchase = false;

                            while idx < ee {
                                let bin_b =
                                    ((pevs[idx] >> 32) as u32 - SEVEN_DAYS_REL) / SESSION_GAP_REL;
                                let mut run_end = idx + 1;
                                while run_end < ee
                                    && ((pevs[run_end] >> 32) as u32 - SEVEN_DAYS_REL)
                                        / SESSION_GAP_REL
                                        == bin_b
                                {
                                    run_end += 1;
                                }

                                let mut b_min = u32::MAX;
                                let mut b_max = 0u32;
                                let mut b_rev = 0i64;
                                let mut b_has_purch = false;
                                for &packed in &pevs[idx..run_end] {
                                    let t = (packed >> 32) as u32;
                                    if t < b_min {
                                        b_min = t;
                                    }
                                    if t > b_max {
                                        b_max = t;
                                    }
                                    let pc = packed as u32;
                                    if pc != 0 {
                                        b_has_purch = true;
                                        b_rev += (pc & 0x00FF_FFFF) as i64;
                                    }
                                }

                                if !has_active || b_min - last_ts_rel > SESSION_GAP_REL {
                                    if has_active {
                                        day_sessions += 1;
                                        day_events += sess_events;
                                        day_rev += sess_rev;
                                        credit_session(
                                            pclks,
                                            &mut c_lo,
                                            &mut c_hi,
                                            session_start_rel,
                                            (sess_day - min_day) as usize,
                                            sess_has_purchase,
                                            sess_rev,
                                            num_days,
                                            organic_idx,
                                            &mut camp_agg,
                                            &mut assist_win,
                                        );
                                    }
                                    let sday =
                                        (base_ts + (b_min as i64)).div_euclid(DAY_MS) as i32;
                                    if sday != cur_day {
                                        if cur_day != i32::MIN {
                                            let top_cat =
                                                finish_day(&mut cat_rev, &mut dirty_cats);
                                            writer.push(
                                                pid,
                                                cur_day,
                                                day_sessions,
                                                day_events,
                                                day_rev,
                                                top_cat,
                                                cats_ref,
                                            );
                                        }
                                        cur_day = sday;
                                        day_sessions = 0;
                                        day_events = 0;
                                        day_rev = 0;
                                    }
                                    has_active = true;
                                    session_start_rel = b_min;
                                    sess_day = sday;
                                    sess_events = (run_end - idx) as i32;
                                    sess_rev = b_rev;
                                    sess_has_purchase = b_has_purch;
                                } else {
                                    sess_events += (run_end - idx) as i32;
                                    sess_rev += b_rev;
                                    sess_has_purchase |= b_has_purch;
                                }
                                last_ts_rel = b_max;

                                if b_has_purch {
                                    for &packed in &pevs[idx..run_end] {
                                        let pc = packed as u32;
                                        if pc != 0 {
                                            let price = (pc & 0x00FF_FFFF) as i64;
                                            let cid = ((pc >> 24) - 1) as u16;
                                            if cat_rev[cid as usize] == 0 {
                                                dirty_cats.push(cid);
                                            }
                                            cat_rev[cid as usize] += price;
                                        }
                                    }
                                }

                                idx = run_end;
                            }

                            if has_active {
                                day_sessions += 1;
                                day_events += sess_events;
                                day_rev += sess_rev;
                                credit_session(
                                    pclks,
                                    &mut c_lo,
                                    &mut c_hi,
                                    session_start_rel,
                                    (sess_day - min_day) as usize,
                                    sess_has_purchase,
                                    sess_rev,
                                    num_days,
                                    organic_idx,
                                    &mut camp_agg,
                                    &mut assist_win,
                                );
                            }
                        } else {
                            if pevs.len() <= 24 {
                                insertion_sort_u64(pevs);
                            } else {
                                pevs.sort_unstable();
                            }

                            let mut idx = 0usize;
                            let ee = pevs.len();
                            while idx < ee {
                                let packed0 = pevs[idx];
                                let session_start_rel = (packed0 >> 32) as u32;
                                let mut last_ts_rel = session_start_rel;
                                let mut sess_events = 1i32;
                                let mut sess_rev = 0i64;
                                let mut sess_has_purchase = false;

                                let session_day = (base_ts + (session_start_rel as i64))
                                    .div_euclid(DAY_MS)
                                    as i32;
                                if session_day != cur_day {
                                    if cur_day != i32::MIN {
                                        let top_cat = finish_day(&mut cat_rev, &mut dirty_cats);
                                        writer.push(
                                            pid,
                                            cur_day,
                                            day_sessions,
                                            day_events,
                                            day_rev,
                                            top_cat,
                                            cats_ref,
                                        );
                                    }
                                    cur_day = session_day;
                                    day_sessions = 0;
                                    day_events = 0;
                                    day_rev = 0;
                                }

                                let pc0 = packed0 as u32;
                                if pc0 != 0 {
                                    let price = (pc0 & 0x00FF_FFFF) as i64;
                                    let cid = ((pc0 >> 24) - 1) as u16;
                                    sess_has_purchase = true;
                                    sess_rev += price;
                                    if cat_rev[cid as usize] == 0 {
                                        dirty_cats.push(cid);
                                    }
                                    cat_rev[cid as usize] += price;
                                }

                                idx += 1;
                                while idx < ee {
                                    let packed = pevs[idx];
                                    let ts_rel = (packed >> 32) as u32;
                                    if ts_rel - last_ts_rel > SESSION_GAP_REL {
                                        break;
                                    }
                                    last_ts_rel = ts_rel;
                                    sess_events += 1;
                                    let pc = packed as u32;
                                    if pc != 0 {
                                        let price = (pc & 0x00FF_FFFF) as i64;
                                        let cid = ((pc >> 24) - 1) as u16;
                                        sess_has_purchase = true;
                                        sess_rev += price;
                                        if cat_rev[cid as usize] == 0 {
                                            dirty_cats.push(cid);
                                        }
                                        cat_rev[cid as usize] += price;
                                    }
                                    idx += 1;
                                }

                                day_sessions += 1;
                                day_events += sess_events;
                                day_rev += sess_rev;

                                credit_session(
                                    pclks,
                                    &mut c_lo,
                                    &mut c_hi,
                                    session_start_rel,
                                    (session_day - min_day) as usize,
                                    sess_has_purchase,
                                    sess_rev,
                                    num_days,
                                    organic_idx,
                                    &mut camp_agg,
                                    &mut assist_win,
                                );
                            }
                        }

                        if cur_day != i32::MIN {
                            let top_cat = finish_day(&mut cat_rev, &mut dirty_cats);
                            writer.push(
                                pid,
                                cur_day,
                                day_sessions,
                                day_events,
                                day_rev,
                                top_cat,
                                cats_ref,
                            );
                        }
                    }
                }

                writer
                    .finish(tid == 0)
                    .expect("failed to write user_daily parquet");
                camp_agg
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let ms_sessions = t4.elapsed().as_secs_f64() * 1000.0;

    let mut total_camp_agg = vec![CampaignAgg::default(); num_campaigns * num_days];
    for cagg in camp_results {
        for (dst, src) in total_camp_agg.iter_mut().zip(cagg.iter()) {
            dst.sessions += src.sessions;
            dst.purchasing_sessions += src.purchasing_sessions;
            dst.revenue_cents += src.revenue_cents;
            dst.assisted_sessions += src.assisted_sessions;
        }
    }

    let mut campaign_rows = Vec::new();
    for cidx in 0..num_campaigns {
        for doff in 0..num_days {
            let agg = total_camp_agg[cidx * num_days + doff];
            if agg.sessions > 0 || agg.assisted_sessions > 0 {
                campaign_rows.push(CampaignDailyRow {
                    campaign_id_idx: cidx as u16,
                    day: min_day + doff as i32,
                    sessions: agg.sessions,
                    purchasing_sessions: agg.purchasing_sessions,
                    revenue_cents: agg.revenue_cents,
                    assisted_sessions: agg.assisted_sessions,
                });
            }
        }
    }

    write_campaign_daily(&out_dir, &campaign_rows, &campaign_list)?;

    if cfg.timing {
        eprintln!(
            "TIMING ms | links={:.1} events_io={:.1} dedup_route={:.1} clicks={:.1} sessions_pq={:.1} total={:.1}",
            ms_links,
            ms_events_io,
            ms_dedup,
            ms_clicks,
            ms_sessions,
            t_total.elapsed().as_secs_f64() * 1000.0
        );
    }

    sys::maybe_write_pad();
    sys::fast_exit();
}
