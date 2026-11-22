use std::fs::File;
use std::io;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
mod linux_sys {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

    const HUGE_PAGE: usize = 2 << 20;
    const ARENA_SIZE: usize = 8 << 20;
    const MED_MIN: usize = 32 << 10;
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 2;
    const MAP_ANONYMOUS: i32 = 0x20;
    const MAP_NORESERVE: i32 = 0x4000;
    const MADV_HUGEPAGE: i32 = 14;
    const MADV_DONTNEED: i32 = 4;

    // Every huge-aligned mapping, so `exit_fast` can release them in parallel. Slots past
    // MAX_REGIONS are simply not tracked (the kernel still reclaims them at exit).
    const MAX_REGIONS: usize = 16384;
    static REGION_COUNT: AtomicUsize = AtomicUsize::new(0);
    static REGION_ADDR: [AtomicUsize; MAX_REGIONS] = [const { AtomicUsize::new(0) }; MAX_REGIONS];
    static REGION_LEN: [AtomicUsize; MAX_REGIONS] = [const { AtomicUsize::new(0) }; MAX_REGIONS];

    unsafe extern "C" {
        fn mmap(
            addr: *mut u8,
            len: usize,
            prot: i32,
            flags: i32,
            fd: i32,
            offset: i64,
        ) -> *mut u8;
        fn munmap(addr: *mut u8, len: usize) -> i32;
        fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
        fn _exit(status: i32) -> !;
    }

    #[inline]
    unsafe fn map_huge_aligned(min_bytes: usize) -> (*mut u8, usize) {
        let len = (min_bytes.max(HUGE_PAGE) + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        let map_len = len + HUGE_PAGE;
        let raw = unsafe {
            mmap(
                ptr::null_mut(),
                map_len,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                -1,
                0,
            )
        };
        if raw as usize == usize::MAX {
            return (ptr::null_mut(), 0);
        }
        let raw_addr = raw as usize;
        let aligned_addr = (raw_addr + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        let head = aligned_addr - raw_addr;
        let tail_start = aligned_addr + len;
        let tail = (raw_addr + map_len) - tail_start;
        unsafe {
            if head > 0 {
                munmap(raw, head);
            }
            if tail > 0 {
                munmap(tail_start as *mut u8, tail);
            }
            madvise(aligned_addr as *mut u8, len, MADV_HUGEPAGE);
        }
        let slot = REGION_COUNT.fetch_add(1, Ordering::Relaxed);
        if slot < MAX_REGIONS {
            REGION_LEN[slot].store(len, Ordering::Relaxed);
            REGION_ADDR[slot].store(aligned_addr, Ordering::Release);
        }
        (aligned_addr as *mut u8, len)
    }

    /// See [`crate::sys::alloc_slab`].
    #[allow(dead_code)]
    pub fn alloc_slab(bytes: usize) -> (*mut u8, usize) {
        let (p, len) = unsafe { map_huge_aligned(bytes) };
        if !p.is_null() {
            // Pre-write one byte per 4 KiB so later read-before-write access never maps the
            // huge zero page and then takes a second (COW) fault on the first store.
            let mut off = 0;
            while off < len {
                unsafe { ptr::write_volatile(p.add(off), 0) };
                off += 4096;
            }
        }
        (p, len)
    }

    static SLAB_LOCK: AtomicUsize = AtomicUsize::new(0);
    static SLAB_CUR: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
    static SLAB_END: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());

    #[inline]
    unsafe fn alloc_medium(size: usize, align: usize) -> *mut u8 {
        // 64 B so Vec storage is cache-line aligned for write-combining streaming stores.
        let align = align.max(64);
        while SLAB_LOCK
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        let cur = SLAB_CUR.load(Ordering::Relaxed) as usize;
        let end = SLAB_END.load(Ordering::Relaxed) as usize;
        let aligned = (cur + align - 1) & !(align - 1);
        let next = aligned.saturating_add(size);
        let res = if cur != 0 && next <= end {
            SLAB_CUR.store(next as *mut u8, Ordering::Relaxed);
            aligned as *mut u8
        } else {
            let (p, len) = unsafe { map_huge_aligned(ARENA_SIZE) };
            if p.is_null() {
                ptr::null_mut()
            } else {
                let p_addr = p as usize;
                let a = (p_addr + align - 1) & !(align - 1);
                SLAB_CUR.store((a + size) as *mut u8, Ordering::Relaxed);
                SLAB_END.store((p_addr + len) as *mut u8, Ordering::Relaxed);
                a as *mut u8
            }
        };
        SLAB_LOCK.store(0, Ordering::Release);
        res
    }

    // Freed huge blocks are kept for reuse instead of leaked: a later phase's buffers land on
    // pages that are already faulted in, so the kernel does not zero fresh 2 MiB pages again.
    const MAX_FREE: usize = 4096;
    static FREE_LOCK: AtomicUsize = AtomicUsize::new(0);
    static FREE_N: AtomicUsize = AtomicUsize::new(0);
    static FREE_ADDR: [AtomicUsize; MAX_FREE] = [const { AtomicUsize::new(0) }; MAX_FREE];
    static FREE_LEN: [AtomicUsize; MAX_FREE] = [const { AtomicUsize::new(0) }; MAX_FREE];

    #[inline]
    fn free_lock() {
        while FREE_LOCK
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
    }

    /// Best fit among freed blocks with need <= len <= 2 * need + 4 MiB.
    unsafe fn take_free(need: usize) -> *mut u8 {
        let need = (need.max(HUGE_PAGE) + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        free_lock();
        let n = FREE_N.load(Ordering::Relaxed);
        let mut best = usize::MAX;
        let mut best_len = usize::MAX;
        for i in 0..n {
            let l = FREE_LEN[i].load(Ordering::Relaxed);
            if l >= need && l < best_len && l <= need * 2 + (4 << 20) {
                best = i;
                best_len = l;
                if l == need {
                    break;
                }
            }
        }
        let res = if best != usize::MAX {
            let a = FREE_ADDR[best].load(Ordering::Relaxed);
            FREE_ADDR[best].store(FREE_ADDR[n - 1].load(Ordering::Relaxed), Ordering::Relaxed);
            FREE_LEN[best].store(FREE_LEN[n - 1].load(Ordering::Relaxed), Ordering::Relaxed);
            FREE_N.store(n - 1, Ordering::Relaxed);
            a as *mut u8
        } else {
            ptr::null_mut()
        };
        FREE_LOCK.store(0, Ordering::Release);
        res
    }

    unsafe fn put_free(p: *mut u8, size: usize) {
        let len = (size.max(HUGE_PAGE) + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        free_lock();
        let n = FREE_N.load(Ordering::Relaxed);
        if n < MAX_FREE {
            FREE_ADDR[n].store(p as usize, Ordering::Relaxed);
            FREE_LEN[n].store(len, Ordering::Relaxed);
            FREE_N.store(n + 1, Ordering::Relaxed);
        }
        FREE_LOCK.store(0, Ordering::Release);
    }

    /// Bypasses musl `mallocng` lock contention and 4 KiB page-fault overhead for >=32 KiB allocations.
    pub struct HugeBlocks;

    unsafe impl GlobalAlloc for HugeBlocks {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let size = layout.size();
            let align = layout.align();
            if size >= HUGE_PAGE && align <= HUGE_PAGE {
                let r = unsafe { take_free(size) };
                if !r.is_null() {
                    return r;
                }
                let (p, _) = unsafe { map_huge_aligned(size) };
                return p;
            }
            if size >= MED_MIN && align <= HUGE_PAGE {
                return unsafe { alloc_medium(size, align) };
            }
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let size = layout.size();
            let align = layout.align();
            // Fresh anonymous mmap slabs are kernel-zeroed; recycled huge blocks are not, so
            // zeroed requests always take a fresh mapping.
            if size >= HUGE_PAGE && align <= HUGE_PAGE {
                return unsafe { map_huge_aligned(size).0 };
            }
            if size >= MED_MIN && align <= HUGE_PAGE {
                return unsafe { self.alloc(layout) };
            }
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let size = layout.size();
            let align = layout.align();
            if size >= HUGE_PAGE && align <= HUGE_PAGE {
                // Never munmap mid-run: each unmap takes the mmap write lock and triggers TLB
                // shootdowns on every worker. Peak RSS rises ~8% (2.25 GB at 20M; 24 GB limit),
                // and _exit reclaims everything.
                unsafe { put_free(ptr, size) };
                return;
            }
            if size >= MED_MIN && align <= HUGE_PAGE {
                return;
            }
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let old_size = layout.size();
            let align = layout.align();
            if old_size < MED_MIN && new_size < MED_MIN {
                return unsafe { System.realloc(ptr, layout, new_size) };
            }
            let Ok(new_layout) = Layout::from_size_align(new_size, align) else {
                return ptr::null_mut();
            };
            let new_ptr = unsafe { self.alloc(new_layout) };
            if !new_ptr.is_null() {
                unsafe {
                    ptr::copy_nonoverlapping(ptr, new_ptr, old_size.min(new_size));
                    self.dealloc(ptr, layout);
                }
            }
            new_ptr
        }
    }

    /// Drops the pages of every tracked mapping with MADV_DONTNEED, split across `threads`
    /// threads. Exit otherwise frees all of them on one core inside exit_mmap, which is still
    /// inside the timed window. Only called once all output is written and no other thread
    /// touches these mappings; MADV_DONTNEED holds mmap_lock for read, so the threads run in
    /// parallel, and nothing is unmapped.
    fn release_parallel(threads: usize) {
        let n = REGION_COUNT.load(Ordering::Acquire).min(MAX_REGIONS);
        let mut regions: Vec<(usize, usize)> = Vec::with_capacity(n);
        let mut total = 0usize;
        for i in 0..n {
            let a = REGION_ADDR[i].load(Ordering::Acquire);
            let l = REGION_LEN[i].load(Ordering::Relaxed);
            if a != 0 && l != 0 {
                regions.push((a, l));
                total += l;
            }
        }
        if threads < 2 || total < (256 << 20) {
            return;
        }
        // Cut the regions into `threads` contiguous shares of ~equal bytes, on 2 MiB boundaries.
        let share = ((total / threads) + HUGE_PAGE - 1) & !(HUGE_PAGE - 1);
        let mut parts: Vec<Vec<(usize, usize)>> = vec![Vec::new(); threads];
        let (mut t, mut used) = (0usize, 0usize);
        for &(a, l) in &regions {
            let mut off = 0;
            while off < l {
                let take = if t + 1 == threads { l - off } else { (l - off).min(share - used) };
                parts[t].push((a + off, take));
                off += take;
                used += take;
                if used == share && t + 1 < threads {
                    t += 1;
                    used = 0;
                }
            }
        }
        std::thread::scope(|s| {
            for part in &parts {
                s.spawn(move || {
                    for &(a, l) in part {
                        unsafe { madvise(a as *mut u8, l, MADV_DONTNEED) };
                    }
                });
            }
        });
    }

    #[inline(never)]
    pub fn exit_fast() -> ! {
        #[cfg(pgo_gen)]
        unsafe {
            unsafe extern "C" {
                fn __llvm_profile_write_file() -> i32;
            }
            __llvm_profile_write_file();
        }
        if !matches!(std::env::var("SHOP_EXIT_RELEASE").as_deref(), Ok("0") | Ok("false")) {
            let threads = std::env::var("NCPU")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from))
                .min(64);
            release_parallel(threads);
        }
        unsafe { _exit(0) }
    }
}

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOCATOR: linux_sys::HugeBlocks = linux_sys::HugeBlocks;

/// Slab API for scatter sinks and large tables (owned by the allocator; callers must not edit
/// sys.rs). Returns `(ptr, len)`: `len >= bytes`, rounded up to 2 MiB; `ptr` is 2 MiB aligned,
/// `MADV_HUGEPAGE`, zero-filled and pre-written (no first-touch zero-page faults later), and it
/// is never unmapped during the run (`_exit` reclaims it). Null on mmap failure.
#[allow(dead_code)] // consumed by P3b/P3c sinks
pub fn alloc_slab(bytes: usize) -> (*mut u8, usize) {
    #[cfg(target_os = "linux")]
    {
        linux_sys::alloc_slab(bytes)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let len = (bytes.max(1) + (2 << 20) - 1) & !((2 << 20) - 1);
        let layout = std::alloc::Layout::from_size_align(len, 2 << 20).unwrap();
        (unsafe { std::alloc::alloc_zeroed(layout) }, len)
    }
}

/// Early writeback of Parquet pages after each row group, unless SHOP_WRITEBACK=0.
pub fn writeback_enabled() -> bool {
    !matches!(std::env::var("SHOP_WRITEBACK").as_deref(), Ok("0") | Ok("false"))
}

/// Starts asynchronous writeback of `f`'s dirty pages (SYNC_FILE_RANGE_WRITE; does not wait).
pub fn start_writeback(f: &File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe extern "C" {
            fn sync_file_range(fd: i32, offset: i64, nbytes: i64, flags: u32) -> i32;
        }
        unsafe {
            sync_file_range(f.as_raw_fd(), 0, 0, 2);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = f;
}

/// Experiment E1 (off by default): SHOP_PAD_MB=N leaves N MiB of dirty non-zero bytes in
/// $SCRATCH_DIR/pad.bin so a scorer run shows whether unflushed output bytes cost wall time.
pub fn maybe_write_pad() {
    let mb = std::env::var("SHOP_PAD_MB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    if mb == 0 {
        return;
    }
    let dir = std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into());
    if let Ok(mut f) = File::create(std::path::Path::new(&dir).join("pad.bin")) {
        use std::io::Write;
        let block = vec![0xA5u8; 1 << 20];
        for _ in 0..mb {
            if f.write_all(&block).is_err() {
                break;
            }
        }
    }
}

#[inline]
pub fn fast_exit() -> ! {
    #[cfg(target_os = "linux")]
    {
        linux_sys::exit_fast();
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::process::exit(0);
    }
}

#[inline]
pub fn read_at_all(file: &File, mut offset: u64, mut dst: &mut [u8]) -> io::Result<usize> {
    #[cfg(unix)]
    use std::os::unix::fs::FileExt;
    #[cfg(windows)]
    use std::os::windows::fs::FileExt;

    let mut total = 0usize;
    while !dst.is_empty() {
        #[cfg(unix)]
        let res = file.read_at(dst, offset);
        #[cfg(windows)]
        let res = file.seek_read(dst, offset);

        match res {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                offset += n as u64;
                dst = &mut dst[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

pub struct OpenCsvFile {
    pub file: File,
    pub len: u64,
    pub data_start: u64,
}

pub struct FileChunk {
    pub file_idx: usize,
    pub c_start: u64,
    pub c_end: u64,
}

/// Opens CSV files, locates end of line 1 (`data_start`), sorts largest-first, and splits into chunks.
pub fn plan_file_chunks(paths: &[PathBuf], target_chunk_bytes: usize) -> io::Result<(Vec<OpenCsvFile>, Vec<FileChunk>)> {
    let mut meta: Vec<(u64, File, u64)> = Vec::with_capacity(paths.len());
    let mut probe = [0u8; 4096];

    for p in paths {
        let file = File::open(p)?;
        let len = file.metadata()?.len();
        if len == 0 {
            continue;
        }
        // Locate first '\n' (header row ending)
        let mut pos = 0u64;
        let mut data_start = len;
        'find_hdr: while pos < len {
            let n = read_at_all(&file, pos, &mut probe)?;
            if n == 0 {
                break;
            }
            if let Some(rel) = probe[..n].iter().position(|&b| b == b'\n') {
                data_start = pos + rel as u64 + 1;
                break 'find_hdr;
            }
            pos += n as u64;
        }
        if data_start < len {
            meta.push((len, file, data_start));
        }
    }

    // Schedule largest files first to minimize tail stragglers across worker threads.
    meta.sort_unstable_by(|a, b| b.0.cmp(&a.0));

    let chunk_sz = (target_chunk_bytes.max(1)) as u64;
    let mut files = Vec::with_capacity(meta.len());
    let mut chunks = Vec::new();
    for (fidx, (len, file, data_start)) in meta.into_iter().enumerate() {
        files.push(OpenCsvFile {
            file,
            len,
            data_start,
        });
        let mut c_start = data_start;
        while c_start < len {
            let c_end = (c_start + chunk_sz).min(len);
            chunks.push(FileChunk {
                file_idx: fidx,
                c_start,
                c_end,
            });
            c_start = c_end;
        }
    }

    Ok((files, chunks))
}

/// Reads `[c_start ..]` aligned cleanly to line boundaries with >=64 trailing zeroed bytes.
/// Returns `(first_line_off_in_buf, own_end_in_buf, scan_len)` so every line with `line_start in [c_start, c_end)`
/// is terminated by `\n` inside `buf[first_line_off..scan_len]`.
pub fn load_chunk_lines(f: &OpenCsvFile, chunk: &FileChunk, buf: &mut Vec<u8>) -> io::Result<(usize, usize, usize)> {
    // TEST_SLOP (compile-time, tests only) shrinks the tail read so lines straddling the slop are exercised.
    const SLOP: usize = match option_env!("TEST_SLOP") {
        Some(_) => 16,
        // Lines are ~100 B; a line that outruns the slop is handled by the tail-extension path.
        // 4 KiB keeps a 256 KiB chunk's re-read at ~1.6% and the buffer L2-resident.
        None => 4096,
    };
    const SIMD_PAD: usize = 64;

    let read_start = if chunk.c_start > f.data_start {
        chunk.c_start - 1
    } else {
        chunk.c_start
    };
    let initial_end = (chunk.c_end + SLOP as u64).min(f.len);
    let need = (initial_end - read_start) as usize;

    // Avoid zeroing the entire chunk buffer on every iteration; only tail SIMD slack is zeroed below.
    if buf.len() < need + SIMD_PAD + 1 {
        buf.resize(need + SIMD_PAD + 1, 0);
    }
    let mut valid = read_at_all(&f.file, read_start, &mut buf[..need])?;

    let own_end = (chunk.c_end - read_start) as usize;

    // Ensure the last line owned by this chunk (starting < own_end) sees a '\n' or EOF.
    let mut scan_from = if chunk.c_start > f.data_start {
        if valid == 0 {
            return Ok((0, 0, 0));
        }
        if buf[0] == b'\n' {
            1usize
        } else {
            // Find first '\n' at index >= 1 (which is file offset >= c_start)
            match buf[1..valid].iter().position(|&b| b == b'\n') {
                Some(rel) => 1 + rel + 1,
                None => {
                    // Long line from previous chunk spans past valid; scan forward until '\n'
                    let mut cur_file_pos = read_start + valid as u64;
                    let mut tmp = [0u8; 65536];
                    let mut found_end = None;
                    while cur_file_pos < f.len {
                        let n = read_at_all(&f.file, cur_file_pos, &mut tmp)?;
                        if n == 0 {
                            break;
                        }
                        if let Some(rel) = tmp[..n].iter().position(|&b| b == b'\n') {
                            let next_start = cur_file_pos + rel as u64 + 1;
                            if next_start >= chunk.c_end {
                                return Ok((0, 0, 0));
                            }
                            let skip_to = (next_start - read_start) as usize;
                            let extra_end = (next_start + SLOP as u64).min(f.len);
                            let total_len = (extra_end - read_start) as usize;
                            if buf.len() < total_len + SIMD_PAD + 1 {
                                buf.resize(total_len + SIMD_PAD + 1, 0);
                            }
                            let tail_start_buf = skip_to;
                            let tail_in_tmp = n - (rel + 1);
                            buf[tail_start_buf..tail_start_buf + tail_in_tmp]
                                .copy_from_slice(&tmp[rel + 1..n]);
                            let more_off = cur_file_pos + n as u64;
                            let remain = total_len - (tail_start_buf + tail_in_tmp);
                            let r2 = if remain > 0 && more_off < f.len {
                                read_at_all(&f.file, more_off, &mut buf[tail_start_buf + tail_in_tmp..total_len])?
                            } else {
                                0
                            };
                            valid = tail_start_buf + tail_in_tmp + r2;
                            found_end = Some(skip_to);
                            break;
                        }
                        cur_file_pos += n as u64;
                    }
                    match found_end {
                        Some(s) => s,
                        None => return Ok((0, 0, 0)),
                    }
                }
            }
        }
    } else {
        0usize
    };

    if scan_from >= own_end || scan_from >= valid {
        return Ok((0, 0, 0));
    }

    // Ensure at least one '\n' exists at or after `own_end - 1` (unless we already reached EOF).
    let search_tail_from = own_end.saturating_sub(1).max(scan_from);
    if read_start + (valid as u64) < f.len && !buf[search_tail_from..valid].contains(&b'\n') {
        let mut ext = 65536usize;
        while read_start + (valid as u64) < f.len {
            let old_valid = valid;
            let new_len = ((old_valid + ext) as u64).min(f.len - read_start) as usize;
            if buf.len() < new_len + SIMD_PAD + 1 {
                buf.resize(new_len + SIMD_PAD + 1, 0);
            }
            let n = read_at_all(&f.file, read_start + old_valid as u64, &mut buf[old_valid..new_len])?;
            valid = old_valid + n;
            if n == 0 || buf[old_valid..valid].contains(&b'\n') {
                break;
            }
            ext = (ext * 2).min(4 * 1024 * 1024);
        }
    }

    let mut scan_len = valid;
    if read_start + (valid as u64) == f.len && valid > scan_from && buf[valid - 1] != b'\n' {
        if buf.len() < valid + 1 + SIMD_PAD {
            buf.resize(valid + 1 + SIMD_PAD, 0);
        }
        buf[valid] = b'\n';
        scan_len = valid + 1;
    } else if buf.len() < valid + SIMD_PAD {
        buf.resize(valid + SIMD_PAD, 0);
    }
    buf[scan_len..scan_len + SIMD_PAD].fill(0);

    if scan_from > 0 && buf[scan_from - 1] != b'\n' && chunk.c_start > f.data_start {
        scan_from = scan_from.min(valid);
    }
    Ok((scan_from, own_end, scan_len))
}
