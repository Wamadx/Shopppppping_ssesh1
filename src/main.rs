mod csv_ts;
mod parquet;

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use csv_ts::{DateCache, Key, parse_fields, parse_price_cents, parse_ts};
use parquet::{CampaignDailyRow, UserDailyRow, write_campaign_daily, write_user_daily_part};

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const SEVEN_DAYS_REL: u32 = SEVEN_DAYS_MS as u32;
const SESSION_GAP_REL: u32 = (30 * 60 * 1_000) as u32;

const NUM_DEDUP_SHARDS: usize = 512;
const DEDUP_SHIFT: u32 = 55; // 64 - 9
const NUM_PERSON_SHARDS: usize = 256;
const PERSON_SHIFT: u32 = 56; // 64 - 8

#[inline(always)]
fn prefetch_ro<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

struct KeyMap<V: Copy + Default> {
    keys: Vec<u64>,
    vals: Vec<V>,
    occupied: Vec<bool>,
    mask: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    fn with_capacity(expected: usize) -> Self {
        let cap = (expected.saturating_mul(2).max(16)).next_power_of_two();
        Self {
            keys: vec![0; cap],
            vals: vec![V::default(); cap],
            occupied: vec![false; cap],
            mask: cap - 1,
        }
    }

    #[inline]
    fn insert(&mut self, key: Key, val: V) {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if !self.occupied[idx] {
                self.occupied[idx] = true;
                self.keys[idx] = k;
                self.vals[idx] = val;
                return;
            }
            if self.keys[idx] == k {
                self.vals[idx] = val;
                return;
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline]
    fn prefetch(&self, key: Key) {
        let idx = (key.hash64() as usize) & self.mask;
        prefetch_ro(self.keys.as_ptr().wrapping_add(idx));
        prefetch_ro(self.occupied.as_ptr().wrapping_add(idx));
    }

    #[inline]
    fn get(&self, key: Key) -> Option<V> {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if !self.occupied[idx] {
                return None;
            }
            if self.keys[idx] == k {
                return Some(self.vals[idx]);
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

/// Packed 8-byte slot dedup table (`(tag32 << 32) | idx32`) with generation stamps.
/// Indexed by low bits of `hash64` (independent of top 9 shard bits).
struct DedupTable {
    slots: Vec<u64>,
    stamps: Vec<u16>,
    epoch: u16,
    mask: usize,
}

impl DedupTable {
    fn new(initial_pow2: usize) -> Self {
        let cap = initial_pow2.next_power_of_two().max(1024);
        Self {
            slots: vec![0; cap],
            stamps: vec![0; cap],
            epoch: 1,
            mask: cap - 1,
        }
    }

    fn reset(&mut self, needed: usize) {
        if needed.saturating_mul(2) >= self.slots.len() {
            let new_cap = (needed.saturating_mul(2)).next_power_of_two();
            self.slots.resize(new_cap, 0);
            self.stamps.resize(new_cap, 0);
            self.mask = new_cap - 1;
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamps.fill(0);
            self.epoch = 1;
        }
    }

    #[inline]
    fn get_or_insert(&mut self, ev: RawEvent, kept: &[RawEvent]) -> Option<u32> {
        let h = ev.event_id.hash64();
        let tag = (h >> 23) & 0xFFFF_FFFF;
        let mut idx = (h as usize) & self.mask;
        loop {
            if self.stamps[idx] != self.epoch {
                self.stamps[idx] = self.epoch;
                self.slots[idx] = (tag << 32) | (kept.len() as u64);
                return None;
            }
            let slot_val = self.slots[idx];
            if (slot_val >> 32) == tag {
                let pos = slot_val as u32;
                if kept[pos as usize].event_id == ev.event_id {
                    return Some(pos);
                }
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

struct LocalPersonMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    stamps: Vec<u16>,
    epoch: u16,
    mask: usize,
}

impl LocalPersonMap {
    fn new(initial_pow2: usize) -> Self {
        let cap = initial_pow2.next_power_of_two().max(1024);
        Self {
            keys: vec![0; cap],
            vals: vec![0; cap],
            stamps: vec![0; cap],
            epoch: 1,
            mask: cap - 1,
        }
    }

    fn reset(&mut self, needed: usize) {
        if needed.saturating_mul(2) >= self.keys.len() {
            let new_cap = (needed.saturating_mul(2)).next_power_of_two();
            self.keys.resize(new_cap, 0);
            self.vals.resize(new_cap, 0);
            self.stamps.resize(new_cap, 0);
            self.mask = new_cap - 1;
        }
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamps.fill(0);
            self.epoch = 1;
        }
    }

    #[inline]
    fn get_or_insert(&mut self, key: Key, next_id: &mut u32) -> (u32, bool) {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if self.stamps[idx] != self.epoch {
                let id = *next_id;
                *next_id = id + 1;
                self.stamps[idx] = self.epoch;
                self.keys[idx] = k;
                self.vals[idx] = id;
                return (id, true);
            }
            if self.keys[idx] == k {
                return (self.vals[idx], false);
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline]
    fn get(&self, key: Key) -> Option<u32> {
        let k = key.0;
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if self.stamps[idx] != self.epoch {
                return None;
            }
            if self.keys[idx] == k {
                return Some(self.vals[idx]);
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

#[inline]
fn for_each_line(slice: &[u8], mut f: impl FnMut(&[u8])) {
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
fn skip_header(data: &[u8]) -> &[u8] {
    match data.iter().position(|&b| b == b'\n') {
        Some(pos) => &data[pos + 1..],
        None => &[],
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

fn load_products(path: &Path) -> io::Result<(KeyMap<u16>, Vec<Box<[u8]>>, u16)> {
    let data = fs::read(path)?;
    let mut pairs: Vec<(Key, Vec<u8>)> = Vec::new();
    let mut cat_set: HashMap<Vec<u8>, ()> = HashMap::new();
    cat_set.insert(b"unknown".to_vec(), ());

    for_each_line(skip_header(&data), |line| {
        if let Some([pid, cat]) = parse_fields::<2>(line) {
            if !pid.is_empty() {
                let cvec = cat.to_vec();
                cat_set.entry(cvec.clone()).or_insert(());
                pairs.push((Key::from_bytes(pid), cvec));
            }
        }
    });

    let mut categories: Vec<Box<[u8]>> = cat_set.into_keys().map(|v| v.into_boxed_slice()).collect();
    categories.sort();
    assert!(categories.len() <= 255);

    let mut cat_to_id: HashMap<&[u8], u16> = HashMap::with_capacity(categories.len());
    for (i, c) in categories.iter().enumerate() {
        cat_to_id.insert(c, i as u16);
    }
    let unknown_id = *cat_to_id.get(b"unknown".as_slice()).unwrap();

    let mut map = KeyMap::with_capacity(pairs.len().max(16));
    for (pid, cvec) in pairs {
        let cid = cat_to_id[cvec.as_slice()];
        map.insert(pid, cid);
    }
    Ok((map, categories, unknown_id))
}

fn load_campaigns(path: &Path) -> io::Result<(KeyMap<u16>, Vec<(Key, Key)>)> {
    let data = fs::read(path)?;
    let mut list: Vec<(Key, Key)> = Vec::new();
    for_each_line(skip_header(&data), |line| {
        if let Some([cid, ch]) = parse_fields::<2>(line) {
            if !cid.is_empty() {
                list.push((Key::from_bytes(cid), Key::from_bytes(ch)));
            }
        }
    });
    let mut map = KeyMap::with_capacity(list.len().max(16));
    for (i, &(cid, _)) in list.iter().enumerate() {
        map.insert(cid, i as u16);
    }
    list.push((Key::from_bytes(b"organic"), Key::from_bytes(b"organic")));
    Ok((map, list))
}

struct UnionFind {
    id_map: HashMap<u64, u32>,
    ids: Vec<Key>,
    parent: Vec<u32>,
    min_id: Vec<Key>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            id_map: HashMap::new(),
            ids: Vec::new(),
            parent: Vec::new(),
            min_id: Vec::new(),
        }
    }

    fn intern(&mut self, s: Key) -> u32 {
        if let Some(&idx) = self.id_map.get(&s.0) {
            return idx;
        }
        let idx = self.ids.len() as u32;
        self.id_map.insert(s.0, idx);
        self.ids.push(s);
        self.parent.push(idx);
        self.min_id.push(s);
        idx
    }

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

fn load_links(dir: &Path) -> io::Result<KeyMap<Key>> {
    let files = list_csvs(dir)?;
    let mut uf = UnionFind::new();
    let mut buf = Vec::new();
    for path in files {
        buf.clear();
        File::open(path)?.read_to_end(&mut buf)?;
        for_each_line(skip_header(&buf), |line| {
            if let Some([a, b]) = parse_fields::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    let ia = uf.intern(Key::from_bytes(a));
                    let ib = uf.intern(Key::from_bytes(b));
                    uf.union(ia, ib);
                }
            }
        });
    }

    let mut link_map = KeyMap::with_capacity(uf.ids.len().max(16));
    for idx in 0..uf.ids.len() {
        let id = uf.ids[idx];
        let root = uf.find(idx as u32);
        let min_id = uf.min_id[root as usize];
        if min_id != id {
            link_map.insert(id, min_id);
        }
    }
    Ok(link_map)
}

#[derive(Clone, Copy)]
struct RawEvent {
    event_id: Key,
    user_id: Key,
    ingest_ts: i64,
    ts_rel: u32,    // u32::MAX means invalid
    price_cat: u32, // 0 = non-purchase, else ((cat_id + 1) << 24) | price_cents
}

#[derive(Clone, Copy)]
struct PersonEvent {
    person_id: Key,
    packed_ev: u64, // ((ts_rel as u64) << 32) | (price_cat as u64)
}

#[derive(Clone, Copy)]
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

#[inline(always)]
fn resolve_link(uid: Key, links: &KeyMap<Key>, cache: &mut [(Key, Key); 1024]) -> Key {
    let slot = ((uid.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 54) as usize) & 1023;
    let entry = cache[slot];
    if entry.0 == uid {
        return entry.1;
    }
    let resolved = links.get(uid).unwrap_or(uid);
    cache[slot] = (uid, resolved);
    resolved
}

fn split_chunks(bufs: &[Vec<u8>], target_bytes: usize) -> Vec<(usize, usize, usize)> {
    let mut chunks = Vec::new();
    for (fidx, data) in bufs.iter().enumerate() {
        let Some(first_nl) = data.iter().position(|&b| b == b'\n') else {
            continue;
        };
        let mut pos = first_nl + 1;
        while pos < data.len() {
            let mut end = (pos + target_bytes).min(data.len());
            if end < data.len() {
                if let Some(rel) = data[end..].iter().position(|&b| b == b'\n') {
                    end += rel + 1;
                } else {
                    end = data.len();
                }
            }
            chunks.push((fidx, pos, end));
            pos = end;
        }
    }
    chunks
}

fn read_event_batches(
    files: &[PathBuf],
    ncpu: usize,
    base_ts: i64,
    window_start: i64,
    window_end: i64,
    products: &KeyMap<u16>,
    unknown_cat_id: u16,
) -> io::Result<Vec<Vec<RawEvent>>> {
    let mut event_shards: Vec<Vec<RawEvent>> = (0..NUM_DEDUP_SHARDS).map(|_| Vec::new()).collect();
    let mut file_idx = 0usize;
    const BATCH_MAX_BYTES: usize = 384 * 1024 * 1024;

    while file_idx < files.len() {
        let mut batch_bufs: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0usize;
        while file_idx < files.len() && (batch_bufs.is_empty() || batch_bytes < BATCH_MAX_BYTES) {
            let data = fs::read(&files[file_idx])?;
            batch_bytes += data.len();
            batch_bufs.push(data);
            file_idx += 1;
        }

        let chunks = split_chunks(&batch_bufs, 2 * 1024 * 1024);
        if chunks.is_empty() {
            continue;
        }

        let next_chunk = AtomicUsize::new(0);
        let worker_shards: Vec<Vec<Vec<RawEvent>>> = thread::scope(|s| {
            let mut handles = Vec::with_capacity(ncpu);
            for _ in 0..ncpu {
                handles.push(s.spawn(|| {
                    let mut local: Vec<Vec<RawEvent>> =
                        (0..NUM_DEDUP_SHARDS).map(|_| Vec::new()).collect();
                    let mut dc = DateCache::new();
                    loop {
                        let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        if cidx >= chunks.len() {
                            break;
                        }
                        let (fidx, start, end) = chunks[cidx];
                        let slice = &batch_bufs[fidx][start..end];
                        for_each_line(slice, |line| {
                            let Some([eid, uid, ts_b, ing_b, etype, pid, price_b]) =
                                parse_fields::<7>(line)
                            else {
                                return;
                            };
                            if eid.is_empty() {
                                return;
                            }
                            let Some(ingest_ts) = dc.parse_ts(ing_b) else {
                                return;
                            };

                            let mut ts_rel = u32::MAX;
                            let mut price_cat = 0u32;

                            if !uid.is_empty() && !pid.is_empty() {
                                if let Some(ts) = dc.parse_ts(ts_b) {
                                    if ts >= window_start
                                        && ts < window_end
                                        && ingest_ts - ts <= DAY_MS
                                    {
                                        match etype {
                                            b"view" | b"cart" => {
                                                ts_rel = (ts - base_ts) as u32;
                                            }
                                            b"purchase" => {
                                                if let Some(p) = parse_price_cents(price_b) {
                                                    ts_rel = (ts - base_ts) as u32;
                                                    let cid = products
                                                        .get(Key::from_bytes(pid))
                                                        .unwrap_or(unknown_cat_id);
                                                    price_cat = (((cid as u32) + 1) << 24)
                                                        | (p as u32);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            let event_id = Key::from_bytes(eid);
                            let user_id = if ts_rel != u32::MAX {
                                Key::from_bytes(uid)
                            } else {
                                Key::EMPTY
                            };
                            let shard = (event_id.hash64() >> DEDUP_SHIFT) as usize;
                            local[shard].push(RawEvent {
                                event_id,
                                user_id,
                                ingest_ts,
                                ts_rel,
                                price_cat,
                            });
                        });
                    }
                    local
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for ws in worker_shards {
            for (sh, mut vec) in ws.into_iter().enumerate() {
                if !vec.is_empty() {
                    if event_shards[sh].is_empty() {
                        event_shards[sh] = vec;
                    } else {
                        event_shards[sh].append(&mut vec);
                    }
                }
            }
        }
    }

    Ok(event_shards)
}

fn read_click_batches(
    files: &[PathBuf],
    ncpu: usize,
    base_ts: i64,
    window_end: i64,
    campaigns: &KeyMap<u16>,
    links: &KeyMap<Key>,
) -> io::Result<Vec<Vec<PersonClick>>> {
    let mut click_shards: Vec<Vec<PersonClick>> =
        (0..NUM_PERSON_SHARDS).map(|_| Vec::new()).collect();
    let mut file_idx = 0usize;
    const BATCH_MAX_BYTES: usize = 384 * 1024 * 1024;

    while file_idx < files.len() {
        let mut batch_bufs: Vec<Vec<u8>> = Vec::new();
        let mut batch_bytes = 0usize;
        while file_idx < files.len() && (batch_bufs.is_empty() || batch_bytes < BATCH_MAX_BYTES) {
            let data = fs::read(&files[file_idx])?;
            batch_bytes += data.len();
            batch_bufs.push(data);
            file_idx += 1;
        }

        let chunks = split_chunks(&batch_bufs, 2 * 1024 * 1024);
        if chunks.is_empty() {
            continue;
        }

        let next_chunk = AtomicUsize::new(0);
        let worker_shards: Vec<Vec<Vec<PersonClick>>> = thread::scope(|s| {
            let mut handles = Vec::with_capacity(ncpu);
            for _ in 0..ncpu {
                handles.push(s.spawn(|| {
                    let mut local: Vec<Vec<PersonClick>> =
                        (0..NUM_PERSON_SHARDS).map(|_| Vec::new()).collect();
                    let mut dc = DateCache::new();
                    let mut link_cache = [(Key::EMPTY, Key::EMPTY); 1024];
                    loop {
                        let cidx = next_chunk.fetch_add(1, Ordering::Relaxed);
                        if cidx >= chunks.len() {
                            break;
                        }
                        let (fidx, start, end) = chunks[cidx];
                        let slice = &batch_bufs[fidx][start..end];
                        for_each_line(slice, |line| {
                            let Some([clid, uid, camp_b, ts_b]) = parse_fields::<4>(line) else {
                                return;
                            };
                            if clid.is_empty() || uid.is_empty() {
                                return;
                            }
                            let Some(campaign_idx) = campaigns.get(Key::from_bytes(camp_b)) else {
                                return;
                            };
                            let Some(ts) = dc.parse_ts(ts_b) else {
                                return;
                            };
                            if ts < base_ts || ts >= window_end {
                                return;
                            }
                            let raw_uid = Key::from_bytes(uid);
                            let person_id = resolve_link(raw_uid, links, &mut link_cache);
                            let click_id = Key::from_bytes(clid);
                            let shard = (person_id.hash64() >> PERSON_SHIFT) as usize;
                            local[shard].push(PersonClick {
                                person_id,
                                click_id,
                                ts_rel: (ts - base_ts) as u32,
                                campaign_idx: campaign_idx as u32,
                            });
                        });
                    }
                    local
                }));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        for ws in worker_shards {
            for (sh, mut vec) in ws.into_iter().enumerate() {
                if !vec.is_empty() {
                    if click_shards[sh].is_empty() {
                        click_shards[sh] = vec;
                    } else {
                        click_shards[sh].append(&mut vec);
                    }
                }
            }
        }
    }

    Ok(click_shards)
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

fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let data_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())));
    let out_dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())));
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

    let (products, categories, unknown_cat_id) = load_products(&data_dir.join("products.csv"))?;
    let (campaigns, campaign_list) = load_campaigns(&data_dir.join("campaigns.csv"))?;
    let organic_idx = (campaign_list.len() - 1) as usize;
    let links = load_links(&data_dir.join("links"))?;

    let event_files = list_csvs(&data_dir.join("events"))?;
    let event_shards = read_event_batches(
        &event_files,
        ncpu,
        base_ts,
        window_start,
        window_end,
        &products,
        unknown_cat_id,
    )?;

    // Phase 2: Dedup events across 512 shards in parallel, resolve links with prefetch, scatter to 256 person shards
    let next_shard = AtomicUsize::new(0);
    let mut person_event_shards: Vec<Vec<PersonEvent>> =
        (0..NUM_PERSON_SHARDS).map(|_| Vec::new()).collect();

    let shard_results: Vec<Vec<Vec<PersonEvent>>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for _ in 0..ncpu {
            handles.push(s.spawn(|| {
                let mut table = DedupTable::new(1 << 14);
                let mut kept: Vec<RawEvent> = Vec::new();
                let mut local_person: Vec<Vec<PersonEvent>> =
                    (0..NUM_PERSON_SHARDS).map(|_| Vec::new()).collect();
                let mut link_cache = [(Key::EMPTY, Key::EMPTY); 1024];

                loop {
                    let sh = next_shard.fetch_add(1, Ordering::Relaxed);
                    if sh >= NUM_DEDUP_SHARDS {
                        break;
                    }
                    let raw = unsafe {
                        &mut *(event_shards.as_ptr().add(sh) as *mut Vec<RawEvent>)
                    };
                    if raw.is_empty() {
                        continue;
                    }
                    table.reset(raw.len());
                    kept.clear();
                    for &ev in raw.iter() {
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

                    let klen = kept.len();
                    for i in 0..klen {
                        if i + 16 < klen && kept[i + 16].ts_rel != u32::MAX {
                            links.prefetch(kept[i + 16].user_id);
                        }
                        let ev = kept[i];
                        if ev.ts_rel != u32::MAX {
                            let person_id = resolve_link(ev.user_id, &links, &mut link_cache);
                            let psh = (person_id.hash64() >> PERSON_SHIFT) as usize;
                            let packed_ev = ((ev.ts_rel as u64) << 32) | (ev.price_cat as u64);
                            local_person[psh].push(PersonEvent {
                                person_id,
                                packed_ev,
                            });
                        }
                    }
                }
                local_person
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(event_shards);

    for ws in shard_results {
        for (sh, mut vec) in ws.into_iter().enumerate() {
            if !vec.is_empty() {
                if person_event_shards[sh].is_empty() {
                    person_event_shards[sh] = vec;
                } else {
                    person_event_shards[sh].append(&mut vec);
                }
            }
        }
    }

    let click_files = list_csvs(&data_dir.join("clicks"))?;
    let person_click_shards = read_click_batches(
        &click_files,
        ncpu,
        base_ts,
        window_end,
        &campaigns,
        &links,
    )?;

    fs::create_dir_all(out_dir.join("user_daily"))?;

    // Phase 3: O(N) counting-sort per person shard, sessionize, attribute, write user_daily parts in parallel
    let next_person_shard = AtomicUsize::new(0);
    let num_campaigns = campaign_list.len();
    let num_cats = categories.len();

    let camp_results: Vec<Vec<CampaignAgg>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for tid in 0..ncpu {
            let out_dir_ref = &out_dir;
            let cats_ref = &categories;
            let pe_shards = &person_event_shards;
            let pc_shards = &person_click_shards;
            let next_ps_ref = &next_person_shard;
            handles.push(s.spawn(move || {
                let mut user_rows = Vec::new();
                let mut camp_agg = vec![CampaignAgg::default(); num_campaigns * num_days];
                let mut cat_rev = vec![0i64; num_cats];
                let mut dirty_cats: Vec<u16> = Vec::with_capacity(16);
                let mut seen_assist = vec![0u32; num_campaigns];
                let mut assist_gen = 1u32;

                let mut pmap = LocalPersonMap::new(1 << 14);
                let mut p_ids: Vec<Key> = Vec::new();
                let mut ev_counts: Vec<u32> = Vec::new();
                let mut ev_lpids: Vec<u32> = Vec::new();
                let mut ev_offs: Vec<u32> = Vec::new();
                let mut sorted_evs: Vec<u64> = Vec::new();

                let mut clk_counts: Vec<u32> = Vec::new();
                let mut clk_tmp: Vec<(u32, CompactClick)> = Vec::new();
                let mut clk_offs: Vec<u32> = Vec::new();
                let mut sorted_clks: Vec<CompactClick> = Vec::new();

                loop {
                    let sh = next_ps_ref.fetch_add(1, Ordering::Relaxed);
                    if sh >= NUM_PERSON_SHARDS {
                        break;
                    }
                    let evs = unsafe {
                        &mut *(pe_shards.as_ptr().add(sh) as *mut Vec<PersonEvent>)
                    };
                    let clks = unsafe {
                        &mut *(pc_shards.as_ptr().add(sh) as *mut Vec<PersonClick>)
                    };
                    if evs.is_empty() {
                        *clks = Vec::new();
                        continue;
                    }

                    pmap.reset(evs.len());
                    p_ids.clear();
                    ev_counts.clear();
                    ev_lpids.clear();
                    ev_lpids.reserve(evs.len());
                    let mut next_pid = 0u32;

                    for ev in evs.iter() {
                        let (lpid, is_new) = pmap.get_or_insert(ev.person_id, &mut next_pid);
                        if is_new {
                            p_ids.push(ev.person_id);
                            ev_counts.push(1);
                        } else {
                            ev_counts[lpid as usize] += 1;
                        }
                        ev_lpids.push(lpid);
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

                    sorted_evs.resize(evs.len(), 0);
                    for (ev, &lpid) in evs.iter().zip(ev_lpids.iter()) {
                        let dst = ev_counts[lpid as usize] as usize;
                        ev_counts[lpid as usize] = (dst + 1) as u32;
                        sorted_evs[dst] = ev.packed_ev;
                    }
                    *evs = Vec::new();

                    // Filter and counting-sort clicks only for people with events
                    clk_counts.clear();
                    clk_counts.resize(num_people, 0);
                    clk_tmp.clear();
                    for clk in clks.iter() {
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

                    // Process each person in the shard
                    for p in 0..num_people {
                        let pid = p_ids[p];
                        let pevs = &mut sorted_evs[ev_offs[p] as usize..ev_offs[p + 1] as usize];
                        if pevs.len() <= 24 {
                            insertion_sort_u64(pevs);
                        } else {
                            pevs.sort_unstable();
                        }

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

                        let mut cur_day = i32::MIN;
                        let mut day_sessions = 0i32;
                        let mut day_events = 0i32;
                        let mut day_rev = 0i64;

                        let mut idx = 0usize;
                        let ee = pevs.len();
                        while idx < ee {
                            let packed0 = pevs[idx];
                            let session_start_rel = (packed0 >> 32) as u32;
                            let mut last_ts_rel = session_start_rel;
                            let mut sess_events = 1i32;
                            let mut sess_rev = 0i64;
                            let mut sess_has_purchase = false;

                            let session_day =
                                (base_ts + (session_start_rel as i64)).div_euclid(DAY_MS) as i32;
                            if session_day != cur_day {
                                if cur_day != i32::MIN {
                                    let top_cat = finish_day(&mut cat_rev, &mut dirty_cats);
                                    user_rows.push(UserDailyRow {
                                        user_id: pid,
                                        day: cur_day,
                                        sessions: day_sessions,
                                        events: day_events,
                                        revenue_cents: day_rev,
                                        top_category: top_cat,
                                    });
                                }
                                cur_day = session_day;
                                day_sessions = 0;
                                day_events = 0;
                                day_rev = 0;
                            }

                            let pc0 = packed0 as u32;
                            if pc0 != 0 {
                                let p = (pc0 & 0x00FF_FFFF) as i64;
                                let cid = ((pc0 >> 24) - 1) as u16;
                                sess_has_purchase = true;
                                sess_rev += p;
                                if cat_rev[cid as usize] == 0 {
                                    dirty_cats.push(cid);
                                }
                                cat_rev[cid as usize] += p;
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
                                    let p = (pc & 0x00FF_FFFF) as i64;
                                    let cid = ((pc >> 24) - 1) as u16;
                                    sess_has_purchase = true;
                                    sess_rev += p;
                                    if cat_rev[cid as usize] == 0 {
                                        dirty_cats.push(cid);
                                    }
                                    cat_rev[cid as usize] += p;
                                }
                                idx += 1;
                            }

                            day_sessions += 1;
                            day_events += sess_events;
                            day_rev += sess_rev;

                            let day_off = (session_day - min_day) as usize;
                            let min_click_rel = session_start_rel - SEVEN_DAYS_REL;
                            while c_hi < pclks.len() && pclks[c_hi].ts_rel <= session_start_rel {
                                c_hi += 1;
                            }
                            while c_lo < c_hi && pclks[c_lo].ts_rel < min_click_rel {
                                c_lo += 1;
                            }

                            if c_lo == c_hi {
                                let cell = &mut camp_agg[organic_idx * num_days + day_off];
                                cell.sessions += 1;
                                if sess_has_purchase {
                                    cell.purchasing_sessions += 1;
                                    cell.revenue_cents += sess_rev;
                                }
                            } else {
                                let cred_camp = pclks[c_hi - 1].campaign_idx as usize;
                                let cell = &mut camp_agg[cred_camp * num_days + day_off];
                                cell.sessions += 1;
                                if sess_has_purchase {
                                    cell.purchasing_sessions += 1;
                                    cell.revenue_cents += sess_rev;
                                }

                                assist_gen = assist_gen.wrapping_add(1);
                                if assist_gen == 0 {
                                    seen_assist.fill(0);
                                    assist_gen = 1;
                                }
                                for clk in &pclks[c_lo..c_hi] {
                                    let ci_idx = clk.campaign_idx as usize;
                                    if seen_assist[ci_idx] != assist_gen {
                                        seen_assist[ci_idx] = assist_gen;
                                        camp_agg[ci_idx * num_days + day_off].assisted_sessions +=
                                            1;
                                    }
                                }
                            }
                        }

                        if cur_day != i32::MIN {
                            let top_cat = finish_day(&mut cat_rev, &mut dirty_cats);
                            user_rows.push(UserDailyRow {
                                user_id: pid,
                                day: cur_day,
                                sessions: day_sessions,
                                events: day_events,
                                revenue_cents: day_rev,
                                top_category: top_cat,
                            });
                        }
                    }
                }

                if !user_rows.is_empty() || tid == 0 {
                    write_user_daily_part(out_dir_ref, tid, &user_rows, cats_ref)
                        .expect("failed to write user_daily parquet");
                }
                camp_agg
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

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
    std::process::exit(0);
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
