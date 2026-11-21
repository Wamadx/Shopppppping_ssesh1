mod csv_ts;
mod parquet;

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::thread;

use csv_ts::{SmallStr, parse_fields, parse_price_cents, parse_ts};
use parquet::{CampaignDailyRow, UserDailyRow, write_campaign_daily, write_user_daily};

const DAY_MS: i64 = 86_400_000;
const SEVEN_DAYS_MS: i64 = 7 * DAY_MS;
const SESSION_GAP_MS: i64 = 30 * 60 * 1_000;
const NUM_SHARDS: usize = 128;
const SHARD_MASK: usize = NUM_SHARDS - 1;

// Open-addressed hash map from SmallStr to u32 / u16 / SmallStr
struct SmallMap<V: Copy + Default> {
    keys: Vec<u128>,
    vals: Vec<V>,
    occupied: Vec<bool>,
    mask: usize,
}

impl<V: Copy + Default> SmallMap<V> {
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
    fn insert(&mut self, key: SmallStr, val: V) {
        let k = key.as_u128();
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
    fn get(&self, key: SmallStr) -> Option<V> {
        let k = key.as_u128();
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

struct DedupTable {
    keys: Vec<u128>,
    vals: Vec<u32>,
    stamps: Vec<u32>,
    epoch: u32,
    count: usize,
    mask: usize,
}

impl DedupTable {
    fn new(initial_pow2: usize) -> Self {
        let cap = initial_pow2.next_power_of_two().max(1024);
        Self {
            keys: vec![0; cap],
            vals: vec![0; cap],
            stamps: vec![0; cap],
            epoch: 1,
            count: 0,
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
        self.count = 0;
    }

    #[inline]
    fn get_or_insert(&mut self, key: SmallStr, new_val: u32) -> Option<u32> {
        let k = key.as_u128();
        let mut idx = (key.hash64() as usize) & self.mask;
        loop {
            if self.stamps[idx] != self.epoch {
                self.stamps[idx] = self.epoch;
                self.keys[idx] = k;
                self.vals[idx] = new_val;
                self.count += 1;
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

fn load_products(path: &Path) -> io::Result<(SmallMap<u16>, Vec<Box<[u8]>>, u16)> {
    let data = fs::read(path)?;
    let mut pairs: Vec<(SmallStr, Vec<u8>)> = Vec::new();
    let mut cat_set: HashMap<Vec<u8>, ()> = HashMap::new();
    cat_set.insert(b"unknown".to_vec(), ());

    for_each_line(skip_header(&data), |line| {
        if let Some([pid, cat]) = parse_fields::<2>(line) {
            if !pid.is_empty() {
                let cvec = cat.to_vec();
                cat_set.entry(cvec.clone()).or_insert(());
                pairs.push((SmallStr::from_bytes(pid), cvec));
            }
        }
    });

    let mut categories: Vec<Box<[u8]>> = cat_set.into_keys().map(|v| v.into_boxed_slice()).collect();
    categories.sort();
    assert!(categories.len() < 32768);

    let mut cat_to_id: HashMap<&[u8], u16> = HashMap::with_capacity(categories.len());
    for (i, c) in categories.iter().enumerate() {
        cat_to_id.insert(c, i as u16);
    }
    let unknown_id = *cat_to_id.get(b"unknown".as_slice()).unwrap();

    let mut map = SmallMap::with_capacity(pairs.len().max(16));
    for (pid, cvec) in pairs {
        let cid = cat_to_id[cvec.as_slice()];
        map.insert(pid, cid);
    }
    Ok((map, categories, unknown_id))
}

fn load_campaigns(path: &Path) -> io::Result<(SmallMap<u16>, Vec<(SmallStr, SmallStr)>)> {
    let data = fs::read(path)?;
    let mut list: Vec<(SmallStr, SmallStr)> = Vec::new();
    for_each_line(skip_header(&data), |line| {
        if let Some([cid, ch]) = parse_fields::<2>(line) {
            if !cid.is_empty() {
                list.push((SmallStr::from_bytes(cid), SmallStr::from_bytes(ch)));
            }
        }
    });
    let mut map = SmallMap::with_capacity(list.len().max(16));
    for (i, &(cid, _)) in list.iter().enumerate() {
        map.insert(cid, i as u16);
    }
    // Append organic at the end
    list.push((SmallStr::from_bytes(b"organic"), SmallStr::from_bytes(b"organic")));
    Ok((map, list))
}

struct UnionFind {
    id_map: HashMap<u128, u32>,
    ids: Vec<SmallStr>,
    parent: Vec<u32>,
    min_id: Vec<SmallStr>,
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

    fn intern(&mut self, s: SmallStr) -> u32 {
        let k = s.as_u128();
        if let Some(&idx) = self.id_map.get(&k) {
            return idx;
        }
        let idx = self.ids.len() as u32;
        self.id_map.insert(k, idx);
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

fn load_links(dir: &Path) -> io::Result<SmallMap<SmallStr>> {
    let files = list_csvs(dir)?;
    let mut uf = UnionFind::new();

    let mut buf = Vec::new();
    for path in files {
        buf.clear();
        File::open(path)?.read_to_end(&mut buf)?;
        for_each_line(skip_header(&buf), |line| {
            if let Some([a, b]) = parse_fields::<2>(line) {
                if !a.is_empty() && !b.is_empty() {
                    let ia = uf.intern(SmallStr::from_bytes(a));
                    let ib = uf.intern(SmallStr::from_bytes(b));
                    uf.union(ia, ib);
                }
            }
        });
    }

    let mut link_map = SmallMap::with_capacity(uf.ids.len().max(16));
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
    event_id: SmallStr,
    user_id: SmallStr,
    ingest_ts: i64,
    ts: i64, // i64::MIN means invalid (kept for dedup only)
    price_cents: i32,
    cat_and_purchase: u16, // bit 15 = is_purchase, bits 0..15 = cat_id
}

#[derive(Clone, Copy)]
struct PersonEvent {
    person_id: SmallStr,
    ts: i64,
    price_cents: i32,
    cat_and_purchase: u16,
}

#[derive(Clone, Copy)]
struct PersonClick {
    person_id: SmallStr,
    click_id: SmallStr,
    ts: i64,
    campaign_idx: u16,
}

#[derive(Clone, Copy, Default)]
struct CampaignAgg {
    sessions: i64,
    purchasing_sessions: i64,
    revenue_cents: i64,
    assisted_sessions: i64,
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
    window_start: i64,
    window_end: i64,
    products: &SmallMap<u16>,
    unknown_cat_id: u16,
) -> io::Result<Vec<Vec<RawEvent>>> {
    let mut event_shards: Vec<Vec<RawEvent>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
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

        let next_chunk = std::sync::atomic::AtomicUsize::new(0);
        let worker_shards: Vec<Vec<Vec<RawEvent>>> = thread::scope(|s| {
            let mut handles = Vec::with_capacity(ncpu);
            for _ in 0..ncpu {
                handles.push(s.spawn(|| {
                    let mut local: Vec<Vec<RawEvent>> =
                        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                    loop {
                        let cidx = next_chunk.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                            let Some(ingest_ts) = parse_ts(ing_b) else {
                                return;
                            };

                            let mut valid_ts = i64::MIN;
                            let mut price_cents = 0i32;
                            let mut cat_and_purchase = 0u16;

                            if !uid.is_empty() && !pid.is_empty() {
                                if let Some(ts) = parse_ts(ts_b) {
                                    if ts >= window_start
                                        && ts < window_end
                                        && ingest_ts - ts <= DAY_MS
                                    {
                                        match etype {
                                            b"view" | b"cart" => {
                                                valid_ts = ts;
                                            }
                                            b"purchase" => {
                                                if let Some(p) = parse_price_cents(price_b) {
                                                    valid_ts = ts;
                                                    price_cents = p;
                                                    let cid = products
                                                        .get(SmallStr::from_bytes(pid))
                                                        .unwrap_or(unknown_cat_id);
                                                    cat_and_purchase = 0x8000 | cid;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            let event_id = SmallStr::from_bytes(eid);
                            let user_id = if valid_ts != i64::MIN {
                                SmallStr::from_bytes(uid)
                            } else {
                                SmallStr::EMPTY
                            };
                            let shard = (event_id.hash64() as usize) & SHARD_MASK;
                            local[shard].push(RawEvent {
                                event_id,
                                user_id,
                                ingest_ts,
                                ts: valid_ts,
                                price_cents,
                                cat_and_purchase,
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
    click_window_start: i64,
    window_end: i64,
    campaigns: &SmallMap<u16>,
    links: &SmallMap<SmallStr>,
) -> io::Result<Vec<Vec<PersonClick>>> {
    let mut click_shards: Vec<Vec<PersonClick>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
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

        let next_chunk = std::sync::atomic::AtomicUsize::new(0);
        let worker_shards: Vec<Vec<Vec<PersonClick>>> = thread::scope(|s| {
            let mut handles = Vec::with_capacity(ncpu);
            for _ in 0..ncpu {
                handles.push(s.spawn(|| {
                    let mut local: Vec<Vec<PersonClick>> =
                        (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                    loop {
                        let cidx = next_chunk.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
                            let Some(campaign_idx) = campaigns.get(SmallStr::from_bytes(camp_b))
                            else {
                                return;
                            };
                            let Some(ts) = parse_ts(ts_b) else {
                                return;
                            };
                            if ts < click_window_start || ts >= window_end {
                                return;
                            }
                            let raw_uid = SmallStr::from_bytes(uid);
                            let person_id = links.get(raw_uid).unwrap_or(raw_uid);
                            let click_id = SmallStr::from_bytes(clid);
                            let shard = (person_id.hash64() as usize) & SHARD_MASK;
                            local[shard].push(PersonClick {
                                person_id,
                                click_id,
                                ts,
                                campaign_idx,
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
    let click_window_start = window_start - SEVEN_DAYS_MS;
    let min_day = window_start.div_euclid(DAY_MS) as i32;
    let max_day = (window_end - 1).div_euclid(DAY_MS) as i32;
    let num_days = (max_day - min_day + 1).max(1) as usize;

    let (products, categories, unknown_cat_id) = load_products(&data_dir.join("products.csv"))?;
    let (campaigns, campaign_list) = load_campaigns(&data_dir.join("campaigns.csv"))?;
    let organic_idx = (campaign_list.len() - 1) as u16;
    let links = load_links(&data_dir.join("links"))?;

    let event_files = list_csvs(&data_dir.join("events"))?;
    let event_shards = read_event_batches(
        &event_files,
        ncpu,
        window_start,
        window_end,
        &products,
        unknown_cat_id,
    )?;

    // Dedup events by event_id across the 128 event_shards in parallel, then route to person_shards
    let next_shard = std::sync::atomic::AtomicUsize::new(0);
    let mut person_event_shards: Vec<Vec<PersonEvent>> =
        (0..NUM_SHARDS).map(|_| Vec::new()).collect();

    let shard_results: Vec<Vec<Vec<PersonEvent>>> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for _ in 0..ncpu {
            handles.push(s.spawn(|| {
                let mut table = DedupTable::new(1 << 14);
                let mut kept: Vec<RawEvent> = Vec::new();
                let mut local_person: Vec<Vec<PersonEvent>> =
                    (0..NUM_SHARDS).map(|_| Vec::new()).collect();
                loop {
                    let sh = next_shard.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if sh >= NUM_SHARDS {
                        break;
                    }
                    let raw = unsafe {
                        // Safe: each sh in 0..NUM_SHARDS is claimed by at most one thread
                        &mut *(event_shards.as_ptr().add(sh) as *mut Vec<RawEvent>)
                    };
                    if raw.is_empty() {
                        continue;
                    }
                    table.reset(raw.len());
                    kept.clear();
                    for &ev in raw.iter() {
                        if let Some(pos) = table.get_or_insert(ev.event_id, kept.len() as u32) {
                            let cur = &mut kept[pos as usize];
                            if ev.ingest_ts < cur.ingest_ts {
                                *cur = ev;
                            }
                        } else {
                            kept.push(ev);
                        }
                    }
                    // Free raw shard memory immediately
                    *raw = Vec::new();
                    for &ev in &kept {
                        if ev.ts != i64::MIN {
                            let person_id = links.get(ev.user_id).unwrap_or(ev.user_id);
                            let psh = (person_id.hash64() as usize) & SHARD_MASK;
                            local_person[psh].push(PersonEvent {
                                person_id,
                                ts: ev.ts,
                                price_cents: ev.price_cents,
                                cat_and_purchase: ev.cat_and_purchase,
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
        click_window_start,
        window_end,
        &campaigns,
        &links,
    )?;

    // Phase 3: process each person_shard in parallel -> UserDailyRow & thread-local CampaignAgg
    let next_person_shard = std::sync::atomic::AtomicUsize::new(0);
    let num_campaigns = campaign_list.len();
    let num_cats = categories.len();

    let results: Vec<(Vec<UserDailyRow>, Vec<CampaignAgg>)> = thread::scope(|s| {
        let mut handles = Vec::with_capacity(ncpu);
        for _ in 0..ncpu {
            handles.push(s.spawn(|| {
                let mut user_rows = Vec::new();
                let mut camp_agg = vec![CampaignAgg::default(); num_campaigns * num_days];
                let mut cat_rev = vec![0i64; num_cats];
                let mut dirty_cats: Vec<u16> = Vec::with_capacity(16);
                let mut seen_assist = vec![0u32; num_campaigns];
                let mut assist_gen = 1u32;

                loop {
                    let sh = next_person_shard.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if sh >= NUM_SHARDS {
                        break;
                    }
                    let evs = unsafe {
                        &mut *(person_event_shards.as_ptr().add(sh) as *mut Vec<PersonEvent>)
                    };
                    if evs.is_empty() {
                        let clks = unsafe {
                            &mut *(person_click_shards.as_ptr().add(sh) as *mut Vec<PersonClick>)
                        };
                        *clks = Vec::new();
                        continue;
                    }
                    let clks = unsafe {
                        &mut *(person_click_shards.as_ptr().add(sh) as *mut Vec<PersonClick>)
                    };

                    evs.sort_unstable_by(|a, b| {
                        a.person_id
                            .as_u128()
                            .cmp(&b.person_id.as_u128())
                            .then_with(|| a.ts.cmp(&b.ts))
                    });
                    if !clks.is_empty() {
                        clks.sort_unstable_by(|a, b| {
                            a.person_id
                                .as_u128()
                                .cmp(&b.person_id.as_u128())
                                .then_with(|| a.click_id.as_u128().cmp(&b.click_id.as_u128()))
                        });
                        clks.dedup_by(|a, b| {
                            a.person_id == b.person_id && a.click_id == b.click_id
                        });
                    }

                    let mut ei = 0usize;
                    let mut ci = 0usize;
                    while ei < evs.len() {
                        let pid = evs[ei].person_id;
                        let pid_k = pid.as_u128();
                        let mut ee = ei + 1;
                        while ee < evs.len() && evs[ee].person_id.as_u128() == pid_k {
                            ee += 1;
                        }

                        while ci < clks.len() && clks[ci].person_id.as_u128() < pid_k {
                            ci += 1;
                        }
                        let c_start = ci;
                        while ci < clks.len() && clks[ci].person_id.as_u128() == pid_k {
                            ci += 1;
                        }
                        let person_clks = &mut clks[c_start..ci];
                        if person_clks.len() > 1 {
                            person_clks.sort_unstable_by(|a, b| {
                                a.ts.cmp(&b.ts)
                                    .then_with(|| b.click_id.cmp_bytes(&a.click_id))
                            });
                        }

                        let mut c_lo = 0usize;
                        let mut c_hi = 0usize;

                        let mut cur_day = i32::MIN;
                        let mut day_sessions = 0i64;
                        let mut day_events = 0i64;
                        let mut day_rev = 0i64;

                        let mut idx = ei;
                        while idx < ee {
                            let session_start = evs[idx].ts;
                            let mut last_ts = session_start;
                            let mut sess_events = 1i64;
                            let mut sess_rev = 0i64;
                            let mut sess_has_purchase = false;

                            let session_day = session_start.div_euclid(DAY_MS) as i32;
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

                            if (evs[idx].cat_and_purchase & 0x8000) != 0 {
                                let p = evs[idx].price_cents as i64;
                                let cid = evs[idx].cat_and_purchase & 0x7FFF;
                                sess_has_purchase = true;
                                sess_rev += p;
                                if cat_rev[cid as usize] == 0 {
                                    dirty_cats.push(cid);
                                }
                                cat_rev[cid as usize] += p;
                            }

                            idx += 1;
                            while idx < ee && evs[idx].ts - last_ts <= SESSION_GAP_MS {
                                let ev = &evs[idx];
                                last_ts = ev.ts;
                                sess_events += 1;
                                if (ev.cat_and_purchase & 0x8000) != 0 {
                                    let p = ev.price_cents as i64;
                                    let cid = ev.cat_and_purchase & 0x7FFF;
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
                            let min_click_ts = session_start - SEVEN_DAYS_MS;
                            while c_hi < person_clks.len() && person_clks[c_hi].ts <= session_start {
                                c_hi += 1;
                            }
                            while c_lo < c_hi && person_clks[c_lo].ts < min_click_ts {
                                c_lo += 1;
                            }

                            if c_lo == c_hi {
                                let cell = &mut camp_agg[organic_idx as usize * num_days + day_off];
                                cell.sessions += 1;
                                if sess_has_purchase {
                                    cell.purchasing_sessions += 1;
                                    cell.revenue_cents += sess_rev;
                                }
                            } else {
                                let cred_camp = person_clks[c_hi - 1].campaign_idx as usize;
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
                                for clk in &person_clks[c_lo..c_hi] {
                                    let ci_idx = clk.campaign_idx as usize;
                                    if seen_assist[ci_idx] != assist_gen {
                                        seen_assist[ci_idx] = assist_gen;
                                        camp_agg[ci_idx * num_days + day_off].assisted_sessions += 1;
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

                        ei = ee;
                    }

                    *evs = Vec::new();
                    *clks = Vec::new();
                }

                (user_rows, camp_agg)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut all_user_rows = Vec::new();
    let mut total_camp_agg = vec![CampaignAgg::default(); num_campaigns * num_days];
    for (mut urows, cagg) in results {
        all_user_rows.append(&mut urows);
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

    write_user_daily(&out_dir, &all_user_rows, &categories)?;
    write_campaign_daily(&out_dir, &campaign_rows, &campaign_list)?;

    Ok(())
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
