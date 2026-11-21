# Shopping Sessions Buildathon — Team `goons` v5/v6 Engineering & Testing Plan

**Repository:** `https://github.com/Wamadx/Shopppppping_ssesh1`  
**Track:** Databricks — Shopping Sessions (`https://www.joinbuilders.org/buildathon/tracks/shopping-sessions`)  
**Current Best Submission (`v5`):** `1.36 s` (`1.355858154296875 s`, Receipt `BB-9941AEDF`, Commit `18bf8b7814ba457ffaee38dadf16f8a62892f3ac`) — **Rank #81 / 173** (`Entered in the final`).  
**Target for `v6`:** **`< 0.70 s`** (current `#1`: `67` at `0.59 s`; PySpark baseline: `172.00 s`).

---

## 1. Quick-Start for Teammate & AI Coding Agent

### A. Workspace Layout (Keep Input Data OUTSIDE Git Repo!)
Hard rule: container image must stay `<= 50 MB` (`busybox:1.37-musl`) and **no generated CSV/Parquet/zip data may ever be committed**. Keep test artifacts in a sibling folder `../shopping-sessions-work`:

```bash
git clone https://github.com/Wamadx/Shopppppping_ssesh1.git shopping-sessions
mkdir -p shopping-sessions-work && cd shopping-sessions-work
```

Download from `https://www.joinbuilders.org/buildathon/tracks/shopping-sessions`:
- `rules.md`
- `starter-polars.zip` (contains `fixtures/`, `scripts/run_fixtures.py`, `scripts/compare.py`)
- `dev-answer.zip`
- `generator.zip` (Rust source for `generate`)

Unzip them into `shopping-sessions-work/`, build `generator`, and create local benchmark datasets:

```bash
# Build official data generator
cd shopping-sessions-work/generator && cargo build --release && cd ..

# 1M dev dataset (exact reference answer is in dev-answer/)
./generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output dev

# 5M benchmark dataset (and create exact ground-truth via current verified v5 binary or starter-polars)
./generator/target/release/generate --seed 42 --events 5000000 --clicks 1250000 --output bench5m
```

*(Note: when benchmarking `dev`, `bench5m`, or `bench20m`, generate a baseline output folder `bench5m_out` using `18bf8b78` or `starter-polars` once so every new optimization can be checked against it with `compare.py`.)*

### B. Build, Correctness & Benchmark Commands

#### Linux / WSL2 / Docker (Closest to Cascade Lake Scorer)
```bash
cd shopping-sessions

# 1. Static musl check (matches Dockerfile target)
RUSTFLAGS="-C target-cpu=cascadelake -C target-feature=-avx512f" \
  cargo check --target x86_64-unknown-linux-musl
RUSTFLAGS="--cfg pgo_gen -C target-cpu=x86-64-v3 -C target-feature=+crt-static" \
  cargo check --target x86_64-unknown-linux-musl

# 2. Local release build (use x86-64-v3 or native on your CPU; Docker uses cascadelake,-avx512f)
RUSTFLAGS="-C target-cpu=native" cargo build --release --bin shopping-sessions

# 3. Official 36 edge-case fixtures (MUST output 36/36 fixtures passed)
cd ../shopping-sessions-work/starter-polars
python3 scripts/run_fixtures.py fixtures -- ../../shopping-sessions/target/release/shopping-sessions

# 4. Exactness on 1M dev & 5M bench5m (MUST output MATCH)
cd ..
rm -rf out_dev && ../shopping-sessions/target/release/shopping-sessions dev out_dev
python3 starter-polars/scripts/compare.py dev-answer out_dev

rm -rf out_5m && ../shopping-sessions/target/release/shopping-sessions bench5m out_5m
python3 starter-polars/scripts/compare.py bench5m_out out_5m

# 5. Stage timing breakdown
TIMING=1 ../shopping-sessions/target/release/shopping-sessions bench5m /tmp/out_t
```

#### Windows PowerShell
```powershell
cd shopping-sessions
$env:RUSTFLAGS="-C target-cpu=haswell"
cargo build --release --bin shopping-sessions
cd ..\shopping-sessions-work\starter-polars
python scripts/run_fixtures.py fixtures -- ..\..\shopping-sessions\target\release\shopping-sessions.exe
cd ..
..\shopping-sessions\target\release\shopping-sessions.exe dev out_dev
python starter-polars\scripts\compare.py dev-answer out_dev
```

---

## 2. CRITICAL: Repository Visibility & Submission Protocol

To prevent other teams from inspecting our source code or this `PLAN.md` during the hackathon:
1. Keep `Wamadx/Shopppppping_ssesh1` **`PRIVATE`** during all local work and benchmarking.
2. **Right before submitting** a new commit on the portal:
   ```bash
   gh repo edit Wamadx/Shopppppping_ssesh1 --visibility public --accept-visibility-change-consequences
   sleep 3   # wait ~2-3s for GitHub git backend to unlock pushes/clones
   git push origin main
   ```
3. Submit via `https://www.joinbuilders.org/buildathon/tracks/shopping-sessions` (practice quota: **2 submissions/hour/team**), and wait until portal status changes to **`Scored`**.
4. **Immediately after `Scored`**, flip back to **`PRIVATE`**:
   ```bash
   gh repo edit Wamadx/Shopppppping_ssesh1 --visibility private --accept-visibility-change-consequences
   ```
5. **MANDATORY BEFORE FINAL CUTOFF (`Mon Nov 23, 2026 12:00 AM GMT+8`):**  
   `rules.md` states: *"Keep your repository public, with every commit you submitted still in it, until results are published. An entry we cannot fetch or restore for the final scores nothing."*  
   You **must** leave the repo **`PUBLIC`** with all submitted commits intact before the deadline until final results are announced.

---

## 3. Score History & Current `v5` Architecture (`1.36 s`)

| Iteration | Commit | Scorer Time (20M hidden) | Leaderboard Rank | Key Architectural Leap |
|---|---|---|---|---|
| `v1` | `036d7e8e` | `6.03 s` (`6.0278 s`) | #127 / 146 | Baseline single-thread CSV / Vec pipeline |
| `v2` | `7af1bf72` | `4.36 s` (`4.3551 s`) | #124 / 153 | Sharding, inline keys, custom Parquet |
| `v3` | `06284426` | `2.02 s` (`2.0241 s`) | #103 / 160 | `HugeBlocks` (`MADV_HUGEPAGE`), `busybox:1.37-musl`, parallel `pread`, AVX2 SIMD CSV, `_exit(0)` |
| `v4` Hybrid | `14a3d15f` | `1.84 s` (`1.8384 s`) | #109 / 164 | Zero-rezero chunks, AoS `KeyMap` + 16-ahead prefetch, `DedupTable`, direct `p`/`c_` LUTs |
| **`v5` (Current)** | **`18bf8b78`** | **`1.36 s` (`1.3559 s`)** | **#81 / 173** | **2-pass LLVM PGO, 128b folded hash, branchless CSV unquote, L1 LUTs, shard prealloc** |

### What Is Already Built in `v5`
- **`Cargo.toml`:** `edition = "2024"`, **zero external dependencies**, `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = true`, `default-run = "shopping-sessions"`.
- **`Dockerfile`:** `rust:1.93-alpine3.21` (`musl-dev` + `rustup component add llvm-tools`) -> runs `src/bin/synth.rs` -> 2-pass LLVM PGO (`--cfg pgo_gen -C target-cpu=x86-64-v3` -> `NCPU=8` + `NCPU=4` -> `llvm-profdata merge` -> `-C target-cpu=cascadelake -C target-feature=-avx512f,+crt-static -Cprofile-use=...`, with automatic fallback) -> final **`busybox:1.37-musl`** container (`USER 1000:1000`).
- **`src/sys.rs`:**
  - `HugeBlocks` (`#[global_allocator]` on Linux): maps `>= 2 MiB` allocations via `2 MiB`-aligned anonymous `mmap` + `madvise(..., MADV_HUGEPAGE)` and bump-allocates `32 KiB..2 MiB` blocks from `8 MiB` hugepage arenas, bypassing slow `musl mallocng` locking.
  - `plan_file_chunks` / `load_chunk_lines`: largest-file-first `pread` chunks, only zeroing the 64B tail SIMD padding (`buf[scan_len..scan_len + SIMD_PAD].fill(0)`).
  - `#[inline(never)] exit_fast()`: invokes `__llvm_profile_write_file()` under `#[cfg(pgo_gen)]` then `libc::_exit(0)`.
- **`src/csv_ts.rs`:**
  - `Key(u64)`: BMI2 `_pext_u64` packs `1..=9` ASCII bytes 7-bit MSB-first so integer `<` == byte-wise lexicographical `<`; 32-sharded `RwLock<Interner>` fallback (`intern_slow` is cold/unused on generator IDs).
  - **1-multiply 128-bit folded `Key::hash64()`:** `(x as u128).wrapping_mul(0x9E37_79B9_7F4A_7C15)` folded `(lo ^ hi)` — swept all top 3 ranks against SplitMix64 in 10-round A/B testing (`bench5m`: `463.1 ms` median vs `485.6 ms` SplitMix).
  - `for_each_record_avx2<const N>`: single-pass 64B AVX2 mask scan + `_mm_prefetch(p.add(256))` + statically-unrolled single-block path (`fidx == 0 && line_commas == N - 1`) + branchless `validate_and_unquote` via `from_raw_parts`.
  - Split `DateCache` (`dc_ing` & `dc_ts`) + SWAR `HH:MM:SS` (`parse_hms_swar`) + SWAR 13-digit `F3` (`parse_8_digits_swar`).
- **`src/main.rs`:**
  - Shrink-fitted L1 LUTs: `ProductCatalog::fast_p` (`Vec<u8>`, ~50 KiB) & `CampaignCatalog::fast_c` (`Vec<u16>`, ~4 KiB) with `parse_dec_suffix` and authoritative range short-circuiting.
  - Pre-allocated shard vectors (`512` dedup shards in `events_io`, `256` person shards in Phase 2 & `clicks`).
  - `2 MiB` non-canonical bit-filter (`BITFILTER_BITS = 1 << 24`) skipping ~59% of `LinkTable` probes, single-cache-line AoS `KeyMap<V>`, single-array `DedupTable`, in-place `WorkerMatrix`, LPT heaviest-first shard scheduling.
  - **Combinatorially Locked Defaults (`bench5m` 10-round winner: `463.1 ms` median / `382.9 ms` min):**
    - `SHOP_CHUNK_KB=1024`, `SHOP_PREFETCH=0`, `SHOP_BITFILTER=1`, `SHOP_SORTLESS=0`.
- **`src/parquet.rs`:** Direct 6-column Parquet V1 PLAIN writer (`UserDailyWriter`) + `write_campaign_daily`.

### Combinatorial Benchmark Results Measured on `bench5m` (5M Events, Folded Hash A/B)
| Rank | Hash | `SHOP_CHUNK_KB` | `SHOP_PREFETCH` | `SHOP_BITFILTER` | `SHOP_SORTLESS` | Median (ms) | Min (ms) | `links` | `ev_io` | `dedup` | `clicks` | `sess_pq` |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **1** | **folded 128b** | **1024** | **0** | **1** | **0** | **463.1** | **382.9** | 45.7 | 147.8 | 136.9 | 24.7 | 89.7 |
| 2 | folded 128b | 512 | 1 | 1 | 0 | 470.9 | 403.8 | 45.1 | 153.2 | 140.6 | 23.4 | 85.7 |
| 3 | folded 128b | 512 | 0 | 1 | 0 | 474.8 | 391.1 | 45.5 | 156.8 | 144.8 | 23.6 | 85.2 |
| 4 | splitmix64 | 512 | 1 | 1 | 0 | 477.7 | 401.0 | 46.0 | 156.9 | 147.5 | 24.8 | 87.8 |
| 5 | splitmix64 | 1024 | 0 | 1 | 0 | 485.6 | 401.1 | 46.5 | 149.7 | 154.6 | 25.8 | 87.0 |
| 6 | splitmix64 | 512 | 0 | 1 | 0 | 488.9 | 405.8 | 47.1 | 160.0 | 151.5 | 24.6 | 88.8 |

---

## 4. Measured Dataset Reality (`bench5m` = 5M Events, ~4× for 20M Practice)
Before optimizing blindly, note what the actual generator/dataset produces (`rules.md` & `generator/src`):
- **Quoting rate:** **79.1%** of event lines, **59.0%** of click lines, and **36.0%** of link lines contain at least one `"` (`20%` per field -> `1 - 0.8^7 = 79.1%`). Only `20.9%` of event lines are quote-free, so `if q_cnt == 0` mispredicts ~21%/79%!
- **Line lengths (5M):** Events `414 MB` (`p50 = 77 B`, `p99 = 89 B`, `max = 100 B`), Clicks `61 MB`, Links **`4.7 MB` (`226k` lines)** — at 20M events, `links` is only ~19 MB!
- **ID shapes (`generator/src/text.rs:192-217`):** Every generated `e`/`u`/`k` ID uses base36 of a 40-bit integer -> **at most 9 chars**. All generated IDs fit inline in `Key(u64)`.
- **Links selectivity:** `303k` linked IDs -> `95.9k` persons -> `207k` non-canonical IDs at 5M (`~830k` at 20M). **38.1%** of events have a non-canonical `user_id`; our 2 MB (`2^24`-bit) single-hash bitfilter has ~4.8% FPR, cutting link table probes to ~41% (`-59%`).
- **Purchases & Campaigns:** ~3% of events are `purchase` -> `products.get()` only runs on ~3% of event rows, while `campaigns.get()` runs on 100% of clicks.

---

## 5. `v6` Optimization Roadmap (`1.36 s` -> `< 0.70 s`)

Based on top public Buildathon repos (`Vibhor7-7/shopping-sessions` [`0.82s`], `Jerryh127/fraun-shopping-sessions` [`0.84s`], `shanmukha-here/eagle-eye-shopping-sessions` [`0.94s`], `axhiao`, `ZheXie`, `Koodsu`, `enkyuan/polaris`) and Cascade Lake 4C/8T micro-architecture analysis:

### Experiment A — 2 MiB-Aligned `MADV_HUGEPAGE` Slab Pool + Non-Temporal `_mm256_stream_si256` Scatter (`-12% to -20%`)
**Bottleneck now:** `events_io` + `dedup_route` push into `512` `Vec<RawEvent>`s + `256` `Vec<PersonEvent>`s using ordinary cached stores (`RFO` cache-line read-before-write misses + `Vec` allocator pressure).
**Implementation rules (critical for correctness & speed):**
1. Carve 64-byte aligned fixed blocks (`struct Pool<T>`) out of `2 MiB`-aligned anonymous `MADV_HUGEPAGE` regions (`src/sys.rs`), and recycle slabs across phases (`Fraun` style) so page faults happen only once.
2. Stage `N = 8` records (`[T; 8]`, `#[repr(C, align(64))]`) per shard (`1024` or `512` shards × `128 B` or `256 B` staging buffer fits cleanly in Cascade Lake's `1 MiB` L2!).
3. **Never write NT without alignment & fence:** Destination slab chunk pointers **must** be 64B-aligned (`#GP` segfault if unaligned `vmovntdq`!), flush complete 64B cache lines with back-to-back `_mm256_stream_si256` stores, flush remainder `< 8` items with ordinary stores in `finish()`, and execute **`_mm_sfence()`** on every writing thread before it finishes/returns.
4. **Always A/B benchmark** `NT=1` vs `NT=0` on target Cascade Lake hardware (`bench5m` AND portal submission) because earlier naive staging *without* `_mm256_stream_si256` regressed (`822.9 ms` vs `675.1 ms`), whereas true `_mm256_stream_si256` eliminates RFO DRAM reads (`Vibhor exp 18: -12% total`).

### Experiment B — Copies-Stream (`16 B Copy` + `16 B Event`) + 2-Bit-Per-Slot Dedup Prefilter (`-20% to -30%`)
**Bottleneck now:** Moving `32 B` `RawEvent` (`~660 MB` @ 20M) in Phase 1 AND scattering `16 B` `PersonEvent` (`~290 MB`) in Phase 2 (`~950 MB` DRAM write+read), plus `1.25 MB`+ per-thread `DedupTable` + `kept` spilling 1 MiB L2 (shared by 2 HTs).
**Architecture (`Vibhor`-style dual stream + exactness guard):**
1. In Phase 1, emit:
   - A `16 B Copy { event_id: Key, ingest_rel_sec: u32, event_ref: u32 }` (dedup-sharded) for every valid-`eid` line.
   - A `16 B CompactEvent` (or `PersonEvent`, **person-sharded directly** using a 16-entry ring buffer to resolve links!) ONLY if the row passes field/window/late validation.
   - *Earlier Pitfall to Avoid:* Our earlier v4 split-stream failed (`733 ms` vs `528 ms`) because it did **unprefetched** link lookups directly in Phase 1 AND pushed into `1,280` dynamically growing `Vec`s (`allocator` + DRAM latency stalls)! With **Experiment A's slab pool + NT stores + a 16-ahead link prefetch ring buffer** + **2 MB bitfilter**, Phase-1 routing eliminates the entire second `PersonEvent` scatter!
2. In Phase 2 dedup, use a **2-bit-per-slot prefilter** (`4n` bytes vs `20n` -> `~80 KB` for 20k copies/shard, stays L1/L2 resident!):
   - Pass 1 marks `00 -> 01` (`seen once`) -> `10/11` (`seen >= 2`).
   - Pass 2 collects only copies hitting `seen >= 2` slots (~5–10%), `sort_unstable_by_key((event_id, ingest_ts))`, and marks all non-earliest duplicates' `event_ref` as `DROPPED` in place!
3. **Exactness Trap (`ingest_ts` Range):** Do NOT clamp `ingest_ts`! Because `ingest_ts` in `rules.md` is valid F1 whole seconds, store `sec - (window_start_sec - 2^31)` as `u32` (±68 years exact) plus an overflow side vector `(copy_idx, i64)` if out of 32-bit range.

### Experiment C — Branchless Vector 7-Column Validator (`_mm_insert_epi32` + `_mm256_permutevar8x32_epi32`) (`-5% to -10%`)
Because **`79.1%` of event lines contain quotes**, `if q_cnt == 0` in `validate_and_unquote` has 21%/79% branch mispredicts (~15–20 cycles each).
Replace with branchless AVX2 delimiter-window validation (`Vibhor reader.rs:95-125`):
- Flatten `, | \n` bitmasks into `delims: [u32; ...]` (with `delims[-1] = u32::MAX` and `>= 8` tail sentinels) + block quote prefix count (`line_quotes = quotes_before(nl) - quotes_before(start)`).
- Load 4-byte windows `(d-1, d, d+1, d+2)` around delimiters `0..7` with `_mm_insert_epi32` (requires >= 2B readable front slack & >= 64B tail slack + trailing `\n`), compare against `"` (`_mm256_cmpeq_epi8`), align closing quote via `_mm256_permutevar8x32_epi32(..., [1,2,3,4,5,6,7,7])`, compute `wrapped = (first == '"' && last == '"' && len >= 2)`, and check `(delims_in_line == 7) && (line_quotes == 2 * popcnt(wrapped))`.
- **Cascade Lake Warning:** Do **NOT** replace `_mm_insert_epi32` with `vpgatherdd` — Cascade Lake GDS/Downfall microcode makes gather several times slower. (Also test the 128-bit scalar `Q`/`C` alternative if port 5 saturates.)

### Experiment D — Two-Stage Thread-Local + Lock-Free Afforest `load_links` (`~45ms` -> `~12ms`)
Currently `load_links` parses CSVs in parallel (`pairs_per_worker`) and merges all edges sequentially into `UnionFind` on 1 thread.
- **Upgrade:** Each worker resolves strings into a **thread-local open-addressing map** and runs sequential Union-Find on its own intra-chunk edges first; then all 8 workers merge surviving cross-chunk edges concurrently using **lock-free Afforest path-halving DSU** (`AtomicU64` table with read-before-CAS to avoid cache invalidation storms on 3–6 shared-device whale hubs, then flatten deterministically to byte-wise minimum `Key`).
- **Thread-Count Rule:** Never spawn more than `ncpu` (`8`) active threads concurrently (run `links` as tasks inside the same 8-worker pool, never alongside 8 busy event workers) so Linux CFS `cpu.max` doesn't throttle. (Note: on 4C/8T Cascade Lake, 8 SMT threads beat 4 threads by ~21% for instruction/latency phases.)

### Experiment E — Zero-Copy `write_vectored` (`writev`) Parquet & Re-Benchmark Sortless Sessionization (`SHOP_SORTLESS=1`)
- Emit Parquet chunks/row-groups via `std::io::IoSlice` (`write_vectored`, `<= 1024` slices) instead of copying 6 contiguous column buffers into one staging `Vec`.
- Re-benchmark `SHOP_SORTLESS=1` (30-min or `17`-min two-pass binned counting sort) once NT hugepage slabs (`Experiment A`) and 16B events (`Experiment B`) land — `SHOP_SORTLESS=1` already hit `82.3–86.4 ms` on `sessions_pq` in `bench5m`.

---

## 6. Combinatorial Testing Discipline
Whenever testing two or more independent optimizations or knobs (`NT`, `BITFILTER`, `SORTLESS`, `CHUNK_KB`, `16B_STREAM`, `VEC_QUOTE`), run an **interleaved multi-round matrix benchmark** on `bench5m` (and `bench20m` if available on Linux):
- Verify **`36/36 fixtures passed`** and **`compare.py MATCH`** on every variant before trusting timing numbers.
- Alternate run order (forward/reverse across rounds) with warmup runs, rank by median runtime, and lock the proven fastest combination in `Config::from_env()` before pushing for a practice submission.
