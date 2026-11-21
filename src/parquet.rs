use std::cell::RefCell;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::csv_ts::Key;
use crate::snappy::Compressor;

pub struct CampaignDailyRow {
    pub campaign_id_idx: u16,
    pub day: i32,
    pub sessions: i32,
    pub purchasing_sessions: i32,
    pub revenue_cents: i64,
    pub assisted_sessions: i32,
}

struct ThriftBuf(Vec<u8>);

impl ThriftBuf {
    fn new() -> Self {
        Self(Vec::with_capacity(256))
    }

    #[inline]
    fn uvarint(&mut self, mut n: u64) {
        loop {
            let b = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                self.0.push(b);
                break;
            }
            self.0.push(b | 0x80);
        }
    }

    #[inline]
    fn zz32(&mut self, n: i32) {
        let u = ((n << 1) ^ (n >> 31)) as u32;
        self.uvarint(u as u64);
    }

    #[inline]
    fn zz64(&mut self, n: i64) {
        let u = ((n << 1) ^ (n >> 63)) as u64;
        self.uvarint(u);
    }

    #[inline]
    fn field_hdr(&mut self, last: &mut i16, id: i16, ty: u8) {
        let delta = id - *last;
        if delta > 0 && delta <= 15 {
            self.0.push(((delta as u8) << 4) | ty);
        } else {
            self.0.push(ty);
            self.zz32(id as i32);
        }
        *last = id;
    }

    #[inline]
    fn stop(&mut self) {
        self.0.push(0);
    }

    #[inline]
    fn i16_field(&mut self, last: &mut i16, id: i16, v: i16) {
        self.field_hdr(last, id, 4);
        self.zz32(v as i32);
    }

    #[inline]
    fn i32_field(&mut self, last: &mut i16, id: i16, v: i32) {
        self.field_hdr(last, id, 5);
        self.zz32(v);
    }

    #[inline]
    fn i64_field(&mut self, last: &mut i16, id: i16, v: i64) {
        self.field_hdr(last, id, 6);
        self.zz64(v);
    }

    #[inline]
    fn bin_field(&mut self, last: &mut i16, id: i16, bytes: &[u8]) {
        self.field_hdr(last, id, 8);
        self.uvarint(bytes.len() as u64);
        self.0.extend_from_slice(bytes);
    }

    #[inline]
    fn list_hdr(&mut self, last: &mut i16, id: i16, elem_ty: u8, len: usize) {
        self.field_hdr(last, id, 9);
        if len < 15 {
            self.0.push(((len as u8) << 4) | elem_ty);
        } else {
            self.0.push(0xF0 | elem_ty);
            self.uvarint(len as u64);
        }
    }
}

#[derive(Clone, Copy)]
enum ColType {
    Int32 = 1,
    Int64 = 2,
    ByteArray = 6,
}

#[derive(Clone, Copy)]
enum Converted {
    None = -1,
    Utf8 = 0,
    Date = 6,
}

struct ColSpec<'a> {
    name: &'a str,
    ty: ColType,
    optional: bool,
    converted: Converted,
}

fn write_schema_element(buf: &mut ThriftBuf, col: &ColSpec<'_>) {
    let mut last = 0i16;
    buf.i32_field(&mut last, 1, col.ty as i32);
    buf.i32_field(&mut last, 3, if col.optional { 1 } else { 0 });
    buf.bin_field(&mut last, 4, col.name.as_bytes());
    match col.converted {
        Converted::Utf8 => {
            buf.i32_field(&mut last, 6, 0);
            buf.field_hdr(&mut last, 10, 12);
            let mut llast = 0i16;
            buf.field_hdr(&mut llast, 1, 12);
            buf.stop();
            buf.stop();
        }
        Converted::Date => {
            buf.i32_field(&mut last, 6, 6);
            buf.field_hdr(&mut last, 10, 12);
            let mut llast = 0i16;
            buf.field_hdr(&mut llast, 6, 12);
            buf.stop();
            buf.stop();
        }
        Converted::None => {}
    }
    buf.stop();
}

fn encode_data_page_header(num_values: i32, uncomp: i32, comp: i32) -> Vec<u8> {
    encode_data_page_header_enc(num_values, uncomp, comp, 0)
}

#[allow(dead_code)]
fn encode_data_page_header_plain_legacy(num_values: i32, payload_len: i32) -> Vec<u8> {
    let mut t = ThriftBuf::new();
    let mut last = 0i16;
    t.i32_field(&mut last, 1, 0);
    t.i32_field(&mut last, 2, payload_len);
    t.i32_field(&mut last, 3, payload_len);
    t.field_hdr(&mut last, 5, 12);
    let mut dlast = 0i16;
    t.i32_field(&mut dlast, 1, num_values);
    t.i32_field(&mut dlast, 2, 0);
    t.i32_field(&mut dlast, 3, 3);
    t.i32_field(&mut dlast, 4, 3);
    t.stop();
    t.stop();
    t.0
}

fn encode_def_levels_from_bitmap(bitmap: &[u8], num_rows: usize, out: &mut Vec<u8>) {
    let total_bytes = num_rows.div_ceil(8);
    let num_rle_headers = total_bytes.div_ceil(63);
    let stream_len = total_bytes + num_rle_headers;
    out.extend_from_slice(&(stream_len as u32).to_le_bytes());
    let mut idx = 0usize;
    let slice = &bitmap[..total_bytes];
    while idx < slice.len() {
        let chunk_end = (idx + 63).min(slice.len());
        let num_groups = chunk_end - idx;
        out.push(((num_groups as u8) << 1) | 1);
        out.extend_from_slice(&slice[idx..chunk_end]);
        idx = chunk_end;
    }
}

struct ChunkMeta<'a> {
    spec: &'a ColSpec<'a>,
    dict_page_offset: Option<i64>,
    data_page_offset: i64,
    total_size: i64,
    total_uncompressed: i64,
    num_values: i64,
}

struct RowGroupMeta<'a> {
    chunks: Vec<ChunkMeta<'a>>,
    total_byte_size: i64,
    total_compressed: i64,
    num_rows: i64,
    file_offset: i64,
}

fn write_file_metadata<W: Write>(
    w: &mut W,
    schema: &[ColSpec<'_>],
    total_rows: i64,
    row_groups: &[RowGroupMeta<'_>],
) -> io::Result<()> {
    let mut t = ThriftBuf::new();
    let mut last = 0i16;
    t.i32_field(&mut last, 1, 1);

    t.list_hdr(&mut last, 2, 12, schema.len() + 1);
    {
        let mut rlast = 0i16;
        t.bin_field(&mut rlast, 4, b"root");
        t.i32_field(&mut rlast, 5, schema.len() as i32);
        t.stop();
    }
    for col in schema {
        write_schema_element(&mut t, col);
    }

    t.i64_field(&mut last, 3, total_rows);

    t.list_hdr(&mut last, 4, 12, row_groups.len());
    for (rg_idx, rg) in row_groups.iter().enumerate() {
        let mut rlast = 0i16;
        t.list_hdr(&mut rlast, 1, 12, rg.chunks.len());
        for ch in &rg.chunks {
            let mut clast = 0i16;
            let first_page = ch.dict_page_offset.unwrap_or(ch.data_page_offset);
            t.i64_field(&mut clast, 2, first_page);
            t.field_hdr(&mut clast, 3, 12);
            {
                let mut mlast = 0i16;
                t.i32_field(&mut mlast, 1, ch.spec.ty as i32);
                t.list_hdr(&mut mlast, 2, 5, 2);
                // PLAIN_DICTIONARY (2) for dictionary-encoded chunks, else PLAIN (0); RLE (3) for levels.
                t.zz32(if ch.dict_page_offset.is_some() { 2 } else { 0 });
                t.zz32(3);
                t.list_hdr(&mut mlast, 3, 8, 1);
                let name = ch.spec.name.as_bytes();
                t.uvarint(name.len() as u64);
                t.0.extend_from_slice(name);
                t.i32_field(&mut mlast, 4, if snappy_enabled() { 1 } else { 0 });
                t.i64_field(&mut mlast, 5, ch.num_values);
                t.i64_field(&mut mlast, 6, ch.total_uncompressed);
                t.i64_field(&mut mlast, 7, ch.total_size);
                t.i64_field(&mut mlast, 9, ch.data_page_offset);
                if let Some(d) = ch.dict_page_offset {
                    t.i64_field(&mut mlast, 11, d);
                }
                t.stop();
            }
            t.stop();
        }
        t.i64_field(&mut rlast, 2, rg.total_byte_size);
        t.i64_field(&mut rlast, 3, rg.num_rows);
        t.i64_field(&mut rlast, 5, rg.file_offset);
        t.i64_field(&mut rlast, 6, rg.total_compressed);
        t.i16_field(&mut rlast, 7, rg_idx as i16);
        t.stop();
    }

    t.bin_field(&mut last, 6, b"shopping-sessions-rust");
    t.stop();

    w.write_all(&t.0)?;
    w.write_all(&(t.0.len() as u32).to_le_bytes())?;
    w.write_all(b"PAR1")?;
    Ok(())
}

static SNAPPY: AtomicU8 = AtomicU8::new(2); // 2 = unset, read SHOP_SNAPPY once

/// Snappy page compression (codec 1) unless SHOP_SNAPPY=0.
fn snappy_enabled() -> bool {
    let v = SNAPPY.load(Ordering::Relaxed);
    if v != 2 {
        return v == 1;
    }
    let on = !matches!(std::env::var("SHOP_SNAPPY").as_deref(), Ok("0") | Ok("false"));
    SNAPPY.store(on as u8, Ordering::Relaxed);
    on
}

thread_local! {
    static PAGE_SCRATCH: RefCell<(Compressor, Vec<u8>, Vec<u8>)> =
        RefCell::new((Compressor::new(), Vec::new(), Vec::new()));
}

/// Writes one page: `make_hdr(uncompressed_len, compressed_len)` followed by the payload formed by
/// concatenating `parts`, Snappy-compressed when enabled. Returns (bytes written, uncompressed bytes).
fn emit_page<W: Write>(
    w: &mut W,
    parts: &[&[u8]],
    make_hdr: impl FnOnce(i32, i32) -> Vec<u8>,
) -> io::Result<(i64, i64)> {
    let uncomp: usize = parts.iter().map(|p| p.len()).sum();
    if !snappy_enabled() {
        let hdr = make_hdr(uncomp as i32, uncomp as i32);
        w.write_all(&hdr)?;
        for p in parts {
            w.write_all(p)?;
        }
        return Ok(((hdr.len() + uncomp) as i64, (hdr.len() + uncomp) as i64));
    }
    PAGE_SCRATCH.with(|sc| {
        let (enc, joined, out) = &mut *sc.borrow_mut();
        let src: &[u8] = if parts.len() == 1 {
            parts[0]
        } else {
            joined.clear();
            for p in parts {
                joined.extend_from_slice(p);
            }
            joined
        };
        out.clear();
        enc.compress(src, out);
        let n = out.len();
        let hdr = make_hdr(uncomp as i32, n as i32);
        w.write_all(&hdr)?;
        w.write_all(&out[..n])?;
        Ok(((hdr.len() + n) as i64, (hdr.len() + uncomp) as i64))
    })
}

fn write_col_page<W: Write>(
    w: &mut W,
    offset: &mut i64,
    num_rows: usize,
    payload: &[u8],
    spec: &'static ColSpec<'static>,
) -> io::Result<ChunkMeta<'static>> {
    let page_offset = *offset;
    let (total_size, total_uncompressed) =
        emit_page(w, &[payload], |u, c| encode_data_page_header(num_rows as i32, u, c))?;
    *offset += total_size;
    Ok(ChunkMeta {
        spec,
        dict_page_offset: None,
        data_page_offset: page_offset,
        total_size,
        total_uncompressed,
        num_values: num_rows as i64,
    })
}

fn write_opt_col_page<W: Write>(
    w: &mut W,
    offset: &mut i64,
    num_rows: usize,
    def_hdr_and_bits: &[u8],
    vals: &[u8],
    spec: &'static ColSpec<'static>,
) -> io::Result<ChunkMeta<'static>> {
    let page_offset = *offset;
    let (total_size, total_uncompressed) = emit_page(w, &[def_hdr_and_bits, vals], |u, c| {
        encode_data_page_header(num_rows as i32, u, c)
    })?;
    *offset += total_size;
    Ok(ChunkMeta {
        spec,
        dict_page_offset: None,
        data_page_offset: page_offset,
        total_size,
        total_uncompressed,
        num_values: num_rows as i64,
    })
}

/// PageHeader for a V1 data page with an explicit value encoding.
fn encode_data_page_header_enc(num_values: i32, uncomp: i32, comp: i32, encoding: i32) -> Vec<u8> {
    let mut t = ThriftBuf::new();
    let mut last = 0i16;
    t.i32_field(&mut last, 1, 0);
    t.i32_field(&mut last, 2, uncomp);
    t.i32_field(&mut last, 3, comp);
    t.field_hdr(&mut last, 5, 12);
    let mut dlast = 0i16;
    t.i32_field(&mut dlast, 1, num_values);
    t.i32_field(&mut dlast, 2, encoding);
    t.i32_field(&mut dlast, 3, 3);
    t.i32_field(&mut dlast, 4, 3);
    t.stop();
    t.stop();
    t.0
}

/// PageHeader for a dictionary page holding `num_values` PLAIN entries (PLAIN_DICTIONARY).
fn encode_dict_page_header(num_values: i32, uncomp: i32, comp: i32) -> Vec<u8> {
    let mut t = ThriftBuf::new();
    let mut last = 0i16;
    t.i32_field(&mut last, 1, 2);
    t.i32_field(&mut last, 2, uncomp);
    t.i32_field(&mut last, 3, comp);
    t.field_hdr(&mut last, 7, 12);
    let mut dlast = 0i16;
    t.i32_field(&mut dlast, 1, num_values);
    t.i32_field(&mut dlast, 2, 2);
    t.stop();
    t.stop();
    t.0
}

const DICT_MAX_VALUE: u32 = 1 << 16;
const DICT_MAX_ENTRIES: usize = 4096;

/// Scratch for dictionary-encoding small non-negative INT32 columns.
pub struct DictScratch {
    slot: Vec<u32>, // value -> index + 1 (0 = absent); reset through `dict` after each use
    dict: Vec<i32>,
    dict_bytes: Vec<u8>,
    idx: Vec<u32>,
    data: Vec<u8>,
}

impl DictScratch {
    pub fn new() -> Self {
        Self {
            slot: vec![0; DICT_MAX_VALUE as usize],
            dict: Vec::with_capacity(DICT_MAX_ENTRIES),
            dict_bytes: Vec::with_capacity(DICT_MAX_ENTRIES * 4),
            idx: Vec::with_capacity(ROW_GROUP_ROWS),
            data: Vec::with_capacity(ROW_GROUP_ROWS * 2),
        }
    }

    fn clear_slots(&mut self) {
        for &v in &self.dict {
            self.slot[v as usize] = 0;
        }
        self.dict.clear();
        self.idx.clear();
    }

    /// Builds dictionary + indices for `vals` (little-endian i32). Returns false when the column is
    /// unsuitable (negative or large values, too many distinct values); the caller then writes PLAIN.
    fn build(&mut self, vals: &[u8]) -> bool {
        self.clear_slots();
        for c in vals.chunks_exact(4) {
            let v = i32::from_le_bytes([c[0], c[1], c[2], c[3]]);
            if (v as u32) >= DICT_MAX_VALUE {
                self.clear_slots();
                return false;
            }
            let sl = &mut self.slot[v as usize];
            if *sl == 0 {
                if self.dict.len() == DICT_MAX_ENTRIES {
                    self.clear_slots();
                    return false;
                }
                self.dict.push(v);
                *sl = self.dict.len() as u32;
            }
            self.idx.push(*sl - 1);
        }
        true
    }

    /// Encodes indices as a 1-byte bit width followed by one bit-packed RLE-hybrid run.
    fn encode_indices(&mut self) {
        let n_dict = self.dict.len().max(2);
        let bw = (usize::BITS - (n_dict - 1).leading_zeros()) as usize;
        self.data.clear();
        self.data.push(bw as u8);
        let groups = self.idx.len().div_ceil(8);
        let mut hdr = ((groups as u64) << 1) | 1;
        loop {
            let b = (hdr & 0x7f) as u8;
            hdr >>= 7;
            if hdr == 0 {
                self.data.push(b);
                break;
            }
            self.data.push(b | 0x80);
        }
        let mut acc: u64 = 0;
        let mut nbits = 0usize;
        for g in 0..groups {
            for k in 0..8 {
                let v = self.idx.get(g * 8 + k).copied().unwrap_or(0) as u64;
                acc |= v << nbits;
                nbits += bw;
                while nbits >= 8 {
                    self.data.push(acc as u8);
                    acc >>= 8;
                    nbits -= 8;
                }
            }
        }
    }
}

/// Writes a required INT32 column chunk, dictionary-encoded when the values allow it.
fn write_int32_col<W: Write>(
    w: &mut W,
    offset: &mut i64,
    num_rows: usize,
    vals: &[u8],
    spec: &'static ColSpec<'static>,
    ds: &mut DictScratch,
) -> io::Result<ChunkMeta<'static>> {
    if !ds.build(vals) {
        return write_col_page(w, offset, num_rows, vals, spec);
    }
    ds.encode_indices();
    ds.dict_bytes.clear();
    for &v in &ds.dict {
        ds.dict_bytes.extend_from_slice(&v.to_le_bytes());
    }
    let dict_off = *offset;
    let n_dict = ds.dict.len() as i32;
    let (dict_size, dict_uncomp) =
        emit_page(w, &[&ds.dict_bytes], |u, c| encode_dict_page_header(n_dict, u, c))?;
    let data_off = dict_off + dict_size;
    let (data_size, data_uncomp) = emit_page(w, &[&ds.data], |u, c| {
        encode_data_page_header_enc(num_rows as i32, u, c, 2)
    })?;
    let total_size = dict_size + data_size;
    *offset += total_size;
    ds.clear_slots();
    Ok(ChunkMeta {
        spec,
        dict_page_offset: Some(dict_off),
        data_page_offset: data_off,
        total_size,
        total_uncompressed: dict_uncomp + data_uncomp,
        num_values: num_rows as i64,
    })
}

const USER_SCHEMA: [ColSpec<'static>; 6] = [
    ColSpec {
        name: "user_id",
        ty: ColType::ByteArray,
        optional: false,
        converted: Converted::Utf8,
    },
    ColSpec {
        name: "day",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::Date,
    },
    ColSpec {
        name: "sessions",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "events",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "revenue_cents",
        ty: ColType::Int64,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "top_category",
        ty: ColType::ByteArray,
        optional: true,
        converted: Converted::Utf8,
    },
];

const CAMPAIGN_SCHEMA: [ColSpec<'static>; 7] = [
    ColSpec {
        name: "campaign_id",
        ty: ColType::ByteArray,
        optional: false,
        converted: Converted::Utf8,
    },
    ColSpec {
        name: "channel",
        ty: ColType::ByteArray,
        optional: false,
        converted: Converted::Utf8,
    },
    ColSpec {
        name: "day",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::Date,
    },
    ColSpec {
        name: "sessions",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "purchasing_sessions",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "revenue_cents",
        ty: ColType::Int64,
        optional: false,
        converted: Converted::None,
    },
    ColSpec {
        name: "assisted_sessions",
        ty: ColType::Int32,
        optional: false,
        converted: Converted::None,
    },
];

const ROW_GROUP_ROWS: usize = 500_000;

/// Direct columnar Parquet writer eliminating intermediate row structs and per-column re-iteration.
pub struct UserDailyWriter<'a> {
    w: Option<BufWriter<File>>,
    out_dir: &'a Path,
    part_idx: usize,
    offset: i64,
    total_rows: i64,
    row_groups: Vec<RowGroupMeta<'static>>,
    rg_rows: usize,
    user_id_buf: Vec<u8>,
    day_buf: Vec<u8>,
    sessions_buf: Vec<u8>,
    events_buf: Vec<u8>,
    rev_buf: Vec<u8>,
    cat_bitmap: Vec<u8>,
    cat_cur_byte: u8,
    cat_vals_buf: Vec<u8>,
    def_enc_scratch: Vec<u8>,
    dict: DictScratch,
}

impl<'a> UserDailyWriter<'a> {
    pub fn new(out_dir: &'a Path, part_idx: usize) -> Self {
        Self {
            w: None,
            out_dir,
            part_idx,
            offset: 4,
            total_rows: 0,
            row_groups: Vec::new(),
            rg_rows: 0,
            user_id_buf: Vec::with_capacity(1 << 20),
            day_buf: Vec::with_capacity(1 << 18),
            sessions_buf: Vec::with_capacity(1 << 18),
            events_buf: Vec::with_capacity(1 << 18),
            rev_buf: Vec::with_capacity(1 << 19),
            cat_bitmap: Vec::with_capacity(ROW_GROUP_ROWS.div_ceil(8) + 8),
            cat_cur_byte: 0,
            cat_vals_buf: Vec::with_capacity(1 << 18),
            def_enc_scratch: Vec::with_capacity(ROW_GROUP_ROWS.div_ceil(8) + 2048),
            dict: DictScratch::new(),
        }
    }

    fn ensure_file(&mut self) -> io::Result<()> {
        if self.w.is_none() {
            let path = self
                .out_dir
                .join("user_daily")
                .join(format!("part-{}.parquet", self.part_idx));
            let mut bw = BufWriter::with_capacity(1 << 20, File::create(path)?);
            bw.write_all(b"PAR1")?;
            self.w = Some(bw);
        }
        Ok(())
    }

    #[inline(always)]
    pub fn push(
        &mut self,
        user_id: Key,
        day: i32,
        sessions: i32,
        events: i32,
        revenue_cents: i64,
        top_category: u16,
        categories: &[Box<[u8]>],
    ) {
        user_id.write_parquet_bytes(&mut self.user_id_buf);
        self.day_buf.extend_from_slice(&day.to_le_bytes());
        self.sessions_buf.extend_from_slice(&sessions.to_le_bytes());
        self.events_buf.extend_from_slice(&events.to_le_bytes());
        self.rev_buf.extend_from_slice(&revenue_cents.to_le_bytes());

        let bit_pos = self.rg_rows & 7;
        if top_category != u16::MAX {
            self.cat_cur_byte |= 1u8 << bit_pos;
            let cat = &categories[top_category as usize];
            self.cat_vals_buf
                .extend_from_slice(&(cat.len() as u32).to_le_bytes());
            self.cat_vals_buf.extend_from_slice(cat);
        }
        if bit_pos == 7 {
            self.cat_bitmap.push(self.cat_cur_byte);
            self.cat_cur_byte = 0;
        }
        self.rg_rows += 1;
        if self.rg_rows == ROW_GROUP_ROWS {
            self.flush_row_group().expect("failed to flush user_daily row group");
        }
    }

    fn flush_row_group(&mut self) -> io::Result<()> {
        if self.rg_rows == 0 {
            return Ok(());
        }
        self.ensure_file()?;
        if (self.rg_rows & 7) != 0 {
            self.cat_bitmap.push(self.cat_cur_byte);
            self.cat_cur_byte = 0;
        }
        let n = self.rg_rows;
        self.def_enc_scratch.clear();
        encode_def_levels_from_bitmap(&self.cat_bitmap, n, &mut self.def_enc_scratch);

        let w = self.w.as_mut().unwrap();
        let rg_offset = self.offset;
        let mut chunks = Vec::with_capacity(6);
        chunks.push(write_col_page(w, &mut self.offset, n, &self.user_id_buf, &USER_SCHEMA[0])?);
        let ds = &mut self.dict;
        chunks.push(write_int32_col(w, &mut self.offset, n, &self.day_buf, &USER_SCHEMA[1], ds)?);
        chunks.push(write_int32_col(w, &mut self.offset, n, &self.sessions_buf, &USER_SCHEMA[2], ds)?);
        chunks.push(write_int32_col(w, &mut self.offset, n, &self.events_buf, &USER_SCHEMA[3], ds)?);
        chunks.push(write_col_page(w, &mut self.offset, n, &self.rev_buf, &USER_SCHEMA[4])?);
        chunks.push(write_opt_col_page(
            w,
            &mut self.offset,
            n,
            &self.def_enc_scratch,
            &self.cat_vals_buf,
            &USER_SCHEMA[5],
        )?);

        let rg_bytes: i64 = chunks.iter().map(|c| c.total_uncompressed).sum();
        let rg_comp: i64 = chunks.iter().map(|c| c.total_size).sum();
        self.row_groups.push(RowGroupMeta {
            chunks,
            total_byte_size: rg_bytes,
            total_compressed: rg_comp,
            num_rows: n as i64,
            file_offset: rg_offset,
        });
        self.total_rows += n as i64;
        self.rg_rows = 0;
        if crate::sys::writeback_enabled() {
            let w = self.w.as_mut().unwrap();
            w.flush()?;
            crate::sys::start_writeback(w.get_ref());
        }
        self.user_id_buf.clear();
        self.day_buf.clear();
        self.sessions_buf.clear();
        self.events_buf.clear();
        self.rev_buf.clear();
        self.cat_bitmap.clear();
        self.cat_vals_buf.clear();
        Ok(())
    }

    pub fn finish(mut self, force_write_empty: bool) -> io::Result<()> {
        if self.rg_rows > 0 {
            self.flush_row_group()?;
        }
        if self.w.is_none() {
            if !force_write_empty {
                return Ok(());
            }
            self.ensure_file()?;
        }
        let w = self.w.as_mut().unwrap();
        write_file_metadata(w, &USER_SCHEMA, self.total_rows, &self.row_groups)?;
        w.flush()
    }
}

pub fn write_campaign_daily(
    out_dir: &Path,
    rows: &[CampaignDailyRow],
    campaigns: &[(Key, Key)],
) -> io::Result<()> {
    let dir = out_dir.join("campaign_daily");
    fs::create_dir_all(&dir)?;
    let mut w = BufWriter::with_capacity(1 << 20, File::create(dir.join("part-0.parquet"))?);
    w.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let mut row_groups = Vec::new();
    let mut buf = Vec::with_capacity(1 << 20);

    for chunk in rows.chunks(ROW_GROUP_ROWS) {
        let rg_offset = offset;
        let mut chunks = Vec::with_capacity(7);

        buf.clear();
        for r in chunk {
            campaigns[r.campaign_id_idx as usize]
                .0
                .write_parquet_bytes(&mut buf);
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[0],
        )?);

        buf.clear();
        for r in chunk {
            campaigns[r.campaign_id_idx as usize]
                .1
                .write_parquet_bytes(&mut buf);
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[1],
        )?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.day.to_le_bytes());
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[2],
        )?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.sessions.to_le_bytes());
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[3],
        )?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.purchasing_sessions.to_le_bytes());
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[4],
        )?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[5],
        )?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.assisted_sessions.to_le_bytes());
        }
        chunks.push(write_col_page(
            &mut w,
            &mut offset,
            chunk.len(),
            &buf,
            &CAMPAIGN_SCHEMA[6],
        )?);

        let rg_bytes: i64 = chunks.iter().map(|c| c.total_uncompressed).sum();
        let rg_comp: i64 = chunks.iter().map(|c| c.total_size).sum();
        row_groups.push(RowGroupMeta {
            chunks,
            total_byte_size: rg_bytes,
            total_compressed: rg_comp,
            num_rows: chunk.len() as i64,
            file_offset: rg_offset,
        });
    }

    write_file_metadata(&mut w, &CAMPAIGN_SCHEMA, rows.len() as i64, &row_groups)?;
    w.flush()
}
