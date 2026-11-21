use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::csv_ts::Key;

pub struct UserDailyRow {
    pub user_id: Key,
    pub day: i32,
    pub sessions: i32,
    pub events: i32,
    pub revenue_cents: i64,
    pub top_category: u16, // u16::MAX means null
}

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

fn encode_data_page_header(num_values: i32, payload_len: i32) -> Vec<u8> {
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

fn encode_def_levels_bitpacked(present: &[bool], out: &mut Vec<u8>) {
    let len_pos = out.len();
    out.extend_from_slice(&[0u8; 4]);
    let mut idx = 0usize;
    while idx < present.len() {
        let chunk_end = (idx + 504).min(present.len());
        let count = chunk_end - idx;
        let num_groups = count.div_ceil(8);
        out.push(((num_groups as u8) << 1) | 1);
        for g in 0..num_groups {
            let mut byte = 0u8;
            for b in 0..8 {
                let pos = idx + g * 8 + b;
                if pos < chunk_end && present[pos] {
                    byte |= 1u8 << b;
                }
            }
            out.push(byte);
        }
        idx = chunk_end;
    }
    let level_bytes = (out.len() - len_pos - 4) as u32;
    out[len_pos..len_pos + 4].copy_from_slice(&level_bytes.to_le_bytes());
}

struct ChunkMeta<'a> {
    spec: &'a ColSpec<'a>,
    data_page_offset: i64,
    total_size: i64,
    num_values: i64,
}

struct RowGroupMeta<'a> {
    chunks: Vec<ChunkMeta<'a>>,
    total_byte_size: i64,
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
            t.i64_field(&mut clast, 2, ch.data_page_offset);
            t.field_hdr(&mut clast, 3, 12);
            {
                let mut mlast = 0i16;
                t.i32_field(&mut mlast, 1, ch.spec.ty as i32);
                t.list_hdr(&mut mlast, 2, 5, 2);
                t.zz32(0);
                t.zz32(3);
                t.list_hdr(&mut mlast, 3, 8, 1);
                let name = ch.spec.name.as_bytes();
                t.uvarint(name.len() as u64);
                t.0.extend_from_slice(name);
                t.i32_field(&mut mlast, 4, 0);
                t.i64_field(&mut mlast, 5, ch.num_values);
                t.i64_field(&mut mlast, 6, ch.total_size);
                t.i64_field(&mut mlast, 7, ch.total_size);
                t.i64_field(&mut mlast, 9, ch.data_page_offset);
                t.stop();
            }
            t.stop();
        }
        t.i64_field(&mut rlast, 2, rg.total_byte_size);
        t.i64_field(&mut rlast, 3, rg.num_rows);
        t.i64_field(&mut rlast, 5, rg.file_offset);
        t.i64_field(&mut rlast, 6, rg.total_byte_size);
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

fn write_col_page<W: Write>(
    w: &mut W,
    offset: &mut i64,
    num_rows: usize,
    payload: &[u8],
    spec: &'static ColSpec<'static>,
) -> io::Result<ChunkMeta<'static>> {
    let hdr = encode_data_page_header(num_rows as i32, payload.len() as i32);
    let page_offset = *offset;
    w.write_all(&hdr)?;
    w.write_all(payload)?;
    let total_size = (hdr.len() + payload.len()) as i64;
    *offset += total_size;
    Ok(ChunkMeta {
        spec,
        data_page_offset: page_offset,
        total_size,
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

pub fn write_user_daily_part(
    out_dir: &Path,
    part_idx: usize,
    rows: &[UserDailyRow],
    categories: &[Box<[u8]>],
) -> io::Result<()> {
    let dir = out_dir.join("user_daily");
    let path = dir.join(format!("part-{part_idx}.parquet"));
    let mut w = BufWriter::with_capacity(1 << 20, File::create(path)?);
    w.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let mut row_groups = Vec::new();
    let mut buf = Vec::with_capacity(1 << 20);
    let mut present = Vec::with_capacity(500_000);

    for chunk in rows.chunks(500_000) {
        let rg_offset = offset;
        let mut chunks = Vec::with_capacity(6);

        buf.clear();
        for r in chunk {
            r.user_id.write_parquet_bytes(&mut buf);
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[0])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.day.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[1])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.sessions.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[2])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.events.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[3])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[4])?);

        buf.clear();
        present.clear();
        for r in chunk {
            present.push(r.top_category != u16::MAX);
        }
        encode_def_levels_bitpacked(&present, &mut buf);
        for r in chunk {
            if r.top_category != u16::MAX {
                let cat = &categories[r.top_category as usize];
                buf.extend_from_slice(&(cat.len() as u32).to_le_bytes());
                buf.extend_from_slice(cat);
            }
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &USER_SCHEMA[5])?);

        let rg_bytes: i64 = chunks.iter().map(|c| c.total_size).sum();
        row_groups.push(RowGroupMeta {
            chunks,
            total_byte_size: rg_bytes,
            num_rows: chunk.len() as i64,
            file_offset: rg_offset,
        });
    }

    write_file_metadata(&mut w, &USER_SCHEMA, rows.len() as i64, &row_groups)?;
    w.flush()
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

    for chunk in rows.chunks(500_000) {
        let rg_offset = offset;
        let mut chunks = Vec::with_capacity(7);

        buf.clear();
        for r in chunk {
            campaigns[r.campaign_id_idx as usize].0.write_parquet_bytes(&mut buf);
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[0])?);

        buf.clear();
        for r in chunk {
            campaigns[r.campaign_id_idx as usize].1.write_parquet_bytes(&mut buf);
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[1])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.day.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[2])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.sessions.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[3])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.purchasing_sessions.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[4])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[5])?);

        buf.clear();
        for r in chunk {
            buf.extend_from_slice(&r.assisted_sessions.to_le_bytes());
        }
        chunks.push(write_col_page(&mut w, &mut offset, chunk.len(), &buf, &CAMPAIGN_SCHEMA[6])?);

        let rg_bytes: i64 = chunks.iter().map(|c| c.total_size).sum();
        row_groups.push(RowGroupMeta {
            chunks,
            total_byte_size: rg_bytes,
            num_rows: chunk.len() as i64,
            file_offset: rg_offset,
        });
    }

    write_file_metadata(&mut w, &CAMPAIGN_SCHEMA, rows.len() as i64, &row_groups)?;
    w.flush()
}
