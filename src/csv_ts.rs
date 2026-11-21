use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

const INTERNED_BIT: u64 = 1u64 << 63;

#[derive(Default)]
struct Interner {
    map: HashMap<&'static [u8], u32>,
    slices: Vec<&'static [u8]>,
}

static INTERNER: OnceLock<RwLock<Interner>> = OnceLock::new();

fn interner() -> &'static RwLock<Interner> {
    INTERNER.get_or_init(|| RwLock::new(Interner::default()))
}

/// 8-byte order-preserving ID key.
/// - Bit 63 == 0: 1..=9 ASCII bytes in 0x01..=0x7F packed 7 bits/char MSB-first.
///   Integer `<` equals byte-wise lexicographical `<`.
/// - Bit 63 == 1: interned in global `INTERNER` (for len > 9, len == 0, NUL, or non-ASCII).
/// - `Key(0)` is `EMPTY` sentinel (never produced by `from_bytes`).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Key(pub u64);

impl Key {
    pub const EMPTY: Self = Self(0);

    #[inline]
    pub fn from_bytes(s: &[u8]) -> Self {
        let len = s.len();
        if len >= 1 && len <= 9 {
            let mut acc = 0u64;
            let mut shift = 56i32;
            let mut i = 0usize;
            while i < len {
                let b = s[i];
                if b == 0 || b >= 0x80 {
                    return Self::intern_slow(s);
                }
                acc |= (b as u64) << shift;
                shift -= 7;
                i += 1;
            }
            return Self(acc);
        }
        Self::intern_slow(s)
    }

    #[cold]
    fn intern_slow(s: &[u8]) -> Self {
        let lock = interner();
        {
            let r = lock.read().unwrap();
            if let Some(&idx) = r.map.get(s) {
                return Self(INTERNED_BIT | (idx as u64));
            }
        }
        let mut w = lock.write().unwrap();
        if let Some(&idx) = w.map.get(s) {
            return Self(INTERNED_BIT | (idx as u64));
        }
        let idx = w.slices.len() as u32;
        let leaked: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
        w.slices.push(leaked);
        w.map.insert(leaked, idx);
        Self(INTERNED_BIT | (idx as u64))
    }

    #[inline]
    pub fn unpack_inline(self, buf: &mut [u8; 9]) -> usize {
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

    #[cold]
    fn long_bytes(self) -> &'static [u8] {
        let idx = (self.0 & !INTERNED_BIT) as usize;
        interner().read().unwrap().slices[idx]
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

    #[inline]
    pub fn hash64(self) -> u64 {
        let mut x = self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    #[inline]
    pub fn cmp_bytes(&self, other: &Self) -> std::cmp::Ordering {
        if ((self.0 | other.0) & INTERNED_BIT) == 0 {
            self.0.cmp(&other.0)
        } else {
            self.cmp_bytes_slow(other)
        }
    }

    #[cold]
    fn cmp_bytes_slow(&self, other: &Self) -> std::cmp::Ordering {
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

#[inline]
fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + if month > 2 { -3 } else { 9 })) + 2) / 5 + day - 1;
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
    entries: [DateEntry; 64],
}

impl DateCache {
    pub fn new() -> Self {
        Self {
            entries: [DateEntry {
                k0: u64::MAX,
                k1: u16::MAX,
                day_ms: i64::MIN,
            }; 64],
        }
    }

    #[inline]
    pub fn parse_ts(&mut self, s: &[u8]) -> Option<i64> {
        let len = s.len();
        if len == 20 && s[19] == b'Z' && s[10] == b'T' && s[13] == b':' && s[16] == b':' {
            let k0 = u64::from_ne_bytes(s[0..8].try_into().unwrap());
            let k1 = u16::from_ne_bytes(s[8..10].try_into().unwrap());
            let slot = ((k0.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (k1 as u64)) >> 58) as usize;
            let ent = &self.entries[slot];
            let day_ms = if ent.k0 == k0 && ent.k1 == k1 {
                if ent.day_ms == i64::MIN {
                    return None;
                }
                ent.day_ms
            } else {
                let d = parse_date_10(&s[0..10]);
                self.entries[slot] = DateEntry {
                    k0,
                    k1,
                    day_ms: d.unwrap_or(i64::MIN),
                };
                d?
            };
            let hour = parse_2(s[11], s[12])?;
            let minute = parse_2(s[14], s[15])?;
            let second = parse_2(s[17], s[18])?;
            if hour > 23 || minute > 59 || second > 59 {
                return None;
            }
            return Some(day_ms + (hour * 3_600 + minute * 60 + second) * 1_000);
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
        let hour = parse_2(s[11], s[12])?;
        let minute = parse_2(s[14], s[15])?;
        let second = parse_2(s[17], s[18])?;
        if hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let mut ms = day_ms + (hour * 3_600 + minute * 60 + second) * 1_000;
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
    if val >= 1 {
        Some(val as i32)
    } else {
        None
    }
}
