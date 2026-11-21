use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

#[derive(Default)]
struct Interner {
    map: HashMap<&'static [u8], u32>,
    slices: Vec<&'static [u8]>,
}

static INTERNER: OnceLock<RwLock<Interner>> = OnceLock::new();

fn interner() -> &'static RwLock<Interner> {
    INTERNER.get_or_init(|| RwLock::new(Interner::default()))
}

/// Compact 16-byte string representation.
/// Length <= 15 is stored inline (`bytes[15] = len`), otherwise `bytes[15] = 0xFF`
/// and `bytes[0..4]` stores the `u32` index into the global interner.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SmallStr(pub [u8; 16]);

impl SmallStr {
    pub const EMPTY: Self = Self([0u8; 16]);

    #[inline]
    pub fn from_bytes(s: &[u8]) -> Self {
        if s.len() <= 15 {
            let mut buf = [0u8; 16];
            buf[..s.len()].copy_from_slice(s);
            buf[15] = s.len() as u8;
            Self(buf)
        } else {
            Self::intern_long(s)
        }
    }

    #[cold]
    fn intern_long(s: &[u8]) -> Self {
        let lock = interner();
        {
            let r = lock.read().unwrap();
            if let Some(&idx) = r.map.get(s) {
                return Self::from_long_idx(idx);
            }
        }
        let mut w = lock.write().unwrap();
        if let Some(&idx) = w.map.get(s) {
            return Self::from_long_idx(idx);
        }
        let idx = w.slices.len() as u32;
        let leaked: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
        w.slices.push(leaked);
        w.map.insert(leaked, idx);
        Self::from_long_idx(idx)
    }

    #[inline]
    fn from_long_idx(idx: u32) -> Self {
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&idx.to_le_bytes());
        buf[15] = 0xFF;
        Self(buf)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let tag = self.0[15];
        if tag != 0xFF {
            &self.0[..tag as usize]
        } else {
            self.as_long_bytes()
        }
    }

    #[cold]
    fn as_long_bytes(&self) -> &'static [u8] {
        let idx = u32::from_le_bytes([self.0[0], self.0[1], self.0[2], self.0[3]]) as usize;
        interner().read().unwrap().slices[idx]
    }

    #[inline]
    pub fn as_u128(self) -> u128 {
        u128::from_ne_bytes(self.0)
    }

    #[inline]
    pub fn hash64(self) -> u64 {
        let lo = u64::from_ne_bytes([
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7],
        ]);
        let hi = u64::from_ne_bytes([
            self.0[8], self.0[9], self.0[10], self.0[11], self.0[12], self.0[13], self.0[14],
            self.0[15],
        ]);
        let mut x = lo ^ hi.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    #[inline]
    pub fn cmp_bytes(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

/// Parses an exact N-field CSV line according to `rules.md`:
/// - Empty line is malformed.
/// - Every comma separates fields.
/// - Any field wrapped in `"` strips outer quotes; quotes anywhere else are malformed.
#[inline]
pub fn parse_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&[][..]; N];
    let mut start = 0usize;
    let mut idx = 0usize;
    let len = line.len();

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

/// Parses timestamps in forms F1, F2, and F3 into epoch milliseconds UTC.
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let len = s.len();
    if len == 0 {
        return None;
    }

    // F1: YYYY-MM-DDTHH:MM:SSZ (20 bytes)
    // F2: YYYY-MM-DDTHH:MM:SS+HH:MM or -HH:MM (25 bytes)
    if (len == 20 && s[19] == b'Z')
        || (len == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':')
    {
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
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
        let hour = parse_2(s[11], s[12])?;
        let minute = parse_2(s[14], s[15])?;
        let second = parse_2(s[17], s[18])?;
        if hour > 23 || minute > 59 || second > 59 {
            return None;
        }

        let mut ms = days_from_civil(year, month, day) * 86_400_000
            + (hour * 3_600 + minute * 60 + second) * 1_000;

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

    // F3: ASCII digits only (epoch milliseconds)
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
