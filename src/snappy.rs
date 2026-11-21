//! Minimal Snappy raw-format compressor (no external crates).
//!
//! Output: varint(uncompressed length), then literal and copy elements. Matches are found with a
//! 4-byte hash table over absolute positions; copies use 1-byte (offset < 2048, len 4..=11) or
//! 2-byte offsets (<= 65535), so any conforming Snappy decoder accepts the stream.

const HASH_BITS: u32 = 14;
const MAX_OFFSET: usize = 65535;

pub struct Compressor {
    table: Vec<u32>, // position + 1 of the last occurrence of a 4-byte hash; 0 = empty
}

impl Compressor {
    pub fn new() -> Self {
        Self { table: vec![0; 1 << HASH_BITS] }
    }

    /// Upper bound on compressed size for `n` input bytes.
    pub fn max_len(n: usize) -> usize {
        32 + n + n / 6
    }

    /// Compresses `src`, appending the Snappy stream to `dst`.
    pub fn compress(&mut self, src: &[u8], dst: &mut Vec<u8>) {
        let n = src.len();
        dst.reserve(Self::max_len(n));
        put_varint(dst, n as u64);
        if n < 8 {
            emit_literal(dst, src);
            return;
        }
        self.table.fill(0);
        let limit = n - 4; // last position where a 4-byte load is in bounds
        let mut next_emit = 0usize;
        let mut i = 0usize;
        while i <= limit {
            let cur = load32(src, i);
            let h = hash(cur);
            let cand = self.table[h] as usize;
            self.table[h] = (i + 1) as u32;
            if cand != 0 {
                let c = cand - 1;
                if i - c <= MAX_OFFSET && load32(src, c) == cur {
                    let mut len = 4;
                    while i + len < n && src[c + len] == src[i + len] {
                        len += 1;
                    }
                    emit_literal(dst, &src[next_emit..i]);
                    emit_copy(dst, i - c, len);
                    i += len;
                    next_emit = i;
                    continue;
                }
            }
            // Skip faster through incompressible stretches.
            i += 1 + ((i - next_emit) >> 5);
        }
        emit_literal(dst, &src[next_emit..]);
    }
}

#[inline(always)]
fn load32(s: &[u8], i: usize) -> u32 {
    u32::from_le_bytes([s[i], s[i + 1], s[i + 2], s[i + 3]])
}

#[inline(always)]
fn hash(v: u32) -> usize {
    (v.wrapping_mul(0x1E35_A7BD) >> (32 - HASH_BITS)) as usize
}

fn put_varint(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

fn emit_literal(dst: &mut Vec<u8>, lit: &[u8]) {
    if lit.is_empty() {
        return;
    }
    let m = lit.len() - 1;
    if m < 60 {
        dst.push((m as u8) << 2);
    } else {
        let nbytes = if m < 1 << 8 {
            1
        } else if m < 1 << 16 {
            2
        } else if m < 1 << 24 {
            3
        } else {
            4
        };
        dst.push(((59 + nbytes) as u8) << 2);
        dst.extend_from_slice(&(m as u32).to_le_bytes()[..nbytes]);
    }
    dst.extend_from_slice(lit);
}

fn emit_copy(dst: &mut Vec<u8>, offset: usize, mut len: usize) {
    while len >= 68 {
        emit_copy_upto64(dst, offset, 64);
        len -= 64;
    }
    if len > 64 {
        emit_copy_upto64(dst, offset, 60);
        len -= 60;
    }
    emit_copy_upto64(dst, offset, len);
}

#[inline(always)]
fn emit_copy_upto64(dst: &mut Vec<u8>, offset: usize, len: usize) {
    debug_assert!((4..=64).contains(&len) && offset >= 1 && offset <= MAX_OFFSET);
    if len <= 11 && offset < 2048 {
        dst.push(1 | (((len - 4) as u8) << 2) | (((offset >> 8) as u8) << 5));
        dst.push(offset as u8);
    } else {
        dst.push(2 | (((len - 1) as u8) << 2));
        dst.extend_from_slice(&(offset as u16).to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decompress(s: &[u8]) -> Vec<u8> {
        let (mut n, mut shift, mut i) = (0usize, 0, 0);
        loop {
            let b = s[i];
            i += 1;
            n |= ((b & 0x7f) as usize) << shift;
            if b < 0x80 {
                break;
            }
            shift += 7;
        }
        let mut out = Vec::with_capacity(n);
        while i < s.len() {
            let tag = s[i];
            i += 1;
            match tag & 3 {
                0 => {
                    let mut m = (tag >> 2) as usize;
                    if m >= 60 {
                        let nb = m - 59;
                        m = 0;
                        for k in 0..nb {
                            m |= (s[i + k] as usize) << (8 * k);
                        }
                        i += nb;
                    }
                    out.extend_from_slice(&s[i..i + m + 1]);
                    i += m + 1;
                }
                1 => {
                    let len = 4 + ((tag >> 2) & 7) as usize;
                    let off = (((tag >> 5) as usize) << 8) | s[i] as usize;
                    i += 1;
                    for _ in 0..len {
                        out.push(out[out.len() - off]);
                    }
                }
                2 => {
                    let len = 1 + (tag >> 2) as usize;
                    let off = u16::from_le_bytes([s[i], s[i + 1]]) as usize;
                    i += 2;
                    for _ in 0..len {
                        out.push(out[out.len() - off]);
                    }
                }
                _ => panic!("copy-4 not emitted"),
            }
        }
        assert_eq!(out.len(), n);
        out
    }

    #[test]
    fn roundtrip() {
        let mut c = Compressor::new();
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut cases: Vec<Vec<u8>> = vec![vec![], vec![7], b"abcdefg".to_vec(), vec![0; 100_000]];
        for len in [9usize, 63, 64, 65, 300, 4096, 70_000, 200_000, 1_000_000] {
            cases.push((0..len).map(|_| rnd() as u8).collect());
            cases.push((0..len).map(|_| b"0123456789abcdefghijklmnopqrstuvwxyz"[(rnd() % 6) as usize]).collect());
            let mut v = Vec::new();
            while v.len() < len {
                v.extend_from_slice(format!("u{:x}\0\0\0", rnd() % 5000).as_bytes());
            }
            cases.push(v);
        }
        for src in cases {
            let mut out = Vec::new();
            c.compress(&src, &mut out);
            assert!(out.len() <= Compressor::max_len(src.len()));
            assert_eq!(decompress(&out), src);
        }
    }
}
