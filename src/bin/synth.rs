use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

const WINDOW_START_SEC: i64 = 1_790_812_800; // 2026-10-01T00:00:00Z
const DAY_SEC: i64 = 86_400;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gen_range(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn pct(&mut self, p1000: u64) -> bool {
        (self.next_u64() % 1000) < p1000
    }
}

#[inline(always)]
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn push_uid(out: &mut String, person: u64, dev: u64, rng: &mut Rng) {
    let q = rng.pct(200);
    if q {
        out.push('"');
    }
    let idx = person * 8 + dev;
    use std::fmt::Write;
    let sel = mix64(idx ^ 0xD6E8_FEB8_6659_FD93);
    if (sel & 7) == 0 {
        let h2 = mix64(sel ^ 0x9E37_79B9_7F4A_7C15);
        let _ = write!(out, "u{:016x}{:016x}", sel, h2);
    } else {
        let _ = write!(out, "u{:x}", 100_000 + idx);
    }
    if q {
        out.push('"');
    }
}

fn push_field_str(out: &mut String, s: &str, rng: &mut Rng) {
    let q = rng.pct(200);
    if q {
        out.push('"');
    }
    out.push_str(s);
    if q {
        out.push('"');
    }
}

fn push_ts_oct(out: &mut String, sec_utc: i64, form: u64, rng: &mut Rng) {
    let q = rng.pct(200);
    if q {
        out.push('"');
    }
    use std::fmt::Write;
    if form < 320 {
        let _ = write!(out, "{}", sec_utc * 1000);
    } else {
        let off_min = if form < 420 {
            if rng.pct(500) { 120i64 } else { -240i64 }
        } else {
            0i64
        };
        let local_sec = sec_utc + off_min * 60;
        let rel_day = (local_sec - WINDOW_START_SEC).div_euclid(DAY_SEC);
        let tod = (local_sec - WINDOW_START_SEC).rem_euclid(DAY_SEC);
        let (yr, mo, dy) = if rel_day < 0 {
            (2026, 9, 31 + rel_day)
        } else if rel_day < 31 {
            (2026, 10, 1 + rel_day)
        } else {
            (2026, 11, rel_day - 30)
        };
        let hh = tod / 3600;
        let mm = (tod % 3600) / 60;
        let ss = tod % 60;
        if off_min == 0 {
            let _ = write!(out, "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", yr, mo, dy, hh, mm, ss);
        } else {
            let sign = if off_min >= 0 { '+' } else { '-' };
            let abs_m = off_min.abs();
            let _ = write!(
                out,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:00",
                yr,
                mo,
                dy,
                hh,
                mm,
                ss,
                sign,
                abs_m / 60
            );
        }
    }
    if q {
        out.push('"');
    }
}

fn main() -> io::Result<()> {
    let out_dir = PathBuf::from(env::args().nth(1).unwrap_or_else(|| "/tmp/train".into()));
    fs::create_dir_all(out_dir.join("links"))?;
    fs::create_dir_all(out_dir.join("events"))?;
    fs::create_dir_all(out_dir.join("clicks"))?;

    fs::write(
        out_dir.join("manifest.json"),
        b"{\"window_start\": \"2026-10-01T00:00:00Z\", \"window_end\": \"2026-10-15T00:00:00Z\"}\n",
    )?;

    let mut rng = Rng(0xDEAD_BEEF_CAFE_BABE);

    {
        let mut w = BufWriter::new(File::create(out_dir.join("products.csv"))?);
        writeln!(w, "product_id,category")?;
        for i in 1..=50_000 {
            writeln!(w, "p{},cat_{}", i, i % 20)?;
        }
    }

    {
        let mut w = BufWriter::new(File::create(out_dir.join("campaigns.csv"))?);
        writeln!(w, "campaign_id,channel")?;
        for i in 1..=2_000 {
            writeln!(w, "c_{},ch_{}", i, i % 5)?;
        }
    }

    let num_people = 35_000u64;
    {
        let mut writers: Vec<BufWriter<File>> = (0..4)
            .map(|i| {
                let mut f = BufWriter::new(
                    File::create(out_dir.join(format!("links/part-{:03}.csv", i))).unwrap(),
                );
                writeln!(f, "user_id_a,user_id_b").unwrap();
                f
            })
            .collect();
        let mut line = String::with_capacity(128);
        for p in 0..num_people {
            let extra = rng.gen_range(3);
            for d in 1..=extra {
                line.clear();
                push_uid(&mut line, p, 0, &mut rng);
                line.push(',');
                push_uid(&mut line, p, d, &mut rng);
                line.push('\n');
                let fidx = (rng.gen_range(100) % 3) as usize;
                writers[fidx].write_all(line.as_bytes())?;
            }
            if rng.pct(80) {
                line.clear();
                push_uid(&mut line, p, 0, &mut rng);
                line.push(',');
                push_uid(&mut line, p, 0, &mut rng);
                line.push('\n');
                writers[0].write_all(line.as_bytes())?;
            }
        }
    }

    {
        let mut writers: Vec<BufWriter<File>> = (0..8)
            .map(|i| {
                let mut f = BufWriter::new(
                    File::create(out_dir.join(format!("events/part-{:03}.csv", i))).unwrap(),
                );
                writeln!(
                    f,
                    "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents"
                )
                .unwrap();
                f
            })
            .collect();
        let mut line = String::with_capacity(192);
        let mut line2 = String::with_capacity(192);
        for i in 0..1_000_000u64 {
            let p = if rng.pct(60) {
                rng.gen_range(5)
            } else {
                rng.gen_range(num_people)
            };
            let dev = rng.gen_range(3);
            let ts_sec = WINDOW_START_SEC + (rng.gen_range(14 * 86_400) as i64);
            let late = rng.pct(10);
            let ing_sec = if late {
                ts_sec + 100_000 + (rng.gen_range(100_000) as i64)
            } else {
                ts_sec + (rng.gen_range(600) as i64)
            };

            let r_type = rng.gen_range(1000);
            let (etype, price_str) = if r_type < 30 {
                ("purchase", format!("{}", 100 + rng.gen_range(50_000)))
            } else if r_type < 150 {
                ("cart", String::new())
            } else if r_type == 999 {
                ("Purchase", String::new())
            } else {
                ("view", String::new())
            };
            let prod = if rng.pct(5) {
                format!("p{}", 50_001 + rng.gen_range(500))
            } else {
                format!("p{}", 1 + rng.gen_range(50_000))
            };

            line.clear();
            let eid_str = if (i & 7) == 0 {
                let h1 = mix64((i as u64).wrapping_add(0x1111_2222_3333_4444));
                let h2 = mix64(h1 ^ 0x5555_6666_7777_8888);
                format!("e{:016x}{:016x}", h1, h2)
            } else {
                format!("e{:07x}", i + 1)
            };
            push_field_str(&mut line, &eid_str, &mut rng);
            line.push(',');
            if rng.pct(2) {
                push_field_str(&mut line, "", &mut rng);
            } else {
                push_uid(&mut line, p, dev, &mut rng);
            }
            line.push(',');
            if rng.pct(5) {
                push_field_str(&mut line, "bad-ts", &mut rng);
            } else {
                let form = rng.gen_range(1000);
                push_ts_oct(&mut line, ts_sec, form, &mut rng);
            }
            line.push(',');
            push_ts_oct(&mut line, ing_sec, 900, &mut rng);
            line.push(',');
            push_field_str(&mut line, etype, &mut rng);
            line.push(',');
            push_field_str(&mut line, &prod, &mut rng);
            line.push(',');
            push_field_str(&mut line, &price_str, &mut rng);
            line.push('\n');

            let fidx = if rng.pct(200) {
                0
            } else {
                1 + (rng.gen_range(6) as usize)
            };
            writers[fidx].write_all(line.as_bytes())?;

            if rng.pct(30) {
                line2.clear();
                push_field_str(&mut line2, &eid_str, &mut rng);
                line2.push(',');
                let dup_p = if rng.pct(16) { (p + 1) % num_people } else { p };
                push_uid(&mut line2, dup_p, dev, &mut rng);
                line2.push(',');
                let form = rng.gen_range(1000);
                push_ts_oct(&mut line2, ts_sec, form, &mut rng);
                line2.push(',');
                push_ts_oct(&mut line2, ing_sec + 30 + (rng.gen_range(300) as i64), 900, &mut rng);
                line2.push(',');
                push_field_str(&mut line2, etype, &mut rng);
                line2.push(',');
                push_field_str(&mut line2, &prod, &mut rng);
                line2.push(',');
                push_field_str(&mut line2, &price_str, &mut rng);
                line2.push('\n');
                writers[fidx].write_all(line2.as_bytes())?;
            }
        }
    }

    {
        let mut writers: Vec<BufWriter<File>> = (0..4)
            .map(|i| {
                let mut f = BufWriter::new(
                    File::create(out_dir.join(format!("clicks/part-{:03}.csv", i))).unwrap(),
                );
                writeln!(f, "click_id,user_id,campaign_id,ts").unwrap();
                f
            })
            .collect();
        let mut line = String::with_capacity(128);
        for i in 0..250_000u64 {
            let p = if rng.pct(100) {
                rng.gen_range(5)
            } else {
                rng.gen_range(num_people)
            };
            let dev = rng.gen_range(3);
            let ts_sec = WINDOW_START_SEC - 7 * DAY_SEC + (rng.gen_range(21 * 86_400) as i64);
            let camp = if rng.pct(5) {
                format!("c_{}", 2001 + rng.gen_range(50))
            } else if rng.pct(300) {
                format!("c_{}", 1 + rng.gen_range(5))
            } else {
                format!("c_{}", 1 + rng.gen_range(2_000))
            };
            line.clear();
            let clid_str = if (i & 7) == 0 {
                let h1 = mix64((i as u64).wrapping_add(0xAAAA_BBBB_CCCC_DDDD));
                let h2 = mix64(h1 ^ 0x2222_4444_6666_8888);
                format!("k{:016x}{:016x}", h1, h2)
            } else {
                format!("k{:07x}", i + 1)
            };
            push_field_str(&mut line, &clid_str, &mut rng);
            line.push(',');
            push_uid(&mut line, p, dev, &mut rng);
            line.push(',');
            push_field_str(&mut line, &camp, &mut rng);
            line.push(',');
            let form = rng.gen_range(1000);
            push_ts_oct(&mut line, ts_sec, form, &mut rng);
            line.push('\n');
            let fidx = if rng.pct(250) { 0 } else { 1 + (rng.gen_range(2) as usize) };
            writers[fidx].write_all(line.as_bytes())?;
            if rng.pct(20) {
                writers[fidx].write_all(line.as_bytes())?;
            }
        }
    }

    Ok(())
}
