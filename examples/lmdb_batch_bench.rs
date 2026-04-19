use std::time::Instant;

use shared_lmdb::{LmdbTimeseriesStore, RotationPolicy, StoreConfig};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let unique = format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let path = std::env::temp_dir()
        .join("shared-lmdb-benches")
        .join(unique);
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn payload(v: u64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn parse_count() -> u64 {
    std::env::args()
        .nth(1)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(100_000)
}

fn main() {
    let n = parse_count();
    let dir = temp_dir("shared-lmdb-bench");
    let cfg = StoreConfig::new(
        "bench",
        RotationPolicy::Circular {
            max_count: n as usize + 8,
        },
    );
    let store = LmdbTimeseriesStore::open(dir.as_path(), cfg, "bench-store").expect("open store");

    let rows = (0..n).map(|ts| (ts, payload(ts))).collect::<Vec<_>>();
    let refs = rows
        .iter()
        .map(|(ts, raw)| (*ts, raw.as_slice()))
        .collect::<Vec<_>>();

    let t_batch_refs = Instant::now();
    store
        .upsert_symbol_batch_refs("BTCUSDT", refs.as_slice(), |_, _, _| Ok(()))
        .expect("batch refs");
    let d_batch_refs = t_batch_refs.elapsed();

    store
        .replace_symbol_history("BTCUSDT", std::iter::empty::<(u64, &[u8])>())
        .expect("clear");

    let t_single = Instant::now();
    for (ts, raw) in &rows {
        store
            .upsert_symbol_sample("BTCUSDT", *ts, raw.as_slice(), |_| Ok(()))
            .expect("single");
    }
    let d_single = t_single.elapsed();

    let loaded = store
        .load_symbol_from("BTCUSDT", 0)
        .expect("load after single");

    println!(
        "encoding={} n={} batch_refs_ms={} single_ms={} batch_refs_per_sec={:.0} single_per_sec={:.0} loaded={}",
        if cfg!(feature = "binary-keys") {
            "binary"
        } else {
            "string"
        },
        n,
        d_batch_refs.as_millis(),
        d_single.as_millis(),
        n as f64 / d_batch_refs.as_secs_f64(),
        n as f64 / d_single.as_secs_f64(),
        loaded.len(),
    );
}
