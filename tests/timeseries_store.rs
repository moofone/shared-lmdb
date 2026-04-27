use std::time::Instant;

use shared_lmdb::{LmdbError, LmdbTimeseriesStore, RotationPolicy, StoreConfig, resolve_data_dir};

fn temp_dir(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(name)
        .tempdir()
        .expect("tempdir")
}

fn open_store(root: &std::path::Path, policy: RotationPolicy) -> LmdbTimeseriesStore {
    let cfg = StoreConfig::new("ts", policy);
    LmdbTimeseriesStore::open(root, cfg, "test-store").expect("open store")
}

fn payload(v: u64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn decode_u64(raw: &[u8]) -> u64 {
    let mut b = [0_u8; 8];
    b.copy_from_slice(raw);
    u64::from_le_bytes(b)
}

#[test]
fn replace_then_load_is_sorted_and_filtered() {
    let dir = temp_dir("shared-lmdb-replace-load");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });
    let rows = [(3_u64, payload(30)), (1, payload(10)), (2, payload(20))];
    let refs = rows
        .iter()
        .map(|(ts, val)| (*ts, val.as_slice()))
        .collect::<Vec<_>>();
    store.replace_history("BTCUSDT", refs).expect("replace");

    let loaded = store.load_from("BTCUSDT", 2).expect("load from filtered");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].0, 2);
    assert_eq!(loaded[1].0, 3);
    assert_eq!(decode_u64(&loaded[0].1), 20);
    assert_eq!(decode_u64(&loaded[1].1), 30);
}

#[test]
fn replace_only_affects_target_symbol() {
    let dir = temp_dir("shared-lmdb-symbol-isolation");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });

    store
        .replace_history("BTCUSDT", vec![(1_u64, payload(1).as_slice())])
        .expect("replace btc");
    store
        .replace_history("ETHUSDT", vec![(1_u64, payload(2).as_slice())])
        .expect("replace eth");

    store
        .replace_history("BTCUSDT", vec![(5_u64, payload(5).as_slice())])
        .expect("replace btc again");

    let btc = store.load_from("BTCUSDT", 0).expect("load btc");
    let eth = store.load_from("ETHUSDT", 0).expect("load eth");
    assert_eq!(btc.len(), 1);
    assert_eq!(btc[0].0, 5);
    assert_eq!(eth.len(), 1);
    assert_eq!(eth[0].0, 1);
}

#[test]
fn upsert_duplicate_validates_existing() {
    let dir = temp_dir("shared-lmdb-upsert-validate");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });

    store
        .upsert_sample("BTCUSDT", 10, payload(1).as_slice(), |_| Ok(()))
        .expect("initial upsert");
    store
        .upsert_sample("BTCUSDT", 10, payload(1).as_slice(), |existing| {
            if decode_u64(existing) == 1 {
                Ok(())
            } else {
                Err(LmdbError::Validation("bad existing".to_string()))
            }
        })
        .expect("same-value upsert");

    let err = store
        .upsert_sample("BTCUSDT", 10, payload(999).as_slice(), |_| {
            Err(LmdbError::Conflict("conflict".to_string()))
        })
        .expect_err("conflicting duplicate should fail");
    assert!(err.to_string().contains("conflict"));
}

#[test]
fn circular_rotation_enforces_rolling_window() {
    let dir = temp_dir("shared-lmdb-trim");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });

    for ts in 0_u64..150 {
        store
            .upsert_sample("BTCUSDT", ts, payload(ts).as_slice(), |_| Ok(()))
            .expect("upsert");
    }

    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.len(), 100);
    assert_eq!(loaded.first().map(|v| v.0), Some(50));
    assert_eq!(loaded.last().map(|v| v.0), Some(149));
}

#[test]
fn max_age_rotation_keeps_recent_time_window() {
    let dir = temp_dir("shared-lmdb-age");
    let store = open_store(dir.path(), RotationPolicy::MaxAgeMs { max_age_ms: 20 });

    for ts in 100_u64..141 {
        store
            .upsert_sample("BTCUSDT", ts, payload(ts).as_slice(), |_| Ok(()))
            .expect("upsert");
    }

    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.first().map(|v| v.0), Some(120));
    assert_eq!(loaded.last().map(|v| v.0), Some(140));
}

#[test]
fn forever_rotation_keeps_all_samples() {
    let dir = temp_dir("shared-lmdb-forever");
    let store = open_store(dir.path(), RotationPolicy::Forever);

    for ts in 0_u64..2_000 {
        store
            .upsert_sample("BTCUSDT", ts, payload(ts).as_slice(), |_| Ok(()))
            .expect("upsert");
    }
    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.len(), 2_000);
}

#[test]
fn batch_upsert_commits_all_rows() {
    let dir = temp_dir("shared-lmdb-batch");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 20_000 });

    let rows = (0_u64..10_000)
        .map(|ts| (ts, payload(ts)))
        .collect::<Vec<_>>();
    store
        .upsert_batch("BTCUSDT", rows.as_slice(), |_, _, _| Ok(()))
        .expect("batch upsert");

    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.len(), 10_000);
    assert_eq!(decode_u64(&loaded[9_999].1), 9_999);
}

#[test]
fn batch_upsert_refs_commits_all_rows() {
    let dir = temp_dir("shared-lmdb-batch-refs");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 20_000 });

    let owned = (0_u64..10_000)
        .map(|ts| (ts, payload(ts)))
        .collect::<Vec<_>>();
    let refs = owned
        .iter()
        .map(|(ts, raw)| (*ts, raw.as_slice()))
        .collect::<Vec<_>>();
    store
        .upsert_batch_refs("BTCUSDT", refs.as_slice(), |_, _, _| Ok(()))
        .expect("batch upsert refs");

    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.len(), 10_000);
    assert_eq!(decode_u64(&loaded[9_999].1), 9_999);
}

#[test]
fn batch_upsert_validates_conflicts() {
    let dir = temp_dir("shared-lmdb-batch-conflict");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });

    store
        .upsert_sample("BTCUSDT", 5, payload(5).as_slice(), |_| Ok(()))
        .expect("seed");
    let rows = vec![(5_u64, payload(999))];

    let err = store
        .upsert_batch("BTCUSDT", rows.as_slice(), |ts, existing, incoming| {
            if decode_u64(existing) != decode_u64(incoming) {
                return Err(LmdbError::Conflict(format!("conflict at {ts}")));
            }
            Ok(())
        })
        .expect_err("batch conflict expected");
    assert!(err.to_string().contains("conflict at 5"));
}

#[test]
fn persistence_survives_reopen() {
    let dir = temp_dir("shared-lmdb-reopen");
    {
        let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });
        store
            .upsert_sample("BTCUSDT", 1, payload(7).as_slice(), |_| Ok(()))
            .expect("upsert");
    }

    {
        let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });
        let loaded = store.load_from("BTCUSDT", 0).expect("load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(decode_u64(&loaded[0].1), 7);
    }
}

#[test]
fn keyspace_isolation_under_heavy_multi_symbol_batch() {
    let dir = temp_dir("shared-lmdb-multi");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 10_000 });

    let btc = (0_u64..5_000)
        .map(|ts| (ts, payload(ts)))
        .collect::<Vec<_>>();
    let eth = (0_u64..5_000)
        .map(|ts| (ts, payload(ts + 100_000)))
        .collect::<Vec<_>>();

    store
        .upsert_batch("BTCUSDT", btc.as_slice(), |_, _, _| Ok(()))
        .expect("btc batch");
    store
        .upsert_batch("ETHUSDT", eth.as_slice(), |_, _, _| Ok(()))
        .expect("eth batch");

    let btc_loaded = store.load_from("BTCUSDT", 0).expect("load btc");
    let eth_loaded = store.load_from("ETHUSDT", 0).expect("load eth");
    assert_eq!(btc_loaded.len(), 5_000);
    assert_eq!(eth_loaded.len(), 5_000);
    assert_eq!(decode_u64(&btc_loaded[4_999].1), 4_999);
    assert_eq!(decode_u64(&eth_loaded[4_999].1), 104_999);
}

#[test]
fn perf_smoke_batch_vs_single_upsert() {
    let dir = temp_dir("shared-lmdb-perf");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 20_010 });

    let n = 20_000_u64;
    let rows = (0_u64..n).map(|ts| (ts, payload(ts))).collect::<Vec<_>>();

    let t1 = Instant::now();
    for (ts, raw) in &rows {
        store
            .upsert_sample("BTCUSDT", *ts, raw.as_slice(), |_| Ok(()))
            .expect("single upsert");
    }
    let single = t1.elapsed();

    store
        .replace_history("BTCUSDT", std::iter::empty::<(u64, &[u8])>())
        .expect("clear symbol");

    let t2 = Instant::now();
    store
        .upsert_batch("BTCUSDT", rows.as_slice(), |_, _, _| Ok(()))
        .expect("batch upsert");
    let batch = t2.elapsed();

    assert!(
        batch <= single * 2,
        "batch unexpectedly slower: batch={batch:?} single={single:?}"
    );
}

#[test]
fn resolve_data_dir_uses_env_override_when_set() {
    let dir = temp_dir("shared-lmdb-env");
    let override_dir = dir.path().join("override");
    std::fs::create_dir_all(&override_dir).expect("create override");
    let key = "RUST_BOT_TEST_SHARED_LMDB_DIR";
    // SAFETY: tests in this crate use unique env keys.
    unsafe { std::env::set_var(key, override_dir.as_os_str()) };
    let resolved = resolve_data_dir(dir.path(), key);
    assert_eq!(resolved, override_dir);
    // SAFETY: paired cleanup for the test-local env key.
    unsafe { std::env::remove_var(key) };
}

#[test]
fn resolve_data_dir_falls_back_to_default_root() {
    let dir = temp_dir("shared-lmdb-env-default");
    let key = "RUST_BOT_TEST_SHARED_LMDB_DIR_DEFAULT";
    // SAFETY: paired cleanup for the test-local env key.
    unsafe { std::env::remove_var(key) };
    let resolved = resolve_data_dir(dir.path(), key);
    assert_eq!(resolved, dir.path());
}

#[cfg(feature = "binary-keys")]
#[test]
fn binary_keys_symbol_prefix_isolation_under_shared_env() {
    let dir = temp_dir("shared-lmdb-binary-prefix-isolation");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 100 });

    let btc_rows = (0_u64..25)
        .map(|ts| (ts, payload(1_000 + ts)))
        .collect::<Vec<_>>();
    let eth_rows = (0_u64..25)
        .map(|ts| (ts, payload(2_000 + ts)))
        .collect::<Vec<_>>();

    store
        .upsert_batch("BTCUSDT", btc_rows.as_slice(), |_, _, _| Ok(()))
        .expect("btc upsert");
    store
        .upsert_batch("ETHUSDT", eth_rows.as_slice(), |_, _, _| Ok(()))
        .expect("eth upsert");

    let btc = store.load_from("BTCUSDT", 0).expect("load btc");
    let eth = store.load_from("ETHUSDT", 0).expect("load eth");
    assert_eq!(btc.len(), 25);
    assert_eq!(eth.len(), 25);
    assert_eq!(decode_u64(&btc[0].1), 1_000);
    assert_eq!(decode_u64(&eth[0].1), 2_000);
}

#[cfg(feature = "binary-keys")]
#[test]
fn binary_keys_support_boundary_timestamps() {
    let dir = temp_dir("shared-lmdb-binary-boundary-ts");
    let store = open_store(dir.path(), RotationPolicy::Circular { max_count: 10 });

    let rows = vec![(0_u64, payload(7)), (u64::MAX, payload(9))];
    store
        .upsert_batch("BTCUSDT", rows.as_slice(), |_, _, _| Ok(()))
        .expect("upsert boundary rows");

    let loaded = store.load_from("BTCUSDT", 0).expect("load");
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].0, 0);
    assert_eq!(loaded[1].0, u64::MAX);
    assert_eq!(decode_u64(&loaded[0].1), 7);
    assert_eq!(decode_u64(&loaded[1].1), 9);
}
