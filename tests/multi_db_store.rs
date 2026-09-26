use shared_lmdb::{LmdbError, LmdbMultiDbStore, MultiDbStoreConfig};
use std::sync::{Arc, Barrier};

fn temp_dir(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(name)
        .tempdir()
        .expect("tempdir")
}

fn open_store(root: &std::path::Path) -> LmdbMultiDbStore {
    let cfg = MultiDbStoreConfig::new(["raft_log", "auth_events", "watermarks"]);
    LmdbMultiDbStore::open(root, cfg, "multi-db-test").expect("open multi db")
}

fn data_file_bytes(root: &std::path::Path) -> Vec<u8> {
    std::fs::read(root.join("data.mdb")).expect("read LMDB data file")
}

fn directory_entries(root: &std::path::Path) -> Vec<std::ffi::OsString> {
    let mut entries = std::fs::read_dir(root)
        .expect("read LMDB directory")
        .map(|entry| entry.expect("read LMDB directory entry").file_name())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

fn named_database_exists(root: &std::path::Path, db_name: &str) -> bool {
    let env = unsafe {
        let mut options = heed::EnvOpenOptions::new();
        options
            .max_dbs(8)
            .max_readers(256)
            .flags(heed::EnvFlags::READ_ONLY);
        options.open(root)
    }
    .expect("open existing LMDB environment read-only");
    let rtxn = env.read_txn().expect("open inspection read transaction");
    env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(db_name))
        .expect("inspect named database")
        .is_some()
}

#[test]
fn read_only_existing_store_reads_and_scans_but_rejects_mutation() {
    let dir = temp_dir("shared-lmdb-existing-read-only");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0002", b"log-entry-2")?;
            txn.put("raft_log", b"0001", b"log-entry-1")?;
            txn.put("watermarks", b"applied", b"0002")
        })
        .expect("seed existing store");
    store.force_sync().expect("sync seeded store");
    drop(store);

    let data_before = data_file_bytes(dir.path());
    let read_only = LmdbMultiDbStore::open_existing_read_only(
        dir.path(),
        MultiDbStoreConfig::new(["raft_log", "auth_events", "watermarks"]),
        "existing-read-only",
    )
    .expect("open all existing databases read-only");

    assert_eq!(
        read_only
            .read("watermarks", b"applied")
            .expect("read existing row"),
        Some(b"0002".to_vec())
    );
    assert_eq!(
        read_only.scan("raft_log").expect("scan existing rows"),
        vec![
            (b"0001".to_vec(), b"log-entry-1".to_vec()),
            (b"0002".to_vec(), b"log-entry-2".to_vec()),
        ]
    );

    let mutation = read_only
        .write_transaction(|txn| txn.put("watermarks", b"applied", b"0003"))
        .expect_err("read-only store must reject a write transaction");
    assert!(matches!(mutation, LmdbError::Heed { .. }));
    drop(read_only);

    assert_eq!(data_file_bytes(dir.path()), data_before);
    let reopened = LmdbMultiDbStore::open_existing_read_only(
        dir.path(),
        MultiDbStoreConfig::new(["raft_log", "auth_events", "watermarks"]),
        "reopened-read-only",
    )
    .expect("reopen unchanged existing store");
    assert_eq!(
        reopened
            .read("watermarks", b"applied")
            .expect("read unchanged row"),
        Some(b"0002".to_vec())
    );
}

#[test]
fn read_only_existing_store_rejects_a_missing_named_db_without_creating_or_modifying_anything() {
    let dir = temp_dir("shared-lmdb-existing-read-only-missing-db");
    let store = LmdbMultiDbStore::open(
        dir.path(),
        MultiDbStoreConfig::new(["present"]),
        "seed-existing-store",
    )
    .expect("create existing store");
    store
        .write_transaction(|txn| txn.put("present", b"key", b"value"))
        .expect("seed existing row");
    store.force_sync().expect("sync seeded store");
    drop(store);

    let entries_before = directory_entries(dir.path());
    let data_before = data_file_bytes(dir.path());
    let error = LmdbMultiDbStore::open_existing_read_only(
        dir.path(),
        MultiDbStoreConfig::new(["present", "missing"]),
        "existing-read-only-missing-db",
    )
    .expect_err("every configured named database must already exist");

    assert!(
        matches!(&error, LmdbError::Validation(message) if message.contains("missing")),
        "unexpected missing-database error: {error}"
    );
    assert_eq!(directory_entries(dir.path()), entries_before);
    assert_eq!(data_file_bytes(dir.path()), data_before);
    assert!(!named_database_exists(dir.path(), "missing"));
    assert!(named_database_exists(dir.path(), "present"));
}

#[test]
fn completed_reads_release_slots_while_reader_threads_remain_alive() {
    let dir = temp_dir("shared-lmdb-live-reader-slots");
    let mut cfg = MultiDbStoreConfig::new(["current"]);
    cfg.max_readers = 4;
    let store = LmdbMultiDbStore::open(dir.path(), cfg, "live-reader-slots")
        .expect("open constrained multi db");
    store
        .write_transaction(|txn| txn.put("current", b"authority", b"record"))
        .expect("seed current record");

    let barrier = Arc::new(Barrier::new(5));
    let readers = (0..4)
        .map(|_| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                assert_eq!(
                    store.read("current", b"authority").expect("thread read"),
                    Some(b"record".to_vec())
                );
                barrier.wait();
                barrier.wait();
            })
        })
        .collect::<Vec<_>>();

    barrier.wait();
    let read_after_threads_completed = store.read("current", b"authority");
    barrier.wait();
    for reader in readers {
        reader.join().expect("reader thread");
    }

    assert_eq!(
        read_after_threads_completed.expect("completed reads must release their LMDB slots"),
        Some(b"record".to_vec())
    );
}

#[test]
fn multi_db_transaction_commits_all_named_dbs_atomically() {
    let dir = temp_dir("shared-lmdb-multi-db-commit");
    let store = open_store(dir.path());

    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0001", b"log-entry-1")?;
            txn.put("auth_events", b"0001", b"event-1")?;
            txn.put("watermarks", b"applied", b"0001")?;
            Ok(())
        })
        .expect("commit transaction");

    assert_eq!(
        store.read("raft_log", b"0001").expect("read log"),
        Some(b"log-entry-1".to_vec())
    );
    assert_eq!(
        store.read("auth_events", b"0001").expect("read event"),
        Some(b"event-1".to_vec())
    );
    assert_eq!(
        store
            .read("watermarks", b"applied")
            .expect("read watermark"),
        Some(b"0001".to_vec())
    );
}

#[test]
fn multi_db_transaction_rolls_back_all_dbs_on_error() {
    let dir = temp_dir("shared-lmdb-multi-db-rollback");
    let store = open_store(dir.path());

    let err = store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0001", b"log-entry-1")?;
            txn.put("auth_events", b"0001", b"event-1")?;
            Err::<(), _>(LmdbError::Conflict("abort transaction".to_string()))
        })
        .expect_err("transaction should abort");

    assert!(err.to_string().contains("abort transaction"));
    assert_eq!(store.read("raft_log", b"0001").expect("read log"), None);
    assert_eq!(
        store.read("auth_events", b"0001").expect("read event"),
        None
    );
}

#[test]
fn write_txn_walks_a_prefix_in_order_from_an_inclusive_start_key() {
    let dir = temp_dir("shared-lmdb-multi-db-txn-walk");
    let store = open_store(dir.path());
    let rows = store
        .write_transaction(|txn| {
            txn.put("raft_log", b"tmp/0003", b"three")?;
            txn.put("raft_log", b"tmp/0001", b"one")?;
            txn.put("raft_log", b"tmp/0002", b"two")?;
            txn.put("raft_log", b"tmp-other/0000", b"other")?;

            let mut rows = Vec::new();
            txn.walk_prefix("raft_log", b"tmp/", b"tmp/0002", |key, value| {
                rows.push((key.to_vec(), value.to_vec()));
                Ok(())
            })?;
            Ok(rows)
        })
        .expect("walk temporary rows");

    assert_eq!(
        rows,
        vec![
            (b"tmp/0002".to_vec(), b"two".to_vec()),
            (b"tmp/0003".to_vec(), b"three".to_vec()),
        ]
    );
}

#[test]
fn write_txn_prefix_delete_returns_count_and_rolls_back_atomically() {
    let dir = temp_dir("shared-lmdb-multi-db-txn-prefix-delete");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"tmp/0001", b"one")?;
            txn.put("raft_log", b"tmp/0002", b"two")?;
            txn.put("raft_log", b"keep", b"keep")
        })
        .expect("seed temporary rows");

    let deleted = store
        .write_transaction(|txn| txn.delete_prefix("raft_log", b"tmp/"))
        .expect("delete temporary rows");
    assert_eq!(deleted, 2);
    assert_eq!(
        store
            .read("raft_log", b"tmp/0001")
            .expect("read deleted row"),
        None
    );
    assert_eq!(
        store.read("raft_log", b"keep").expect("read retained row"),
        Some(b"keep".to_vec())
    );

    store
        .write_transaction(|txn| txn.put("raft_log", b"tmp/0003", b"three"))
        .expect("seed row for rollback");
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"tmp/0004", b"four")?;
            txn.put("auth_events", b"event", b"event")?;
            let _ = txn.delete_prefix("raft_log", b"tmp/")?;
            Err::<(), _>(LmdbError::Conflict("abort temporary cleanup".to_string()))
        })
        .expect_err("failed cleanup must abort the whole transaction");

    assert_eq!(
        store
            .read("raft_log", b"tmp/0003")
            .expect("read rolled-back row"),
        Some(b"three".to_vec())
    );
    assert_eq!(
        store
            .read("raft_log", b"tmp/0004")
            .expect("read uncommitted row"),
        None
    );
    assert_eq!(
        store
            .read("auth_events", b"event")
            .expect("read rolled-back event"),
        None
    );
}

#[test]
fn multi_db_transaction_can_validate_existing_values_before_write() {
    let dir = temp_dir("shared-lmdb-multi-db-validate");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| txn.put("watermarks", b"applied", b"0001"))
        .expect("seed");

    store
        .write_transaction(|txn| {
            let current = txn.get("watermarks", b"applied")?;
            if current.as_deref() != Some(b"0001".as_slice()) {
                return Err(LmdbError::Conflict("unexpected watermark".to_string()));
            }
            txn.put("auth_events", b"0002", b"event-2")?;
            txn.put("watermarks", b"applied", b"0002")
        })
        .expect("validated commit");

    assert_eq!(
        store
            .read("watermarks", b"applied")
            .expect("read watermark"),
        Some(b"0002".to_vec())
    );
    assert_eq!(
        store.read("auth_events", b"0002").expect("read event"),
        Some(b"event-2".to_vec())
    );
}

#[test]
fn multi_db_scan_returns_sorted_rows_for_one_named_db() {
    let dir = temp_dir("shared-lmdb-multi-db-scan");
    let store = open_store(dir.path());

    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0002", b"log-entry-2")?;
            txn.put("auth_events", b"0001", b"event-1")?;
            txn.put("raft_log", b"0001", b"log-entry-1")
        })
        .expect("seed rows");

    assert_eq!(
        store.scan("raft_log").expect("scan raft log"),
        vec![
            (b"0001".to_vec(), b"log-entry-1".to_vec()),
            (b"0002".to_vec(), b"log-entry-2".to_vec()),
        ]
    );
}

#[test]
fn multi_db_store_can_force_a_committed_transaction_to_stable_storage() {
    let dir = temp_dir("shared-lmdb-multi-db-force-sync");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| txn.put("watermarks", b"applied", b"0001"))
        .expect("commit transaction");

    store.force_sync().expect("force stable-storage sync");
}

#[test]
fn snapshot_install_streams_a_stable_source_and_replaces_destinations_atomically() {
    let dir = temp_dir("shared-lmdb-snapshot-install");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0001", b"snapshot-row-1")?;
            txn.put("raft_log", b"0002", b"snapshot-row-2")?;
            txn.put("auth_events", b"stale", b"stale-event")?;
            txn.put("watermarks", b"stale", b"stale-watermark")
        })
        .expect("seed source and stale destinations");

    let installed_rows = store
        .install_snapshot_from_db(
            "raft_log",
            &["raft_log", "auth_events", "watermarks"],
            |txn, key, value| {
                txn.put("raft_log", key, value)?;
                txn.put("auth_events", key, value)
            },
            |txn| {
                txn.put("watermarks", b"installed", b"0002")?;
                Ok(2_usize)
            },
        )
        .expect("install snapshot");

    assert_eq!(installed_rows, 2);
    assert_eq!(
        store.scan("raft_log").expect("scan replaced source"),
        vec![
            (b"0001".to_vec(), b"snapshot-row-1".to_vec()),
            (b"0002".to_vec(), b"snapshot-row-2".to_vec()),
        ]
    );
    assert_eq!(
        store
            .scan("auth_events")
            .expect("scan replaced destination"),
        vec![
            (b"0001".to_vec(), b"snapshot-row-1".to_vec()),
            (b"0002".to_vec(), b"snapshot-row-2".to_vec()),
        ]
    );
    assert_eq!(
        store
            .scan("watermarks")
            .expect("scan finalized destination"),
        vec![(b"installed".to_vec(), b"0002".to_vec())]
    );
}

#[test]
fn snapshot_install_rolls_back_clears_and_rows_when_a_row_callback_fails() {
    let dir = temp_dir("shared-lmdb-snapshot-row-rollback");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0001", b"snapshot-row-1")?;
            txn.put("raft_log", b"0002", b"snapshot-row-2")?;
            txn.put("auth_events", b"old", b"old-event")?;
            txn.put("watermarks", b"installed", b"old-position")
        })
        .expect("seed source and installed state");

    let err = store
        .install_snapshot_from_db(
            "raft_log",
            &["auth_events", "watermarks"],
            |txn, key, value| {
                if key == b"0002" {
                    return Err(LmdbError::Conflict("reject snapshot row".to_string()));
                }
                txn.put("auth_events", key, value)
            },
            |_| Ok(()),
        )
        .expect_err("row rejection must abort the whole install");

    assert!(err.to_string().contains("reject snapshot row"));
    assert_eq!(
        store.scan("auth_events").expect("scan rolled-back events"),
        vec![(b"old".to_vec(), b"old-event".to_vec())]
    );
    assert_eq!(
        store
            .scan("watermarks")
            .expect("scan rolled-back watermark"),
        vec![(b"installed".to_vec(), b"old-position".to_vec())]
    );
}

#[test]
fn snapshot_install_rolls_back_when_the_finalize_callback_fails() {
    let dir = temp_dir("shared-lmdb-snapshot-finalize-rollback");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            txn.put("raft_log", b"0001", b"snapshot-row-1")?;
            txn.put("auth_events", b"old", b"old-event")?;
            txn.put("watermarks", b"installed", b"old-position")
        })
        .expect("seed source and installed state");

    let err = store
        .install_snapshot_from_db(
            "raft_log",
            &["auth_events", "watermarks"],
            |txn, key, value| txn.put("auth_events", key, value),
            |txn| {
                txn.put("watermarks", b"installed", b"new-position")?;
                Err::<(), _>(LmdbError::Conflict("reject final state".to_string()))
            },
        )
        .expect_err("finalize rejection must abort the whole install");

    assert!(err.to_string().contains("reject final state"));
    assert_eq!(
        store.scan("auth_events").expect("scan rolled-back events"),
        vec![(b"old".to_vec(), b"old-event".to_vec())]
    );
    assert_eq!(
        store
            .scan("watermarks")
            .expect("scan rolled-back watermark"),
        vec![(b"installed".to_vec(), b"old-position".to_vec())]
    );
}

/// Callers that are handed an already-open store (shared-sync's
/// `from_lmdb_at`) must prove which on-disk image backs it. The store reports
/// the canonical directory its environment was opened at, so a caller can
/// compare the `data.mdb` it expects with the one actually mapped.
#[test]
fn env_path_is_the_canonical_directory_the_env_was_opened_at() {
    let dir = temp_dir("shared-lmdb-env-path");
    let root = dir.path().join("image");
    let store = open_store(&root);
    let canonical = root.canonicalize().expect("canonical image root");
    assert_eq!(store.env_path(), canonical.as_path());
    assert!(store.env_path().join("data.mdb").is_file());

    // Opened through a symlink, the reported path is still the real directory.
    #[cfg(unix)]
    {
        drop(store);
        let link = dir.path().join("image-link");
        std::os::unix::fs::symlink(&root, &link).expect("symlink to image");
        let through_link = open_store(&link);
        assert_eq!(through_link.env_path(), canonical.as_path());
    }
}

/// Page readers (shared-sync current-state paging) need `get` + a borrowed
/// prefix walk over one coherent snapshot WITHOUT holding the LMDB writer
/// lock. A read transaction must let a concurrent writer commit, and must keep
/// seeing its own snapshot while it does.
#[test]
fn read_transaction_walks_a_snapshot_without_blocking_writers() {
    let dir = temp_dir("shared-lmdb-read-txn");
    let store = Arc::new(open_store(dir.path()));
    store
        .write_transaction(|txn| {
            txn.put("auth_events", b"p/1", b"one")?;
            txn.put("auth_events", b"p/2", b"two")?;
            txn.put("auth_events", b"q/1", b"outside")?;
            txn.put("watermarks", b"p/1", b"v1")
        })
        .expect("seed");

    let writer_store = Arc::clone(&store);
    let (seen, committed_during_read) = store
        .read_transaction(|txn| {
            let mut seen = Vec::new();
            txn.walk_prefix("auth_events", b"p/", b"p/", |key, value| {
                seen.push((key.to_vec(), value.to_vec()));
                Ok(())
            })?;
            // A writer on another thread must commit while this read is open.
            let writer = std::thread::spawn(move || {
                writer_store.write_transaction(|txn| txn.put("auth_events", b"p/3", b"three"))
            });
            let committed = writer.join().expect("writer thread").is_ok();
            // Still the original snapshot: p/3 is not visible here.
            assert_eq!(txn.get("auth_events", b"p/3")?, None);
            assert_eq!(txn.get("watermarks", b"p/1")?.as_deref(), Some(&b"v1"[..]));
            Ok((seen, committed))
        })
        .expect("read transaction");

    assert!(
        committed_during_read,
        "a writer must not wait on an open read transaction"
    );
    assert_eq!(
        seen,
        vec![
            (b"p/1".to_vec(), b"one".to_vec()),
            (b"p/2".to_vec(), b"two".to_vec())
        ]
    );
    assert_eq!(
        store.read("auth_events", b"p/3").unwrap().as_deref(),
        Some(&b"three"[..])
    );
}

#[test]
fn read_transaction_walk_starts_at_the_inclusive_start_key_and_stops_at_the_prefix() {
    let dir = temp_dir("shared-lmdb-read-txn-start");
    let store = open_store(dir.path());
    store
        .write_transaction(|txn| {
            for key in [&b"p/1"[..], b"p/2", b"p/3", b"pz", b"q/1"] {
                txn.put("auth_events", key, b"v")?;
            }
            Ok(())
        })
        .expect("seed");
    let keys = store
        .read_transaction(|txn| {
            let mut keys = Vec::new();
            txn.walk_prefix("auth_events", b"p/", b"p/2", |key, _| {
                keys.push(key.to_vec());
                Ok(())
            })?;
            Ok(keys)
        })
        .expect("read transaction");
    assert_eq!(keys, vec![b"p/2".to_vec(), b"p/3".to_vec()]);
}
