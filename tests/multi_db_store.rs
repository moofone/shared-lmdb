use shared_lmdb::{LmdbError, LmdbMultiDbStore, MultiDbStoreConfig};

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
