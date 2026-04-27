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
