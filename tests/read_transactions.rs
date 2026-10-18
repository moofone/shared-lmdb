use shared_lmdb::{LmdbError, LmdbMultiDbStore, MultiDbStoreConfig};
use std::{ops::ControlFlow, sync::mpsc, time::Duration};

fn store() -> (tempfile::TempDir, LmdbMultiDbStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = LmdbMultiDbStore::open(
        dir.path(),
        MultiDbStoreConfig::new(["rows", "versions"]),
        "read-transaction-test",
    )
    .unwrap();
    (dir, store)
}

#[test]
fn coherent_borrowed_reads_allow_concurrent_writer() {
    let (_dir, store) = store();
    store
        .write_transaction(|tx| {
            tx.put("rows", b"p/a", b"old")?;
            tx.put("versions", b"p/a", b"1")
        })
        .unwrap();
    store
        .read_transaction(|tx| {
            let old = tx.get_borrowed("rows", b"p/a")?.unwrap();
            let writer = store.clone();
            let (send, recv) = mpsc::channel();
            let worker = std::thread::spawn(move || {
                let result = writer.write_transaction(|tx| {
                    tx.put("rows", b"p/a", b"new")?;
                    tx.put("versions", b"p/a", b"2")
                });
                send.send(result).unwrap();
            });
            recv.recv_timeout(Duration::from_secs(5))
                .expect("reader must not hold writer lock")?;
            worker.join().unwrap();
            assert_eq!(old, b"old");
            tx.walk_prefix_while("rows", b"p/", b"", |key, value| {
                assert_eq!(value, b"old");
                assert_eq!(tx.get_borrowed("versions", key)?, Some(b"1".as_slice()));
                Ok(ControlFlow::Continue(()))
            })
        })
        .unwrap();
    store
        .read_transaction(|tx| {
            assert_eq!(tx.get_borrowed("rows", b"p/a")?, Some(b"new".as_slice()));
            assert_eq!(tx.get_borrowed("versions", b"p/a")?, Some(b"2".as_slice()));
            Ok(())
        })
        .unwrap();
}

#[test]
fn prefix_seek_stop_and_errors_are_explicit() {
    let (_dir, store) = store();
    store
        .write_transaction(|tx| {
            for key in [b"a/0", b"p/a", b"p/b", b"p/c", b"q/0"] {
                tx.put("rows", key, key)?;
            }
            Ok(())
        })
        .unwrap();
    store
        .read_transaction(|tx| {
            let mut keys = Vec::new();
            tx.walk_prefix_while("rows", b"p/", b"p/b", |key, _| {
                keys.push(key.to_vec());
                Ok(ControlFlow::Continue(()))
            })?;
            assert_eq!(keys, [b"p/b", b"p/c"]);
            let mut calls = 0;
            tx.walk_prefix_while("rows", b"p/", b"", |key, _| {
                assert_eq!(key, b"p/a");
                calls += 1;
                Ok(ControlFlow::Break(()))
            })?;
            assert_eq!(calls, 1);
            tx.walk_prefix_while("rows", b"p/", b"q/0", |_, _| panic!("outside prefix"))?;
            assert!(matches!(
                tx.get_borrowed("unknown", b"k"),
                Err(LmdbError::InvalidKey(_))
            ));
            assert_eq!(tx.get_borrowed("rows", b"absent")?, None);
            let err = tx
                .walk_prefix("rows", b"p/", b"", |_, _| {
                    Err(LmdbError::Validation("callback failure".into()))
                })
                .unwrap_err();
            assert!(matches!(err, LmdbError::Validation(ref s) if s == "callback failure"));
            Ok(())
        })
        .unwrap();
    // Success/error closures must both release their reader slots.
    for _ in 0..512 {
        let result: Result<(), _> =
            store.read_transaction(|_| Err(LmdbError::Validation("stop".into())));
        assert!(result.is_err());
        store.read_transaction(|_| Ok(())).unwrap();
    }
}
