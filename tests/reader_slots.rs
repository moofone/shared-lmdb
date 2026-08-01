use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use shared_lmdb::{LmdbMultiDbStore, MultiDbStoreConfig};

const CHILD_MODE_ENV: &str = "SHARED_LMDB_READER_SLOT_CHILD_MODE";
const ROOT_ENV: &str = "SHARED_LMDB_READER_SLOT_ROOT";
const READY_ENV: &str = "SHARED_LMDB_READER_SLOT_READY";

fn constrained_config() -> MultiDbStoreConfig {
    let mut config = MultiDbStoreConfig::new(["current"]);
    config.max_readers = 4;
    config
}

fn child_command(root: &Path, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .args(["--exact", "reader_slot_child", "--nocapture"])
        .env(CHILD_MODE_ENV, mode)
        .env(ROOT_ENV, root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

fn wait_until_ready(child: &mut Child, marker: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() {
        if let Some(status) = child.try_wait().expect("poll reader child") {
            let stderr = child
                .stderr
                .take()
                .map(|mut stderr| {
                    let mut output = String::new();
                    std::io::Read::read_to_string(&mut stderr, &mut output)
                        .expect("read reader child stderr");
                    output
                })
                .unwrap_or_default();
            panic!("reader child exited before claiming its slot: {status}: {stderr}");
        }
        assert!(
            Instant::now() < deadline,
            "reader child did not become ready"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn reader_slot_child() {
    let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT_ENV).expect("reader child root"));
    if mode == "verify-open" {
        let store = LmdbMultiDbStore::open(&root, constrained_config(), "reader-slot-verifier")
            .expect("open store after killed readers");
        store
            .read("current", b"authority")
            .expect("read after killed readers");
        return;
    }
    assert_eq!(mode, "hold-read");

    // SAFETY: this fixture uses one dedicated temporary environment, and all
    // other handles are opened in separate processes as LMDB requires.
    let env = unsafe {
        let config = constrained_config();
        let mut options = heed::EnvOpenOptions::new();
        options
            .map_size(config.map_size_bytes)
            .max_dbs(config.max_dbs)
            .max_readers(config.max_readers);
        options.flags(heed::EnvFlags::NO_SYNC);
        options.open(&root)
    }
    .expect("open raw reader environment");
    let _read_transaction = env.read_txn().expect("claim reader slot");
    let marker = PathBuf::from(std::env::var_os(READY_ENV).expect("reader child marker"));
    std::fs::write(marker, b"ready").expect("publish reader readiness");
    loop {
        std::thread::park();
    }
}

#[test]
fn store_open_reclaims_reader_slots_left_by_killed_processes() {
    let root = tempfile::tempdir().expect("reader slot tempdir");
    let _keeper = LmdbMultiDbStore::open(root.path(), constrained_config(), "reader-slot-keeper")
        .expect("open reader slot keeper");

    for slot in 0..4 {
        let marker = root.path().join(format!("reader-{slot}.ready"));
        let mut command = child_command(root.path(), "hold-read");
        command.env(READY_ENV, &marker);
        let mut child = command.spawn().expect("spawn reader child");
        wait_until_ready(&mut child, &marker);
        child.kill().expect("kill reader child");
        child.wait().expect("reap reader child");
    }

    let verified = child_command(root.path(), "verify-open")
        .output()
        .expect("run post-kill verifier");
    assert!(
        verified.status.success(),
        "store open did not reclaim killed readers: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
}
