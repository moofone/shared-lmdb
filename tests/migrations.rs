#![cfg(feature = "migrations")]

use std::path::Path;
use std::sync::Mutex;

use shared_lmdb::migrations::{
    Codec, CodecAad, CodecError, Migration, MigrationError, MigrationOutSink, MigrationRegistry,
    MigrationRunner, ProgressEvent, ProgressSink, RunnerPolicies, verify_migration_artifacts,
};
use shared_lmdb::{LmdbTimeseriesStore, RotationPolicy, StoreConfig};

#[cfg(feature = "migrations-test")]
use proptest::prelude::*;

type SeenAad = (String, u64, Option<String>, u32, &'static str);

fn open_store(root: &Path) -> LmdbTimeseriesStore {
    LmdbTimeseriesStore::open(
        root,
        StoreConfig::new("ts", RotationPolicy::Forever),
        "migration-test-store",
    )
    .expect("open store")
}

fn run_policy() -> RunnerPolicies {
    RunnerPolicies {
        batch_size: 2,
        fsync_every_batch: true,
        allow_lossy: false,
        require_admin_signature_for_lossy: true,
        validate_with_dry_run_first: false,
    }
}

fn lossy_policy_without_signature_requirement() -> RunnerPolicies {
    RunnerPolicies {
        allow_lossy: true,
        require_admin_signature_for_lossy: false,
        ..run_policy()
    }
}

#[derive(Default)]
struct AadRecordingCodec {
    seen: Mutex<Vec<SeenAad>>,
    fail_decode_prefix: Option<Vec<u8>>,
    fail_encode_prefix: Option<Vec<u8>>,
}

impl AadRecordingCodec {
    fn with_fail_decode(prefix: &[u8]) -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            fail_decode_prefix: Some(prefix.to_vec()),
            fail_encode_prefix: None,
        }
    }

    fn seen(&self) -> Vec<SeenAad> {
        self.seen.lock().expect("seen lock").clone()
    }
}

impl Codec for AadRecordingCodec {
    fn decode(&self, raw: &[u8], aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        self.seen.lock().expect("seen lock").push((
            aad.series_key.to_string(),
            aad.timestamp,
            aad.sub_db_name.map(str::to_string),
            aad.schema_epoch,
            "decode",
        ));
        if self
            .fail_decode_prefix
            .as_ref()
            .is_some_and(|prefix| raw.starts_with(prefix))
        {
            return Err(CodecError::Message("decode rejected".to_string()));
        }
        Ok(raw.to_vec())
    }

    fn encode(&self, plain: &[u8], aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        self.seen.lock().expect("seen lock").push((
            aad.series_key.to_string(),
            aad.timestamp,
            aad.sub_db_name.map(str::to_string),
            aad.schema_epoch,
            "encode",
        ));
        if self
            .fail_encode_prefix
            .as_ref()
            .is_some_and(|prefix| plain.starts_with(prefix))
        {
            return Err(CodecError::Message("encode rejected".to_string()));
        }
        Ok(plain.to_vec())
    }

    fn key_fingerprint(&self) -> [u8; 32] {
        [7; 32]
    }
}

struct CorruptingEncodeCodec;

impl Codec for CorruptingEncodeCodec {
    fn decode(&self, raw: &[u8], _aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        if raw.starts_with(b"corrupt") {
            return Err(CodecError::Message("corrupt payload rejected".to_string()));
        }
        Ok(raw.to_vec())
    }

    fn encode(&self, _plain: &[u8], _aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        Ok(b"corrupt".to_vec())
    }

    fn key_fingerprint(&self) -> [u8; 32] {
        [7; 32]
    }
}

struct DifferentFingerprintCodec;

impl Codec for DifferentFingerprintCodec {
    fn decode(&self, raw: &[u8], _aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        Ok(raw.to_vec())
    }

    fn encode(&self, plain: &[u8], _aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError> {
        Ok(plain.to_vec())
    }

    fn key_fingerprint(&self) -> [u8; 32] {
        [8; 32]
    }
}

struct V1ToV2;

impl Migration for V1ToV2 {
    const FROM: u32 = 1;
    const TO: u32 = 2;

    fn migrate(input: &[u8], out: &mut MigrationOutSink<'_>) -> Result<(), MigrationError> {
        let mut migrated = b"v2:".to_vec();
        migrated.extend_from_slice(input);
        out.push(migrated);
        Ok(())
    }

    fn validate_batch(out: &[Vec<u8>]) -> Result<(), MigrationError> {
        if out.iter().all(|row| row.starts_with(b"v2:")) {
            Ok(())
        } else {
            Err(MigrationError::Validation(
                "batch contains unversioned row".to_string(),
            ))
        }
    }
}

struct V2ToV3;

impl Migration for V2ToV3 {
    const FROM: u32 = 2;
    const TO: u32 = 3;

    fn migrate(input: &[u8], out: &mut MigrationOutSink<'_>) -> Result<(), MigrationError> {
        let mut migrated = input.to_vec();
        migrated.extend_from_slice(b":v3");
        out.push(migrated);
        Ok(())
    }
}

struct LossyDrop;

impl Migration for LossyDrop {
    const FROM: u32 = 3;
    const TO: u32 = 4;
    const LOSSY: bool = true;

    fn migrate(_input: &[u8], _out: &mut MigrationOutSink<'_>) -> Result<(), MigrationError> {
        Ok(())
    }
}

struct MultiOutput;

impl Migration for MultiOutput {
    const FROM: u32 = 4;
    const TO: u32 = 5;

    fn migrate(input: &[u8], out: &mut MigrationOutSink<'_>) -> Result<(), MigrationError> {
        out.push(input.to_vec());
        out.push(input.to_vec());
        Ok(())
    }
}

#[derive(Default)]
struct RecordingProgress {
    events: Mutex<Vec<String>>,
}

impl ProgressSink for RecordingProgress {
    fn on_event(&self, event: ProgressEvent<'_>) {
        let label = match event {
            ProgressEvent::Started { .. } => "started",
            ProgressEvent::BatchCommitted { .. } => "batch",
            ProgressEvent::Finished { .. } => "finished",
        };
        self.events
            .lock()
            .expect("progress lock")
            .push(label.to_string());
    }
}

#[test]
fn registry_rejects_gaps_duplicates_backwards_and_lossy_without_policy() {
    let gap = MigrationRegistry::new().add::<V1ToV2>();
    let err = gap.validate_chain(1, 3).expect_err("gap");
    assert!(
        err.to_string()
            .contains("missing migration step from epoch 2")
    );

    let duplicate = MigrationRegistry::new().add::<V1ToV2>().add::<V1ToV2>();
    let err = duplicate.validate_chain(1, 2).expect_err("duplicate");
    assert!(err.to_string().contains("duplicate migration step 1->2"));

    let err = MigrationRegistry::new()
        .validate_chain(2, 1)
        .expect_err("backward");
    assert!(err.to_string().contains("target epoch 1 is lower"));

    let lossy = MigrationRegistry::new()
        .add::<V1ToV2>()
        .add::<V2ToV3>()
        .add::<LossyDrop>();
    let err = lossy.validate_chain(1, 4).expect_err("lossy");
    assert!(err.to_string().contains("lossy migration step 3->4"));
}

#[test]
fn lossy_migration_requires_admin_signature_policy_to_be_disabled_for_now() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
        .expect("seed");
    let registry = MigrationRegistry::new()
        .add::<V1ToV2>()
        .add::<V2ToV3>()
        .add::<LossyDrop>();
    let codec = AadRecordingCodec::default();

    let err = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 4,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: RunnerPolicies {
            allow_lossy: true,
            require_admin_signature_for_lossy: true,
            ..run_policy()
        },
    }
    .dry_run()
    .expect_err("signature required");
    assert!(err.to_string().contains("admin signature"));

    let report = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 4,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: lossy_policy_without_signature_requirement(),
    }
    .dry_run()
    .expect("lossy dry run when signature requirement disabled");
    assert_eq!(report.records_in, 1);
    assert_eq!(report.records_out, 0);
}

#[test]
fn multi_output_migration_is_rejected_without_timestamp_remapper() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
        .expect("seed");
    let registry = MigrationRegistry::new().add::<MultiOutput>();
    let codec = AadRecordingCodec::default();
    let err = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 4,
        target_epoch: 5,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: run_policy(),
    }
    .dry_run()
    .expect_err("multi output rejected");
    assert!(err.to_string().contains("multi-output migrations"));
}

#[test]
fn dry_run_migrates_selected_series_with_codec_aad_and_keeps_live_env_unchanged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history(
            "auth",
            vec![(1_u64, b"alice".as_slice()), (2, b"bob".as_slice())],
        )
        .expect("seed auth");
    store
        .replace_history("other", vec![(1_u64, b"untouched".as_slice())])
        .expect("seed other");

    let registry = MigrationRegistry::new().add::<V1ToV2>().add::<V2ToV3>();
    let codec = AadRecordingCodec::default();
    let progress = RecordingProgress::default();
    let report = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 3,
        codec_in: &codec,
        codec_out: &codec,
        progress: Some(&progress),
        policies: run_policy(),
    }
    .dry_run()
    .expect("dry run");

    assert_eq!(report.records_in, 2);
    assert_eq!(report.records_out, 2);
    assert!(!backup.exists());
    assert_eq!(
        store.load_from("auth", 0).expect("live auth")[0].1,
        b"alice"
    );

    let migrated = open_store(&shadow)
        .load_from("auth", 0)
        .expect("shadow auth");
    assert_eq!(migrated[0].1, b"v2:alice:v3");
    assert_eq!(migrated[1].1, b"v2:bob:v3");
    assert!(
        open_store(&shadow)
            .load_from("other", 0)
            .expect("other")
            .is_empty()
    );

    let seen = codec.seen();
    assert!(seen.contains(&("auth".to_string(), 1, Some("ts".to_string()), 1, "decode")));
    assert!(seen.contains(&("auth".to_string(), 1, Some("ts".to_string()), 3, "encode")));
    assert!(
        progress
            .events
            .lock()
            .expect("progress lock")
            .contains(&"finished".to_string())
    );
}

#[test]
fn commit_promotes_shadow_keeps_backup_and_writes_manifest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    {
        let store = open_store(&live);
        store
            .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        let codec = AadRecordingCodec::default();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: run_policy(),
        }
        .run()
        .expect("run");
    }

    let live_rows = open_store(&live).load_from("auth", 0).expect("live rows");
    assert_eq!(live_rows[0].1, b"v2:alice");
    let backup_rows = open_store(&backup)
        .load_from("auth", 0)
        .expect("backup rows");
    assert_eq!(backup_rows[0].1, b"alice");
    let manifest = std::fs::read_to_string(live.join(".migration.manifest")).expect("manifest");
    assert!(manifest.contains("source_epoch=1"));
    assert!(manifest.contains("target_epoch=2"));
    assert!(manifest.contains("records_in=1"));
    assert!(manifest.contains("signature="));
    assert!(live.join(".migration.manifest.history").exists());
    assert!(!live.join(".in_progress").exists());
}

#[test]
fn committed_env_verification_checks_manifest_audit_counts_and_codec_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let codec = AadRecordingCodec::default();
    {
        let store = open_store(&live);
        store
            .replace_history(
                "auth",
                vec![(1_u64, b"alice".as_slice()), (2, b"bob".as_slice())],
            )
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: run_policy(),
        }
        .run()
        .expect("run");
    }

    let live_store = open_store(&live);
    let verified =
        verify_migration_artifacts(live_store.env(), &codec).expect("verify committed env");
    assert_eq!(verified.records_out, 2);
    assert_eq!(verified.manifest.source_epoch, 1);
    assert_eq!(verified.manifest.target_epoch, 2);
}

#[test]
fn committed_env_verification_rejects_manifest_tampering() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let codec = AadRecordingCodec::default();
    {
        let store = open_store(&live);
        store
            .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: run_policy(),
        }
        .run()
        .expect("run");
    }

    let manifest_path = live.join(".migration.manifest");
    let manifest = std::fs::read_to_string(&manifest_path).expect("manifest");
    std::fs::write(
        &manifest_path,
        manifest.replace("target_epoch=2", "target_epoch=3"),
    )
    .expect("tamper manifest");

    let live_store = open_store(&live);
    let err = verify_migration_artifacts(live_store.env(), &codec).expect_err("tamper must fail");
    assert!(err.to_string().contains("manifest hash mismatch"));
}

#[test]
fn committed_env_verification_rejects_codec_key_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let codec = AadRecordingCodec::default();
    {
        let store = open_store(&live);
        store
            .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: run_policy(),
        }
        .run()
        .expect("run");
    }

    let live_store = open_store(&live);
    let err = verify_migration_artifacts(live_store.env(), &DifferentFingerprintCodec)
        .expect_err("wrong codec key must fail");
    assert!(err.to_string().contains("codec fingerprint mismatch"));
}

#[test]
fn committed_env_verification_rejects_leaked_sentinel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let codec = AadRecordingCodec::default();
    {
        let store = open_store(&live);
        store
            .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: run_policy(),
        }
        .run()
        .expect("run");
    }
    std::fs::write(live.join(".in_progress"), "stale").expect("write sentinel");

    let live_store = open_store(&live);
    let err = verify_migration_artifacts(live_store.env(), &codec).expect_err("sentinel must fail");
    assert!(err.to_string().contains("sentinel is present"));
}

#[test]
fn shadow_payload_validation_aborts_before_promotion_when_encoded_payload_is_not_decodable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
        .expect("seed");
    let registry = MigrationRegistry::new().add::<V1ToV2>();
    let codec = CorruptingEncodeCodec;
    let err = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 2,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: run_policy(),
    }
    .run()
    .expect_err("shadow validation failure");

    assert!(err.to_string().contains("shadow validation decode failed"));
    assert!(!backup.exists());
    assert_eq!(store.load_from("auth", 0).expect("source")[0].1, b"alice");
}

#[test]
fn commit_with_default_policy_runs_and_removes_preflight_dry_run_shadow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let dry_shadow = dir.path().join("live.shadow.dry-run");
    let backup = dir.path().join("live.preupgrade");
    {
        let store = open_store(&live);
        store
            .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
            .expect("seed");
        let registry = MigrationRegistry::new().add::<V1ToV2>();
        let codec = AadRecordingCodec::default();
        MigrationRunner {
            registry: &registry,
            source_env: store.env(),
            shadow_dir: &shadow,
            backup_dir: &backup,
            series_keys: &["auth"],
            source_epoch: 1,
            target_epoch: 2,
            codec_in: &codec,
            codec_out: &codec,
            progress: None,
            policies: RunnerPolicies::default(),
        }
        .run()
        .expect("run with preflight");
    }

    assert!(!dry_shadow.exists());
    assert_eq!(
        open_store(&live).load_from("auth", 0).expect("live rows")[0].1,
        b"v2:alice"
    );
}

#[test]
fn codec_decode_failure_aborts_before_shadow_write_and_keeps_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history("auth", vec![(1_u64, b"bad-record".as_slice())])
        .expect("seed");

    let registry = MigrationRegistry::new().add::<V1ToV2>();
    let codec = AadRecordingCodec::with_fail_decode(b"bad");
    let err = MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 2,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: run_policy(),
    }
    .run()
    .expect_err("decode failure");

    assert!(err.to_string().contains("decode failed"));
    assert!(!backup.exists());
    assert_eq!(
        store.load_from("auth", 0).expect("source")[0].1,
        b"bad-record"
    );
}

#[test]
fn resumable_rejects_stale_shadow_parameters_by_rebuilding_clean_shadow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let live = dir.path().join("live");
    let shadow = dir.path().join("live.shadow");
    let backup = dir.path().join("live.preupgrade");
    let store = open_store(&live);
    store
        .replace_history("auth", vec![(1_u64, b"alice".as_slice())])
        .expect("seed");
    std::fs::create_dir_all(&shadow).expect("shadow dir");
    std::fs::write(shadow.join(".in_progress"), "source_epoch=9\n").expect("sentinel");
    std::fs::write(shadow.join("junk"), "stale").expect("junk");

    let registry = MigrationRegistry::new().add::<V1ToV2>();
    let codec = AadRecordingCodec::default();
    MigrationRunner {
        registry: &registry,
        source_env: store.env(),
        shadow_dir: &shadow,
        backup_dir: &backup,
        series_keys: &["auth"],
        source_epoch: 1,
        target_epoch: 2,
        codec_in: &codec,
        codec_out: &codec,
        progress: None,
        policies: run_policy(),
    }
    .run_resumable()
    .expect("resumable");

    assert!(!shadow.exists());
    assert!(!backup.join("junk").exists());
    drop(store);
    assert_eq!(
        open_store(&live).load_from("auth", 0).expect("live rows")[0].1,
        b"v2:alice"
    );
}

#[cfg(feature = "migrations-test")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn prop_dry_run_is_deterministic_for_random_auth_batches(rows in proptest::collection::btree_map(0_u16..200_u16, proptest::collection::vec(any::<u8>(), 0..64), 0..80)) {
        let dir = tempfile::tempdir().expect("tempdir");
        let live = dir.path().join("live");
        let shadow_a = dir.path().join("live.shadow.a");
        let shadow_b = dir.path().join("live.shadow.b");
        let backup = dir.path().join("live.preupgrade");
        let store = open_store(&live);
        let samples = rows
            .iter()
            .map(|(ts, raw)| (u64::from(*ts), raw.as_slice()))
            .collect::<Vec<_>>();
        store.replace_history("auth", samples).expect("seed");

        let registry = MigrationRegistry::new().add::<V1ToV2>().add::<V2ToV3>();
        let codec = AadRecordingCodec::default();
        for shadow in [&shadow_a, &shadow_b] {
            MigrationRunner {
                registry: &registry,
                source_env: store.env(),
                shadow_dir: shadow,
                backup_dir: &backup,
                series_keys: &["auth"],
                source_epoch: 1,
                target_epoch: 3,
                codec_in: &codec,
                codec_out: &codec,
                progress: None,
                policies: RunnerPolicies {
                    batch_size: 128,
                    fsync_every_batch: false,
                    ..run_policy()
                },
            }
            .dry_run()
            .expect("dry run");
        }

        let loaded_a = open_store(&shadow_a).load_from("auth", 0).expect("load a");
        let loaded_b = open_store(&shadow_b).load_from("auth", 0).expect("load b");
        prop_assert_eq!(&loaded_a, &loaded_b);
        for (ts, raw) in loaded_a {
            let original = rows.get(&(ts as u16)).expect("original row");
            let mut expected = b"v2:".to_vec();
            expected.extend_from_slice(original);
            expected.extend_from_slice(b":v3");
            prop_assert_eq!(raw, expected);
        }
    }
}
