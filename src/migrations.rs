use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use heed::types::{Bytes, DecodeIgnore, Str};
use heed::{Database, Env, EnvOpenOptions};
use thiserror::Error;

use crate::{
    DEFAULT_LMDB_MAP_SIZE_BYTES, DEFAULT_LMDB_MAX_DBS, DEFAULT_LMDB_MAX_READERS,
    parse_timestamp_from_key, series_prefix,
};

pub type SchemaEpoch = u32;
pub type BodyVersion = u16;

const AUDIT_DB_NAME: &str = "__shared_lmdb_migration_audit";
const AUDIT_KEY: &[u8] = b"manifest_hash";
const SENTINEL_FILE: &str = ".in_progress";
const MANIFEST_FILE: &str = ".migration.manifest";
const MANIFEST_HISTORY_FILE: &str = ".migration.manifest.history";

pub trait Migration: Send + Sync + 'static {
    const FROM: SchemaEpoch;
    const TO: SchemaEpoch;
    const LOSSY: bool = false;

    fn migrate(input: &[u8], out: &mut MigrationOutSink<'_>) -> Result<(), MigrationError>;

    fn validate_batch(_out: &[Vec<u8>]) -> Result<(), MigrationError> {
        Ok(())
    }
}

pub struct MigrationOutSink<'a> {
    records: &'a mut Vec<Vec<u8>>,
}

impl<'a> MigrationOutSink<'a> {
    pub fn new(records: &'a mut Vec<Vec<u8>>) -> Self {
        Self { records }
    }

    pub fn push(&mut self, record: impl Into<Vec<u8>>) {
        self.records.push(record.into());
    }
}

pub trait Codec: Send + Sync {
    fn decode(&self, raw: &[u8], aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError>;
    fn encode(&self, plain: &[u8], aad: CodecAad<'_>) -> Result<Vec<u8>, CodecError>;
    fn key_fingerprint(&self) -> [u8; 32];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecAad<'a> {
    pub series_key: &'a str,
    pub timestamp: u64,
    pub sub_db_name: Option<&'a str>,
    pub schema_epoch: SchemaEpoch,
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("{0}")]
    Message(String),
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("target epoch {to} is lower than source epoch {from}")]
    Backward { from: SchemaEpoch, to: SchemaEpoch },
    #[error("missing migration step from epoch {0}")]
    Gap(SchemaEpoch),
    #[error("duplicate migration step {from}->{to}")]
    Duplicate { from: SchemaEpoch, to: SchemaEpoch },
    #[error("invalid migration step {from}->{to}")]
    InvalidStep { from: SchemaEpoch, to: SchemaEpoch },
    #[error("lossy migration step {from}->{to} requires explicit policy consent")]
    LossyRejected { from: SchemaEpoch, to: SchemaEpoch },
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{context}: {source}")]
    Heed {
        context: String,
        #[source]
        source: heed::Error,
    },
    #[error("{context}: {source}")]
    Codec {
        context: String,
        #[source]
        source: CodecError,
    },
    #[error("{0}")]
    Validation(String),
}

#[derive(Clone, Copy)]
struct MigrationStep {
    from: SchemaEpoch,
    to: SchemaEpoch,
    lossy: bool,
    migrate: fn(&[u8], &mut MigrationOutSink<'_>) -> Result<(), MigrationError>,
    validate_batch: fn(&[Vec<u8>]) -> Result<(), MigrationError>,
}

impl MigrationStep {
    fn from_type<M: Migration>() -> Self {
        Self {
            from: M::FROM,
            to: M::TO,
            lossy: M::LOSSY,
            migrate: M::migrate,
            validate_batch: M::validate_batch,
        }
    }
}

#[derive(Clone, Default)]
pub struct MigrationRegistry {
    steps: Vec<MigrationStep>,
}

impl MigrationRegistry {
    pub const fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn add<M: Migration>(mut self) -> Self {
        self.steps.push(MigrationStep::from_type::<M>());
        self
    }

    pub fn validate_chain(&self, from: SchemaEpoch, to: SchemaEpoch) -> Result<(), RegistryError> {
        self.plan(from, to, false).map(|_| ())
    }

    fn plan(
        &self,
        from: SchemaEpoch,
        to: SchemaEpoch,
        allow_lossy: bool,
    ) -> Result<Vec<MigrationStep>, RegistryError> {
        if to < from {
            return Err(RegistryError::Backward { from, to });
        }
        let mut seen = BTreeSet::new();
        let mut by_from = BTreeMap::new();
        for step in &self.steps {
            if step.to <= step.from {
                return Err(RegistryError::InvalidStep {
                    from: step.from,
                    to: step.to,
                });
            }
            if !seen.insert((step.from, step.to)) || by_from.insert(step.from, *step).is_some() {
                return Err(RegistryError::Duplicate {
                    from: step.from,
                    to: step.to,
                });
            }
        }

        let mut epoch = from;
        let mut out = Vec::new();
        while epoch < to {
            let step = by_from
                .get(&epoch)
                .copied()
                .ok_or(RegistryError::Gap(epoch))?;
            if step.to != epoch.saturating_add(1) {
                return Err(RegistryError::Gap(epoch));
            }
            if step.lossy && !allow_lossy {
                return Err(RegistryError::LossyRejected {
                    from: step.from,
                    to: step.to,
                });
            }
            out.push(step);
            epoch = step.to;
        }
        Ok(out)
    }
}

pub trait ProgressSink: Send + Sync {
    fn on_event(&self, event: ProgressEvent<'_>);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressEvent<'a> {
    Started {
        source_epoch: SchemaEpoch,
        target_epoch: SchemaEpoch,
        expected_records: u64,
    },
    BatchCommitted {
        batch_index: u64,
        records_in: u64,
        records_out: u64,
    },
    Finished {
        records_in: u64,
        records_out: u64,
        manifest_hash: &'a str,
    },
}

#[derive(Debug, Clone)]
pub struct RunnerPolicies {
    pub batch_size: usize,
    pub fsync_every_batch: bool,
    pub allow_lossy: bool,
    pub require_admin_signature_for_lossy: bool,
    pub validate_with_dry_run_first: bool,
}

impl Default for RunnerPolicies {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            fsync_every_batch: true,
            allow_lossy: false,
            require_admin_signature_for_lossy: true,
            validate_with_dry_run_first: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationManifest {
    pub source_epoch: SchemaEpoch,
    pub target_epoch: SchemaEpoch,
    pub source_path: PathBuf,
    pub database_name: String,
    pub series_keys: Vec<String>,
    pub records_in: u64,
    pub records_out: u64,
    pub source_fingerprint: [u8; 32],
    pub codec_in_fingerprint: [u8; 32],
    pub codec_out_fingerprint: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub signature: [u8; 32],
}

impl MigrationManifest {
    pub fn manifest_hash_hex(&self) -> String {
        hex_lower(&self.manifest_hash)
    }

    pub fn signature_hex(&self) -> String {
        hex_lower(&self.signature)
    }

    pub fn verify(&self, codec_out_fingerprint: [u8; 32]) -> Result<(), MigrationError> {
        let expected_hash = manifest_hash(self);
        if self.manifest_hash != expected_hash {
            return Err(MigrationError::Validation(
                "migration manifest hash mismatch".to_string(),
            ));
        }
        if self.codec_out_fingerprint != codec_out_fingerprint {
            return Err(MigrationError::Validation(
                "migration manifest codec fingerprint mismatch".to_string(),
            ));
        }
        let expected_signature = manifest_signature(self);
        if self.signature != expected_signature {
            return Err(MigrationError::Validation(
                "migration manifest signature mismatch".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MigrationReport {
    pub records_in: u64,
    pub records_out: u64,
    pub elapsed: Duration,
    pub manifest: MigrationManifest,
}

pub struct MigrationRunner<'a> {
    pub registry: &'a MigrationRegistry,
    pub source_env: &'a Env,
    pub shadow_dir: &'a Path,
    pub backup_dir: &'a Path,
    pub series_keys: &'a [&'a str],
    pub source_epoch: SchemaEpoch,
    pub target_epoch: SchemaEpoch,
    pub codec_in: &'a dyn Codec,
    pub codec_out: &'a dyn Codec,
    pub progress: Option<&'a dyn ProgressSink>,
    pub policies: RunnerPolicies,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMigrationArtifacts {
    pub manifest: MigrationManifest,
    pub records_out: u64,
}

pub fn verify_migration_artifacts(
    env: &Env,
    codec_out: &dyn Codec,
) -> Result<VerifiedMigrationArtifacts, MigrationError> {
    let manifest_path = env.path().join(MANIFEST_FILE);
    let manifest_text =
        fs::read_to_string(&manifest_path).map_err(|source| MigrationError::Io {
            context: format!("failed to read manifest {}", manifest_path.display()),
            source,
        })?;
    let manifest = parse_manifest(manifest_text.as_str())?;
    manifest.verify(codec_out.key_fingerprint())?;
    if env.path().join(SENTINEL_FILE).exists() {
        return Err(MigrationError::Validation(
            "migration sentinel is present in verified env".to_string(),
        ));
    }
    verify_audit_record(env, &manifest)?;
    let series_keys = manifest
        .series_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let records_out = count_selected_records(env, manifest.database_name.as_str(), &series_keys)?;
    if records_out != manifest.records_out {
        return Err(MigrationError::Validation(format!(
            "manifest records_out mismatch: manifest={}, env={records_out}",
            manifest.records_out
        )));
    }
    Ok(VerifiedMigrationArtifacts {
        manifest,
        records_out,
    })
}

impl<'a> MigrationRunner<'a> {
    pub fn run(self) -> Result<MigrationReport, MigrationError> {
        self.run_inner(RunMode::Commit, ResumeMode::Fresh)
    }

    pub fn run_resumable(self) -> Result<MigrationReport, MigrationError> {
        self.run_inner(RunMode::Commit, ResumeMode::Resumable)
    }

    pub fn dry_run(self) -> Result<MigrationReport, MigrationError> {
        self.run_inner(RunMode::DryRun, ResumeMode::Fresh)
    }

    fn run_inner(
        self,
        mode: RunMode,
        resume_mode: ResumeMode,
    ) -> Result<MigrationReport, MigrationError> {
        if mode == RunMode::Commit && self.policies.validate_with_dry_run_first {
            let dry_shadow_dir = preflight_shadow_dir(self.shadow_dir)?;
            if dry_shadow_dir.exists() {
                fs::remove_dir_all(&dry_shadow_dir).map_err(|source| MigrationError::Io {
                    context: format!(
                        "failed to remove existing dry-run shadow {}",
                        dry_shadow_dir.display()
                    ),
                    source,
                })?;
            }
            let mut dry_policies = self.policies.clone();
            dry_policies.validate_with_dry_run_first = false;
            MigrationRunner {
                registry: self.registry,
                source_env: self.source_env,
                shadow_dir: &dry_shadow_dir,
                backup_dir: self.backup_dir,
                series_keys: self.series_keys,
                source_epoch: self.source_epoch,
                target_epoch: self.target_epoch,
                codec_in: self.codec_in,
                codec_out: self.codec_out,
                progress: None,
                policies: dry_policies,
            }
            .dry_run()?;
            fs::remove_dir_all(&dry_shadow_dir).map_err(|source| MigrationError::Io {
                context: format!(
                    "failed to remove successful dry-run shadow {}",
                    dry_shadow_dir.display()
                ),
                source,
            })?;
        }

        let started = Instant::now();
        let batch_size = self.policies.batch_size.max(1);
        let plan = self.registry.plan(
            self.source_epoch,
            self.target_epoch,
            self.policies.allow_lossy,
        )?;
        if self.policies.allow_lossy
            && self.policies.require_admin_signature_for_lossy
            && plan.iter().any(|step| step.lossy)
        {
            return Err(MigrationError::Validation(
                "lossy migrations require an admin signature; signature verification is not configured"
                    .to_string(),
            ));
        }
        let db_name = discover_single_payload_db(self.source_env)?;
        let source_fingerprint = source_fingerprint(self.source_env, db_name.as_str())?;
        let expected_records =
            count_selected_records(self.source_env, db_name.as_str(), self.series_keys)?;

        if let Some(progress) = self.progress {
            progress.on_event(ProgressEvent::Started {
                source_epoch: self.source_epoch,
                target_epoch: self.target_epoch,
                expected_records,
            });
        }

        prepare_shadow(
            self.shadow_dir,
            resume_mode,
            self.source_epoch,
            self.target_epoch,
            source_fingerprint,
        )?;

        let shadow_env = open_shadow_env(self.shadow_dir, self.source_env)?;
        let mut records_in = 0_u64;
        let mut records_out = 0_u64;
        let mut batch_index = 0_u64;
        let mut plain_batch = Vec::with_capacity(batch_size);

        {
            let rtxn = self
                .source_env
                .read_txn()
                .map_err(|source| MigrationError::Heed {
                    context: "failed to open source read transaction".to_string(),
                    source,
                })?;
            let source_db: Database<Bytes, Bytes> = self
                .source_env
                .open_database(&rtxn, Some(db_name.as_str()))
                .map_err(|source| MigrationError::Heed {
                    context: format!("failed to open source database {db_name}"),
                    source,
                })?
                .ok_or_else(|| {
                    MigrationError::Validation(format!("source database {db_name} not found"))
                })?;

            let mut wtxn = shadow_env
                .write_txn()
                .map_err(|source| MigrationError::Heed {
                    context: "failed to open shadow write transaction".to_string(),
                    source,
                })?;
            let shadow_db: Database<Bytes, Bytes> = shadow_env
                .create_database(&mut wtxn, Some(db_name.as_str()))
                .map_err(|source| MigrationError::Heed {
                    context: format!("failed to create shadow database {db_name}"),
                    source,
                })?;

            for series_key in self.series_keys {
                let prefix = series_prefix(series_key);
                let iter = source_db
                    .prefix_iter(&rtxn, prefix.as_slice())
                    .map_err(|source| MigrationError::Heed {
                        context: format!("failed to iterate source series {series_key}"),
                        source,
                    })?;
                for row in iter {
                    let (key, raw) = row.map_err(|source| MigrationError::Heed {
                        context: format!("failed reading source row for series {series_key}"),
                        source,
                    })?;
                    let timestamp = parse_timestamp_from_key(key, series_key)
                        .map_err(|err| MigrationError::Validation(err.to_string()))?;
                    let source_aad = CodecAad {
                        series_key,
                        timestamp,
                        sub_db_name: Some(db_name.as_str()),
                        schema_epoch: self.source_epoch,
                    };
                    let plain = self.codec_in.decode(raw, source_aad).map_err(|source| {
                        MigrationError::Codec {
                            context: format!(
                                "decode failed for series_key={series_key} ts={timestamp}"
                            ),
                            source,
                        }
                    })?;
                    let migrated = apply_plan(plain.as_slice(), &plan)?;
                    if migrated.len() > 1 {
                        return Err(MigrationError::Validation(
                            "multi-output migrations require timestamp remapping and are unsupported"
                                .to_string(),
                        ));
                    }

                    records_in = records_in.checked_add(1).ok_or_else(|| {
                        MigrationError::Validation("records_in overflow".to_string())
                    })?;

                    if let Some(plain_out) = migrated.first() {
                        let target_aad = CodecAad {
                            series_key,
                            timestamp,
                            sub_db_name: Some(db_name.as_str()),
                            schema_epoch: self.target_epoch,
                        };
                        let encoded =
                            self.codec_out
                                .encode(plain_out, target_aad)
                                .map_err(|source| MigrationError::Codec {
                                    context: format!(
                                        "encode failed for series_key={series_key} ts={timestamp}"
                                    ),
                                    source,
                                })?;
                        shadow_db.put(&mut wtxn, key, encoded.as_slice()).map_err(|source| {
                            MigrationError::Heed {
                                context: format!(
                                    "failed writing shadow row for series_key={series_key} ts={timestamp}"
                                ),
                                source,
                            }
                        })?;
                        plain_batch.push(plain_out.clone());
                        records_out = records_out.checked_add(1).ok_or_else(|| {
                            MigrationError::Validation("records_out overflow".to_string())
                        })?;
                    } else if !self.policies.allow_lossy {
                        return Err(MigrationError::Validation(
                            "migration dropped a record without allow_lossy".to_string(),
                        ));
                    }

                    if records_in.is_multiple_of(batch_size as u64) {
                        validate_steps(&plan, &plain_batch)?;
                        plain_batch.clear();
                        wtxn.commit().map_err(|source| MigrationError::Heed {
                            context: "failed to commit shadow batch".to_string(),
                            source,
                        })?;
                        if self.policies.fsync_every_batch {
                            shadow_env
                                .force_sync()
                                .map_err(|source| MigrationError::Heed {
                                    context: "failed to fsync shadow batch".to_string(),
                                    source,
                                })?;
                        }
                        if let Some(progress) = self.progress {
                            progress.on_event(ProgressEvent::BatchCommitted {
                                batch_index,
                                records_in,
                                records_out,
                            });
                        }
                        batch_index = batch_index.checked_add(1).ok_or_else(|| {
                            MigrationError::Validation("batch index overflow".to_string())
                        })?;
                        wtxn = shadow_env
                            .write_txn()
                            .map_err(|source| MigrationError::Heed {
                                context: "failed to open next shadow write transaction".to_string(),
                                source,
                            })?;
                    }
                }
            }

            validate_steps(&plan, &plain_batch)?;
            wtxn.commit().map_err(|source| MigrationError::Heed {
                context: "failed to commit final shadow batch".to_string(),
                source,
            })?;
        }

        if records_in != expected_records {
            return Err(MigrationError::Validation(format!(
                "source record count changed during migration: expected {expected_records}, saw {records_in}"
            )));
        }

        let mut manifest = MigrationManifest {
            source_epoch: self.source_epoch,
            target_epoch: self.target_epoch,
            source_path: self.source_env.path().to_path_buf(),
            database_name: db_name,
            series_keys: self.series_keys.iter().map(|s| (*s).to_string()).collect(),
            records_in,
            records_out,
            source_fingerprint,
            codec_in_fingerprint: self.codec_in.key_fingerprint(),
            codec_out_fingerprint: self.codec_out.key_fingerprint(),
            manifest_hash: [0; 32],
            signature: [0; 32],
        };
        manifest.manifest_hash = manifest_hash(&manifest);
        manifest.signature = manifest_signature(&manifest);

        write_audit_record(&shadow_env, &manifest)?;
        validate_shadow_payloads(&shadow_env, &manifest, self.codec_out)?;
        shadow_env
            .force_sync()
            .map_err(|source| MigrationError::Heed {
                context: "failed to fsync complete shadow env".to_string(),
                source,
            })?;
        let sentinel = self.shadow_dir.join(SENTINEL_FILE);
        if sentinel.exists() {
            fs::remove_file(&sentinel).map_err(|source| MigrationError::Io {
                context: format!("failed to remove sentinel {}", sentinel.display()),
                source,
            })?;
        }
        fsync_dir(self.shadow_dir)?;

        if mode == RunMode::Commit {
            atomic_promote(
                self.source_env.path(),
                self.shadow_dir,
                self.backup_dir,
                &manifest,
            )?;
        }

        let manifest_hash_hex = manifest.manifest_hash_hex();
        if let Some(progress) = self.progress {
            progress.on_event(ProgressEvent::Finished {
                records_in,
                records_out,
                manifest_hash: manifest_hash_hex.as_str(),
            });
        }

        Ok(MigrationReport {
            records_in,
            records_out,
            elapsed: started.elapsed(),
            manifest,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Commit,
    DryRun,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeMode {
    Fresh,
    Resumable,
}

fn apply_plan(input: &[u8], plan: &[MigrationStep]) -> Result<Vec<Vec<u8>>, MigrationError> {
    let mut current = vec![input.to_vec()];
    for step in plan {
        let mut next = Vec::new();
        for record in &current {
            let mut sink = MigrationOutSink::new(&mut next);
            (step.migrate)(record, &mut sink)?;
        }
        current = next;
    }
    Ok(current)
}

fn validate_steps(plan: &[MigrationStep], batch: &[Vec<u8>]) -> Result<(), MigrationError> {
    for step in plan {
        (step.validate_batch)(batch)?;
    }
    Ok(())
}

fn discover_single_payload_db(env: &Env) -> Result<String, MigrationError> {
    let rtxn = env.read_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open read transaction for database discovery".to_string(),
        source,
    })?;
    let names_db: Database<Str, DecodeIgnore> = env
        .open_database(&rtxn, None)
        .map_err(|source| MigrationError::Heed {
            context: "failed to open unnamed database for database discovery".to_string(),
            source,
        })?
        .ok_or_else(|| MigrationError::Validation("unnamed database not found".to_string()))?;

    let mut names = Vec::new();
    let iter = names_db
        .iter(&rtxn)
        .map_err(|source| MigrationError::Heed {
            context: "failed to iterate database names".to_string(),
            source,
        })?;
    for row in iter {
        let (name, ()) = row.map_err(|source| MigrationError::Heed {
            context: "failed reading database name".to_string(),
            source,
        })?;
        if name != AUDIT_DB_NAME
            && env
                .open_database::<Bytes, Bytes>(&rtxn, Some(name))
                .map_err(|source| MigrationError::Heed {
                    context: format!("failed to probe database {name}"),
                    source,
                })?
                .is_some()
        {
            names.push(name.to_string());
        }
    }

    match names.as_slice() {
        [name] => Ok(name.clone()),
        [] => Err(MigrationError::Validation(
            "no named payload database found".to_string(),
        )),
        _ => Err(MigrationError::Validation(format!(
            "expected exactly one named payload database, found {}: {}",
            names.len(),
            names.join(",")
        ))),
    }
}

fn count_selected_records(
    env: &Env,
    db_name: &str,
    series_keys: &[&str],
) -> Result<u64, MigrationError> {
    let rtxn = env.read_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open read transaction for record count".to_string(),
        source,
    })?;
    let db: Database<Bytes, Bytes> = env
        .open_database(&rtxn, Some(db_name))
        .map_err(|source| MigrationError::Heed {
            context: format!("failed to open database {db_name} for record count"),
            source,
        })?
        .ok_or_else(|| MigrationError::Validation(format!("database {db_name} not found")))?;

    let mut count = 0_u64;
    for series_key in series_keys {
        let prefix = series_prefix(series_key);
        let iter =
            db.prefix_iter(&rtxn, prefix.as_slice())
                .map_err(|source| MigrationError::Heed {
                    context: format!("failed to count series {series_key}"),
                    source,
                })?;
        for row in iter {
            row.map_err(|source| MigrationError::Heed {
                context: format!("failed reading count row for series {series_key}"),
                source,
            })?;
            count = count
                .checked_add(1)
                .ok_or_else(|| MigrationError::Validation("record count overflow".to_string()))?;
        }
    }
    Ok(count)
}

fn source_fingerprint(env: &Env, db_name: &str) -> Result<[u8; 32], MigrationError> {
    let rtxn = env.read_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open read transaction for source fingerprint".to_string(),
        source,
    })?;
    let db: Database<Bytes, Bytes> = env
        .open_database(&rtxn, Some(db_name))
        .map_err(|source| MigrationError::Heed {
            context: format!("failed to open database {db_name} for source fingerprint"),
            source,
        })?
        .ok_or_else(|| MigrationError::Validation(format!("database {db_name} not found")))?;

    let mut hasher = blake3::Hasher::new();
    let iter = db.iter(&rtxn).map_err(|source| MigrationError::Heed {
        context: "failed to iterate source fingerprint".to_string(),
        source,
    })?;
    for row in iter {
        let (key, value) = row.map_err(|source| MigrationError::Heed {
            context: "failed reading source fingerprint row".to_string(),
            source,
        })?;
        update_len_prefixed(&mut hasher, key);
        update_len_prefixed(&mut hasher, value);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn prepare_shadow(
    shadow_dir: &Path,
    resume_mode: ResumeMode,
    source_epoch: SchemaEpoch,
    target_epoch: SchemaEpoch,
    source_fingerprint: [u8; 32],
) -> Result<(), MigrationError> {
    let sentinel = shadow_dir.join(SENTINEL_FILE);
    if shadow_dir.exists() {
        if resume_mode == ResumeMode::Resumable && sentinel.exists() {
            let existing = fs::read_to_string(&sentinel).map_err(|source| MigrationError::Io {
                context: format!("failed to read sentinel {}", sentinel.display()),
                source,
            })?;
            let expected = sentinel_contents(source_epoch, target_epoch, source_fingerprint);
            if existing == expected {
                fs::remove_dir_all(shadow_dir).map_err(|source| MigrationError::Io {
                    context: format!("failed to reset resumable shadow {}", shadow_dir.display()),
                    source,
                })?;
            } else {
                fs::remove_dir_all(shadow_dir).map_err(|source| MigrationError::Io {
                    context: format!("failed to remove stale shadow {}", shadow_dir.display()),
                    source,
                })?;
            }
        } else {
            fs::remove_dir_all(shadow_dir).map_err(|source| MigrationError::Io {
                context: format!("failed to remove existing shadow {}", shadow_dir.display()),
                source,
            })?;
        }
    }
    fs::create_dir_all(shadow_dir).map_err(|source| MigrationError::Io {
        context: format!("failed to create shadow {}", shadow_dir.display()),
        source,
    })?;
    write_atomic(
        &sentinel,
        sentinel_contents(source_epoch, target_epoch, source_fingerprint).as_bytes(),
    )
}

fn preflight_shadow_dir(shadow_dir: &Path) -> Result<PathBuf, MigrationError> {
    let file_name = shadow_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            MigrationError::Validation(format!(
                "shadow dir {} must have a valid utf8 final path component",
                shadow_dir.display()
            ))
        })?;
    Ok(shadow_dir.with_file_name(format!("{file_name}.dry-run")))
}

fn open_shadow_env(shadow_dir: &Path, source_env: &Env) -> Result<Env, MigrationError> {
    let map_size = source_env.info().map_size.max(DEFAULT_LMDB_MAP_SIZE_BYTES);
    unsafe {
        EnvOpenOptions::new()
            .map_size(map_size)
            .max_dbs(DEFAULT_LMDB_MAX_DBS)
            .max_readers(DEFAULT_LMDB_MAX_READERS)
            .open(shadow_dir)
    }
    .map_err(|source| MigrationError::Heed {
        context: format!("failed to open shadow env {}", shadow_dir.display()),
        source,
    })
}

fn write_audit_record(env: &Env, manifest: &MigrationManifest) -> Result<(), MigrationError> {
    let mut wtxn = env.write_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open audit write transaction".to_string(),
        source,
    })?;
    let audit_db: Database<Bytes, Bytes> = env
        .create_database(&mut wtxn, Some(AUDIT_DB_NAME))
        .map_err(|source| MigrationError::Heed {
            context: "failed to create migration audit database".to_string(),
            source,
        })?;
    let hash_hex = manifest.manifest_hash_hex();
    audit_db
        .put(&mut wtxn, AUDIT_KEY, hash_hex.as_bytes())
        .map_err(|source| MigrationError::Heed {
            context: "failed to write migration audit record".to_string(),
            source,
        })?;
    wtxn.commit().map_err(|source| MigrationError::Heed {
        context: "failed to commit migration audit record".to_string(),
        source,
    })
}

fn verify_audit_record(env: &Env, manifest: &MigrationManifest) -> Result<(), MigrationError> {
    let rtxn = env.read_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open audit read transaction".to_string(),
        source,
    })?;
    let audit_db: Database<Bytes, Bytes> = env
        .open_database(&rtxn, Some(AUDIT_DB_NAME))
        .map_err(|source| MigrationError::Heed {
            context: "failed to open migration audit database".to_string(),
            source,
        })?
        .ok_or_else(|| {
            MigrationError::Validation("migration audit database is missing".to_string())
        })?;
    let raw = audit_db
        .get(&rtxn, AUDIT_KEY)
        .map_err(|source| MigrationError::Heed {
            context: "failed to read migration audit record".to_string(),
            source,
        })?
        .ok_or_else(|| {
            MigrationError::Validation("migration audit record is missing".to_string())
        })?;
    let audit_hash = std::str::from_utf8(raw)
        .map_err(|err| MigrationError::Validation(format!("audit hash is not utf8: {err}")))?;
    if audit_hash != manifest.manifest_hash_hex() {
        return Err(MigrationError::Validation(
            "migration audit hash does not match manifest".to_string(),
        ));
    }
    Ok(())
}

fn validate_shadow_payloads(
    env: &Env,
    manifest: &MigrationManifest,
    codec_out: &dyn Codec,
) -> Result<(), MigrationError> {
    manifest.verify(codec_out.key_fingerprint())?;
    verify_audit_record(env, manifest)?;
    let series_keys = manifest
        .series_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let records_out = count_selected_records(env, manifest.database_name.as_str(), &series_keys)?;
    if records_out != manifest.records_out {
        return Err(MigrationError::Validation(format!(
            "shadow records_out mismatch: manifest={}, shadow={records_out}",
            manifest.records_out
        )));
    }

    let rtxn = env.read_txn().map_err(|source| MigrationError::Heed {
        context: "failed to open shadow validation read transaction".to_string(),
        source,
    })?;
    let db: Database<Bytes, Bytes> = env
        .open_database(&rtxn, Some(manifest.database_name.as_str()))
        .map_err(|source| MigrationError::Heed {
            context: format!(
                "failed to open shadow database {} for validation",
                manifest.database_name
            ),
            source,
        })?
        .ok_or_else(|| {
            MigrationError::Validation(format!(
                "shadow database {} not found",
                manifest.database_name
            ))
        })?;
    for series_key in &manifest.series_keys {
        let prefix = series_prefix(series_key);
        let iter =
            db.prefix_iter(&rtxn, prefix.as_slice())
                .map_err(|source| MigrationError::Heed {
                    context: format!("failed to validate shadow series {series_key}"),
                    source,
                })?;
        for row in iter {
            let (key, raw) = row.map_err(|source| MigrationError::Heed {
                context: format!("failed reading shadow row for series {series_key}"),
                source,
            })?;
            let timestamp = parse_timestamp_from_key(key, series_key)
                .map_err(|err| MigrationError::Validation(err.to_string()))?;
            codec_out
                .decode(
                    raw,
                    CodecAad {
                        series_key,
                        timestamp,
                        sub_db_name: Some(manifest.database_name.as_str()),
                        schema_epoch: manifest.target_epoch,
                    },
                )
                .map_err(|source| MigrationError::Codec {
                    context: format!(
                        "shadow validation decode failed for series_key={series_key} ts={timestamp}"
                    ),
                    source,
                })?;
        }
    }
    Ok(())
}

fn atomic_promote(
    source_dir: &Path,
    shadow_dir: &Path,
    backup_dir: &Path,
    manifest: &MigrationManifest,
) -> Result<(), MigrationError> {
    if backup_dir.exists() {
        fs::remove_dir_all(backup_dir).map_err(|source| MigrationError::Io {
            context: format!("failed to remove existing backup {}", backup_dir.display()),
            source,
        })?;
    }
    fs::rename(source_dir, backup_dir).map_err(|source| MigrationError::Io {
        context: format!(
            "failed to rename source {} to backup {}",
            source_dir.display(),
            backup_dir.display()
        ),
        source,
    })?;
    fs::rename(shadow_dir, source_dir).map_err(|source| MigrationError::Io {
        context: format!(
            "failed to rename shadow {} to source {}",
            shadow_dir.display(),
            source_dir.display()
        ),
        source,
    })?;
    if let Some(parent) = source_dir.parent() {
        fsync_dir(parent)?;
    }
    write_manifest_files(source_dir, manifest)
}

fn write_manifest_files(
    live_dir: &Path,
    manifest: &MigrationManifest,
) -> Result<(), MigrationError> {
    let body = manifest_text(manifest);
    write_atomic(&live_dir.join(MANIFEST_FILE), body.as_bytes())?;
    let mut history = OpenOptions::new()
        .create(true)
        .append(true)
        .open(live_dir.join(MANIFEST_HISTORY_FILE))
        .map_err(|source| MigrationError::Io {
            context: format!(
                "failed to open manifest history {}",
                live_dir.join(MANIFEST_HISTORY_FILE).display()
            ),
            source,
        })?;
    history
        .write_all(body.as_bytes())
        .and_then(|_| history.write_all(b"\n---\n"))
        .and_then(|_| history.sync_all())
        .map_err(|source| MigrationError::Io {
            context: "failed to append manifest history".to_string(),
            source,
        })
}

fn manifest_hash(manifest: &MigrationManifest) -> [u8; 32] {
    let mut clone = manifest.clone();
    clone.manifest_hash = [0; 32];
    clone.signature = [0; 32];
    *blake3::hash(manifest_text(&clone).as_bytes()).as_bytes()
}

fn manifest_signature(manifest: &MigrationManifest) -> [u8; 32] {
    blake3::keyed_hash(&manifest.codec_out_fingerprint, &manifest.manifest_hash)
        .as_bytes()
        .to_owned()
}

fn manifest_text(manifest: &MigrationManifest) -> String {
    format!(
        "source_epoch={}\ntarget_epoch={}\nsource_path={}\ndatabase_name={}\nseries_keys={}\nrecords_in={}\nrecords_out={}\nsource_fingerprint={}\ncodec_in_fingerprint={}\ncodec_out_fingerprint={}\nmanifest_hash={}\nsignature={}\n",
        manifest.source_epoch,
        manifest.target_epoch,
        manifest.source_path.display(),
        manifest.database_name,
        manifest.series_keys.join(","),
        manifest.records_in,
        manifest.records_out,
        hex_lower(&manifest.source_fingerprint),
        hex_lower(&manifest.codec_in_fingerprint),
        hex_lower(&manifest.codec_out_fingerprint),
        hex_lower(&manifest.manifest_hash),
        hex_lower(&manifest.signature),
    )
}

fn parse_manifest(input: &str) -> Result<MigrationManifest, MigrationError> {
    let fields = input
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<BTreeMap<_, _>>();
    let required = [
        "source_epoch",
        "target_epoch",
        "source_path",
        "database_name",
        "series_keys",
        "records_in",
        "records_out",
        "source_fingerprint",
        "codec_in_fingerprint",
        "codec_out_fingerprint",
        "manifest_hash",
        "signature",
    ];
    for field in required {
        if !fields.contains_key(field) {
            return Err(MigrationError::Validation(format!(
                "migration manifest missing field {field}"
            )));
        }
    }
    Ok(MigrationManifest {
        source_epoch: parse_u32_field(&fields, "source_epoch")?,
        target_epoch: parse_u32_field(&fields, "target_epoch")?,
        source_path: PathBuf::from(required_field(&fields, "source_path")?),
        database_name: required_field(&fields, "database_name")?.to_string(),
        series_keys: parse_series_keys(required_field(&fields, "series_keys")?),
        records_in: parse_u64_field(&fields, "records_in")?,
        records_out: parse_u64_field(&fields, "records_out")?,
        source_fingerprint: parse_hex_32(required_field(&fields, "source_fingerprint")?)?,
        codec_in_fingerprint: parse_hex_32(required_field(&fields, "codec_in_fingerprint")?)?,
        codec_out_fingerprint: parse_hex_32(required_field(&fields, "codec_out_fingerprint")?)?,
        manifest_hash: parse_hex_32(required_field(&fields, "manifest_hash")?)?,
        signature: parse_hex_32(required_field(&fields, "signature")?)?,
    })
}

fn required_field<'a>(
    fields: &'a BTreeMap<&str, &str>,
    field: &str,
) -> Result<&'a str, MigrationError> {
    fields.get(field).copied().ok_or_else(|| {
        MigrationError::Validation(format!("migration manifest missing field {field}"))
    })
}

fn parse_u32_field(fields: &BTreeMap<&str, &str>, field: &str) -> Result<u32, MigrationError> {
    required_field(fields, field)?
        .parse::<u32>()
        .map_err(|err| MigrationError::Validation(format!("invalid manifest {field}: {err}")))
}

fn parse_u64_field(fields: &BTreeMap<&str, &str>, field: &str) -> Result<u64, MigrationError> {
    required_field(fields, field)?
        .parse::<u64>()
        .map_err(|err| MigrationError::Validation(format!("invalid manifest {field}: {err}")))
}

fn parse_series_keys(raw: &str) -> Vec<String> {
    raw.split(',')
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_hex_32(raw: &str) -> Result<[u8; 32], MigrationError> {
    if raw.len() != 64 {
        return Err(MigrationError::Validation(format!(
            "expected 32-byte hex field, got {} chars",
            raw.len()
        )));
    }
    let mut out = [0_u8; 32];
    for (idx, byte) in out.iter_mut().enumerate() {
        let offset = idx * 2;
        *byte = u8::from_str_radix(&raw[offset..offset + 2], 16).map_err(|err| {
            MigrationError::Validation(format!("invalid 32-byte hex field at byte {idx}: {err}"))
        })?;
    }
    Ok(out)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), MigrationError> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp).map_err(|source| MigrationError::Io {
            context: format!("failed to create temp file {}", tmp.display()),
            source,
        })?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|source| MigrationError::Io {
                context: format!("failed to write temp file {}", tmp.display()),
                source,
            })?;
    }
    fs::rename(&tmp, path).map_err(|source| MigrationError::Io {
        context: format!("failed to rename {} to {}", tmp.display(), path.display()),
        source,
    })?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

fn fsync_dir(path: &Path) -> Result<(), MigrationError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| MigrationError::Io {
            context: format!("failed to fsync dir {}", path.display()),
            source,
        })
}

fn sentinel_contents(
    source_epoch: SchemaEpoch,
    target_epoch: SchemaEpoch,
    source_fingerprint: [u8; 32],
) -> String {
    format!(
        "source_epoch={source_epoch}\ntarget_epoch={target_epoch}\nsource_fingerprint={}\n",
        hex_lower(&source_fingerprint)
    )
}

fn update_len_prefixed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}
