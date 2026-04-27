use std::path::{Path, PathBuf};

use heed::types::Bytes;
use thiserror::Error;

#[cfg(feature = "migrations")]
pub mod migrations;
#[cfg(feature = "postgres-sync")]
pub mod postgres_sync;

pub const DEFAULT_LMDB_MAP_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_LMDB_MAX_DBS: u32 = 8;
pub const DEFAULT_LMDB_MAX_READERS: u32 = 256;

#[derive(Debug, Error)]
pub enum LmdbError {
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
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    InvalidKey(String),
    #[cfg(feature = "postgres-sync")]
    #[error("{context}: {source}")]
    Postgres {
        context: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[cfg(feature = "postgres-sync")]
    #[error("{context}: {source}")]
    Join {
        context: String,
        #[source]
        source: tokio::task::JoinError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationPolicy {
    Forever,
    Circular { max_count: usize },
    MaxAgeMs { max_age_ms: u64 },
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub db_name: String,
    pub map_size_bytes: usize,
    pub max_dbs: u32,
    pub max_readers: u32,
    pub rotation_policy: RotationPolicy,
}

impl StoreConfig {
    pub fn new(db_name: impl Into<String>, rotation_policy: RotationPolicy) -> Self {
        Self {
            db_name: db_name.into(),
            map_size_bytes: DEFAULT_LMDB_MAP_SIZE_BYTES,
            max_dbs: DEFAULT_LMDB_MAX_DBS,
            max_readers: DEFAULT_LMDB_MAX_READERS,
            rotation_policy,
        }
    }
}

#[derive(Clone)]
pub struct LmdbTimeseriesStore {
    env: heed::Env,
    db: heed::Database<Bytes, Bytes>,
    label: String,
    rotation_policy: RotationPolicy,
}

impl std::fmt::Debug for LmdbTimeseriesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbTimeseriesStore")
            .field("label", &self.label)
            .field("rotation_policy", &self.rotation_policy)
            .finish()
    }
}

impl LmdbTimeseriesStore {
    #[cfg(feature = "migrations")]
    pub fn env(&self) -> &heed::Env {
        &self.env
    }

    pub fn open(
        root: &Path,
        config: StoreConfig,
        label: impl Into<String>,
    ) -> Result<Self, LmdbError> {
        let label = label.into();
        std::fs::create_dir_all(root).map_err(|source| LmdbError::Io {
            context: format!("failed to create {label} dir {}", root.display()),
            source,
        })?;

        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(config.map_size_bytes)
                .max_dbs(config.max_dbs)
                .max_readers(config.max_readers)
                .open(root)
        }
        .map_err(|source| LmdbError::Heed {
            context: format!("failed to open {label} env {}", root.display()),
            source,
        })?;

        let mut wtxn = env.write_txn().map_err(|source| LmdbError::Heed {
            context: format!("failed to open {label} write txn"),
            source,
        })?;
        let db = env
            .create_database::<Bytes, Bytes>(&mut wtxn, Some(config.db_name.as_str()))
            .map_err(|source| LmdbError::Heed {
                context: format!("failed to create {label} db {}", config.db_name),
                source,
            })?;
        wtxn.commit().map_err(|source| LmdbError::Heed {
            context: format!("failed to commit {label} db init"),
            source,
        })?;

        Ok(Self {
            env,
            db,
            label,
            rotation_policy: config.rotation_policy,
        })
    }

    pub fn replace_history<'a, I>(&self, series_key: &str, samples: I) -> Result<(), LmdbError>
    where
        I: IntoIterator<Item = (u64, &'a [u8])>,
    {
        let mut wtxn = self.env.write_txn().map_err(|source| LmdbError::Heed {
            context: format!("failed to open {} write txn", self.label),
            source,
        })?;

        let keys = list_series_keys(&self.db, &wtxn, series_key, self.label.as_str())?;
        for key in keys {
            self.db
                .delete(&mut wtxn, key.as_slice())
                .map_err(|source| LmdbError::Heed {
                    context: format!(
                        "failed deleting {} key {}",
                        self.label,
                        key_for_log(key.as_slice())
                    ),
                    source,
                })?;
        }

        for (ts, value) in samples {
            let key = encode_key(series_key, ts);
            self.db
                .put(&mut wtxn, key.as_slice(), value)
                .map_err(|source| LmdbError::Heed {
                    context: format!(
                        "failed writing {} key {}",
                        self.label,
                        key_for_log(key.as_slice())
                    ),
                    source,
                })?;
        }

        apply_rotation_policy(
            series_key,
            self.rotation_policy,
            &self.db,
            &mut wtxn,
            self.label.as_str(),
        )?;
        wtxn.commit().map_err(|source| LmdbError::Heed {
            context: format!(
                "failed to commit {} history replace for series_key={series_key}",
                self.label
            ),
            source,
        })?;
        Ok(())
    }

    pub fn upsert_sample<F>(
        &self,
        series_key: &str,
        timestamp_ms: u64,
        value: &[u8],
        validate_existing: F,
    ) -> Result<(), LmdbError>
    where
        F: FnOnce(&[u8]) -> Result<(), LmdbError>,
    {
        let key = encode_key(series_key, timestamp_ms);
        let mut wtxn = self.env.write_txn().map_err(|source| LmdbError::Heed {
            context: format!("failed to open {} write txn", self.label),
            source,
        })?;

        if let Some(existing) =
            self.db
                .get(&wtxn, key.as_slice())
                .map_err(|source| LmdbError::Heed {
                    context: format!(
                        "failed reading {} key {}",
                        self.label,
                        key_for_log(key.as_slice())
                    ),
                    source,
                })?
        {
            validate_existing(existing)?;
        }

        self.db
            .put(&mut wtxn, key.as_slice(), value)
            .map_err(|source| LmdbError::Heed {
                context: format!(
                    "failed writing {} key {}",
                    self.label,
                    key_for_log(key.as_slice())
                ),
                source,
            })?;

        apply_rotation_policy(
            series_key,
            self.rotation_policy,
            &self.db,
            &mut wtxn,
            self.label.as_str(),
        )?;
        wtxn.commit().map_err(|source| LmdbError::Heed {
            context: format!(
                "failed to commit {} upsert for series_key={series_key} ts={timestamp_ms}",
                self.label
            ),
            source,
        })?;
        Ok(())
    }

    pub fn upsert_batch<F>(
        &self,
        series_key: &str,
        samples: &[(u64, Vec<u8>)],
        validate_existing: F,
    ) -> Result<(), LmdbError>
    where
        F: FnMut(u64, &[u8], &[u8]) -> Result<(), LmdbError>,
    {
        let refs = samples
            .iter()
            .map(|(ts, value)| (*ts, value.as_slice()))
            .collect::<Vec<_>>();
        self.upsert_batch_refs(series_key, refs.as_slice(), validate_existing)
    }

    pub fn upsert_batch_refs<F>(
        &self,
        series_key: &str,
        samples: &[(u64, &[u8])],
        mut validate_existing: F,
    ) -> Result<(), LmdbError>
    where
        F: FnMut(u64, &[u8], &[u8]) -> Result<(), LmdbError>,
    {
        let mut wtxn = self.env.write_txn().map_err(|source| LmdbError::Heed {
            context: format!("failed to open {} write txn", self.label),
            source,
        })?;

        for (timestamp_ms, value) in samples {
            let key = encode_key(series_key, *timestamp_ms);
            if let Some(existing) =
                self.db
                    .get(&wtxn, key.as_slice())
                    .map_err(|source| LmdbError::Heed {
                        context: format!(
                            "failed reading {} key {}",
                            self.label,
                            key_for_log(key.as_slice())
                        ),
                        source,
                    })?
            {
                validate_existing(*timestamp_ms, existing, value)?;
            }
            self.db
                .put(&mut wtxn, key.as_slice(), value)
                .map_err(|source| LmdbError::Heed {
                    context: format!(
                        "failed writing {} key {}",
                        self.label,
                        key_for_log(key.as_slice())
                    ),
                    source,
                })?;
        }

        apply_rotation_policy(
            series_key,
            self.rotation_policy,
            &self.db,
            &mut wtxn,
            self.label.as_str(),
        )?;
        wtxn.commit().map_err(|source| LmdbError::Heed {
            context: format!(
                "failed to commit {} batch upsert for series_key={series_key}",
                self.label
            ),
            source,
        })?;
        Ok(())
    }

    pub fn load_from(
        &self,
        series_key: &str,
        start_ms: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, LmdbError> {
        let rtxn = self.env.read_txn().map_err(|source| LmdbError::Heed {
            context: format!("failed to open {} read txn", self.label),
            source,
        })?;
        let mut out = Vec::new();
        let prefix = series_prefix(series_key);
        let iter = self
            .db
            .prefix_iter(&rtxn, prefix.as_slice())
            .map_err(|source| LmdbError::Heed {
                context: format!(
                    "failed to iterate {} rows for series_key={series_key}",
                    self.label
                ),
                source,
            })?;
        for row in iter {
            let (key, raw) = row.map_err(|source| LmdbError::Heed {
                context: format!("failed reading {} row", self.label),
                source,
            })?;
            let ts = parse_timestamp_from_key(key, series_key)?;
            if ts >= start_ms {
                out.push((ts, raw.to_vec()));
            }
        }
        Ok(out)
    }
}

pub fn resolve_data_dir(default_root: &Path, env_var: &str) -> PathBuf {
    std::env::var(env_var)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| default_root.to_path_buf())
}

#[cfg(feature = "binary-keys")]
const KEY_SEPARATOR_BYTE: u8 = b'|';
#[cfg(not(feature = "binary-keys"))]
const KEY_SEPARATOR_BYTE: u8 = b':';

fn encode_key(series_key: &str, timestamp_ms: u64) -> Vec<u8> {
    #[cfg(feature = "binary-keys")]
    {
        let mut out = Vec::with_capacity(series_key.len() + 1 + 8);
        out.extend_from_slice(series_key.as_bytes());
        out.push(KEY_SEPARATOR_BYTE);
        out.extend_from_slice(&timestamp_ms.to_be_bytes());
        out
    }
    #[cfg(not(feature = "binary-keys"))]
    {
        format!("{series_key}:{timestamp_ms:020}").into_bytes()
    }
}

fn series_prefix(series_key: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(series_key.len() + 1);
    prefix.extend_from_slice(series_key.as_bytes());
    prefix.push(KEY_SEPARATOR_BYTE);
    prefix
}

fn list_series_keys(
    db: &heed::Database<Bytes, Bytes>,
    txn: &heed::RwTxn<'_>,
    series_key: &str,
    label: &str,
) -> Result<Vec<Vec<u8>>, LmdbError> {
    let prefix = series_prefix(series_key);
    let mut out = Vec::new();
    let iter = db
        .prefix_iter(txn, prefix.as_slice())
        .map_err(|source| LmdbError::Heed {
            context: format!("failed to list {label} keys for series_key={series_key}"),
            source,
        })?;
    for row in iter {
        let (key, _) = row.map_err(|source| LmdbError::Heed {
            context: format!("failed reading {label} key row"),
            source,
        })?;
        out.push(key.to_vec());
    }
    Ok(out)
}

fn apply_rotation_policy(
    series_key: &str,
    policy: RotationPolicy,
    db: &heed::Database<Bytes, Bytes>,
    wtxn: &mut heed::RwTxn<'_>,
    label: &str,
) -> Result<(), LmdbError> {
    match policy {
        RotationPolicy::Forever => Ok(()),
        RotationPolicy::Circular { max_count } => {
            trim_to_max_count(series_key, max_count, db, wtxn, label)
        }
        RotationPolicy::MaxAgeMs { max_age_ms } => {
            trim_to_max_age(series_key, max_age_ms, db, wtxn, label)
        }
    }
}

fn trim_to_max_count(
    series_key: &str,
    max_count: usize,
    db: &heed::Database<Bytes, Bytes>,
    wtxn: &mut heed::RwTxn<'_>,
    label: &str,
) -> Result<(), LmdbError> {
    let prefix = series_prefix(series_key);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let iter = db
        .prefix_iter(wtxn, prefix.as_slice())
        .map_err(|source| LmdbError::Heed {
            context: format!("failed to scan {label} rows for trim series_key={series_key}"),
            source,
        })?;
    for row in iter {
        let (key, _) = row.map_err(|source| LmdbError::Heed {
            context: format!("failed reading {label} trim row"),
            source,
        })?;
        keys.push(key.to_vec());
    }
    if keys.len() <= max_count {
        return Ok(());
    }
    let trim_count = keys.len().saturating_sub(max_count);
    for key in keys.into_iter().take(trim_count) {
        db.delete(wtxn, key.as_slice())
            .map_err(|source| LmdbError::Heed {
                context: format!(
                    "failed deleting trimmed {label} key {}",
                    key_for_log(key.as_slice())
                ),
                source,
            })?;
    }
    Ok(())
}

fn trim_to_max_age(
    series_key: &str,
    max_age_ms: u64,
    db: &heed::Database<Bytes, Bytes>,
    wtxn: &mut heed::RwTxn<'_>,
    label: &str,
) -> Result<(), LmdbError> {
    let prefix = series_prefix(series_key);
    let mut keys_with_ts: Vec<(Vec<u8>, u64)> = Vec::new();
    let mut newest_ts = None::<u64>;
    let iter = db
        .prefix_iter(wtxn, prefix.as_slice())
        .map_err(|source| LmdbError::Heed {
            context: format!("failed to scan {label} rows for age trim series_key={series_key}"),
            source,
        })?;
    for row in iter {
        let (key, _) = row.map_err(|source| LmdbError::Heed {
            context: format!("failed reading {label} age trim row"),
            source,
        })?;
        let ts = parse_timestamp_from_key(key, series_key)?;
        newest_ts = Some(newest_ts.map_or(ts, |cur| cur.max(ts)));
        keys_with_ts.push((key.to_vec(), ts));
    }

    let Some(latest) = newest_ts else {
        return Ok(());
    };
    let cutoff = latest.saturating_sub(max_age_ms);
    for (key, ts) in keys_with_ts {
        if ts < cutoff {
            db.delete(wtxn, key.as_slice())
                .map_err(|source| LmdbError::Heed {
                    context: format!(
                        "failed deleting age-trimmed {label} key {}",
                        key_for_log(key.as_slice())
                    ),
                    source,
                })?;
        }
    }
    Ok(())
}

fn parse_timestamp_from_key(key: &[u8], series_key: &str) -> Result<u64, LmdbError> {
    #[cfg(feature = "binary-keys")]
    {
        let prefix = series_prefix(series_key);
        if !key.starts_with(prefix.as_slice()) {
            return Err(LmdbError::InvalidKey(format!(
                "invalid binary timeseries key prefix for series_key={series_key}: {}",
                key_for_log(key)
            )));
        }
        if key.len() != prefix.len() + 8 {
            return Err(LmdbError::InvalidKey(format!(
                "invalid binary timeseries key len={} for series_key={series_key}",
                key.len()
            )));
        }
        let mut ts_bytes = [0_u8; 8];
        ts_bytes.copy_from_slice(&key[prefix.len()..]);
        Ok(u64::from_be_bytes(ts_bytes))
    }
    #[cfg(not(feature = "binary-keys"))]
    {
        let _ = series_key;
        let key = std::str::from_utf8(key)
            .map_err(|err| LmdbError::InvalidKey(format!("invalid utf8 timeseries key: {err}")))?;
        let (_, ts) = key
            .rsplit_once(':')
            .ok_or_else(|| LmdbError::InvalidKey(format!("invalid timeseries key: {key}")))?;
        ts.parse::<u64>().map_err(|err| {
            LmdbError::InvalidKey(format!("invalid timeseries key timestamp {key}: {err}"))
        })
    }
}

fn key_for_log(key: &[u8]) -> String {
    if let Ok(as_utf8) = std::str::from_utf8(key) {
        return as_utf8.to_string();
    }
    format!("0x{}", hex_lower(key))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[cfg(all(test, feature = "binary-keys"))]
mod tests {
    use super::*;

    #[cfg(feature = "binary-keys")]
    #[test]
    fn binary_key_roundtrip_timestamp_parse() {
        let key = encode_key("BTCUSDT", 1_234_567_890_u64);
        let ts = parse_timestamp_from_key(key.as_slice(), "BTCUSDT").expect("parse ts");
        assert_eq!(ts, 1_234_567_890_u64);
    }

    #[cfg(feature = "binary-keys")]
    #[test]
    fn binary_key_parse_rejects_invalid_prefix() {
        let key = encode_key("ETHUSDT", 42);
        let err = parse_timestamp_from_key(key.as_slice(), "BTCUSDT")
            .expect_err("prefix mismatch should fail");
        assert!(
            err.to_string()
                .contains("invalid binary timeseries key prefix")
        );
    }

    #[cfg(feature = "binary-keys")]
    #[test]
    fn binary_key_parse_rejects_invalid_length() {
        let malformed = b"BTCUSDT|short".to_vec();
        let err = parse_timestamp_from_key(malformed.as_slice(), "BTCUSDT")
            .expect_err("invalid length should fail");
        assert!(
            err.to_string()
                .contains("invalid binary timeseries key len")
        );
    }
}
