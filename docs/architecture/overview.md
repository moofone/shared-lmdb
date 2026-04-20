# Architecture Overview

## What It Is

`shared-lmdb` is a Rust library providing a timeseries key-value store backed by LMDB via the `heed` crate. It maps `(series_key, timestamp_ms)` pairs to opaque binary payloads, with built-in rotation policies for data retention.

## Architecture

```
LMDB Environment (heed::Env)
  |
  +-- Database (heed::Database<Bytes, Bytes>)
        |
        +-- Key: "SYMBOL|TIMESTAMP"  (binary-keys) or "SYMBOL:TIMESTAMP" (default)
        +-- Value: raw bytes
```

A single `heed::Env` is opened against a directory on disk. One named database is created inside it. All data is stored in a single `Database<Bytes, Bytes>` instance with series_key-prefixed keys, enabling prefix iteration per series_key.

## Data Model

Each record is:

- **Key**: series_key string + separator + encoded timestamp.
  - Default (string keys): `"{series_key}:{timestamp_ms:020}"` -- zero-padded 20-digit timestamp for lexicographic ordering.
  - `binary-keys` feature: `series_key_bytes | 0x7C | timestamp_ms.to_be_bytes()` -- compact 8-byte big-endian timestamp.
- **Value**: arbitrary `&[u8]`.

Keys are sorted by LMDB's native byte comparison. Prefix iteration with `series_key + separator` yields all rows for a given series_key in timestamp order.

## Public API

```rust
// Main store handle. Clone is cheap -- shares the underlying Env.
pub struct LmdbTimeseriesStore { /* env, db, label, rotation_policy */ }

// Configuration passed to LmdbTimeseriesStore::open.
pub struct StoreConfig {
    pub db_name: String,
    pub map_size_bytes: usize,       // default 2 GB
    pub max_dbs: u32,                // default 8
    pub max_readers: u32,            // default 256
    pub rotation_policy: RotationPolicy,
}

// Rotation policies applied after every write.
pub enum RotationPolicy {
    Forever,
    Circular { max_count: usize },
    MaxAgeMs { max_age_ms: u64 },
}
```

Key methods on `LmdbTimeseriesStore`:

| Method | Txn Type | Description |
|---|---|---|
| `open(root, config, label)` | write | Create env + database |
| `replace_history(series_key, samples)` | write | Delete all rows for series_key, insert new ones |
| `upsert_sample(series_key, ts, value, validate)` | write | Insert or update a single row with conflict callback |
| `upsert_batch(series_key, samples, validate)` | write | Batch upsert with per-row conflict callback |
| `load_from(series_key, start_ms)` | read | Read rows for series_key with optional start offset |

Utility:

```rust
pub fn resolve_data_dir(default_root: &Path, env_var: &str) -> PathBuf;
```

Returns the path from the environment variable if set and non-empty, otherwise `default_root`.

## Rotation Policies

Applied synchronously after every write operation within the same write transaction:

- **Forever**: no trimming.
- **Circular**: keeps the newest `max_count` rows, deletes the rest.
- **MaxAgeMs**: deletes rows older than `newest_timestamp - max_age_ms`.

## Feature Flags

| Flag | Default | Description |
|---|---|---|
| (none) | yes | String-based keys with zero-padded timestamps |
| `binary-keys` | no | Compact binary key encoding (`series_key\|u64_be`) |
| `postgres-sync` | no | Sync/restore per-series_key data to/from PostgreSQL |

The `postgres-sync` feature adds the `postgres_sync` module with `sync_series_to_postgres` and `restore_series_from_postgres` functions. It pulls in `tokio-postgres` and `tokio`.

## Thread Safety

`LmdbTimeseriesStore` is `Clone` (not `Sync`). Cloning shares the same `heed::Env` handle. LMDB's concurrency model is:

- **Multiple concurrent readers**: each `read_txn` is isolated and does not block writers.
- **Single writer**: `write_txn` is exclusive. Callers must ensure no concurrent writes from the same thread or coordinate externally.

The `postgres-sync` module bridges the synchronous LMDB API with async tokio-postgres by using `tokio::task::spawn_blocking` for LMDB operations.

## Dependencies

| Crate | Version | Required | Purpose |
|---|---|---|---|
| `heed` | 0.20 | yes | LMDB wrapper |
| `thiserror` | 2 | yes | Error derive macros |
| `tokio-postgres` | 0.7 | no (`postgres-sync`) | PostgreSQL client |
| `tokio` | 1 | no (`postgres-sync`) | Runtime for spawn_blocking |
