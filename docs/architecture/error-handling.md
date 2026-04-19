# Error Handling

## LmdbError Enum

All fallible operations return `Result<T, LmdbError>`.

```rust
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
```

## Variant Semantics

| Variant | Source | When it occurs |
|---|---|---|
| `Io` | `std::io::Error` | Directory creation in `open` |
| `Heed` | `heed::Error` | Any LMDB operation: env open, txn begin/commit, read, write, delete, iterate |
| `Validation` | (none) | Bad input: empty series_key, negative postgres timestamp, table name validation |
| `Conflict` | (none) | Conflict callback rejected an upsert |
| `InvalidKey` | (none) | Key parse failure: wrong prefix, wrong length, non-UTF8, unparseable timestamp |
| `Postgres` | `tokio_postgres::Error` | Any postgres operation (requires `postgres-sync`) |
| `Join` | `tokio::task::JoinError` | `spawn_blocking` task panicked or was cancelled (requires `postgres-sync`) |

## Context Strings

Every `Io`, `Heed`, `Postgres`, and `Join` error carries a `context: String` field describing the operation that failed. The `#[error("{context}: {source}")]` format produces messages like:

```
failed to open my_store env /data/store: No such file or directory (os error 2)
failed committing my_store db init: ...
postgres batch insert failed: ...
spawn_blocking load_from join failed: ...
```

The `label` field on `LmdbTimeseriesStore` is embedded in context strings to distinguish multiple store instances in logs.

## Error Propagation

Errors are propagated with `map_err`, attaching context at the call site:

```rust
let env = unsafe {
    heed::EnvOpenOptions::new()
        .map_size(config.map_size_bytes)
        .open(root)
}
.map_err(|source| LmdbError::Heed {
    context: format!("failed to open {label} env {}", root.display()),
    source,
})?;
```

The `#[source]` attribute (from `thiserror`) preserves the original error for downstream inspection via `Error::source()`.

## Conflict Validation Callbacks

Upsert methods accept caller-provided validation closures that run when a row already exists for the target key. These closures return `Result<(), LmdbError>` -- typically `LmdbError::Conflict`.

### Single upsert

```rust
pub fn upsert_sample<F>(
    &self,
    series_key: &str,
    timestamp_ms: u64,
    value: &[u8],
    validate_existing: F,
) -> Result<(), LmdbError>
where
    F: FnOnce(&[u8]) -> Result<(), LmdbError>,
```

Called with the existing payload bytes. If it returns `Err`, the write is aborted and the transaction is not committed.

### Batch upsert

```rust
pub fn upsert_batch_refs<F>(
    &self,
    series_key: &str,
    samples: &[(u64, &[u8])],
    mut validate_existing: F,
) -> Result<(), LmdbError>
where
    F: FnMut(u64, &[u8], &[u8]) -> Result<(), LmdbError>,
```

Called for each conflicting row with `(timestamp_ms, existing_payload, new_payload)`. A conflict abort from any row rolls back the entire batch.

## Transaction Semantics on Error

All write methods perform their work inside a single `RwTxn`. If any error occurs (including a conflict validation failure), the transaction is dropped without committing -- LMDB discards all pending changes. No partial writes are possible.
