# Postgres Sync

Optional feature: `postgres-sync`. Gated by `#[cfg(feature = "postgres-sync")]`.

## Dependencies

```toml
[dependencies]
tokio-postgres = { version = "0.7", optional = true }
tokio = { version = "1", features = ["rt"], optional = true }

[features]
postgres-sync = ["dep:tokio-postgres", "dep:tokio"]
```

## Error Variants

Two additional `LmdbError` variants are enabled when the feature is active:

```rust
#[error("{context}: {source}")]
Postgres { context: String, source: tokio_postgres::Error }

#[error("{context}: {source}")]
Join { context: String, source: tokio::task::JoinError }
```

## Public API

### `PostgresTimeseriesConfig`

```rust
pub struct PostgresTimeseriesConfig {
    pub table_name: String,
    pub chunk_size: usize, // default 5000
}
```

Construct via `PostgresTimeseriesConfig::new(table_name)` which sets `chunk_size` to 5000.

### `ensure_timeseries_schema(client, config)`

Creates the target table if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS <table> (
    series_key  TEXT    NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    payload     BYTEA   NOT NULL,
    PRIMARY KEY (series_key, timestamp_ms)
)
```

### `sync_series_to_postgres(store, client, series_key, config) -> Result<usize>`

Full replace of a series_key's data in Postgres from LMDB:

1. Normalize the series_key (`normalize_series_key`).
2. Load all rows from LMDB via `load_series_rows_nonblocking` (spawns a `spawn_blocking` task to call `store.load_from(series_key, 0)`).
3. Begin a Postgres transaction.
4. `DELETE FROM <table> WHERE series_key = $1`.
5. If rows are empty, commit and return `Ok(0)`.
6. Insert rows in chunks of `config.chunk_size` using `unnest()` arrays:

```sql
INSERT INTO <table> (series_key, timestamp_ms, payload)
SELECT $1, item.timestamp_ms, item.payload
FROM unnest($2::bigint[], $3::bytea[]) AS item(timestamp_ms, payload)
```

7. Commit. Returns the total row count inserted.

### `restore_series_from_postgres(store, client, series_key, config) -> Result<usize>`

Loads a series_key's data from Postgres and replaces the LMDB contents:

1. Normalize the series_key.
2. Query Postgres:

```sql
SELECT timestamp_ms, payload FROM <table> WHERE series_key = $1 ORDER BY timestamp_ms ASC
```

3. Decode each row: cast `timestamp_ms` from `i64` to `u64` (reject negatives), extract `payload` as `Vec<u8>`.
4. Call `replace_series_rows_nonblocking` which spawns a `spawn_blocking` task calling `store.replace_history`.
5. Returns the row count restored.

## Internal Helpers

### `normalize_series_key(series_key: &str) -> Result<String>`

Trims whitespace, converts to ASCII uppercase. Returns `LmdbError::Validation` if the result is empty.

### `quote_table_identifier(raw: &str) -> Result<String>`

Validates that each dot-separated part of the identifier contains only ASCII alphanumeric characters and underscores. Wraps each part in double quotes. Rejects empty parts, empty input, and any characters outside the allowed set.

Examples:

| Input | Output |
|---|---|
| `lmdb_timeseries` | `"lmdb_timeseries"` |
| `public.lmdb_timeseries` | `"public"."lmdb_timeseries"` |

### `load_series_rows_nonblocking(store, series_key) -> Result<Vec<(u64, Vec<u8>)>>`

Bridges synchronous LMDB reads into the tokio runtime via `task::spawn_blocking`. Clones the store handle (cheap -- `LmdbTimeseriesStore` is `Clone` via `heed::Env` Arc semantics).

### `replace_series_rows_nonblocking(store, series_key, rows) -> Result<()>`

Same pattern: converts `Vec<(u64, Vec<u8>)>` into `Vec<(u64, &[u8])>` references, then calls `store.replace_history` inside `spawn_blocking`.
