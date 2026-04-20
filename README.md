# shared-lmdb

Timeseries key-value store backed by LMDB. Stores binary payloads keyed by `(series_key, timestamp_ms)`, where `series_key` is an arbitrary application-defined identifier.

## Usage

```toml
[dependencies]
shared-lmdb = { version = "0.1", features = ["binary-keys"] }
```

```rust
use shared_lmdb::{LmdbTimeseriesStore, StoreConfig, RotationPolicy};
use std::path::Path;

let config = StoreConfig::new("my_db", RotationPolicy::Circular { max_count: 1000 });
let store = LmdbTimeseriesStore::open(Path::new("./data"), config, "my-store")?;

let series_key = "tenant-a:device-42:temperature-c";

// Write
let payload = b"some binary data";
store.upsert_sample(series_key, 1713523200000, payload, |_| Ok(()))?;

// Read from timestamp
let rows = store.load_from(series_key, 0)?;
for (ts, data) in &rows {
    println!("{ts}: {:?}", String::from_utf8_lossy(data));
}
```

## Feature Flags

| Flag | Description |
|---|---|
| `binary-keys` | Compact binary key encoding (`series_key\|be_bytes(ts)`) instead of zero-padded strings |
| `postgres-sync` | Sync/restore data to/from PostgreSQL (adds `tokio-postgres`, `tokio`) |

## Rotation Policies

- **`Forever`** -- keep all entries
- **`Circular { max_count }`** -- keep only the newest N entries per series_key
- **`MaxAgeMs { max_age_ms }`** -- evict entries older than newest minus the given age

Rotation is applied automatically after every write operation.

## Details

See `docs/integration-guide.md` for LLM/AI integration patterns and `docs/architecture/` for internal design.

## Breaking Changes

The API is generic around `series_key` (not domain-specific `symbol`):

- `replace_symbol_history` -> `replace_history`
- `upsert_symbol_sample` -> `upsert_sample`
- `upsert_symbol_batch` -> `upsert_batch`
- `upsert_symbol_batch_refs` -> `upsert_batch_refs`
- `load_symbol_from` -> `load_from`
- `sync_symbol_to_postgres` -> `sync_series_to_postgres`
- `restore_symbol_from_postgres` -> `restore_series_from_postgres`
