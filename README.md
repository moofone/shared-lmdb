# shared-lmdb

Timeseries key-value store backed by LMDB. Stores binary payloads keyed by `(symbol, timestamp_ms)`.

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

// Write
let payload = b"some binary data";
store.upsert_symbol_sample("BTCUSDT", 1713523200000, payload, |_| Ok(()))?;

// Read from timestamp
let rows = store.load_symbol_from("BTCUSDT", 0)?;
for (ts, data) in &rows {
    println!("{ts}: {:?}", String::from_utf8_lossy(data));
}
```

## Feature Flags

| Flag | Description |
|---|---|
| `binary-keys` | Compact binary key encoding (`symbol\|be_bytes(ts)`) instead of zero-padded strings |
| `postgres-sync` | Sync/restore data to/from PostgreSQL (adds `tokio-postgres`, `tokio`) |

## Rotation Policies

- **`Forever`** -- keep all entries
- **`Circular { max_count }`** -- keep only the newest N entries per symbol
- **`MaxAgeMs { max_age_ms }`** -- evict entries older than newest minus the given age

Rotation is applied automatically after every write operation.

## Details

See `docs/integration-guide.md` for LLM/AI integration patterns and `docs/architecture/` for internal design.
