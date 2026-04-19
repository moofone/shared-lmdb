# Rotation Policies

Every `LmdbTimeseriesStore` carries a `RotationPolicy` set at construction time via `StoreConfig`. The policy is applied automatically after every mutating write, inside the same LMDB write transaction.

## `RotationPolicy` Enum

```rust
pub enum RotationPolicy {
    Forever,
    Circular { max_count: usize },
    MaxAgeMs { max_age_ms: u64 },
}
```

| Variant | Behavior |
|---|---|
| `Forever` | No trimming. All samples are retained indefinitely. |
| `Circular { max_count }` | Keeps only the newest `max_count` samples per symbol. Oldest samples are deleted. |
| `MaxAgeMs { max_age_ms }` | Keeps only samples whose timestamp is within `max_age_ms` milliseconds of the newest sample for that symbol. |

## Invocation Points

`apply_rotation_policy()` is called at the end of each write method, before the write transaction is committed:

- `upsert_symbol_sample`
- `upsert_symbol_batch_refs` (and by extension `upsert_symbol_batch`)
- `replace_symbol_history`

If rotation fails, the error propagates and the transaction is not committed; the store is left unchanged.

## Internal Implementation

### `trim_to_max_count(symbol, max_count, db, wtxn, label)`

1. Build the symbol prefix bytes via `symbol_prefix(symbol)`.
2. Iterate all keys matching the prefix using `db.prefix_iter(wtxn, ...)`.
3. Collect every key into a `Vec<Vec<u8>>`. Because LMDB sorts keys lexicographically (and timestamps are zero-padded to 20 digits in text mode or stored as big-endian bytes in binary-keys mode), the iteration order is oldest-first.
4. If `keys.len() <= max_count`, return immediately.
5. Otherwise, delete the first `keys.len() - max_count` keys (the oldest).

### `trim_to_max_age(symbol, max_age_ms, db, wtxn, label)`

1. Build the symbol prefix bytes.
2. Iterate all keys matching the prefix. For each key, parse the timestamp via `parse_timestamp_from_key`.
3. Track the maximum timestamp seen (`newest_ts`).
4. If no keys exist, return immediately.
5. Compute `cutoff = newest_ts.saturating_sub(max_age_ms)`.
6. Delete every key whose timestamp is strictly less than `cutoff`.

## Key Encoding

Timestamp ordering depends on the key encoding, which varies by feature flag:

- **Default (text keys):** `format!("{symbol}:{timestamp_ms:020}")` -- zero-padded to 20 digits, so lexicographic order matches chronological order.
- **`binary-keys` feature:** `symbol_bytes | separator | timestamp_ms.to_be_bytes()` -- big-endian encoding preserves ordering.

Both encodings guarantee that `prefix_iter` returns keys oldest-first, which `trim_to_max_count` relies on.
