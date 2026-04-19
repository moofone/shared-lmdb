# Codex QA Validation Report - Iteration 2

**Crate**: `shared-lmdb` v0.1.0
**Files reviewed**: `src/lib.rs`, `src/postgres_sync.rs`, `tests/timeseries_store.rs`
**Date**: 2026-04-19
**Focus areas**: memory safety, concurrency safety, error handling, LMDB usage patterns

---

## 1. Unsafe Block Analysis

### `src/lib.rs:55` -- `heed::EnvOpenOptions::new().open(root)`

```rust
let env = unsafe {
    heed::EnvOpenOptions::new()
        .map_size(config.map_size_bytes)
        .max_dbs(config.max_dbs)
        .open(root)
}
```

**Verdict: SOUND.** This is the standard `heed` API contract. The `unsafe` block is required by `heed` because LMDB memory-maps files and the caller must guarantee the path is valid and the environment is opened correctly. The code:
- Creates the directory with `create_dir_all` before opening.
- Binds the env to the `LmdbTimeseriesStore` struct, which is `Clone` (cloning the `heed::Env` Arc-like handle), so the environment is not prematurely dropped.
- Uses `heed::Database<Str, Bytes>`, which are zero-copy borrowed types safe for LMDB's memory-mapped access.

**Severity**: N/A (sound usage)

### `tests/timeseries_store.rs:269,273,281` -- `std::env::set_var` / `remove_var`

```rust
unsafe { std::env::set_var(key, override_dir.as_os_str()) };
unsafe { std::env::remove_var(key) };
```

**Verdict: SOUND (test-only).** These are marked unsafe since Rust 1.82 because mutating environment variables is not thread-safe. The usage is confined to unit tests with unique keys and paired set/remove calls. No concurrent access to the same env key occurs in these sequential tests.

**Severity**: N/A (test-only, sound)

---

## 2. Concurrency Safety Analysis

### LMDB Transaction Model

**Key finding**: LMDB enforces a single-writer model. `heed` models this through Rust's type system -- `RwTxn` borrows the environment mutably, and LMDB itself blocks or errors if a second write transaction is attempted. This crate uses short-lived transactions (open, operate, commit) which is the correct pattern.

**`LmdbTimeseriesStore` is `Clone`**: The `env` and `db` handles are internally Arc-like in heed. Cloning the store and calling methods from multiple threads is safe because each call creates its own independent transaction.

**`postgres_sync.rs` uses `spawn_blocking`**: The `load_symbol_rows_nonblocking` and `replace_symbol_rows_nonblocking` functions correctly move cloned store handles into `spawn_blocking` tasks. This avoids blocking the async runtime while performing LMDB operations. However, there is a subtlety worth noting:

#### MEDIUM -- Potential write-transaction contention under concurrent `spawn_blocking`

**File**: `src/postgres_sync.rs:131,143`

If two tokio tasks call `sync_symbol_to_postgres` or `restore_symbol_from_postgres` concurrently for the same store, both `spawn_blocking` closures will attempt to open write transactions on the same LMDB environment. LMDB allows only one write transaction at a time -- the second will block (by default) or fail depending on configuration. This is not a data race or UB, but it could cause thread-pool stalls or deadlocks in the blocking thread pool if many concurrent syncs are queued.

**Severity**: MEDIUM -- This is an operational concern (thread-pool exhaustion) rather than a soundness bug. The code is correct but should document the single-writer constraint or add a mutex if concurrent writes are expected.

### Read path: `load_symbol_from` (lib.rs:207-234)

Opens a read transaction, iterates all keys with a prefix filter. Multiple readers can coexist with LMDB's MVCC model. This is safe.

---

## 3. Error Handling Review

### No `.unwrap()` or `.expect()` in library code

The library source files (`src/lib.rs`, `src/postgres_sync.rs`) contain zero `.unwrap()` or `.expect()` calls outside of unit tests. All fallible operations use `.map_err()` to convert errors to `Result<_, String>`. This is thorough and correct.

### `.expect()` calls in test files only

The test file uses `.expect("open store")`, `.expect("replace")`, etc. These are acceptable in test code.

### Error propagation chain is complete

Every LMDB operation (`write_txn`, `read_txn`, `put`, `get`, `delete`, `commit`, `iter`) and every postgres operation (`execute`, `query`, `transaction`) has its error caught and propagated with contextual error messages including the store label, key, and original error.

**Severity**: N/A (no issues found)

---

## 4. LMDB Transaction Lifecycle Analysis

### Transaction scoping: CORRECT

All transactions follow the same pattern:
1. Open transaction
2. Perform operations
3. Commit (for writes) or drop (for reads)

Write transactions are never held open across function boundaries. The commit is the last operation before returning `Ok(())`.

### `list_symbol_keys` uses a read-write transaction (`RwTxn`)

**File**: `src/lib.rs:250-268`

```rust
fn list_symbol_keys(
    db: &heed::Database<Str, Bytes>,
    txn: &heed::RwTxn<'_>,
    ...
)
```

This function is called from `replace_symbol_history` (lib.rs:89) which already holds a write transaction. The function only reads (iterates), so using the existing `RwTxn` for reads is fine -- LMDB permits reads within a write transaction. No issue here.

### Iterator drops before mutation

**Files**: `src/lib.rs:306`, `src/lib.rs:340`

In `trim_to_max_count` and `trim_to_max_age`, the code collects keys into a `Vec`, then explicitly drops the iterator with `drop(iter)` before calling `db.delete()`. This is necessary and correct -- LMDB iterators borrow the transaction, and attempting mutation while an iterator is live would violate the borrow checker. The explicit drop is the right approach.

**Severity**: N/A (correct pattern)

---

## 5. Integer Overflow / Off-by-One / Size Calculation Analysis

### `encode_key` -- No overflow risk

**File**: `src/lib.rs:246-248`

```rust
format!("{symbol}:{timestamp_ms:020}")
```

The key is a string. No integer arithmetic is involved. The zero-padded 20-digit format handles timestamps up to `u64::MAX` (18446744073709551615 -- 20 digits). This is correct.

### `parse_timestamp_from_key` -- Sound parsing

**File**: `src/lib.rs:355-361`

Uses `rsplit_once(':')` to extract the timestamp portion. Handles the error case where no colon exists. Parses to `u64` with error propagation. Correct.

### `trim_to_max_count` -- Off-by-one analysis

**File**: `src/lib.rs:294-316`

```rust
if keys.len() <= max_count {
    return Ok(());
}
let trim_count = keys.len().saturating_sub(max_count);
for key in keys.into_iter().take(trim_count) {
```

- When `keys.len() == max_count`, no trimming occurs. Correct.
- When `keys.len() == max_count + 1`, `trim_count == 1`, one oldest key is deleted. Correct.
- `saturating_sub` prevents underflow. Correct.
- LMDB keys are sorted lexicographically, and the key format `{symbol}:{timestamp:020}` ensures lexicographic order matches timestamp order. The oldest keys (smallest timestamps) come first in iteration, so `take(trim_count)` removes the oldest. Correct.

### `trim_to_max_age` -- Boundary condition analysis

**File**: `src/lib.rs:318-353`

```rust
let cutoff = latest.saturating_sub(max_age_ms);
for (key, ts) in keys_with_ts {
    if ts < cutoff {
        db.delete(...)
    }
}
```

- Uses `saturating_sub` to prevent underflow when `latest < max_age_ms` (would delete nothing, which is correct).
- The boundary is `ts < cutoff` (strict less-than), meaning entries exactly at the cutoff age are kept. This is a reasonable design choice and is consistent with the test assertion at line 129 of the test file.

### `ts_i64 as u64` cast in postgres_sync.rs

**File**: `src/postgres_sync.rs:118`

```rust
decoded.push((ts_i64 as u64, payload));
```

This is preceded by a check at line 112-113:
```rust
if ts_i64 < 0 {
    return Err(...);
}
```

The negative check before the cast makes this sound. A negative `i64` would be caught before the cast. The remaining range `[0, i64::MAX]` fits losslessly into `u64`.

**Severity**: N/A (all sound)

---

## 6. Send/Sync Implementations

No manual `unsafe impl Send` or `unsafe impl Sync` exist in this crate. The `LmdbTimeseriesStore` derives `Clone`, and `heed::Env` + `heed::Database` are `Send + Sync` by default in the heed crate. The `postgres_sync` module uses `spawn_blocking` which requires `Send`, and the store satisfies this through the cloned Arc-like handle. No issues.

---

## 7. Resource Leak Analysis

### Environment handle

`heed::Env` is internally reference-counted. It is dropped when the last clone is dropped, which closes the LMDB environment. No leak risk.

### Transactions

All transactions are either committed (writes) or dropped on scope exit (reads). No leaked transactions.

### Postgres transactions

In `sync_symbol_to_postgres` (postgres_sync.rs:43-82), the transaction is either committed on success or dropped on error (which rolls back). No leak risk.

### File descriptors / directory handles

The `tempfile::TempDir` in tests is properly scoped. The production code uses `create_dir_all` but does not hold any open directory handles.

**Severity**: N/A (no leaks)

---

## 8. Performance Observations

### HIGH -- Full-database scan in `load_symbol_from`

**File**: `src/lib.rs:218-233`

```rust
let mut iter = self.db.iter(&rtxn)...
while let Some(row) = iter.next() {
    let (key, raw) = row...;
    if !key.starts_with(prefix.as_str()) {
        continue;
    }
```

This iterates over **all keys in the database** and filters by prefix. For a database with many symbols, this is O(N) where N is the total number of keys across all symbols. LMDB supports range scans via cursor seek, which would make this O(M + log N) where M is the number of matching keys. The same pattern appears in `list_symbol_keys` (lib.rs:258), `trim_to_max_count` (lib.rs:298), and `trim_to_max_age` (lib.rs:328).

For the rotation-trim functions, the full scan is somewhat inherent to the counting use case, but `load_symbol_from` and `list_symbol_keys` could use a range scan starting at the prefix.

**Severity**: HIGH for production use with large databases -- this will degrade linearly as the total key count grows. The test suite validates correctness but the 220-second `perf_smoke` test (20,000 single upserts) suggests performance is already sensitive to data volume.

### MEDIUM -- Per-key memory allocation in rotation trim functions

**Files**: `src/lib.rs:294-316`, `src/lib.rs:318-353`

Both `trim_to_max_count` and `trim_to_max_age` collect all matching keys into a `Vec<String>` before trimming. For very large symbol histories, this means allocating a vector of all keys just to delete a few. An alternative would be to use a cursor-based approach that deletes during iteration, but LMDB's iterator invalidation rules make this tricky without the explicit `drop(iter)` pattern already in use.

### LOW -- Clippy: `while let` on iterator

**File**: `src/lib.rs:222,261,300,331`

Clippy warns that `while let Some(row) = iter.next()` should be `for row in iter`. This is a style/performance hint -- the `for` form can be slightly more optimized. Not a correctness issue.

---

## 9. SQL Injection Analysis

**File**: `src/postgres_sync.rs:163-186`

The `quote_table_identifier` function validates that all parts of the table name contain only `[a-zA-Z0-9_]` characters and wraps each part in double quotes. This prevents SQL injection through table names.

The `symbol` value in `sync_symbol_to_postgres` is passed as a parameterized query value (`$1`), not interpolated into the SQL string. This prevents SQL injection through symbol names.

**Severity**: N/A (correct)

---

## 10. Test Coverage Assessment

- **15 tests pass** (2 unit, 13 integration)
- Covers: basic CRUD, rotation policies (circular, max-age, forever), batch operations, conflict validation, persistence across reopens, multi-symbol isolation, environment variable override
- **Missing**: No tests for concurrent access (multiple threads), no tests for the postgres sync module end-to-end, no tests for edge cases like empty symbol history, symbol with special characters, or extremely large values
- The `perf_smoke` test takes ~220 seconds, which is excessive for a CI gate

---

## Summary of Findings

| # | Severity | Category | File:Line | Description |
|---|----------|----------|-----------|-------------|
| 1 | HIGH | Performance | `lib.rs:218-233` | `load_symbol_from` does a full-database scan instead of a range scan on the symbol prefix. Performance degrades linearly with total key count across all symbols. Same pattern in `list_symbol_keys`, `trim_to_max_count`, `trim_to_max_age`. |
| 2 | MEDIUM | Concurrency | `postgres_sync.rs:131,143` | `spawn_blocking` closures can contend on the single LMDB write transaction if multiple syncs run concurrently. Could stall the blocking thread pool. Should be documented or serialized with a mutex. |
| 3 | MEDIUM | Performance | `lib.rs:294-353` | Rotation trim functions allocate a `Vec` of all matching keys before deleting. For large histories this is wasteful. |
| 4 | LOW | Style | `lib.rs:222,261,300,331` | Clippy warns about `while let` on iterators -- should use `for` loop form. |
| 5 | LOW | Testing | `tests/timeseries_store.rs:231-260` | Perf smoke test takes 220 seconds, which is too slow for CI. Consider reducing N or gating behind a feature flag. |

---

## Overall Assessment: PASS

The crate is well-written with thorough error handling, no `unwrap()` in library code, sound `unsafe` usage, correct LMDB transaction lifecycle management, proper resource cleanup, and no memory safety concerns. The `heed` wrapper correctly leverages Rust's type system for concurrency safety. The integer arithmetic is defensive with `saturating_sub` and negative-value guards before casts.

The HIGH-severity finding (full-database scan pattern) is a performance concern that should be addressed before the database grows large, but it does not affect correctness or safety. The crate passes all quality gates for memory safety, concurrency soundness, and error handling correctness.
