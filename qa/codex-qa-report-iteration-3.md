# Codex QA Validation Report - Iteration 3 (Test Coverage, Quality, and Correctness)

## Scope

Crate: `shared-lmdb` v0.1.0
Focus: Test coverage, test quality, and test correctness.

Files reviewed:
- `src/lib.rs` (374 lines)
- `src/postgres_sync.rs` (213 lines, including 2 unit tests)
- `tests/timeseries_store.rs` (305 lines, 14 integration tests)
- `Cargo.toml`

All tests executed successfully:
- Default features: 13 integration tests pass (175.5s total, dominated by perf smoke)
- With `postgres-sync` feature: 14 integration tests + 2 unit tests pass (257.6s total)

Compiler warning observed: `unused variable: prefix` at `src/lib.rs:237` when `postgres-sync` feature is enabled (dead code in `load_symbol_from`).

---

## Test Inventory

### Integration Tests (`tests/timeseries_store.rs`)

| # | Test Name | Line | Primary Function Exercised |
|---|-----------|------|---------------------------|
| 1 | `replace_then_load_is_sorted_and_filtered` | 27 | `replace_symbol_history`, `load_symbol_from` (with `start_ms > 0`) |
| 2 | `replace_only_affects_target_symbol` | 50 | `replace_symbol_history` (cross-symbol isolation) |
| 3 | `upsert_duplicate_validates_existing` | 74 | `upsert_symbol_sample` (validator callback, conflict path) |
| 4 | `circular_rotation_enforces_rolling_window` | 100 | `RotationPolicy::Circular`, `trim_to_max_count` |
| 5 | `max_age_rotation_keeps_recent_time_window` | 117 | `RotationPolicy::MaxAgeMs`, `trim_to_max_age` |
| 6 | `forever_rotation_keeps_all_samples` | 133 | `RotationPolicy::Forever` |
| 7 | `batch_upsert_commits_all_rows` | 147 | `upsert_symbol_batch` (owned `Vec<u8>` variant) |
| 8 | `batch_upsert_refs_commits_all_rows` | 164 | `upsert_symbol_batch_refs` (borrowed `&[u8]` variant) |
| 9 | `batch_upsert_validates_conflicts` | 185 | `upsert_symbol_batch` (validator callback, conflict path) |
| 10 | `persistence_survives_reopen` | 206 | `LmdbTimeseriesStore::open` (reopen path) |
| 11 | `keyspace_isolation_under_heavy_multi_symbol_batch` | 224 | Multi-symbol `upsert_symbol_batch` + `load_symbol_from` |
| 12 | `perf_smoke_batch_vs_single_upsert` | 251 | Performance comparison: single vs batch |
| 13 | `resolve_data_dir_uses_env_override_when_set` | 282 | `resolve_data_dir` (env var override path) |
| 14 | `resolve_data_dir_falls_back_to_default_root` | 295 | `resolve_data_dir` (default fallback path) |

### Unit Tests (`src/postgres_sync.rs`, lines 188-212)

| # | Test Name | Line | Primary Function Exercised |
|---|-----------|------|---------------------------|
| 1 | `quote_identifier_accepts_safe_names` | 192 | `quote_table_identifier` (happy path: simple and schema-qualified) |
| 2 | `quote_identifier_rejects_unsafe_names` | 204 | `quote_table_identifier` (rejection path: 5 invalid inputs) |

---

## Function-Level Coverage Matrix

### `src/lib.rs` -- Public API

| Function | Tested? | Tests | Gaps |
|----------|---------|-------|------|
| `LmdbTimeseriesStore::open` | PARTIAL | #10 (reopen), indirect via all tests | Error paths untested: bad path, permission denied, invalid map_size, non-directory path |
| `replace_symbol_history` | YES | #1, #2, #12 (clear via empty iter) | Rotation interaction when replace inserts more than `max_count`; empty-into-empty not independently verified |
| `upsert_symbol_sample` | YES | #3, #4, #5, #6, #10 | First-insert (no existing) runs through all tests but the validator is only exercised for duplicate detection |
| `upsert_symbol_batch` | YES | #7, #9, #11, #12 | Empty batch not tested |
| `upsert_symbol_batch_refs` | YES | #8 | Only runs when `postgres-sync` feature enabled -- method itself is not feature-gated |
| `load_symbol_from` | YES | #1, #2, #4, #5, #6, #7, #8, #11 | `start_ms > 0` only in test #1; `start_ms = u64::MAX` not tested; loading from empty store not tested |
| `resolve_data_dir` | YES | #13, #14 | Whitespace-only env var not tested; env var with trailing/leading whitespace is tested indirectly via `.trim()` |
| `encode_key` | INDIRECT | Every write/read operation | No direct unit test for the encoding format string |

### `src/lib.rs` -- Private Functions

| Function | Tested? | How | Gaps |
|----------|---------|-----|------|
| `list_symbol_keys` | INDIRECT | Via `replace_symbol_history` | Never tested in isolation |
| `apply_rotation_policy` | INDIRECT | Via every write method | Trivial match dispatch; no direct test needed |
| `trim_to_max_count` | YES | Test #4 | Boundary: `max_count = 0`, `max_count = 1` not tested |
| `trim_to_max_age` | YES | Test #5 | Boundary: `max_age_ms = 0` not tested; single-sample scenario not tested |
| `parse_timestamp_from_key` | INDIRECT | Via every `load_symbol_from` call | Malformed key not tested (no colon, non-numeric suffix) |

### `src/postgres_sync.rs` -- Public API

| Function | Tested? | Gaps |
|----------|---------|------|
| `ensure_timeseries_schema` | NO | Requires live Postgres connection; no mock infrastructure |
| `sync_symbol_to_postgres` | NO | Requires live Postgres connection; no mock infrastructure |
| `restore_symbol_from_postgres` | NO | Requires live Postgres connection; no mock infrastructure |
| `PostgresTimeseriesConfig::new` | NO | No direct test of constructor defaults |
| `quote_table_identifier` | YES | Two unit tests cover safe/unsafe names |

### `src/postgres_sync.rs` -- Private Functions

| Function | Tested? | Gaps |
|----------|---------|------|
| `normalize_symbol` | NO | Whitespace trimming, uppercase conversion, empty rejection all untested |
| `load_symbol_rows_nonblocking` | NO | Requires tokio runtime + LMDB store |
| `replace_symbol_rows_nonblocking` | NO | Requires tokio runtime + LMDB store |

---

## Acceptance Criteria Validation

**AC 1 -- All public API functions have test coverage.** FAIL

Four public functions in `postgres_sync.rs` have zero test coverage: `ensure_timeseries_schema`, `sync_symbol_to_postgres`, `restore_symbol_from_postgres`, and `PostgresTimeseriesConfig::new`. These all require a live Postgres connection. While this is understandable, `normalize_symbol` (a pure function that guards SQL safety) has no tests despite containing non-trivial logic.

**AC 2 -- Edge cases are covered.** PARTIAL

- **Empty data:** `replace_symbol_history` with an empty iterator is used as a "clear" in test #12 (line 267) but the result is immediately overwritten -- the empty-clear behavior is never independently verified.
- **Max sizes:** Test #7 uses 10,000 rows; test #11 uses 5,000 per symbol. No test approaches the LMDB map size limit (2 GB default).
- **Concurrent access:** NO concurrent access tests exist. LMDB's primary feature (concurrent readers with single writer) is completely untested. No test creates multiple threads or tokio tasks accessing the store simultaneously.
- **Error paths:** NO error-path tests exist in `tests/timeseries_store.rs`. Every test uses `.expect()` on results. No test verifies error messages for invalid inputs, disk-full conditions, or permission failures.
- **`start_ms` filtering:** Only test #1 exercises `start_ms > 0`. No test verifies `start_ms` beyond the maximum timestamp (should return empty), or `start_ms = u64::MAX`.
- **Rotation boundaries:** `max_count = 0`, `max_count = 1`, and `max_age_ms = 0` are not tested.

**AC 3 -- Tests assert meaningful things.** PASS (with one exception)

- 13 of 14 tests assert exact row counts, specific timestamp values, and decoded payload values. This is strong.
- Test #12 (`perf_smoke_batch_vs_single_upsert`) asserts a timing relationship (`batch <= single * 2`), which is inherently flaky. See Flaky Test Patterns below.

**AC 4 -- Both integration and unit tests exist.** PARTIAL

- Integration tests: YES (14 in `tests/timeseries_store.rs`).
- Unit tests: Only 2, both in `postgres_sync.rs` for `quote_table_identifier`. No `#[cfg(test)] mod tests` block exists in `src/lib.rs`. All private functions in `lib.rs` are tested only indirectly through public API calls.

**AC 5 -- Critical paths have test coverage.** PARTIAL

- Rotation policies: PASS -- all three variants tested.
- Key encoding correctness: PASS -- verified indirectly through load assertions.
- Persistence: PASS -- test #10 verifies data survives reopen.
- Error handling: FAIL -- no error-path tests.
- Concurrency: FAIL -- no concurrent access tests.
- Symbol normalization: FAIL -- no tests for `normalize_symbol`.

**AC 6 -- Tests are isolated from each other.** PASS

- Each test creates its own `tempfile::TempDir` with a unique prefix.
- LMDB environments are fully isolated (different directories).
- No shared state between tests.
- The two `resolve_data_dir` tests use unique environment variable names.

---

## Algorithmic Correctness of Tests

### Rotation correctness (Tests #4, #5, #6)

- **Test #4 (circular):** Inserts 150 entries (ts=0..150) with `max_count=100`. Asserts `len() == 100`, `first().0 == 50`, `last().0 == 149`. This correctly verifies that the 50 oldest entries (ts=0..49) were trimmed, retaining ts=50..149. PASS.
- **Test #5 (max-age):** Inserts 41 entries (ts=100..141) with `max_age_ms=20`. Newest entry is ts=140, cutoff = 140 - 20 = 120. Asserts `first().0 == 120`, `last().0 == 140`. Entries with ts < 120 are removed. The boundary entry ts=120 survives because the comparison is `ts < cutoff` (strict less-than). PASS.
- **Test #6 (forever):** Inserts 2,000 entries with `Forever` policy. Asserts all 2,000 survive. PASS.

### Load filtering (Test #1)

- Inserts ts=3,1,2 (out of order). Loads with `start_ms=2`. Asserts 2 results: ts=2 and ts=3 with correct payload values. The `>=` comparison at `src/lib.rs:248` is correctly tested. PASS.

### Batch validation (Test #9)

- Seeds ts=5 with value 5. Batch-upserts ts=5 with value 999. Validator detects mismatch (`decode_u64(existing) != decode_u64(incoming)`) and returns `Err("conflict at 5")`. The error propagates correctly. The test asserts the error string contains `"conflict at 5"`. PASS.

### Symbol isolation (Tests #2, #11)

- Test #2: Seeds BTC and ETH, replaces BTC, verifies ETH unchanged. PASS.
- Test #11: Seeds 5,000 entries each for BTC and ETH, verifies both retain correct counts and payload values independently. PASS.

### Persistence (Test #10)

- Opens store, writes one entry, drops store (inner scope). Reopens store from same directory, reads entry, verifies value. PASS.

### Upsert validator (Test #3)

- First upsert: no existing entry, validator is a closure `|_| Ok(())` that is never called (correct -- `validate_existing` is `FnOnce` and only called when existing is `Some`).
- Second upsert: same ts, same value, validator checks `decode_u64(existing) == 1` -- PASS.
- Third upsert: validator always returns `Err("conflict")`, result is `Err`, test asserts error contains "conflict" -- PASS.

---

## Memory Management in Tests

- **TempDir lifecycle:** Each test creates a `tempfile::TempDir` that is dropped at test end, cleaning up the directory and all LMDB files. PASS.
- **No leaked LMDB environments:** The `LmdbTimeseriesStore` is dropped at end of each test (or inner scope in test #10), which closes the environment via `heed::Env`'s Arc-like reference counting. PASS.
- **Large allocations:** Tests #7, #8, and #11 allocate 5,000-10,000 `Vec<u8>` buffers (8 bytes each). Total ~80 KB per test. Well within reasonable limits. PASS.

---

## Placeholder Code Detection

- **TODO/FIXME/XXX/HACK comments in test files:** NONE.
- **Stub implementations:** NONE.
- **Hardcoded magic values in tests:** `max_count: 100`, `max_count: 20_000`, `max_count: 10_000`, `max_age_ms: 20`. These are appropriate test fixtures, not magic numbers. PASS.
- **`unwrap()` vs `expect()` in tests:** Tests consistently use `.expect()` with descriptive messages. PASS.

---

## Flaky Test Patterns

### HIGH: Test #12 (`perf_smoke_batch_vs_single_upsert`) -- Timing-Dependent Assertion

- **File:** `tests/timeseries_store.rs:251-280`
- **Assertion:** `assert!(batch <= single * 2, "batch unexpectedly slower: batch={batch:?} single={single:?}")`
- **Problem:** This test compares wall-clock timing of two operations. Under CI conditions (CPU throttling, container scheduling, disk I/O variability, shared runners), the batch path could take longer than `single * 2`, causing intermittent failures. The test already consumed 175.5 seconds locally, indicating the single-upsert path is extremely slow (20,000 individual LMDB write transactions).
- **Severity:** HIGH -- this test WILL fail intermittently under CI load.
- **Recommendation:** Either:
  (a) Remove the timing assertion and only verify both paths produce correct results.
  (b) Reduce N from 20,000 to 1,000 to reduce variance and duration.
  (c) Mark with `#[ignore]` and run only in a dedicated perf suite.
  (d) Use a much more generous multiplier (e.g., `single * 10`).

### MEDIUM: Tests #13, #14 (`resolve_data_dir`) -- Global State Mutation

- **File:** `tests/timeseries_store.rs:282-294, 295-305`
- **Pattern:** `unsafe { std::env::set_var(key, ...) }` / `unsafe { std::env::remove_var(key) }`
- **Problem:** Modifies process-global state. The tests use unique env keys and clean up, but `set_var` is `unsafe` since Rust 2024 edition (which this crate targets). If either test panics between `set_var` and `remove_var`, the env var leaks to subsequent tests in the same process.
- **Severity:** MEDIUM -- the unique key names prevent interference with other tests, but panic-safety is not guaranteed.
- **Recommendation:** Use a RAII guard (e.g., `scopeguard::guard`) or a `Drop`-based helper struct to ensure `remove_var` runs even on panic.

### LOW: Test Suite Duration

- Full suite: 175-258 seconds. Test #12 alone accounts for ~95% of this time.
- **Recommendation:** Reduce N in test #12, or split it into a separate `#[ignore]` test.

---

## Missing Test Coverage -- Specific Gaps

### HIGH: No error-path tests for `LmdbTimeseriesStore`

No test in `tests/timeseries_store.rs` exercises any `Err` return path from the library. Every call uses `.expect()`. The following error scenarios are untested:

- `open` with a path under a read-only parent directory
- `open` with a file path instead of a directory
- `open` with `map_size_bytes = 0` or extremely small values
- Write operations on a store opened in read-only mode (if applicable)
- `load_symbol_from` on a store with corrupted data

### HIGH: No concurrent access tests

LMDB's primary correctness guarantee is concurrent readers with a single writer. No test verifies:

- Multiple threads reading simultaneously from the same store
- A reader seeing a consistent snapshot while a writer is active (read-during-write visibility)
- The `heed::RwTxn` exclusivity contract -- two concurrent write attempts should block or fail, not corrupt

### MEDIUM: `normalize_symbol` is untested

- **File:** `src/postgres_sync.rs:155-161`
- This function performs three non-trivial operations: `trim()`, `to_ascii_uppercase()`, and empty-string rejection. It guards SQL injection safety. None of these operations are tested.
- Untested edge cases:
  - `"  btcusdt  "` should normalize to `"BTCUSDT"` (whitespace trimming + uppercase)
  - `""` should return `Err("symbol is required")`
  - `"   "` should return `Err("symbol is required")` (whitespace-only, empty after trim)
  - `"btc_usdt"` should normalize to `"BTC_USDT"` (underscore preserved)

### MEDIUM: `replace_symbol_history` with rotation policy interaction not tested

When `replace_symbol_history` is called with a `Circular` or `MaxAgeMs` policy, the function calls `apply_rotation_policy` after inserting all samples (line 107). This means replacing history with more samples than `max_count` will trigger trimming. This interaction is never explicitly tested. Test #1 uses `Circular { max_count: 100 }` but only inserts 3 samples, well under the limit.

### MEDIUM: `StoreConfig` field customization not tested

`StoreConfig::new` creates a config with hardcoded defaults. No test creates a `StoreConfig` with custom `map_size_bytes`, `max_dbs`, or `max_readers` and verifies the store respects those values. The `max_readers` field was recently added (present in working tree but not in committed version) and has zero test coverage.

### MEDIUM: `parse_timestamp_from_key` edge cases untested

- **File:** `src/lib.rs:367-373`
- No test for a key with no colon (e.g., `"nocolon"`) -- should return `Err`.
- No test for a key with a colon but non-numeric suffix (e.g., `"BTCUSDT:abc"`) -- should return `Err`.
- No test for a key with multiple colons (e.g., `"BTC:USDT:00000000000000000001"`) -- should parse `00000000000000000001` via `rsplit_once`.

### MEDIUM: `upsert_symbol_batch_refs` test is feature-gated

- **File:** `tests/timeseries_store.rs:164`
- Test #8 (`batch_upsert_refs_commits_all_rows`) only runs when the `postgres-sync` feature is enabled. But `upsert_symbol_batch_refs` is a public method available in the default feature set. This means running `cargo test` (default features) does NOT test this method. It is only tested when running `cargo test --features postgres-sync`.

### LOW: `load_symbol_from` with `start_ms` beyond max timestamp

No test verifies what happens when `start_ms` is larger than any timestamp in the store. Should return an empty `Vec`.

### LOW: Empty batch upsert

No test calls `upsert_symbol_batch` with an empty `samples` slice. This should succeed and change nothing.

### LOW: `replace_symbol_history` with empty samples on a symbol with no existing data

This is a no-op that should succeed silently. Not tested.

---

## Test Quality Assessment

### Strengths

1. **Descriptive test names.** Every test name clearly states the behavior being verified. No generic names like `test1` or `test_basic`. Example: `replace_only_affects_target_symbol` -- the name is the specification.

2. **Proper use of `expect()` with messages.** Every `.expect()` call includes a descriptive string (e.g., `expect("replace")`, `expect("open store")`, `expect("seed")`). This aids debugging.

3. **Clean helper functions.** `temp_dir()`, `open_store()`, `payload()`, `decode_u64()` reduce boilerplate without hiding important details. They are defined at module level and used consistently.

4. **Symbol isolation is explicitly tested.** Tests #2 and #11 verify that multi-symbol operations do not interfere. This is a common LMDB bug source and the tests catch it.

5. **Persistence is tested.** Test #10 verifies data survives store close and reopen. This is the most critical property of a durable store.

6. **Rotation boundary conditions are verified precisely.** Test #4 verifies exact trimming counts (150 entries, max 100, retains ts=50..149). Test #5 verifies exact age cutoff (newest=140, cutoff=120, boundary ts=120 survives).

7. **Test isolation is thorough.** Each test uses its own `TempDir` with unique prefix. No shared state. No test ordering dependencies.

### Weaknesses

1. **No negative tests for `lib.rs`.** Every test in `tests/timeseries_store.rs` exercises the happy path. The only error-path test is test #9 (`batch_upsert_validates_conflicts`), which tests a user-supplied validator, not an internally-generated error. Zero tests verify that the library correctly reports LMDB errors, invalid arguments, or filesystem failures.

2. **No `#[cfg(test)] mod tests` in `src/lib.rs`.** All test code is in the integration test file. This means private functions (`trim_to_max_count`, `trim_to_max_age`, `parse_timestamp_from_key`, `list_symbol_keys`) can only be tested indirectly through public API calls. Unit tests for these functions would make the test suite more precise and easier to debug when a private function breaks.

3. **Test #12 is too slow for practical development.** 175+ seconds for a single test makes `cargo test` unacceptably slow during development iteration. The 20,000-row volume is excessive for a smoke test. This discourages developers from running the test suite frequently.

4. **Test #8 is feature-gated unnecessarily.** `upsert_symbol_batch_refs` is always available but its test only runs with `postgres-sync` feature. This means default `cargo test` misses this method entirely.

5. **No concurrent access tests.** LMDB's raison d'etre is safe concurrent access. The test suite is entirely single-threaded. This is a significant gap for a library wrapping a concurrent key-value store.

---

## Compiler Warning

When running with `--features postgres-sync`, the compiler emits:

```
warning: unused variable: `prefix`
   --> shared-lmdb/src/lib.rs:237:13
    |
237 |         let prefix = format!("{symbol}:");
    |             ^^^^^^ help: if this is intentional, prefix it with an underscore: `_prefix`
```

This is a dead-code artifact from the `load_symbol_from` function. The `prefix` variable is constructed but never used because the function now uses `prefix_iter` with the prefix passed directly. The variable should be removed.

**Severity:** LOW (compiler warning, no correctness impact)

---

## Critical Issues Found

None. No placeholder code, no unimplemented features, no incorrect test assertions.

---

## Recommendations

1. **HIGH: Add error-path tests for `LmdbTimeseriesStore::open`.** Test with an invalid path (file instead of directory), a path under a non-existent parent, and a path with insufficient permissions. These can be simple `assert!(result.is_err())` tests that verify the error message contains expected context.

2. **HIGH: Add concurrent access tests.** Spawn multiple threads that read from the store while a writer is inserting data. Verify that readers see a consistent snapshot. This tests LMDB's primary correctness guarantee.

3. **HIGH: Fix the timing assertion in test #12.** Either remove the timing comparison and only verify correctness, reduce N from 20,000 to 1,000, or mark the test `#[ignore]` with a perf-test opt-in. The current form will flake under CI.

4. **MEDIUM: Add unit tests for `normalize_symbol`.** Test whitespace trimming, uppercase conversion, and empty-string rejection. This function is part of the SQL safety chain and deserves direct test coverage.

5. **MEDIUM: Add unit tests for `parse_timestamp_from_key`.** Test malformed keys (no colon, non-numeric suffix, multiple colons). This function is called on every row read and is a potential failure point.

6. **MEDIUM: Add a `#[cfg(test)] mod tests` block in `src/lib.rs`.** Unit-test private functions directly: `trim_to_max_count`, `trim_to_max_age`, `list_symbol_keys`, `parse_timestamp_from_key`. This makes the test suite more precise and easier to debug.

7. **MEDIUM: Add edge-case tests for rotation policies.** Test `Circular { max_count: 0 }` (should delete all), `Circular { max_count: 1 }` (boundary), and `MaxAgeMs { max_age_ms: 0 }` (should delete everything except the newest entry).

8. **MEDIUM: Test `replace_symbol_history` interaction with rotation.** Insert more samples than `max_count` in a single `replace_symbol_history` call and verify rotation is applied to the newly inserted data.

9. **MEDIUM: Move test #8 (`batch_upsert_refs_commits_all_rows`) out of the `postgres-sync` feature gate.** The `upsert_symbol_batch_refs` method is always available, so its test should always run. Alternatively, verify that the test is not feature-gated in the current source.

10. **LOW: Remove the unused `prefix` variable in `load_symbol_from` (`src/lib.rs:237`).** The variable is constructed but never used after the switch to `prefix_iter`. This silences the compiler warning.

11. **LOW: Add a `Drop`-based guard for env var cleanup in `resolve_data_dir` tests.** Use `scopeguard::guard` or a custom struct to ensure `remove_var` runs even on panic, preventing env var leakage.

---

## Overall Assessment

**PASS with recommendations.**

The test suite provides solid coverage of core happy-path operations: the full CRUD lifecycle, all three rotation policies, persistence across reopens, cross-symbol isolation, and batch operations. Tests are well-named, properly isolated, and assert meaningful values (exact counts, timestamps, decoded payloads).

Three significant gaps prevent a full pass:

1. **Zero error-path coverage.** Every test exercises only successful operations. The library has extensive error handling (every LMDB call is wrapped in `map_err`) but none of it is tested.
2. **Zero concurrent access coverage.** LMDB's primary feature -- safe concurrent readers -- is completely untested. This is the most important correctness property of the underlying engine.
3. **A timing-dependent test** that dominates suite duration (175+ seconds) and will flake under CI conditions.

The postgres sync module has partial unit test coverage: table identifier quoting is tested, but symbol normalization and all async operations are not. The `normalize_symbol` gap is particularly notable because it guards SQL injection.

The overall test-to-code ratio is healthy: 16 tests across 587 lines of source (1 test per 37 lines). The missing dimension is depth (error paths, edge cases, concurrency) rather than breadth (all major API surfaces are exercised at least once).
