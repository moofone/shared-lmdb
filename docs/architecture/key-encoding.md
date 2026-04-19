# Key Encoding

Timeseries keys in `shared-lmdb` are composed of a **symbol** string and a **timestamp** (milliseconds, `u64`). The encoding is selected at compile time via the `binary-keys` Cargo feature.

## Modes

### String mode (default)

Enabled when the `binary-keys` feature is **not** set.

```rust
// Format: "{symbol}:{timestamp_ms:020}" as UTF-8 bytes
let key = format!("{symbol}:{timestamp_ms:020}").into_bytes();

// Example: symbol = "BTCUSDT", ts = 1_234_567_890
// b"BTCUSDT:00000000001234567890"
```

- Separator: `b':'` (0x3A)
- Timestamp: zero-padded to 20 decimal digits
- Lexicographic byte order matches chronological order (monotonically increasing timestamp within a symbol)

### Binary mode (`feature = "binary-keys"`)

```rust
// Format: symbol_bytes | 0x7C | timestamp_ms.to_be_bytes()
let mut out = Vec::with_capacity(symbol.len() + 1 + 8);
out.extend_from_slice(symbol.as_bytes());
out.push(b'|');
out.extend_from_slice(&timestamp_ms.to_be_bytes());

// Example: symbol = "BTCUSDT", ts = 42
// b"BTCUSDT|\x00\x00\x00\x00\x00\x00\x00\x2A"
```

- Separator: `b'|'` (0x7C)
- Timestamp: 8 bytes, big-endian `u64`
- Big-endian preserves chronological ordering under byte comparison

## Comparison

| Property              | String mode                     | Binary mode                          |
|-----------------------|---------------------------------|--------------------------------------|
| Feature flag          | *(default)*                     | `binary-keys`                        |
| Separator byte        | `:` (0x3A)                      | `\|` (0x7C)                          |
| Timestamp width       | 20 bytes (zero-padded decimal)  | 8 bytes (big-endian `u64`)           |
| Total key size        | `symbol_len + 1 + 20` bytes     | `symbol_len + 1 + 8` bytes           |
| Human-readable        | Yes                             | No                                   |
| Chronological sort    | Yes                             | Yes                                  |
| Encode/decode cost    | `format!` / `str::parse`        | `to_be_bytes` / `from_be_bytes`      |

## Internal API

Four functions handle key encoding, all in `src/lib.rs`:

```rust
// Compose a full key from symbol and timestamp.
fn encode_key(symbol: &str, timestamp_ms: u64) -> Vec<u8>;

// Return symbol bytes + separator byte, used as a prefix for LMDB prefix iteration.
fn symbol_prefix(symbol: &str) -> Vec<u8>;

// Extract the timestamp from an encoded key. Validates prefix and length in binary mode.
fn parse_timestamp_from_key(key: &[u8], symbol: &str) -> Result<u64, LmdbError>;

// Debug display: returns the key as a UTF-8 string, or falls back to "0x" + hex.
fn key_for_log(key: &[u8]) -> String;
```

## Prefix iteration

Both modes construct the scan prefix as `symbol.as_bytes() + [separator_byte]`. This is passed to `heed::Database::prefix_iter` to retrieve all entries for a given symbol, which underpins range queries, rotation policies (`Circular`, `MaxAgeMs`), and enumeration endpoints.

## Tests (binary mode only)

Three tests are gated behind `#[cfg(feature = "binary-keys")]`:

- `binary_key_roundtrip_timestamp_parse` -- encode then decode, assert timestamp matches
- `binary_key_parse_rejects_invalid_prefix` -- encoding with one symbol, parsing with another returns `InvalidKey`
- `binary_key_parse_rejects_invalid_length` -- a key shorter than `prefix_len + 8` returns `InvalidKey`
