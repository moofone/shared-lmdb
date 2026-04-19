# LLM/AI Integration Guide

This guide covers using `shared-lmdb` with the `binary-keys` feature for LLM and AI workloads: conversation history, embedding caches, agent memory, and token usage tracking.

## Why binary-keys for LLM Workloads

By default, `shared-lmdb` encodes keys as zero-padded strings (`"symbol:00000000000000000123"`). The `binary-keys` feature switches to a compact binary layout: `symbol_bytes | be_bytes(timestamp_ms)`. This eliminates the 20-byte zero-padded timestamp in favor of a fixed 8-byte big-endian encoding.

The result is smaller keys and faster encoding, which matters when you are ingesting high volumes of agent traces, conversation turns, or embedding lookups. Benchmarks in `examples/lmdb_batch_bench.rs` show measurable throughput improvement for batch workloads because the database spends less time on key comparison and encoding.

Enable it in `Cargo.toml`:

```toml
[dependencies]
shared-lmdb = { version = "0.1", features = ["binary-keys"] }
```

**Important:** `binary-keys` changes the on-disk key format. Databases created without it are not compatible with builds that have it enabled, and vice versa. Pick one mode and use it consistently.

## Quick Start

```rust
use shared_lmdb::{LmdbTimeseriesStore, StoreConfig, RotationPolicy, resolve_data_dir};
use std::path::Path;

fn main() -> Result<(), shared_lmdb::LmdbError> {
    let data_dir = resolve_data_dir(Path::new("./data"), "MY_APP_LMDB_DIR");

    let config = StoreConfig::new("chat_history", RotationPolicy::Circular { max_count: 500 });
    let store = LmdbTimeseriesStore::open(&data_dir, config, "chat-store")?;

    // Write a message
    let payload = r#"{"role":"user","content":"Hello"}"#.as_bytes();
    let ts = 1713523200000_u64; // millisecond timestamp
    store.upsert_symbol_sample("session-42", ts, payload, |_| Ok(()))?;

    // Read back
    let rows = store.load_symbol_from("session-42", 0)?;
    for (timestamp, data) in &rows {
        println!("ts={timestamp} payload={}", String::from_utf8_lossy(data));
    }

    Ok(())
}
```

## Schema Design Patterns

### Conversation History

Store each conversation session as a separate symbol. The payload is a serialized message (JSON, bincode, or any format you choose).

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    role: String,    // "system" | "user" | "assistant"
    content: String,
    tokens: u32,
}

fn append_turn(
    store: &LmdbTimeseriesStore,
    session_id: &str,
    ts_ms: u64,
    msg: &ChatMessage,
) -> Result<(), shared_lmdb::LmdbError> {
    let payload = serde_json::to_vec(msg).expect("serialize");
    store.upsert_symbol_sample(session_id, ts_ms, &payload, |_| Ok(()))
}

fn load_context(
    store: &LmdbTimeseriesStore,
    session_id: &str,
    from_ms: u64,
) -> Result<Vec<ChatMessage>, shared_lmdb::LmdbError> {
    let rows = store.load_symbol_from(session_id, from_ms)?;
    Ok(rows.into_iter().filter_map(|(_, raw)| serde_json::from_slice(&raw).ok()).collect())
}
```

Use `symbol = session_id` so each session is isolated. Use `load_symbol_from` with a cutoff timestamp to load only recent context.

### Embedding Cache

Cache embedding results keyed by a hash of the input text. Use the timestamp field to store a deterministic hash, and the payload for the raw embedding bytes.

```rust
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

fn cache_key(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

fn store_embedding(
    store: &LmdbTimeseriesStore,
    model: &str,
    text: &str,
    embedding: &[f32],
) -> Result<(), shared_lmdb::LmdbError> {
    let hash = cache_key(text);
    let payload = embedding.iter().flat_map(|f| f.to_be_bytes()).collect::<Vec<u8>>();
    store.upsert_symbol_sample(model, hash, &payload, |_| Ok(()))
}

fn lookup_embedding(
    store: &LmdbTimeseriesStore,
    model: &str,
    text: &str,
) -> Result<Option<Vec<f32>>, shared_lmdb::LmdbError> {
    let hash = cache_key(text);
    let rows = store.load_symbol_from(model, hash)?;
    Ok(rows.first().map(|(_, raw)| {
        raw.chunks_exact(4).map(|c| f32::from_be_bytes(c.try_into().unwrap())).collect()
    }))
}
```

Use `MaxAgeMs` rotation to evict stale embeddings after a TTL.

### Token Usage Tracking

Track per-model token consumption over time. Each row records usage stats for a single request.

```rust
#[derive(Serialize, Deserialize)]
struct TokenUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    model_version: String,
}

fn record_usage(
    store: &LmdbTimeseriesStore,
    model_id: &str,
    request_ts_ms: u64,
    usage: &TokenUsage,
) -> Result<(), shared_lmdb::LmdbError> {
    let payload = serde_json::to_vec(usage).expect("serialize");
    store.upsert_symbol_sample(model_id, request_ts_ms, &payload, |_| Ok(()))
}

fn total_tokens_since(
    store: &LmdbTimeseriesStore,
    model_id: &str,
    since_ms: u64,
) -> Result<u64, shared_lmdb::LmdbError> {
    let rows = store.load_symbol_from(model_id, since_ms)?;
    Ok(rows.iter().filter_map(|(_, raw)| {
        let u: TokenUsage = serde_json::from_slice(raw).ok()?;
        Some(u.prompt_tokens as u64 + u.completion_tokens as u64)
    }).sum())
}
```

### Agent Memory

Use composite symbols to partition memory by agent and type.

```rust
fn memory_symbol(agent_id: &str, memory_type: &str) -> String {
    format!("{agent_id}::{memory_type}")
}

// Write an event
fn record_event(
    store: &LmdbTimeseriesStore,
    agent_id: &str,
    event_type: &str,
    ts_ms: u64,
    event_data: &[u8],
) -> Result<(), shared_lmdb::LmdbError> {
    store.upsert_symbol_sample(&memory_symbol(agent_id, event_type), ts_ms, event_data, |_| Ok(()))
}

// Recall recent events
fn recall(
    store: &LmdbTimeseriesStore,
    agent_id: &str,
    event_type: &str,
    from_ms: u64,
) -> Result<Vec<(u64, Vec<u8>)>, shared_lmdb::LmdbError> {
    store.load_symbol_from(&memory_symbol(agent_id, event_type), from_ms)
}
```

Symbol examples: `"agent-7::episodes"`, `"agent-7::reflections"`, `"agent-7::tool_calls"`.

## Rotation Strategies for LLM Data

### Circular -- Sliding Window Context

Keep only the N most recent entries per symbol. Older entries are trimmed after each write. Ideal for maintaining a rolling context window.

```rust
// Keep the last 200 turns per session
let config = StoreConfig::new(
    "chat_history",
    RotationPolicy::Circular { max_count: 200 },
);
```

### MaxAgeMs -- TTL-Based Cache Eviction

Remove entries older than the newest entry minus `max_age_ms`. Good for embedding caches or short-lived agent state.

```rust
// Evict embeddings older than 24 hours relative to the newest entry
let config = StoreConfig::new(
    "embedding_cache",
    RotationPolicy::MaxAgeMs { max_age_ms: 86_400_000 },
);
```

### Forever -- Unlimited Retention

Keep everything. Suitable for audit logs or permanent memory stores.

```rust
let config = StoreConfig::new("audit_log", RotationPolicy::Forever);
```

## Batch Ingestion

When loading conversation histories in bulk (e.g., restoring from a backup or replaying a session), use batch upserts. All rows are written in a single LMDB transaction.

```rust
fn restore_conversation(
    store: &LmdbTimeseriesStore,
    session_id: &str,
    turns: Vec<(u64, Vec<u8>)>,
) -> Result<(), shared_lmdb::LmdbError> {
    // Owned data -- pass slices of (u64, Vec<u8>)
    store.upsert_symbol_batch(session_id, turns.as_slice(), |_, _, _| Ok(()))
}

fn restore_from_refs(
    store: &LmdbTimeseriesStore,
    session_id: &str,
    turns: &[(u64, &[u8])],
) -> Result<(), shared_lmdb::LmdbError> {
    // Borrowed data -- avoids copies when you already have slices
    store.upsert_symbol_batch_refs(session_id, turns, |_, _, _| Ok(()))
}
```

Batch writes are significantly faster than individual upserts for large datasets because they amortize the transaction overhead across all rows.

## Conflict Resolution

The `upsert_symbol_sample` and batch upsert methods accept a validation callback that runs when an existing row with the same key is found. Return `Ok(())` to allow the overwrite, or `Err(LmdbError::Conflict(...))` to abort.

### Single Upsert

The callback receives the existing value:

```rust
use shared_lmdb::LmdbError;

store.upsert_symbol_sample("session-42", ts, &new_payload, |existing| {
    let old_version: Message = serde_json::from_slice(existing).expect("deserialize");
    if old_version.sequence >= incoming_sequence {
        return Err(LmdbError::Conflict("stale write".into()));
    }
    Ok(())
})?;
```

### Batch Upsert

The callback receives the timestamp, existing value, and incoming value:

```rust
store.upsert_symbol_batch("session-42", &rows, |ts, existing, incoming| {
    let old: Message = serde_json::from_slice(existing).expect("deserialize");
    let new: Message = serde_json::from_slice(incoming).expect("deserialize");
    if new.version <= old.version {
        return Err(LmdbError::Conflict(format!("stale at {ts}")));
    }
    Ok(())
})?;
```

If the callback returns an error, the entire batch transaction is aborted -- no partial writes.

## Data Directory Setup

Use `resolve_data_dir` to allow runtime override via environment variables:

```rust
use shared_lmdb::resolve_data_dir;
use std::path::Path;

// Use LMDB_DATA_DIR env var if set, otherwise fall back to ./data
let data_dir = resolve_data_dir(Path::new("./data"), "LMDB_DATA_DIR");

let store = LmdbTimeseriesStore::open(
    &data_dir,
    StoreConfig::new("my_db", RotationPolicy::Forever),
    "my-label",
)?;
```

This is useful for containerized deployments where the storage path is injected via environment.
