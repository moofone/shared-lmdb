use crate::{LmdbError, LmdbTimeseriesStore};
use tokio::task;

#[derive(Debug, Clone)]
pub struct PostgresTimeseriesConfig {
    pub table_name: String,
    pub chunk_size: usize,
}

impl PostgresTimeseriesConfig {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            chunk_size: 5_000,
        }
    }
}

pub async fn ensure_timeseries_schema(
    client: &tokio_postgres::Client,
    config: &PostgresTimeseriesConfig,
) -> Result<(), LmdbError> {
    let table = quote_table_identifier(config.table_name.as_str())?;
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (series_key TEXT NOT NULL, timestamp_ms BIGINT NOT NULL, payload BYTEA NOT NULL, PRIMARY KEY(series_key, timestamp_ms))"
    );
    client
        .execute(sql.as_str(), &[])
        .await
        .map_err(|source| LmdbError::Postgres {
            context: "postgres ensure schema failed".to_string(),
            source,
        })?;
    Ok(())
}

pub async fn sync_series_to_postgres(
    store: &LmdbTimeseriesStore,
    client: &mut tokio_postgres::Client,
    series_key: &str,
    config: &PostgresTimeseriesConfig,
) -> Result<usize, LmdbError> {
    let series_key = normalize_series_key(series_key)?;
    let rows = load_series_rows_nonblocking(store, series_key.clone()).await?;
    let table = quote_table_identifier(config.table_name.as_str())?;
    let tx = client
        .transaction()
        .await
        .map_err(|source| LmdbError::Postgres {
            context: "postgres tx begin failed".to_string(),
            source,
        })?;

    let delete_sql = format!("DELETE FROM {table} WHERE series_key = $1");
    tx.execute(delete_sql.as_str(), &[&series_key])
        .await
        .map_err(|source| LmdbError::Postgres {
            context: "postgres delete existing rows failed".to_string(),
            source,
        })?;

    if rows.is_empty() {
        tx.commit().await.map_err(|source| LmdbError::Postgres {
            context: "postgres tx commit failed".to_string(),
            source,
        })?;
        return Ok(0);
    }

    let insert_sql = format!(
        "INSERT INTO {table} (series_key, timestamp_ms, payload) SELECT $1, item.timestamp_ms, item.payload FROM unnest($2::bigint[], $3::bytea[]) AS item(timestamp_ms, payload)"
    );

    let chunk_size = config.chunk_size.max(1);
    for chunk in rows.chunks(chunk_size) {
        let mut timestamps = Vec::with_capacity(chunk.len());
        let mut payloads = Vec::with_capacity(chunk.len());
        for (timestamp_ms, payload) in chunk {
            let ts = i64::try_from(*timestamp_ms).map_err(|_| {
                LmdbError::Validation(format!(
                    "timestamp overflow for postgres i64: {timestamp_ms}"
                ))
            })?;
            timestamps.push(ts);
            payloads.push(payload.clone());
        }
        tx.execute(insert_sql.as_str(), &[&series_key, &timestamps, &payloads])
            .await
            .map_err(|source| LmdbError::Postgres {
                context: "postgres batch insert failed".to_string(),
                source,
            })?;
    }

    tx.commit().await.map_err(|source| LmdbError::Postgres {
        context: "postgres tx commit failed".to_string(),
        source,
    })?;
    Ok(rows.len())
}

pub async fn restore_series_from_postgres(
    store: &LmdbTimeseriesStore,
    client: &tokio_postgres::Client,
    series_key: &str,
    config: &PostgresTimeseriesConfig,
) -> Result<usize, LmdbError> {
    let series_key = normalize_series_key(series_key)?;
    let table = quote_table_identifier(config.table_name.as_str())?;
    let sql = format!(
        "SELECT timestamp_ms, payload FROM {table} WHERE series_key = $1 ORDER BY timestamp_ms ASC"
    );

    let rows = client
        .query(sql.as_str(), &[&series_key])
        .await
        .map_err(|source| LmdbError::Postgres {
            context: "postgres restore query failed".to_string(),
            source,
        })?;

    if rows.is_empty() {
        replace_series_rows_nonblocking(store, series_key.clone(), Vec::new()).await?;
        return Ok(0);
    }

    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        let ts_i64: i64 = row.try_get(0).map_err(|source| LmdbError::Postgres {
            context: "postgres row timestamp decode failed".to_string(),
            source,
        })?;
        if ts_i64 < 0 {
            return Err(LmdbError::Validation(format!(
                "postgres row timestamp is negative: {ts_i64}"
            )));
        }
        let payload: Vec<u8> = row.try_get(1).map_err(|source| LmdbError::Postgres {
            context: "postgres row payload decode failed".to_string(),
            source,
        })?;
        decoded.push((ts_i64 as u64, payload));
    }

    replace_series_rows_nonblocking(store, series_key, decoded.clone()).await?;
    Ok(decoded.len())
}

async fn load_series_rows_nonblocking(
    store: &LmdbTimeseriesStore,
    series_key: String,
) -> Result<Vec<(u64, Vec<u8>)>, LmdbError> {
    let store = store.clone();
    let rows: Result<Vec<(u64, Vec<u8>)>, LmdbError> =
        task::spawn_blocking(move || store.load_from(series_key.as_str(), 0))
            .await
            .map_err(|source| LmdbError::Join {
                context: "spawn_blocking load_from join failed".to_string(),
                source,
            })?;
    rows
}

async fn replace_series_rows_nonblocking(
    store: &LmdbTimeseriesStore,
    series_key: String,
    rows: Vec<(u64, Vec<u8>)>,
) -> Result<(), LmdbError> {
    let store = store.clone();
    let result: Result<(), LmdbError> = task::spawn_blocking(move || {
        let refs = rows
            .iter()
            .map(|(timestamp_ms, payload)| (*timestamp_ms, payload.as_slice()))
            .collect::<Vec<_>>();
        store.replace_history(series_key.as_str(), refs)
    })
    .await
    .map_err(|source| LmdbError::Join {
        context: "spawn_blocking replace_history join failed".to_string(),
        source,
    })?;
    result
}

fn normalize_series_key(series_key: &str) -> Result<String, LmdbError> {
    let out = series_key.trim().to_string();
    if out.is_empty() {
        return Err(LmdbError::Validation("series_key is required".to_string()));
    }
    Ok(out)
}

fn quote_table_identifier(raw: &str) -> Result<String, LmdbError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(LmdbError::Validation("table_name is required".to_string()));
    }
    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() {
        return Err(LmdbError::Validation("table_name is required".to_string()));
    }
    let mut quoted = Vec::with_capacity(parts.len());
    for part in parts {
        if part.is_empty() {
            return Err(LmdbError::Validation(format!(
                "invalid table identifier: {raw}"
            )));
        }
        if !part
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return Err(LmdbError::Validation(format!(
                "invalid table identifier: {raw}"
            )));
        }
        quoted.push(format!("\"{part}\""));
    }
    Ok(quoted.join("."))
}

#[cfg(test)]
mod tests {
    use super::quote_table_identifier;

    #[test]
    fn quote_identifier_accepts_safe_names() {
        assert_eq!(
            quote_table_identifier("lmdb_timeseries").expect("quoted"),
            "\"lmdb_timeseries\""
        );
        assert_eq!(
            quote_table_identifier("public.lmdb_timeseries").expect("quoted"),
            "\"public\".\"lmdb_timeseries\""
        );
    }

    #[test]
    fn quote_identifier_rejects_unsafe_names() {
        let bad = ["", "public.", "my-table", "x;drop table y", "a.b.c-"];
        for name in bad {
            let err = quote_table_identifier(name).expect_err("should fail");
            let msg = err.to_string();
            assert!(msg.contains("invalid") || msg.contains("required"));
        }
    }
}
