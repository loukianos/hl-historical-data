use crate::config::QuestDbConfig;
use anyhow::{anyhow, Result};
use std::pin::Pin;
use tokio_postgres::{types::ToSql, NoTls, Row, RowStream};
use tokio_stream::StreamExt;

type SqlParam<'a> = &'a (dyn ToSql + Sync);
type SqlParams<'a> = &'a [SqlParam<'a>];

/// QuestDB row stream wrapper that keeps the underlying client alive
/// for the lifetime of the stream.
pub struct QueryRowStream {
    _client: tokio_postgres::Client,
    row_stream: Pin<Box<RowStream>>,
}

impl QueryRowStream {
    fn new(client: tokio_postgres::Client, row_stream: RowStream) -> Self {
        Self {
            _client: client,
            row_stream: Box::pin(row_stream),
        }
    }

    pub async fn next(&mut self) -> Option<std::result::Result<Row, tokio_postgres::Error>> {
        self.row_stream.next().await
    }
}

/// QuestDB reader via PostgreSQL wire protocol (port 8812).
pub struct QuestDbReader {
    config: QuestDbConfig,
}

impl QuestDbReader {
    pub fn new(config: &QuestDbConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn connection_string(&self) -> String {
        format!(
            "host={} port={} user=admin password=quest dbname=qdb",
            self.config.pg_host, self.config.pg_port
        )
    }

    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) =
            tokio_postgres::connect(&self.connection_string(), NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("QuestDB connection error: {}", e);
            }
        });
        Ok(client)
    }

    /// Execute a read query and collect all rows in memory.
    pub async fn query(&self, sql: &str, params: SqlParams<'_>) -> Result<Vec<Row>> {
        let client = self.connect().await?;
        Ok(client.query(sql, params).await?)
    }

    /// Execute a read query and return a row stream.
    ///
    /// This is useful for gRPC server-side streaming where we don't want to buffer
    /// full result sets in memory.
    pub async fn query_stream(&self, sql: &str, params: SqlParams<'_>) -> Result<QueryRowStream> {
        let client = self.connect().await?;
        let row_stream = client.query_raw(sql, params.iter().copied()).await?;
        Ok(QueryRowStream::new(client, row_stream))
    }

    /// Execute a statement (DDL / DELETE / ALTER, etc).
    pub async fn execute(&self, sql: &str, params: SqlParams<'_>) -> Result<u64> {
        let client = self.connect().await?;
        Ok(client.execute(sql, params).await?)
    }

    /// Execute a query that must return exactly one row.
    pub async fn query_one(&self, sql: &str, params: SqlParams<'_>) -> Result<Row> {
        let client = self.connect().await?;
        Ok(client.query_one(sql, params).await?)
    }

    /// Small smoke-test query for connectivity checks.
    pub async fn select_one(&self) -> Result<i32> {
        let row = self.query_one("SELECT 1", &[]).await?;

        let value = row
            .try_get::<_, i32>(0)
            .or_else(|_| row.try_get::<_, i64>(0).map(|v| v as i32))
            .map_err(|e| anyhow!("Could not decode SELECT 1 result: {e}"))?;

        Ok(value)
    }

    /// DB ping helper used by startup checks.
    pub async fn ping(&self) -> Result<()> {
        let value = self.select_one().await?;
        if value != 1 {
            return Err(anyhow!("Unexpected ping response from QuestDB: {value}"));
        }
        Ok(())
    }

    /// Create fills and fills_quarantine tables if they don't exist.
    ///
    /// Type/index rationale is documented in `docs/adr/0001-questdb-types.md`:
    /// - TIMESTAMP: all temporal fields
    /// - DOUBLE: price/size/fee/pnl numeric fields
    /// - LONG: IDs and block numbers
    /// - BOOLEAN: logical flags
    /// - SYMBOL: low-cardinality/query-heavy dimensions in `fills`
    /// - VARCHAR: high-cardinality/unbounded strings to avoid symbol-table explosion
    pub async fn ensure_tables(&self) -> Result<()> {
        let client = self.connect().await?;

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS fills (
                    time TIMESTAMP,
                    block_time TIMESTAMP,
                    block_number LONG,
                    address SYMBOL INDEX,
                    coin SYMBOL INDEX,
                    type SYMBOL,
                    px DOUBLE,
                    sz DOUBLE,
                    is_buy BOOLEAN,
                    start_position DOUBLE,
                    is_gaining_inventory BOOLEAN,
                    closed_pnl DOUBLE,
                    hash VARCHAR,
                    oid LONG,
                    crossed BOOLEAN,
                    fee DOUBLE,
                    tid LONG,
                    fee_token SYMBOL,
                    cloid VARCHAR,
                    builder_fee VARCHAR,
                    builder VARCHAR,
                    local_time TIMESTAMP
                ) TIMESTAMP(time) PARTITION BY DAY;",
                &[],
            )
            .await?;

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS fills_quarantine (
                    time TIMESTAMP,
                    block_time TIMESTAMP,
                    block_number LONG,
                    address VARCHAR,
                    coin VARCHAR,
                    type VARCHAR,
                    px DOUBLE,
                    sz DOUBLE,
                    is_buy BOOLEAN,
                    start_position DOUBLE,
                    is_gaining_inventory BOOLEAN,
                    closed_pnl DOUBLE,
                    hash VARCHAR,
                    oid LONG,
                    crossed BOOLEAN,
                    fee DOUBLE,
                    tid LONG,
                    fee_token VARCHAR,
                    cloid VARCHAR,
                    builder_fee VARCHAR,
                    builder VARCHAR,
                    local_time TIMESTAMP,
                    reason SYMBOL
                ) TIMESTAMP(time) PARTITION BY DAY;",
                &[],
            )
            .await?;

        // Best effort: if table already existed without indexes, try to add them.
        ensure_symbol_index(&client, "fills", "coin").await?;
        ensure_symbol_index(&client, "fills", "address").await?;

        tracing::info!("QuestDB tables ensured");
        Ok(())
    }
}

/// QuestDB writer via InfluxDB Line Protocol (port 9009).
pub struct QuestDbWriter {
    config: QuestDbConfig,
}

async fn ensure_symbol_index(
    client: &tokio_postgres::Client,
    table: &str,
    column: &str,
) -> Result<()> {
    let sql = format!("ALTER TABLE {table} ALTER COLUMN {column} ADD INDEX");
    if let Err(err) = client.execute(&sql, &[]).await {
        let msg = err.to_string().to_ascii_lowercase();
        if msg.contains("already") || msg.contains("exists") {
            tracing::debug!("Index already exists for {}.{}", table, column);
            return Ok(());
        }
        return Err(err.into());
    }

    tracing::info!("Ensured index on {}.{}", table, column);
    Ok(())
}

impl QuestDbWriter {
    pub fn new(config: &QuestDbConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    pub fn ilp_address(&self) -> String {
        format!("{}:{}", self.config.ilp_host, self.config.ilp_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires local QuestDB on localhost:8812"]
    async fn select_one_returns_one_against_local_questdb() {
        let reader = QuestDbReader::new(&QuestDbConfig::default());
        let value = reader
            .select_one()
            .await
            .expect("SELECT 1 should succeed against local QuestDB");

        assert_eq!(value, 1);
    }
}
