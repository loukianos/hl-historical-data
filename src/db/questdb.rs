use crate::config::QuestDbConfig;
use anyhow::{anyhow, bail, Context, Result};
use chrono::NaiveDateTime;
use std::io;
use std::pin::Pin;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_postgres::{types::ToSql, NoTls, Row, RowStream};
use tokio_stream::StreamExt;

type SqlParam<'a> = &'a (dyn ToSql + Sync);
type SqlParams<'a> = &'a [SqlParam<'a>];

/// Default maximum number of ILP lines per write batch.
pub const DEFAULT_ILP_BATCH_MAX_LINES: usize = 5_000;
/// Default maximum ILP payload size per write batch.
pub const DEFAULT_ILP_BATCH_MAX_BYTES: usize = 1_000_000;

/// Supported ILP field value types for formatting line protocol records.
#[derive(Debug, Clone, PartialEq)]
pub enum IlpFieldValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
}

impl From<&str> for IlpFieldValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for IlpFieldValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

/// Build a single ILP line.
///
/// `tags` are encoded as ILP symbols (QuestDB SYMBOL columns), while `fields`
/// are encoded as typed ILP fields.
pub fn format_ilp_line(
    table: &str,
    tags: &[(&str, &str)],
    fields: &[(&str, IlpFieldValue)],
    timestamp_nanos: i64,
) -> Result<String> {
    if fields.is_empty() {
        bail!("ILP line must contain at least one field");
    }

    let mut line = String::new();
    line.push_str(&escape_measurement(table));

    for (key, value) in tags {
        line.push(',');
        line.push_str(&escape_tag_or_key(key));
        line.push('=');
        line.push_str(&escape_tag_or_key(value));
    }

    line.push(' ');
    for (idx, (key, value)) in fields.iter().enumerate() {
        if idx > 0 {
            line.push(',');
        }
        line.push_str(&escape_tag_or_key(key));
        line.push('=');
        line.push_str(&format_ilp_field_value(value)?);
    }

    line.push(' ');
    line.push_str(&timestamp_nanos.to_string());

    Ok(line)
}

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

    /// Delete `fills` rows for a half-open time range [start, end_exclusive).
    pub async fn delete_fills_in_range(
        &self,
        start: NaiveDateTime,
        end_exclusive: NaiveDateTime,
    ) -> Result<u64> {
        self.execute(
            "DELETE FROM fills WHERE time >= $1 AND time < $2",
            &[&start, &end_exclusive],
        )
        .await
    }

    /// Delete `fills_quarantine` rows for a half-open time range [start, end_exclusive).
    pub async fn delete_fills_quarantine_in_range(
        &self,
        start: NaiveDateTime,
        end_exclusive: NaiveDateTime,
    ) -> Result<u64> {
        self.execute(
            "DELETE FROM fills_quarantine WHERE time >= $1 AND time < $2",
            &[&start, &end_exclusive],
        )
        .await
    }

    /// Query the most recent ingested `fills.time` timestamp.
    pub async fn max_fills_time(&self) -> Result<Option<NaiveDateTime>> {
        let row = self
            .query_one("SELECT max(time) FROM fills", &[])
            .await
            .context("failed to query max(time) from fills")?;

        row.try_get::<_, Option<NaiveDateTime>>(0)
            .map_err(|e| anyhow!("Could not decode max(time) from fills: {e}"))
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
    batch_max_lines: usize,
    batch_max_bytes: usize,
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
        Self::with_batch_limits(
            config,
            DEFAULT_ILP_BATCH_MAX_LINES,
            DEFAULT_ILP_BATCH_MAX_BYTES,
        )
    }

    pub fn with_batch_limits(
        config: &QuestDbConfig,
        batch_max_lines: usize,
        batch_max_bytes: usize,
    ) -> Self {
        Self {
            config: config.clone(),
            batch_max_lines: batch_max_lines.max(1),
            batch_max_bytes: batch_max_bytes.max(1),
        }
    }

    pub fn ilp_address(&self) -> String {
        format!("{}:{}", self.config.ilp_host, self.config.ilp_port)
    }

    pub async fn connect(&self) -> Result<TcpStream> {
        let address = self.ilp_address();
        let stream = TcpStream::connect(&address)
            .await
            .with_context(|| format!("Failed to connect to QuestDB ILP endpoint at {address}"))?;
        Ok(stream)
    }

    /// Write already-formatted ILP lines.
    ///
    /// Data is flushed per batch. If a broken pipe (or related connection error)
    /// occurs while writing a batch, the writer reconnects once and retries that batch.
    pub async fn write_lines<S: AsRef<str>>(&self, lines: &[S]) -> Result<()> {
        if lines.is_empty() {
            return Ok(());
        }

        let mut stream = self.connect().await?;
        let mut batch = String::new();
        let mut batch_lines = 0usize;
        let mut batch_bytes = 0usize;

        for line in lines {
            let line = line.as_ref();
            if line.trim().is_empty() {
                continue;
            }

            let line_bytes = line.len() + 1;
            let batch_full = batch_lines > 0
                && (batch_lines >= self.batch_max_lines
                    || batch_bytes.saturating_add(line_bytes) > self.batch_max_bytes);

            if batch_full {
                self.write_batch_with_reconnect(&mut stream, batch.as_bytes())
                    .await?;
                batch.clear();
                batch_lines = 0;
                batch_bytes = 0;
            }

            batch.push_str(line);
            if !line.ends_with('\n') {
                batch.push('\n');
            }
            batch_lines += 1;
            batch_bytes = batch_bytes.saturating_add(line_bytes);
        }

        if batch_lines > 0 {
            self.write_batch_with_reconnect(&mut stream, batch.as_bytes())
                .await?;
        }

        Ok(())
    }

    pub async fn write_line(&self, line: &str) -> Result<()> {
        self.write_lines(&[line]).await
    }

    async fn write_batch_with_reconnect(
        &self,
        stream: &mut TcpStream,
        payload: &[u8],
    ) -> Result<()> {
        match write_payload(stream, payload).await {
            Ok(()) => Ok(()),
            Err(first_err) if should_retry_with_reconnect(&first_err) => {
                tracing::warn!(
                    "ILP batch write failed ({}). Reconnecting once and retrying.",
                    first_err
                );
                *stream = self.connect().await?;

                write_payload(stream, payload)
                    .await
                    .with_context(|| {
                        format!(
                            "ILP batch write failed after reconnect ({} bytes)",
                            payload.len()
                        )
                    })
                    .map_err(Into::into)
            }
            Err(err) => Err(anyhow!(err)).with_context(|| {
                format!(
                    "ILP batch write failed ({} bytes) to {}",
                    payload.len(),
                    self.ilp_address()
                )
            }),
        }
    }
}

async fn write_payload(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    stream.write_all(payload).await?;
    stream.flush().await
}

fn should_retry_with_reconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
}

fn format_ilp_field_value(value: &IlpFieldValue) -> Result<String> {
    let formatted = match value {
        IlpFieldValue::Integer(value) => format!("{value}i"),
        IlpFieldValue::Float(value) => format_float(*value)?,
        IlpFieldValue::Boolean(true) => "t".to_string(),
        IlpFieldValue::Boolean(false) => "f".to_string(),
        IlpFieldValue::String(value) => format!("\"{}\"", escape_field_string(value)),
    };

    Ok(formatted)
}

fn format_float(value: f64) -> Result<String> {
    if !value.is_finite() {
        bail!("ILP float fields must be finite, got {value}");
    }

    let mut rendered = value.to_string();
    if !rendered.contains('.') && !rendered.contains('e') && !rendered.contains('E') {
        rendered.push_str(".0");
    }
    Ok(rendered)
}

fn escape_measurement(value: &str) -> String {
    escape_ilp(value, &[',', ' '])
}

fn escape_tag_or_key(value: &str) -> String {
    escape_ilp(value, &[',', ' ', '='])
}

fn escape_field_string(value: &str) -> String {
    escape_ilp(value, &['\\', '"'])
}

fn escape_ilp(value: &str, chars_to_escape: &[char]) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if chars_to_escape.contains(&ch) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn format_ilp_line_formats_supported_field_types() {
        let line = format_ilp_line(
            "fills",
            &[("coin", "BTC"), ("address", "0xabc")],
            &[
                ("px", IlpFieldValue::Float(123.45)),
                ("oid", IlpFieldValue::Integer(42)),
                ("crossed", IlpFieldValue::Boolean(true)),
                ("hash", IlpFieldValue::from("0xdeadbeef")),
            ],
            1_700_000_000_000_000_000,
        )
        .expect("line should format");

        assert_eq!(
            line,
            "fills,coin=BTC,address=0xabc px=123.45,oid=42i,crossed=t,hash=\"0xdeadbeef\" 1700000000000000000"
        );
    }

    #[test]
    fn format_ilp_line_escapes_measurement_tags_and_string_fields() {
        let line = format_ilp_line(
            "fills prod",
            &[("co in", "BTC,USD=PERP")],
            &[("ha sh", IlpFieldValue::from("a\"b\\c"))],
            1,
        )
        .expect("line should format");

        assert_eq!(
            line,
            "fills\\ prod,co\\ in=BTC\\,USD\\=PERP ha\\ sh=\"a\\\"b\\\\c\" 1"
        );
    }

    #[test]
    fn format_ilp_line_rejects_invalid_rows() {
        let empty_fields = format_ilp_line("fills", &[], &[], 1);
        assert!(empty_fields.is_err());

        let nan_float = format_ilp_line("fills", &[], &[("px", IlpFieldValue::Float(f64::NAN))], 1);
        assert!(nan_float.is_err());
    }

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

    #[tokio::test]
    #[ignore = "requires local QuestDB on localhost:8812 and localhost:9009"]
    async fn ilp_writer_inserts_row_visible_via_pg_query() {
        let config = QuestDbConfig::default();
        let reader = QuestDbReader::new(&config);
        reader
            .ensure_tables()
            .await
            .expect("tables should be created");

        let writer = QuestDbWriter::new(&config);
        let timestamp_nanos = chrono::Utc::now()
            .timestamp_nanos_opt()
            .expect("current timestamp should fit in i64");
        let unique_hash = format!("0xilp-test-{}", timestamp_nanos);

        let line = format_ilp_line(
            "fills",
            &[
                ("address", "0xilp-test-wallet"),
                ("coin", "BTC"),
                ("type", "PERP"),
                ("fee_token", "USDC"),
            ],
            &[
                ("block_number", IlpFieldValue::Integer(1)),
                ("px", IlpFieldValue::Float(100.0)),
                ("sz", IlpFieldValue::Float(0.001)),
                ("is_buy", IlpFieldValue::Boolean(true)),
                ("start_position", IlpFieldValue::Float(0.0)),
                ("is_gaining_inventory", IlpFieldValue::Boolean(true)),
                ("closed_pnl", IlpFieldValue::Float(0.0)),
                ("hash", IlpFieldValue::String(unique_hash.clone())),
                ("oid", IlpFieldValue::Integer(42)),
                ("crossed", IlpFieldValue::Boolean(true)),
                ("fee", IlpFieldValue::Float(0.0)),
                ("tid", IlpFieldValue::Integer(777)),
            ],
            timestamp_nanos,
        )
        .expect("line should format");

        writer
            .write_line(&line)
            .await
            .expect("line should be written via ILP");

        let mut count = 0_i64;
        for _ in 0..20 {
            let row = reader
                .query_one("SELECT count() FROM fills WHERE hash = $1", &[&unique_hash])
                .await
                .expect("count query should succeed");

            count = row
                .try_get::<_, i64>(0)
                .or_else(|_| row.try_get::<_, i32>(0).map(i64::from))
                .expect("count should decode as integer");

            if count >= 1 {
                break;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert_eq!(count, 1);
    }
}
