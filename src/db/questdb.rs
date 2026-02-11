use crate::config::QuestDbConfig;
use anyhow::Result;
use tokio_postgres::NoTls;

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

    pub async fn connect(&self) -> Result<tokio_postgres::Client> {
        let conn_str = format!(
            "host={} port={} user=admin password=quest dbname=qdb",
            self.config.pg_host, self.config.pg_port
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("QuestDB connection error: {}", e);
            }
        });
        Ok(client)
    }

    pub async fn ping(&self) -> Result<()> {
        let client = self.connect().await?;
        client.query("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Create fills and fills_quarantine tables if they don't exist.
    pub async fn ensure_tables(&self) -> Result<()> {
        let client = self.connect().await?;

        client
            .execute(
                "CREATE TABLE IF NOT EXISTS fills (
                    time TIMESTAMP,
                    block_time TIMESTAMP,
                    block_number LONG,
                    address SYMBOL,
                    coin SYMBOL,
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
                    reason VARCHAR
                ) TIMESTAMP(time) PARTITION BY DAY;",
                &[],
            )
            .await?;

        tracing::info!("QuestDB tables ensured");
        Ok(())
    }
}

/// QuestDB writer via InfluxDB Line Protocol (port 9009).
pub struct QuestDbWriter {
    config: QuestDbConfig,
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
