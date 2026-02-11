use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub questdb: QuestDbConfig,
    pub grpc: GrpcConfig,
    pub backfill: BackfillConfig,
    pub retention: RetentionConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QuestDbConfig {
    pub ilp_host: String,
    pub ilp_port: u16,
    pub pg_host: String,
    pub pg_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BackfillConfig {
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub temp_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetentionConfig {
    pub ttl_days: u32,
}

pub fn load(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;
    let config: Config = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", path, e))?;
    Ok(config)
}
