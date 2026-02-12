use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub questdb: QuestDbConfig,
    pub grpc: GrpcConfig,
    pub backfill: BackfillConfig,
    pub retention: RetentionConfig,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QuestDbConfig {
    pub ilp_host: String,
    pub ilp_port: u16,
    pub pg_host: String,
    pub pg_port: u16,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BackfillConfig {
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub temp_dir: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    pub ttl_days: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            questdb: QuestDbConfig::default(),
            grpc: GrpcConfig::default(),
            backfill: BackfillConfig::default(),
            retention: RetentionConfig::default(),
        }
    }
}

impl Default for QuestDbConfig {
    fn default() -> Self {
        Self {
            ilp_host: "localhost".to_string(),
            ilp_port: 9009,
            pg_host: "localhost".to_string(),
            pg_port: 8812,
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 50051,
        }
    }
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            s3_bucket: "hl-mainnet-node-data".to_string(),
            s3_prefix: "node_fills_by_block/hourly".to_string(),
            temp_dir: "/tmp/hl-backfill".to_string(),
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self { ttl_days: 0 }
    }
}

pub fn load(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read config file '{}': {}", path, e))?;

    let config: Config = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse config file '{}': {}", path, e))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_config(content: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before unix epoch")
            .as_nanos();
        path.push(format!("hl_historical_config_{suffix}.toml"));
        fs::write(&path, content).expect("failed to write temp config");
        path
    }

    #[test]
    fn config_example_matches_defaults() {
        let config = load("config.example.toml").expect("config.example.toml should parse");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn load_returns_helpful_error_for_missing_file() {
        let path = "/tmp/does-not-exist-hl-historical.toml";
        let err = load(path).expect_err("loading a missing config should fail");
        let err_text = err.to_string();

        assert!(err_text.contains("Failed to read config file"));
        assert!(err_text.contains(path));
    }

    #[test]
    fn load_returns_helpful_error_for_missing_required_key() {
        let temp_path = write_temp_config(
            r#"
[questdb]
ilp_host = "localhost"
ilp_port = 9009
pg_host = "localhost"

[grpc]
host = "127.0.0.1"
port = 50051

[backfill]
s3_bucket = "hl-mainnet-node-data"
s3_prefix = "node_fills_by_block/hourly"
temp_dir = "/tmp/hl-backfill"

[retention]
ttl_days = 0
"#,
        );

        let err = load(
            temp_path
                .to_str()
                .expect("temp config path should be valid utf-8"),
        )
        .expect_err("config missing a required key should fail");

        let _ = fs::remove_file(&temp_path);
        let err_text = err.to_string();

        assert!(err_text.contains("pg_port"));
    }

    #[test]
    fn load_returns_helpful_error_for_unknown_key() {
        let temp_path = write_temp_config(
            r#"
[questdb]
ilp_host = "localhost"
ilp_port = 9009
pg_host = "localhost"
pg_port = 8812
extra_field = "oops"

[grpc]
host = "127.0.0.1"
port = 50051

[backfill]
s3_bucket = "hl-mainnet-node-data"
s3_prefix = "node_fills_by_block/hourly"
temp_dir = "/tmp/hl-backfill"

[retention]
ttl_days = 0
"#,
        );

        let err = load(
            temp_path
                .to_str()
                .expect("temp config path should be valid utf-8"),
        )
        .expect_err("config with unknown key should fail");

        let _ = fs::remove_file(&temp_path);
        let err_text = err.to_string();

        assert!(err_text.contains("unknown field"));
        assert!(err_text.contains("extra_field"));
    }
}
