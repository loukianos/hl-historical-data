use crate::config::BackfillConfig;
use anyhow::Result;

/// Generate S3 keys for hourly files in a date range.
pub fn generate_hourly_keys(prefix: &str, from: &str, to: &str) -> Result<Vec<String>> {
    // TODO: parse from/to as YYYYMMDD, iterate hours, build keys
    // Format: {prefix}/{YYYYMMDD}/{HH}.lz4
    let _ = (prefix, from, to);
    Ok(vec![])
}

/// Download a single S3 object to a local path.
pub async fn download_file(_config: &BackfillConfig, _key: &str, _dest: &str) -> Result<()> {
    // TODO: implement S3 download with requester-pays
    Ok(())
}
