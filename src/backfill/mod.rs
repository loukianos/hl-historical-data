pub mod ingest;
pub mod parse;
pub mod s3;
pub mod status;

use crate::config::Config;
use anyhow::Result;

/// Run backfill for a specific date range.
pub async fn run(_config: &Config, from: &str, to: &str) -> Result<()> {
    tracing::info!("Backfill from={} to={}", from, to);
    // TODO: implement full backfill pipeline
    // 1. Generate hourly S3 keys for date range
    // 2. For each hour: download, decompress, parse, validate, ingest
    // 3. Print summary
    anyhow::bail!("Backfill not yet implemented")
}

/// Sync from last ingested hour to present.
pub async fn sync_to_present(_config: &Config) -> Result<()> {
    tracing::info!("Sync to present");
    // TODO: implement sync-to-present
    // 1. Query max(time) from fills
    // 2. Compute resume hour with 1-hour overlap
    // 3. Backfill from resume to now
    anyhow::bail!("Sync to present not yet implemented")
}
