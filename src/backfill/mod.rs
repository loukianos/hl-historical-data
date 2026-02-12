pub mod decompress;
pub mod ingest;
pub mod parse;
pub mod s3;
pub mod status;

use crate::config::Config;
use anyhow::{Context, Result};
use std::io::ErrorKind;
use std::path::Path;

/// Run backfill for a specific date range.
pub async fn run(config: &Config, from: &str, to: &str) -> Result<()> {
    tracing::info!("Backfill from={} to={}", from, to);

    let keys = s3::generate_hourly_keys(&config.backfill.s3_prefix, from, to)?;
    let total_hours = keys.len();

    let mut files_missing = 0_usize;
    let mut files_failed = 0_usize;
    let mut files_succeeded = 0_usize;

    for key in keys {
        let files = decompress::hour_files(&config.backfill.temp_dir, &key)
            .with_context(|| format!("failed to compute temp file paths for key {}", key))?;

        let lz4_dest = files.lz4_path.to_string_lossy().into_owned();
        match s3::download_file(&config.backfill, &key, &lz4_dest).await? {
            s3::DownloadOutcome::Missing => {
                files_missing += 1;
                continue;
            }
            s3::DownloadOutcome::Downloaded => {}
        }

        let lz4_path = files.lz4_path.clone();
        let jsonl_path = files.jsonl_path.clone();

        let decompress_result = tokio::task::spawn_blocking(move || {
            decompress::decompress_lz4_to_jsonl(&lz4_path, &jsonl_path)
        })
        .await
        .context("lz4 decompression worker task failed")?;

        let stats = match decompress_result {
            Ok(stats) => stats,
            Err(err) => {
                tracing::warn!(key = %key, error = %err, "LZ4 decompression failed; skipping hour");

                if !config.backfill.keep_temp_files {
                    if let Err(cleanup_err) = cleanup_temp_file(&files.lz4_path).await {
                        tracing::warn!(
                            path = %files.lz4_path.display(),
                            error = %cleanup_err,
                            "Failed to clean up temporary file after decompression error"
                        );
                    }
                }

                files_failed += 1;
                continue;
            }
        };

        tracing::info!(
            key = %key,
            output_path = %files.jsonl_path.display(),
            decompressed_bytes = stats.decompressed_bytes,
            "Decompressed hourly archive"
        );

        if !config.backfill.keep_temp_files {
            if let Err(err) = cleanup_temp_file(&files.lz4_path).await {
                tracing::warn!(
                    path = %files.lz4_path.display(),
                    error = %err,
                    "Failed to clean up temporary file"
                );
            }

            if let Err(err) = cleanup_temp_file(&files.jsonl_path).await {
                tracing::warn!(
                    path = %files.jsonl_path.display(),
                    error = %err,
                    "Failed to clean up temporary file"
                );
            }
        }

        files_succeeded += 1;
    }

    tracing::info!(
        total_hours,
        files_succeeded,
        files_missing,
        files_failed,
        keep_temp_files = config.backfill.keep_temp_files,
        "Backfill download+decompress pass finished"
    );

    Ok(())
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

async fn cleanup_temp_file(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove file {}", path.display())),
    }
}
