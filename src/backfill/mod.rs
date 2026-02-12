pub mod decompress;
pub mod hour_window;
pub mod ingest;
pub mod parse;
pub mod report;
pub mod s3;
pub mod status;

use crate::config::Config;
use crate::db::questdb::{QuestDbReader, QuestDbWriter};
use anyhow::{Context, Result};
use report::BackfillRunReport;
use std::io::ErrorKind;
use std::path::Path;

/// Run backfill for a specific date range.
pub async fn run(config: &Config, from: &str, to: &str) -> Result<BackfillRunReport> {
    tracing::info!("Backfill from={} to={}", from, to);

    let keys = s3::generate_hourly_keys(&config.backfill.s3_prefix, from, to)?;
    let mut report = BackfillRunReport::new(from, to, keys.len());

    // Ensure target tables exist before we start streaming hourly ingestion.
    let reader = QuestDbReader::new(&config.questdb);
    reader
        .ensure_tables()
        .await
        .context("failed to ensure QuestDB tables before backfill")?;
    let writer = QuestDbWriter::new(&config.questdb);

    for key in keys {
        let files = decompress::hour_files(&config.backfill.temp_dir, &key)
            .with_context(|| format!("failed to compute temp file paths for key {}", key))?;

        let lz4_dest = files.lz4_path.to_string_lossy().into_owned();
        let download_outcome = match s3::download_file(&config.backfill, &key, &lz4_dest).await {
            Ok(outcome) => outcome,
            Err(err @ s3::DownloadError::Auth { .. }) => return Err(err.into()),
            Err(err @ s3::DownloadError::Other { .. }) => {
                tracing::warn!(
                    key = %key,
                    error = %err,
                    "S3 download failed; skipping hour"
                );
                report.failed_download += 1;
                continue;
            }
        };

        match download_outcome {
            s3::DownloadOutcome::Missing => {
                report.hours_missing += 1;
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

                    if let Err(cleanup_err) = cleanup_temp_file(&files.jsonl_path).await {
                        tracing::warn!(
                            path = %files.jsonl_path.display(),
                            error = %cleanup_err,
                            "Failed to clean up temporary JSONL file after decompression error"
                        );
                    }
                }

                report.failed_decompress += 1;
                continue;
            }
        };

        tracing::info!(
            key = %key,
            output_path = %files.jsonl_path.display(),
            decompressed_bytes = stats.decompressed_bytes,
            "Decompressed hourly archive"
        );

        let (deleted_fills, deleted_quarantine) =
            match delete_hour_rows_before_ingest(&reader, &key).await {
                Ok(counts) => counts,
                Err(err) => {
                    tracing::warn!(
                        key = %key,
                        error = %err,
                        "Hourly dedup delete failed; skipping hour before ingest"
                    );

                    if !config.backfill.keep_temp_files {
                        if let Err(cleanup_err) = cleanup_temp_file(&files.lz4_path).await {
                            tracing::warn!(
                                path = %files.lz4_path.display(),
                                error = %cleanup_err,
                                "Failed to clean up temporary file after dedup delete error"
                            );
                        }

                        if let Err(cleanup_err) = cleanup_temp_file(&files.jsonl_path).await {
                            tracing::warn!(
                                path = %files.jsonl_path.display(),
                                error = %cleanup_err,
                                "Failed to clean up temporary JSONL file after dedup delete error"
                            );
                        }
                    }

                    report.failed_dedup += 1;
                    continue;
                }
            };

        tracing::info!(
            key = %key,
            deleted_fills,
            deleted_quarantine,
            "Deleted existing rows for hour before ingest"
        );

        let ingest_stats = match ingest::ingest_jsonl_hour(&writer, &key, &files.jsonl_path).await {
            Ok(stats) => stats,
            Err(err) => {
                tracing::warn!(
                    key = %key,
                    path = %files.jsonl_path.display(),
                    error = %err,
                    "JSONL ingest failed; skipping hour"
                );

                match delete_hour_rows_before_ingest(&reader, &key).await {
                    Ok((rolled_back_fills, rolled_back_quarantine)) => {
                        tracing::warn!(
                            key = %key,
                            rolled_back_fills,
                            rolled_back_quarantine,
                            "Rolled back hour rows after ingest failure"
                        );
                    }
                    Err(rollback_err) => {
                        return Err(rollback_err).with_context(|| {
                            format!("failed to rollback hour {} after ingest failure", key)
                        });
                    }
                }

                if !config.backfill.keep_temp_files {
                    if let Err(cleanup_err) = cleanup_temp_file(&files.lz4_path).await {
                        tracing::warn!(
                            path = %files.lz4_path.display(),
                            error = %cleanup_err,
                            "Failed to clean up temporary file after ingest error"
                        );
                    }

                    if let Err(cleanup_err) = cleanup_temp_file(&files.jsonl_path).await {
                        tracing::warn!(
                            path = %files.jsonl_path.display(),
                            error = %cleanup_err,
                            "Failed to clean up temporary JSONL file after ingest error"
                        );
                    }
                }

                report.failed_ingest += 1;
                continue;
            }
        };

        report.rows_inserted += ingest_stats.rows_inserted;
        report.rows_quarantined += ingest_stats.rows_quarantined;
        report.parse_errors += ingest_stats.parse_errors;

        tracing::info!(
            key = %key,
            rows_inserted = ingest_stats.rows_inserted,
            rows_quarantined = ingest_stats.rows_quarantined,
            parse_errors = ingest_stats.parse_errors,
            "Ingested decompressed hourly file"
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

        report.hours_processed += 1;
    }

    tracing::info!(
        hours_total = report.hours_total,
        hours_processed = report.hours_processed,
        hours_skipped = report.hours_skipped(),
        files_missing = report.hours_missing,
        failed_download = report.failed_download,
        failed_decompress = report.failed_decompress,
        failed_dedup = report.failed_dedup,
        failed_ingest = report.failed_ingest,
        rows_inserted = report.rows_inserted,
        rows_quarantined = report.rows_quarantined,
        parse_errors = report.parse_errors,
        keep_temp_files = config.backfill.keep_temp_files,
        "Backfill download+decompress+ingest pass finished"
    );

    Ok(report)
}

async fn delete_hour_rows_before_ingest(reader: &QuestDbReader, key: &str) -> Result<(u64, u64)> {
    let window = hour_window::hour_window_from_key(key)
        .with_context(|| format!("failed to derive hour window from key {}", key))?;

    let deleted_fills = reader
        .delete_fills_in_range(window.start, window.end_exclusive)
        .await
        .with_context(|| format!("failed to delete fills rows for hour key {}", key))?;

    let deleted_quarantine = reader
        .delete_fills_quarantine_in_range(window.start, window.end_exclusive)
        .await
        .with_context(|| format!("failed to delete quarantine rows for hour key {}", key))?;

    Ok((deleted_fills, deleted_quarantine))
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
