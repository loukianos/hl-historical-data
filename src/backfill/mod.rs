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
use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike, Utc};
use report::BackfillRunReport;
use std::io::ErrorKind;
use std::path::Path;

const SYNC_DATE_FORMAT: &str = "%Y%m%d";

/// Run backfill for a specific date range.
pub async fn run(config: &Config, from: &str, to: &str) -> Result<BackfillRunReport> {
    tracing::info!("Backfill from={} to={}", from, to);

    let keys = s3::generate_hourly_keys(&config.backfill.s3_prefix, from, to)?;
    let report = BackfillRunReport::new(from, to, keys.len());
    run_with_keys(config, keys, report).await
}

async fn run_with_keys(
    config: &Config,
    keys: Vec<String>,
    mut report: BackfillRunReport,
) -> Result<BackfillRunReport> {
    // Ensure target tables exist before we start streaming hourly ingestion.
    let reader = QuestDbReader::new(&config.questdb);
    reader
        .ensure_tables()
        .await
        .context("failed to ensure QuestDB tables before backfill")?;
    let writer = QuestDbWriter::new(&config.questdb);

    for key in keys {
        process_hour_key(config, &reader, &writer, &key, &mut report).await?;
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

async fn process_hour_key(
    config: &Config,
    reader: &QuestDbReader,
    writer: &QuestDbWriter,
    key: &str,
    report: &mut BackfillRunReport,
) -> Result<()> {
    let files = decompress::hour_files(&config.backfill.temp_dir, key)
        .with_context(|| format!("failed to compute temp file paths for key {}", key))?;

    let lz4_dest = files.lz4_path.to_string_lossy().into_owned();
    let download_outcome = match s3::download_file(&config.backfill, key, &lz4_dest).await {
        Ok(outcome) => outcome,
        Err(err @ s3::DownloadError::Auth { .. }) => return Err(err.into()),
        Err(err @ s3::DownloadError::Other { .. }) => {
            tracing::warn!(
                key = %key,
                error = %err,
                "S3 download failed; skipping hour"
            );
            report.failed_download += 1;
            return Ok(());
        }
    };

    match download_outcome {
        s3::DownloadOutcome::Missing => {
            report.hours_missing += 1;
            return Ok(());
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
            return Ok(());
        }
    };

    tracing::info!(
        key = %key,
        output_path = %files.jsonl_path.display(),
        decompressed_bytes = stats.decompressed_bytes,
        "Decompressed hourly archive"
    );

    let (deleted_fills, deleted_quarantine) =
        match delete_hour_rows_before_ingest(reader, key).await {
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
                return Ok(());
            }
        };

    tracing::info!(
        key = %key,
        deleted_fills,
        deleted_quarantine,
        "Deleted existing rows for hour before ingest"
    );

    let ingest_stats = match ingest::ingest_jsonl_hour(writer, key, &files.jsonl_path).await {
        Ok(stats) => stats,
        Err(err) => {
            tracing::warn!(
                key = %key,
                path = %files.jsonl_path.display(),
                error = %err,
                "JSONL ingest failed; skipping hour"
            );

            match delete_hour_rows_before_ingest(reader, key).await {
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
            return Ok(());
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

    Ok(())
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
pub async fn sync_to_present(config: &Config) -> Result<BackfillRunReport> {
    tracing::info!("Sync to present");

    let reader = QuestDbReader::new(&config.questdb);
    reader
        .ensure_tables()
        .await
        .context("failed to ensure QuestDB tables before sync")?;

    let max_fills_time = reader
        .max_fills_time()
        .await
        .context("failed to fetch max(time) from fills")?;

    let now_utc = Utc::now().naive_utc();
    let Some((start_inclusive, end_exclusive)) = compute_sync_window(now_utc, max_fills_time)
    else {
        let today = now_utc.format(SYNC_DATE_FORMAT).to_string();
        tracing::info!(
            now_utc = %now_utc,
            max_fills_time = ?max_fills_time,
            "No sync work needed: already up to date"
        );
        return Ok(BackfillRunReport::new(&today, &today, 0));
    };

    let keys = s3::generate_hourly_keys_between(
        &config.backfill.s3_prefix,
        start_inclusive,
        end_exclusive,
    )
    .with_context(|| {
        format!(
            "failed to generate hourly keys for sync window [{}, {})",
            start_inclusive, end_exclusive
        )
    })?;

    let from = start_inclusive.format(SYNC_DATE_FORMAT).to_string();
    let to = (end_exclusive - Duration::hours(1))
        .format(SYNC_DATE_FORMAT)
        .to_string();

    tracing::info!(
        max_fills_time = ?max_fills_time,
        start_inclusive = %start_inclusive,
        end_exclusive = %end_exclusive,
        hours_total = keys.len(),
        "Computed sync-to-present backfill window"
    );

    let report = BackfillRunReport::new(&from, &to, keys.len());
    run_with_keys(config, keys, report).await
}

fn first_available_hour_start() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2025, 7, 27)
        .expect("first available sync date must be valid")
        .and_hms_opt(0, 0, 0)
        .expect("first available sync time must be valid")
}

fn floor_to_hour(ts: NaiveDateTime) -> NaiveDateTime {
    ts.date()
        .and_hms_opt(ts.hour(), 0, 0)
        .expect("hour truncation should always produce a valid timestamp")
}

fn compute_sync_window(
    now_utc: NaiveDateTime,
    max_fills_time: Option<NaiveDateTime>,
) -> Option<(NaiveDateTime, NaiveDateTime)> {
    let first_available = first_available_hour_start();
    let end_exclusive = floor_to_hour(now_utc);

    let mut start_inclusive = match max_fills_time {
        Some(max_time) => floor_to_hour(max_time) - Duration::hours(1),
        None => first_available,
    };

    if start_inclusive < first_available {
        start_inclusive = first_available;
    }

    (start_inclusive < end_exclusive).then_some((start_inclusive, end_exclusive))
}

async fn cleanup_temp_file(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove file {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_sync_window, first_available_hour_start};
    use chrono::{Duration, NaiveDate, NaiveDateTime};

    fn dt(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    #[test]
    fn sync_window_starts_from_first_available_when_table_is_empty() {
        let now = dt(2025, 7, 28, 5, 15);
        let (start, end) = compute_sync_window(now, None).expect("window should exist");

        assert_eq!(start, first_available_hour_start());
        assert_eq!(end, dt(2025, 7, 28, 5, 0));
    }

    #[test]
    fn sync_window_uses_one_hour_overlap_from_last_ingested_hour() {
        let now = dt(2025, 7, 28, 12, 45);
        let max_fills_time = Some(dt(2025, 7, 28, 11, 59));

        let (start, end) = compute_sync_window(now, max_fills_time).expect("window should exist");

        assert_eq!(start, dt(2025, 7, 28, 10, 0));
        assert_eq!(end, dt(2025, 7, 28, 12, 0));
        assert_eq!(end - start, Duration::hours(2));
    }

    #[test]
    fn sync_window_clamps_before_first_available_hour() {
        let now = dt(2025, 7, 27, 3, 30);
        let max_fills_time = Some(dt(2025, 7, 27, 0, 5));

        let (start, end) = compute_sync_window(now, max_fills_time).expect("window should exist");

        assert_eq!(start, first_available_hour_start());
        assert_eq!(end, dt(2025, 7, 27, 3, 0));
    }

    #[test]
    fn sync_window_returns_none_when_already_up_to_date() {
        let now = dt(2025, 7, 27, 0, 30);
        let window = compute_sync_window(now, None);

        assert!(window.is_none());
    }
}
