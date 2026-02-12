use crate::config::BackfillConfig;
use anyhow::{bail, Context, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::types::RequestPayer;
use aws_sdk_s3::Client;
use chrono::{Duration, NaiveDate};
use std::path::Path;
use tokio::io::AsyncWriteExt;

const DATE_FORMAT: &str = "%Y%m%d";

/// Result of attempting to download a single hourly S3 object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadOutcome {
    Downloaded,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadErrorClass {
    Missing,
    Auth,
    Other,
}

/// Generate S3 keys for hourly files in a date range.
///
/// Date semantics are UTC and day-inclusive:
/// - `from=20250727` starts at `2025-07-27T00:00:00Z`
/// - `to=20250727` ends at `2025-07-27T23:00:00Z`
pub fn generate_hourly_keys(prefix: &str, from: &str, to: &str) -> Result<Vec<String>> {
    let from_date = parse_yyyymmdd(from, "from")?;
    let to_date = parse_yyyymmdd(to, "to")?;

    if to_date < from_date {
        bail!("invalid date range: --to ({to}) is before --from ({from})");
    }

    let mut current = from_date
        .and_hms_opt(0, 0, 0)
        .context("failed to construct start-of-day timestamp")?
        .and_utc();
    let end_exclusive = to_date
        .succ_opt()
        .context("date overflow while computing end of range")?
        .and_hms_opt(0, 0, 0)
        .context("failed to construct end-of-day timestamp")?
        .and_utc();

    let normalized_prefix = prefix.trim_matches('/');
    let mut keys = Vec::new();

    while current < end_exclusive {
        let date = current.format(DATE_FORMAT);
        let hour = current.format("%H");

        let key = if normalized_prefix.is_empty() {
            format!("{date}/{hour}.lz4")
        } else {
            format!("{normalized_prefix}/{date}/{hour}.lz4")
        };

        keys.push(key);
        current += Duration::hours(1);
    }

    Ok(keys)
}

fn parse_yyyymmdd(value: &str, flag_name: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, DATE_FORMAT)
        .with_context(|| format!("invalid --{flag_name} date '{value}', expected YYYYMMDD"))
}

/// Download a single S3 object to a local path.
///
/// - Uses AWS default credential resolution (env, profile, IAM role, etc.)
/// - Sends `request_payer=requester` for requester-pays buckets
/// - Missing keys are treated as non-fatal (`DownloadOutcome::Missing`)
/// - Auth/access failures are returned as actionable errors
pub async fn download_file(
    config: &BackfillConfig,
    key: &str,
    dest: &str,
) -> Result<DownloadOutcome> {
    let s3 = build_client().await;

    let response = s3
        .get_object()
        .bucket(&config.s3_bucket)
        .key(key)
        .request_payer(RequestPayer::Requester)
        .send()
        .await;

    let output = match response {
        Ok(output) => output,
        Err(err) => match classify_get_object_error(&err) {
            DownloadErrorClass::Missing => {
                tracing::warn!(
                    "S3 object missing; skipping s3://{}/{}",
                    config.s3_bucket,
                    key
                );
                return Ok(DownloadOutcome::Missing);
            }
            DownloadErrorClass::Auth => {
                bail!(
                    "Failed to download s3://{}/{} due to AWS auth/permission error: {}. \
                     Ensure AWS credentials are configured and have requester-pays GetObject access.",
                    config.s3_bucket,
                    key,
                    err
                );
            }
            DownloadErrorClass::Other => {
                return Err(err).with_context(|| {
                    format!("failed to download s3://{}/{}", config.s3_bucket, key)
                });
            }
        },
    };

    let dest_path = Path::new(dest);
    if let Some(parent) = dest_path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create destination directory {}",
                    parent.display()
                )
            })?;
        }
    }

    let mut file = tokio::fs::File::create(dest_path)
        .await
        .with_context(|| format!("failed to create destination file {}", dest_path.display()))?;

    let mut body_reader = output.body.into_async_read();
    let bytes_written = tokio::io::copy(&mut body_reader, &mut file).await;

    match bytes_written {
        Ok(bytes_written) => {
            file.flush().await.with_context(|| {
                format!("failed to flush destination file {}", dest_path.display())
            })?;
            tracing::info!(
                "Downloaded s3://{}/{} -> {} ({} bytes)",
                config.s3_bucket,
                key,
                dest_path.display(),
                bytes_written
            );
            Ok(DownloadOutcome::Downloaded)
        }
        Err(err) => {
            let _ = tokio::fs::remove_file(dest_path).await;
            Err(err).with_context(|| {
                format!(
                    "failed while writing downloaded object to {}",
                    dest_path.display()
                )
            })
        }
    }
}

async fn build_client() -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    Client::new(&aws_config)
}

fn classify_get_object_error<R>(err: &SdkError<GetObjectError, R>) -> DownloadErrorClass {
    let code = err
        .as_service_error()
        .and_then(|service_err| service_err.code());

    classify_error(code, &err.to_string())
}

fn classify_error(code: Option<&str>, message: &str) -> DownloadErrorClass {
    if let Some(code) = code {
        if code.eq_ignore_ascii_case("NoSuchKey") {
            return DownloadErrorClass::Missing;
        }

        if code.eq_ignore_ascii_case("AccessDenied")
            || code.eq_ignore_ascii_case("InvalidAccessKeyId")
            || code.eq_ignore_ascii_case("SignatureDoesNotMatch")
            || code.eq_ignore_ascii_case("ExpiredToken")
            || code.eq_ignore_ascii_case("AuthorizationHeaderMalformed")
            || code.eq_ignore_ascii_case("RequestTimeTooSkewed")
        {
            return DownloadErrorClass::Auth;
        }
    }

    let message = message.to_ascii_lowercase();
    if message.contains("nosuchkey") || message.contains("no such key") {
        return DownloadErrorClass::Missing;
    }

    if message.contains("accessdenied")
        || message.contains("invalidaccesskeyid")
        || message.contains("signaturedoesnotmatch")
        || message.contains("expiredtoken")
        || message.contains("credentials")
        || message.contains("credential")
        || message.contains("forbidden")
    {
        return DownloadErrorClass::Auth;
    }

    DownloadErrorClass::Other
}

#[cfg(test)]
mod tests {
    use super::{classify_error, generate_hourly_keys, DownloadErrorClass};

    #[test]
    fn single_day_generates_24_hourly_keys() {
        let keys = generate_hourly_keys("node_fills_by_block/hourly", "20250727", "20250727")
            .expect("single-day range should parse");

        assert_eq!(keys.len(), 24);
        assert_eq!(
            keys.first().unwrap(),
            "node_fills_by_block/hourly/20250727/00.lz4"
        );
        assert_eq!(
            keys.last().unwrap(),
            "node_fills_by_block/hourly/20250727/23.lz4"
        );
    }

    #[test]
    fn multi_day_range_is_day_inclusive() {
        let keys = generate_hourly_keys("node_fills_by_block/hourly", "20250727", "20250728")
            .expect("two-day range should parse");

        assert_eq!(keys.len(), 48);
        assert_eq!(keys[0], "node_fills_by_block/hourly/20250727/00.lz4");
        assert_eq!(keys[24], "node_fills_by_block/hourly/20250728/00.lz4");
        assert_eq!(keys[47], "node_fills_by_block/hourly/20250728/23.lz4");
    }

    #[test]
    fn prefix_is_normalized() {
        let keys = generate_hourly_keys("/node_fills_by_block/hourly/", "20250727", "20250727")
            .expect("range should parse");

        assert_eq!(keys[0], "node_fills_by_block/hourly/20250727/00.lz4");
    }

    #[test]
    fn utc_iteration_always_has_24_hours_per_day() {
        let keys = generate_hourly_keys("node_fills_by_block/hourly", "20251102", "20251102")
            .expect("range should parse");

        assert_eq!(keys.len(), 24);
        assert!(keys.iter().any(|k| k.ends_with("/00.lz4")));
        assert!(keys.iter().any(|k| k.ends_with("/23.lz4")));
    }

    #[test]
    fn rejects_invalid_date_format() {
        let err = generate_hourly_keys("node_fills_by_block/hourly", "2025-07-27", "20250727")
            .expect_err("bad date format should fail");

        assert!(err.to_string().contains("expected YYYYMMDD"));
    }

    #[test]
    fn rejects_when_to_is_before_from() {
        let err = generate_hourly_keys("node_fills_by_block/hourly", "20250728", "20250727")
            .expect_err("reversed range should fail");

        assert!(err.to_string().contains("is before --from"));
    }

    #[test]
    fn classifies_missing_key_errors() {
        assert_eq!(
            classify_error(Some("NoSuchKey"), "boom"),
            DownloadErrorClass::Missing
        );
        assert_eq!(
            classify_error(None, "service error: NoSuchKey"),
            DownloadErrorClass::Missing
        );
    }

    #[test]
    fn classifies_auth_errors() {
        assert_eq!(
            classify_error(Some("AccessDenied"), "boom"),
            DownloadErrorClass::Auth
        );
        assert_eq!(
            classify_error(None, "Unable to load credentials"),
            DownloadErrorClass::Auth
        );
    }

    #[test]
    fn leaves_unknown_errors_as_other() {
        assert_eq!(
            classify_error(Some("SlowDown"), "throttled"),
            DownloadErrorClass::Other
        );
    }
}
