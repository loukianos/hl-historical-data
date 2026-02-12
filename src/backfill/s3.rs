use crate::config::BackfillConfig;
use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDate};

const DATE_FORMAT: &str = "%Y%m%d";

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
pub async fn download_file(_config: &BackfillConfig, _key: &str, _dest: &str) -> Result<()> {
    // TODO: implement S3 download with requester-pays
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::generate_hourly_keys;

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
}
