use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourWindow {
    pub start: NaiveDateTime,
    pub end_exclusive: NaiveDateTime,
}

pub fn hour_window_from_key(key: &str) -> Result<HourWindow> {
    let mut parts = key.rsplit('/');
    let hour_part = parts
        .next()
        .context("missing hour segment in hourly object key")?;
    let date_part = parts
        .next()
        .context("missing date segment in hourly object key")?;

    let hour_str = hour_part.strip_suffix(".lz4").unwrap_or(hour_part);
    let hour = hour_str
        .parse::<u32>()
        .with_context(|| format!("invalid hour segment '{hour_str}' in key '{key}'"))?;
    if hour > 23 {
        anyhow::bail!("hour out of range in key '{}': {}", key, hour);
    }

    let date = NaiveDate::parse_from_str(date_part, "%Y%m%d")
        .with_context(|| format!("invalid date segment '{date_part}' in key '{key}'"))?;
    let start = date
        .and_hms_opt(hour, 0, 0)
        .context("failed to build hour start timestamp")?;
    let end_exclusive = start + Duration::hours(1);

    Ok(HourWindow {
        start,
        end_exclusive,
    })
}

pub fn hour_start_nanos_from_key(key: &str) -> Result<i64> {
    let window = hour_window_from_key(key)?;
    window
        .start
        .and_utc()
        .timestamp_nanos_opt()
        .context("hour start timestamp out of range for nanoseconds")
}

#[cfg(test)]
mod tests {
    use super::{hour_start_nanos_from_key, hour_window_from_key, HourWindow};
    use chrono::NaiveDate;

    #[test]
    fn parses_hour_window_from_key() {
        let window =
            hour_window_from_key("node_fills_by_block/hourly/20250727/08.lz4").expect("parses");

        assert_eq!(
            window,
            HourWindow {
                start: NaiveDate::from_ymd_opt(2025, 7, 27)
                    .unwrap()
                    .and_hms_opt(8, 0, 0)
                    .unwrap(),
                end_exclusive: NaiveDate::from_ymd_opt(2025, 7, 27)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
            }
        );
    }

    #[test]
    fn parses_hour_window_with_or_without_lz4_suffix() {
        let with_suffix =
            hour_window_from_key("node_fills_by_block/hourly/20250727/08.lz4").expect("parses");
        let without_suffix =
            hour_window_from_key("node_fills_by_block/hourly/20250727/08").expect("parses");

        assert_eq!(with_suffix, without_suffix);
    }

    #[test]
    fn rejects_invalid_hour() {
        let err = hour_window_from_key("node_fills_by_block/hourly/20250727/24.lz4")
            .expect_err("invalid hour should fail");
        assert!(err.to_string().contains("hour out of range"));
    }

    #[test]
    fn rejects_invalid_date() {
        let err = hour_window_from_key("node_fills_by_block/hourly/20251327/08.lz4")
            .expect_err("invalid date should fail");
        assert!(err.to_string().contains("invalid date segment"));
    }

    #[test]
    fn hour_start_nanos_is_deterministic() {
        let ts = hour_start_nanos_from_key("node_fills_by_block/hourly/20250727/08.lz4")
            .expect("timestamp should parse");
        assert_eq!(ts, 1_753_603_200_000_000_000);
    }
}
