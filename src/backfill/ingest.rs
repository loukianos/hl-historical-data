use super::{
    hour_window::hour_start_nanos_from_key,
    parse::{self, ParsedFill},
};
use crate::db::questdb::{
    format_ilp_line, IlpFieldValue, QuestDbWriter, DEFAULT_ILP_BATCH_MAX_LINES,
};
use anyhow::{Context, Result};
use chrono::Utc;
use std::path::Path;
use tokio::io::{AsyncBufReadExt, BufReader};

const MIN_TIME_MS: i64 = 1_753_574_400_000; // 2025-07-27T00:00:00Z
const MIN_SKEW_MS: i64 = 5 * 60 * 1_000; // 5 minutes
const MAX_FUTURE_MS: i64 = 24 * 60 * 60 * 1_000; // 24 hours

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestStats {
    pub rows_inserted: i64,
    pub rows_quarantined: i64,
    pub parse_errors: i64,
}

#[derive(Debug, Default)]
struct LineRouting {
    ilp_lines: Vec<String>,
    rows_inserted: i64,
    rows_quarantined: i64,
    parse_errors: i64,
}

/// Validate a fill. Returns Ok(()) if valid, Err with reason if invalid.
pub fn validate_fill(fill: &ParsedFill) -> std::result::Result<(), String> {
    if !fill.px.is_finite() {
        return Err("px_not_finite".to_string());
    }
    if fill.px <= 0.0 {
        return Err("px<=0".to_string());
    }

    if !fill.sz.is_finite() {
        return Err("sz_not_finite".to_string());
    }
    if fill.sz <= 0.0 {
        return Err("sz<=0".to_string());
    }

    if !fill.start_position.is_finite() {
        return Err("start_position_not_finite".to_string());
    }
    if !fill.closed_pnl.is_finite() {
        return Err("closed_pnl_not_finite".to_string());
    }
    if !fill.fee.is_finite() {
        return Err("fee_not_finite".to_string());
    }

    // Timestamp sanity:
    // - not before launch day (allow small negative skew)
    // - not more than 24h in the future
    let min_allowed_ms = MIN_TIME_MS - MIN_SKEW_MS;
    let max_allowed_ms = Utc::now().timestamp_millis() + MAX_FUTURE_MS;
    if fill.time_ms < min_allowed_ms || fill.time_ms > max_allowed_ms {
        return Err("bad_time".to_string());
    }

    Ok(())
}

/// Format a valid fill row for `fills` ILP ingestion.
pub fn format_fill_ilp(fill: &ParsedFill) -> Result<String> {
    let timestamp_nanos = timestamp_ms_to_nanos(fill.time_ms)?;

    let tags = vec![
        ("address", fill.address.as_str()),
        ("coin", fill.coin.as_str()),
        ("type", fill.fill_type.as_str()),
        ("fee_token", fill.fee_token.as_str()),
    ];

    let mut fields = vec![
        ("block_number", IlpFieldValue::Integer(fill.block_number)),
        ("px", IlpFieldValue::Float(fill.px)),
        ("sz", IlpFieldValue::Float(fill.sz)),
        ("is_buy", IlpFieldValue::Boolean(fill.is_buy)),
        ("start_position", IlpFieldValue::Float(fill.start_position)),
        (
            "is_gaining_inventory",
            IlpFieldValue::Boolean(fill.is_gaining_inventory),
        ),
        ("closed_pnl", IlpFieldValue::Float(fill.closed_pnl)),
        ("hash", IlpFieldValue::String(fill.hash.clone())),
        ("oid", IlpFieldValue::Integer(fill.oid)),
        ("crossed", IlpFieldValue::Boolean(fill.crossed)),
        ("fee", IlpFieldValue::Float(fill.fee)),
        ("tid", IlpFieldValue::Integer(fill.tid)),
    ];

    if let Some(value) = &fill.cloid {
        fields.push(("cloid", IlpFieldValue::String(value.clone())));
    }
    if let Some(value) = &fill.builder_fee {
        fields.push(("builder_fee", IlpFieldValue::String(value.clone())));
    }
    if let Some(value) = &fill.builder {
        fields.push(("builder", IlpFieldValue::String(value.clone())));
    }

    format_ilp_line("fills", &tags, &fields, timestamp_nanos)
}

/// Format an invalid fill row for `fills_quarantine` ILP ingestion.
pub fn format_quarantine_ilp(fill: &ParsedFill, reason: &str) -> Result<String> {
    let timestamp_nanos = timestamp_ms_to_nanos(fill.time_ms)?;
    format_quarantine_ilp_at_timestamp(fill, reason, timestamp_nanos)
}

fn format_quarantine_ilp_at_timestamp(
    fill: &ParsedFill,
    reason: &str,
    timestamp_nanos: i64,
) -> Result<String> {
    let fields = quarantine_fields(fill);

    format_ilp_line(
        "fills_quarantine",
        &[("reason", reason)],
        &fields,
        timestamp_nanos,
    )
}

fn quarantine_fields(fill: &ParsedFill) -> Vec<(&'static str, IlpFieldValue)> {
    let mut fields = vec![
        ("block_number", IlpFieldValue::Integer(fill.block_number)),
        ("address", IlpFieldValue::String(fill.address.clone())),
        ("coin", IlpFieldValue::String(fill.coin.clone())),
        ("type", IlpFieldValue::String(fill.fill_type.clone())),
        ("is_buy", IlpFieldValue::Boolean(fill.is_buy)),
        (
            "is_gaining_inventory",
            IlpFieldValue::Boolean(fill.is_gaining_inventory),
        ),
        ("hash", IlpFieldValue::String(fill.hash.clone())),
        ("oid", IlpFieldValue::Integer(fill.oid)),
        ("crossed", IlpFieldValue::Boolean(fill.crossed)),
        ("tid", IlpFieldValue::Integer(fill.tid)),
        ("fee_token", IlpFieldValue::String(fill.fee_token.clone())),
    ];

    maybe_push_finite_float_field(&mut fields, "px", fill.px);
    maybe_push_finite_float_field(&mut fields, "sz", fill.sz);
    maybe_push_finite_float_field(&mut fields, "start_position", fill.start_position);
    maybe_push_finite_float_field(&mut fields, "closed_pnl", fill.closed_pnl);
    maybe_push_finite_float_field(&mut fields, "fee", fill.fee);

    if let Some(value) = &fill.cloid {
        fields.push(("cloid", IlpFieldValue::String(value.clone())));
    }
    if let Some(value) = &fill.builder_fee {
        fields.push(("builder_fee", IlpFieldValue::String(value.clone())));
    }
    if let Some(value) = &fill.builder {
        fields.push(("builder", IlpFieldValue::String(value.clone())));
    }

    fields
}

fn maybe_push_finite_float_field(
    fields: &mut Vec<(&'static str, IlpFieldValue)>,
    name: &'static str,
    value: f64,
) {
    if value.is_finite() {
        fields.push((name, IlpFieldValue::Float(value)));
    }
}

/// Format a synthetic quarantine line for an unparseable JSONL record.
pub fn format_parse_error_quarantine_ilp(key: &str, line_number: u64) -> Result<String> {
    let timestamp_nanos = hour_start_nanos_from_key(key).unwrap_or_else(|err| {
        tracing::warn!(
            key = %key,
            line_number,
            error = %err,
            "Failed to derive hour timestamp from key; using current time for parse_error quarantine row"
        );
        now_timestamp_nanos()
    });

    let synthetic_hash = parse_error_hash(key, line_number);

    format_ilp_line(
        "fills_quarantine",
        &[("reason", "parse_error")],
        &[("hash", IlpFieldValue::String(synthetic_hash))],
        timestamp_nanos,
    )
}

/// Ingest a decompressed JSONL hourly file.
pub async fn ingest_jsonl_hour(
    writer: &QuestDbWriter,
    key: &str,
    jsonl_path: &Path,
) -> Result<IngestStats> {
    let file = tokio::fs::File::open(jsonl_path)
        .await
        .with_context(|| format!("failed to open JSONL file {}", jsonl_path.display()))?;

    let mut reader = BufReader::new(file).lines();
    let mut stats = IngestStats::default();
    let mut batch = Vec::with_capacity(DEFAULT_ILP_BATCH_MAX_LINES);
    let mut line_number = 0_u64;

    while let Some(line) = reader
        .next_line()
        .await
        .with_context(|| format!("failed reading JSONL file {}", jsonl_path.display()))?
    {
        line_number += 1;
        if line.trim().is_empty() {
            continue;
        }

        let routed = route_jsonl_line(key, line_number, &line)?;
        stats.rows_inserted += routed.rows_inserted;
        stats.rows_quarantined += routed.rows_quarantined;
        stats.parse_errors += routed.parse_errors;
        batch.extend(routed.ilp_lines);

        if batch.len() >= DEFAULT_ILP_BATCH_MAX_LINES {
            flush_batch(writer, key, jsonl_path, &mut batch).await?;
        }
    }

    flush_batch(writer, key, jsonl_path, &mut batch).await?;

    Ok(stats)
}

/// Ingest a batch of validated fills into QuestDB via ILP.
pub async fn ingest_fills(writer: &QuestDbWriter, fills: &[ParsedFill]) -> Result<()> {
    if fills.is_empty() {
        return Ok(());
    }

    let mut lines = Vec::with_capacity(fills.len());
    for fill in fills {
        lines.push(format_fill_ilp(fill)?);
    }

    writer.write_lines(&lines).await
}

/// Insert an invalid fill into the quarantine table.
pub async fn quarantine_fill(
    writer: &QuestDbWriter,
    fill: &ParsedFill,
    reason: &str,
) -> Result<()> {
    let line = format_quarantine_ilp(fill, reason)?;
    writer.write_lines(std::slice::from_ref(&line)).await
}

async fn flush_batch(
    writer: &QuestDbWriter,
    key: &str,
    jsonl_path: &Path,
    batch: &mut Vec<String>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    writer.write_lines(batch).await.with_context(|| {
        format!(
            "failed writing ILP batch for key {} from {}",
            key,
            jsonl_path.display()
        )
    })?;
    batch.clear();

    Ok(())
}

fn route_jsonl_line(key: &str, line_number: u64, line: &str) -> Result<LineRouting> {
    match parse::parse_line(line) {
        Ok(fills) => {
            let mut routed = LineRouting::default();

            for fill in fills {
                match validate_fill(&fill) {
                    Ok(()) => {
                        routed.ilp_lines.push(format_fill_ilp(&fill)?);
                        routed.rows_inserted += 1;
                    }
                    Err(reason) => {
                        let quarantine_timestamp_nanos =
                            quarantine_timestamp_nanos_for_reason(key, &fill, &reason);
                        routed.ilp_lines.push(format_quarantine_ilp_at_timestamp(
                            &fill,
                            &reason,
                            quarantine_timestamp_nanos,
                        )?);
                        routed.rows_quarantined += 1;
                    }
                }
            }

            Ok(routed)
        }
        Err(err) => {
            tracing::warn!(
                key = %key,
                line_number,
                error = %err,
                "Failed to parse JSONL line; routing to quarantine"
            );

            Ok(LineRouting {
                ilp_lines: vec![format_parse_error_quarantine_ilp(key, line_number)?],
                rows_inserted: 0,
                rows_quarantined: 1,
                parse_errors: 1,
            })
        }
    }
}

fn quarantine_timestamp_nanos_for_reason(fill_key: &str, fill: &ParsedFill, reason: &str) -> i64 {
    if reason == "bad_time" {
        return hour_start_nanos_from_key(fill_key).unwrap_or_else(|err| {
            tracing::warn!(
                key = %fill_key,
                fill_time_ms = fill.time_ms,
                error = %err,
                "Failed to derive hour timestamp from key for bad_time quarantine row; using current time"
            );
            now_timestamp_nanos()
        });
    }

    match timestamp_ms_to_nanos(fill.time_ms) {
        Ok(timestamp_nanos) => timestamp_nanos,
        Err(err) => {
            tracing::warn!(
                key = %fill_key,
                fill_time_ms = fill.time_ms,
                reason = %reason,
                error = %err,
                "Failed to convert fill timestamp for quarantine row; falling back to hour start"
            );

            hour_start_nanos_from_key(fill_key).unwrap_or_else(|hour_err| {
                tracing::warn!(
                    key = %fill_key,
                    error = %hour_err,
                    "Failed to derive hour timestamp from key for quarantine row; using current time"
                );
                now_timestamp_nanos()
            })
        }
    }
}

fn timestamp_ms_to_nanos(timestamp_ms: i64) -> Result<i64> {
    timestamp_ms
        .checked_mul(1_000_000)
        .context("timestamp conversion overflowed while converting ms to ns")
}

fn parse_error_hash(key: &str, line_number: u64) -> String {
    let mut parts = key.rsplit('/');
    let hour_segment = parts.next().unwrap_or("unknown");
    let date_segment = parts.next().unwrap_or("unknown");
    let hour = hour_segment.strip_suffix(".lz4").unwrap_or(hour_segment);

    format!("parse_error:{date_segment}/{hour}:{line_number}")
}

fn now_timestamp_nanos() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis().saturating_mul(1_000_000))
}

#[cfg(test)]
mod tests {
    use super::{
        format_fill_ilp, format_parse_error_quarantine_ilp, format_quarantine_ilp,
        parse_error_hash, route_jsonl_line, validate_fill,
    };
    use crate::backfill::parse::ParsedFill;

    fn sample_fill() -> ParsedFill {
        ParsedFill {
            time_ms: 1_753_606_210_273,
            block_time: "2025-07-27T08:50:10.273720809".to_string(),
            block_number: 676_607_012,
            address: "0xabc".to_string(),
            coin: "BTC".to_string(),
            fill_type: "PERP".to_string(),
            px: 118_136.0,
            sz: 0.00009,
            is_buy: true,
            start_position: -1.41864,
            is_gaining_inventory: true,
            closed_pnl: -0.003753,
            hash: "0xe782204".to_string(),
            oid: 121_670_079_265,
            crossed: false,
            fee: -0.000212,
            tid: 161_270_588_369_408,
            fee_token: "USDC".to_string(),
            cloid: Some("0x09367b".to_string()),
            builder_fee: Some("0.005528".to_string()),
            builder: Some("0x49ae63".to_string()),
            local_time: "2025-07-27T08:50:10.334741319".to_string(),
        }
    }

    #[test]
    fn validate_fill_rejects_issue_14_constraints() {
        let mut fill = sample_fill();
        fill.px = f64::NAN;
        assert_eq!(validate_fill(&fill), Err("px_not_finite".to_string()));

        let mut fill = sample_fill();
        fill.px = 0.0;
        assert_eq!(validate_fill(&fill), Err("px<=0".to_string()));

        let mut fill = sample_fill();
        fill.sz = f64::NAN;
        assert_eq!(validate_fill(&fill), Err("sz_not_finite".to_string()));

        let mut fill = sample_fill();
        fill.sz = 0.0;
        assert_eq!(validate_fill(&fill), Err("sz<=0".to_string()));

        let mut fill = sample_fill();
        fill.start_position = f64::INFINITY;
        assert_eq!(
            validate_fill(&fill),
            Err("start_position_not_finite".to_string())
        );

        let mut fill = sample_fill();
        fill.closed_pnl = f64::NEG_INFINITY;
        assert_eq!(
            validate_fill(&fill),
            Err("closed_pnl_not_finite".to_string())
        );

        let mut fill = sample_fill();
        fill.fee = f64::NAN;
        assert_eq!(validate_fill(&fill), Err("fee_not_finite".to_string()));

        let mut fill = sample_fill();
        fill.time_ms = 1_753_574_400_000 - (6 * 60 * 1_000);
        assert_eq!(validate_fill(&fill), Err("bad_time".to_string()));
    }

    #[test]
    fn format_fill_ilp_uses_symbol_tags_for_fills_table() {
        let line = format_fill_ilp(&sample_fill()).expect("line should format");

        assert!(line.starts_with("fills,address=0xabc,coin=BTC,type=PERP,fee_token=USDC "));
        assert!(line.contains("px=118136.0"));
        assert!(line.contains("hash=\"0xe782204\""));
    }

    #[test]
    fn format_quarantine_ilp_uses_reason_tag_and_string_fields() {
        let line = format_quarantine_ilp(&sample_fill(), "px<=0").expect("line should format");

        assert!(line.starts_with("fills_quarantine,reason=px<\\=0 "));
        assert!(line.contains("address=\"0xabc\""));
        assert!(line.contains("coin=\"BTC\""));
        assert!(!line.contains(",address=0xabc"));
    }

    #[test]
    fn format_parse_error_quarantine_ilp_is_deterministic() {
        let line =
            format_parse_error_quarantine_ilp("node_fills_by_block/hourly/20250727/00.lz4", 17)
                .expect("line should format");

        assert!(line.starts_with("fills_quarantine,reason=parse_error "));
        assert!(line.contains("hash=\"parse_error:20250727/00:17\""));
    }

    #[test]
    fn route_jsonl_line_routes_parse_errors_to_quarantine() {
        let routed = route_jsonl_line(
            "node_fills_by_block/hourly/20250727/00.lz4",
            1,
            "{not json}",
        )
        .expect("parse errors should be converted to quarantine records");

        assert_eq!(routed.rows_inserted, 0);
        assert_eq!(routed.rows_quarantined, 1);
        assert_eq!(routed.parse_errors, 1);
        assert_eq!(routed.ilp_lines.len(), 1);
        assert!(routed.ilp_lines[0].contains("reason=parse_error"));
    }

    #[test]
    fn route_jsonl_line_splits_valid_and_invalid_fills() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0xok",{
                    "coin":"BTC",
                    "px":"118136.0",
                    "sz":"0.00009",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.41864",
                    "closedPnl":"-0.003753",
                    "hash":"0xgood",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC"
                }],
                ["0xbad",{
                    "coin":"ETH",
                    "px":"0",
                    "sz":"1.0",
                    "side":"A",
                    "time":1753606210274,
                    "startPosition":"1.0",
                    "closedPnl":"0.0",
                    "hash":"0xbad",
                    "oid":121670079266,
                    "crossed":true,
                    "fee":"-0.10",
                    "tid":161270588369409,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let routed = route_jsonl_line("node_fills_by_block/hourly/20250727/08.lz4", 1, line)
            .expect("line should be routed");

        assert_eq!(routed.rows_inserted, 1);
        assert_eq!(routed.rows_quarantined, 1);
        assert_eq!(routed.parse_errors, 0);
        assert_eq!(routed.ilp_lines.len(), 2);
        assert!(routed
            .ilp_lines
            .iter()
            .any(|line| line.starts_with("fills,")));
        assert!(routed
            .ilp_lines
            .iter()
            .any(|line| line.starts_with("fills_quarantine,reason=px<\\=0")));
    }

    #[test]
    fn route_jsonl_line_bad_time_uses_hour_timestamp_for_quarantine() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0xbadtime",{
                    "coin":"BTC",
                    "px":"118136.0",
                    "sz":"0.00009",
                    "side":"B",
                    "time":9000000000000000,
                    "startPosition":"-1.41864",
                    "closedPnl":"-0.003753",
                    "hash":"0xbadtime",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let routed = route_jsonl_line("node_fills_by_block/hourly/20250727/08.lz4", 1, line)
            .expect("line should be routed");

        assert_eq!(routed.rows_inserted, 0);
        assert_eq!(routed.rows_quarantined, 1);
        assert_eq!(routed.parse_errors, 0);
        assert_eq!(routed.ilp_lines.len(), 1);
        assert!(routed.ilp_lines[0].starts_with("fills_quarantine,reason=bad_time "));
        assert!(routed.ilp_lines[0].ends_with(" 1753603200000000000"));
    }

    #[test]
    fn route_jsonl_line_non_finite_values_are_quarantined_without_ilp_failure() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0xnan",{
                    "coin":"BTC",
                    "px":"NaN",
                    "sz":"0.00009",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.41864",
                    "closedPnl":"-0.003753",
                    "hash":"0xnan",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let routed = route_jsonl_line("node_fills_by_block/hourly/20250727/08.lz4", 1, line)
            .expect("line should be routed");

        assert_eq!(routed.rows_inserted, 0);
        assert_eq!(routed.rows_quarantined, 1);
        assert_eq!(routed.parse_errors, 0);
        assert_eq!(routed.ilp_lines.len(), 1);
        assert!(routed.ilp_lines[0].starts_with("fills_quarantine,reason=px_not_finite "));
        assert!(!routed.ilp_lines[0].contains("px="));
    }

    #[test]
    fn parse_error_hash_includes_hour_and_line() {
        assert_eq!(
            parse_error_hash("node_fills_by_block/hourly/20250727/09.lz4", 3),
            "parse_error:20250727/09:3"
        );
    }
}
