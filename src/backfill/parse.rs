use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

type ExtraFields = BTreeMap<String, Value>;

const UNKNOWN_FIELD_LOG_EVERY: u64 = 10_000;
const GAINING_INVENTORY_EPSILON: f64 = 1e-12;
static UNKNOWN_ENVELOPE_FIELD_COUNT: AtomicU64 = AtomicU64::new(0);
static UNKNOWN_FILL_FIELD_COUNT: AtomicU64 = AtomicU64::new(0);

/// Raw fill as it appears in the JSONL file.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawFill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub time: i64,
    pub start_position: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: i64,
    pub crossed: bool,
    pub fee: String,
    pub tid: i64,
    pub fee_token: String,
    #[serde(rename = "type")]
    pub explicit_type: Option<String>,
    // Known but currently unused field seen in source data.
    #[allow(dead_code)]
    pub dir: Option<String>,
    // Optional fields
    pub cloid: Option<String>,
    pub builder_fee: Option<String>,
    pub builder: Option<String>,
    #[serde(default, flatten)]
    pub extra: ExtraFields,
}

/// Block envelope from JSONL.
#[derive(Debug, Deserialize)]
pub struct BlockEnvelope {
    pub local_time: String,
    pub block_time: String,
    pub block_number: i64,
    pub events: Vec<(String, RawFill)>,
    #[serde(default, flatten)]
    pub extra: ExtraFields,
}

/// Parsed and validated fill ready for ingestion.
#[derive(Debug)]
pub struct ParsedFill {
    pub time_ms: i64,
    pub block_time: String,
    pub block_number: i64,
    pub address: String,
    pub coin: String,
    pub fill_type: String,
    pub px: f64,
    pub sz: f64,
    pub is_buy: bool,
    pub start_position: f64,
    pub is_gaining_inventory: bool,
    pub closed_pnl: f64,
    pub hash: String,
    pub oid: i64,
    pub crossed: bool,
    pub fee: f64,
    pub tid: i64,
    pub fee_token: String,
    pub cloid: Option<String>,
    pub builder_fee: Option<String>,
    pub builder: Option<String>,
    pub local_time: String,
}

fn maybe_log_unknown_fields(kind: &'static str, extra: &ExtraFields) {
    if extra.is_empty() {
        return;
    }

    let counter = match kind {
        "envelope" => &UNKNOWN_ENVELOPE_FIELD_COUNT,
        "fill" => &UNKNOWN_FILL_FIELD_COUNT,
        _ => return,
    };

    let seen = counter.fetch_add(1, Ordering::Relaxed) + 1;
    if seen == 1 || seen % UNKNOWN_FIELD_LOG_EVERY == 0 {
        let keys: Vec<&str> = extra.keys().take(8).map(String::as_str).collect();
        tracing::warn!(
            parser_kind = kind,
            seen,
            unknown_field_count = extra.len(),
            keys = ?keys,
            "Ignoring unknown JSONL fields while parsing node_fills_by_block"
        );
    }
}

fn derive_is_buy(side: &str) -> bool {
    side == "B"
}

fn derive_fill_type(explicit_type: Option<&str>, coin: &str) -> String {
    if let Some(explicit_type) = explicit_type {
        let normalized = explicit_type.trim().to_ascii_uppercase();
        if !normalized.is_empty() {
            return normalized;
        }
    }

    if coin.starts_with('@') {
        "SPOT".to_string()
    } else {
        "PERP".to_string()
    }
}

fn derive_is_gaining_inventory(start_position: f64, sz: f64, is_buy: bool) -> bool {
    let result_position = start_position + if is_buy { sz } else { -sz };
    result_position.abs() > start_position.abs() + GAINING_INVENTORY_EPSILON
}

/// Parse a single JSONL line into zero or more fills.
pub fn parse_line(line: &str) -> Result<Vec<ParsedFill>> {
    let envelope: BlockEnvelope = serde_json::from_str(line)?;
    maybe_log_unknown_fields("envelope", &envelope.extra);

    let mut fills = Vec::new();

    for (address, raw) in envelope.events {
        maybe_log_unknown_fields("fill", &raw.extra);

        let px: f64 = raw.px.parse()?;
        let sz: f64 = raw.sz.parse()?;
        let is_buy = derive_is_buy(&raw.side);
        let start_position: f64 = raw.start_position.parse()?;

        let fill_type = derive_fill_type(raw.explicit_type.as_deref(), &raw.coin);
        let is_gaining_inventory = derive_is_gaining_inventory(start_position, sz, is_buy);

        fills.push(ParsedFill {
            time_ms: raw.time,
            block_time: envelope.block_time.clone(),
            block_number: envelope.block_number,
            address,
            coin: raw.coin,
            fill_type,
            px,
            sz,
            is_buy,
            start_position,
            is_gaining_inventory,
            closed_pnl: raw.closed_pnl.parse()?,
            hash: raw.hash,
            oid: raw.oid,
            crossed: raw.crossed,
            fee: raw.fee.parse()?,
            tid: raw.tid,
            fee_token: raw.fee_token,
            cloid: raw.cloid,
            builder_fee: raw.builder_fee,
            builder: raw.builder,
            local_time: envelope.local_time.clone(),
        });
    }

    Ok(fills)
}

#[cfg(test)]
mod tests {
    use super::parse_line;

    #[test]
    fn parse_line_parses_expected_number_of_fills() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0xabc",{
                    "coin":"BTC",
                    "px":"118136.0",
                    "sz":"0.00009",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.41864",
                    "closedPnl":"-0.003753",
                    "hash":"0xe782204",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC",
                    "cloid":"0x09367b",
                    "builderFee":"0.005528",
                    "builder":"0x49ae63"
                }],
                ["0xdef",{
                    "coin":"@PURR/USDC",
                    "px":"0.323",
                    "sz":"12.0",
                    "side":"A",
                    "time":1753606210274,
                    "startPosition":"20.0",
                    "closedPnl":"0.0",
                    "hash":"0xe782205",
                    "oid":121670079266,
                    "crossed":true,
                    "fee":"-0.10",
                    "tid":161270588369409,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let fills = parse_line(line).expect("line should parse");
        assert_eq!(fills.len(), 2);

        assert_eq!(fills[0].address, "0xabc");
        assert_eq!(fills[0].coin, "BTC");
        assert_eq!(fills[0].time_ms, 1753606210273);
        assert!((fills[0].px - 118136.0).abs() < 1e-12);

        assert_eq!(fills[1].address, "0xdef");
        assert_eq!(fills[1].fill_type, "SPOT");
    }

    #[test]
    fn parse_line_derives_issue_13_fields() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0x1",{
                    "coin":"BTC",
                    "px":"100.0",
                    "sz":"0.1",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.0",
                    "closedPnl":"0.0",
                    "hash":"0x1",
                    "oid":1,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":11,
                    "feeToken":"USDC"
                }],
                ["0x2",{
                    "coin":"BTC",
                    "px":"100.0",
                    "sz":"0.1",
                    "side":"A",
                    "time":1753606210274,
                    "startPosition":"-1.0",
                    "closedPnl":"0.0",
                    "hash":"0x2",
                    "oid":2,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":12,
                    "feeToken":"USDC"
                }],
                ["0x3",{
                    "coin":"ETH",
                    "px":"100.0",
                    "sz":"0.1",
                    "side":"B",
                    "time":1753606210275,
                    "startPosition":"1.0",
                    "closedPnl":"0.0",
                    "hash":"0x3",
                    "oid":3,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":13,
                    "feeToken":"USDC"
                }],
                ["0x4",{
                    "coin":"ETH",
                    "px":"100.0",
                    "sz":"0.1",
                    "side":"A",
                    "time":1753606210276,
                    "startPosition":"1.0",
                    "closedPnl":"0.0",
                    "hash":"0x4",
                    "oid":4,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":14,
                    "feeToken":"USDC"
                }],
                ["0x5",{
                    "coin":"@PURR/USDC",
                    "px":"0.32",
                    "sz":"2.0",
                    "side":"A",
                    "time":1753606210277,
                    "startPosition":"10.0",
                    "closedPnl":"0.0",
                    "hash":"0x5",
                    "oid":5,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":15,
                    "feeToken":"USDC"
                }],
                ["0x6",{
                    "coin":"@PURR/USDC",
                    "type":"perp",
                    "px":"0.31",
                    "sz":"1.0",
                    "side":"B",
                    "time":1753606210278,
                    "startPosition":"10.0",
                    "closedPnl":"0.0",
                    "hash":"0x6",
                    "oid":6,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":16,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let fills = parse_line(line).expect("line should parse");
        assert_eq!(fills.len(), 6);

        // B/A mapping for is_buy.
        assert!(fills[0].is_buy);
        assert!(!fills[1].is_buy);

        // Type derivation and explicit override.
        assert_eq!(fills[0].fill_type, "PERP");
        assert_eq!(fills[4].fill_type, "SPOT");
        assert_eq!(fills[5].fill_type, "PERP");

        // Inventory direction examples.
        assert!(!fills[0].is_gaining_inventory); // -1.0 + buy 0.1 => -0.9
        assert!(fills[1].is_gaining_inventory); // -1.0 + sell 0.1 => -1.1
        assert!(fills[2].is_gaining_inventory); // +1.0 + buy 0.1 => +1.1
        assert!(!fills[3].is_gaining_inventory); // +1.0 + sell 0.1 => +0.9
    }

    #[test]
    fn parse_line_uses_epsilon_for_is_gaining_inventory() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0x1",{
                    "coin":"BTC",
                    "px":"100.0",
                    "sz":"0.0000000000005",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"1.0",
                    "closedPnl":"0.0",
                    "hash":"0xe1",
                    "oid":21,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":121,
                    "feeToken":"USDC"
                }],
                ["0x2",{
                    "coin":"BTC",
                    "px":"100.0",
                    "sz":"0.000000000002",
                    "side":"B",
                    "time":1753606210274,
                    "startPosition":"1.0",
                    "closedPnl":"0.0",
                    "hash":"0xe2",
                    "oid":22,
                    "crossed":false,
                    "fee":"0.0",
                    "tid":122,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let fills = parse_line(line).expect("line should parse");
        assert_eq!(fills.len(), 2);
        assert!(!fills[0].is_gaining_inventory);
        assert!(fills[1].is_gaining_inventory);
    }

    #[test]
    fn parse_line_handles_missing_optional_fields() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[
                ["0xabc",{
                    "coin":"BTC",
                    "px":"118136.0",
                    "sz":"0.00009",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.41864",
                    "closedPnl":"-0.003753",
                    "hash":"0xe782204",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC"
                }]
            ]
        }"#;

        let fills = parse_line(line).expect("line should parse");
        assert_eq!(fills.len(), 1);
        assert!(fills[0].cloid.is_none());
        assert!(fills[0].builder_fee.is_none());
        assert!(fills[0].builder.is_none());
    }

    #[test]
    fn parse_line_ignores_unknown_fields() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "version":1,
            "events":[
                ["0xabc",{
                    "coin":"BTC",
                    "px":"118136.0",
                    "sz":"0.00009",
                    "side":"B",
                    "time":1753606210273,
                    "startPosition":"-1.41864",
                    "dir":"Close Short",
                    "closedPnl":"-0.003753",
                    "hash":"0xe782204",
                    "oid":121670079265,
                    "crossed":false,
                    "fee":"-0.000212",
                    "tid":161270588369408,
                    "feeToken":"USDC",
                    "newField":123
                }]
            ]
        }"#;

        let fills = parse_line(line).expect("line should parse with unknown fields");
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].address, "0xabc");
    }

    #[test]
    fn parse_line_with_empty_events_returns_empty_vec() {
        let line = r#"{
            "local_time":"2025-07-27T08:50:10.334741319",
            "block_time":"2025-07-27T08:50:10.273720809",
            "block_number":676607012,
            "events":[]
        }"#;

        let fills = parse_line(line).expect("line should parse");
        assert!(fills.is_empty());
    }
}
