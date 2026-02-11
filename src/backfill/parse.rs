use anyhow::Result;
use serde::Deserialize;

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
    // Optional fields
    pub cloid: Option<String>,
    pub builder_fee: Option<String>,
    pub builder: Option<String>,
}

/// Block envelope from JSONL.
#[derive(Debug, Deserialize)]
pub struct BlockEnvelope {
    pub local_time: String,
    pub block_time: String,
    pub block_number: i64,
    pub events: Vec<(String, RawFill)>,
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

/// Parse a single JSONL line into zero or more fills.
pub fn parse_line(line: &str) -> Result<Vec<ParsedFill>> {
    let envelope: BlockEnvelope = serde_json::from_str(line)?;
    let mut fills = Vec::new();

    for (address, raw) in envelope.events {
        let px: f64 = raw.px.parse()?;
        let sz: f64 = raw.sz.parse()?;
        let is_buy = raw.side == "B";
        let start_position: f64 = raw.start_position.parse()?;

        // Derive fill type
        let fill_type = if raw.coin.starts_with('@') {
            "SPOT".to_string()
        } else {
            "PERP".to_string()
        };

        // Derive is_gaining_inventory
        let result_position = if is_buy {
            start_position + sz
        } else {
            start_position - sz
        };
        let is_gaining_inventory = result_position.abs() > start_position.abs();

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
