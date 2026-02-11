use super::parse::ParsedFill;
use anyhow::Result;

/// Validate a fill. Returns Ok(()) if valid, Err with reason if invalid.
pub fn validate_fill(fill: &ParsedFill) -> std::result::Result<(), String> {
    if fill.px <= 0.0 {
        return Err("px<=0".to_string());
    }
    if fill.sz <= 0.0 {
        return Err("sz<=0".to_string());
    }
    // Timestamp sanity: not before 2025-07-27 (ms) and not more than 24h in future
    let min_time_ms = 1753574400000_i64; // 2025-07-27T00:00:00Z
    let max_time_ms = chrono::Utc::now().timestamp_millis() + 86_400_000;
    if fill.time_ms < min_time_ms || fill.time_ms > max_time_ms {
        return Err("bad_time".to_string());
    }
    Ok(())
}

/// Ingest a batch of validated fills into QuestDB via ILP.
pub async fn ingest_fills(_fills: &[ParsedFill]) -> Result<()> {
    // TODO: connect to QuestDB ILP and write fill rows
    Ok(())
}

/// Insert invalid fills into the quarantine table.
pub async fn quarantine_fill(_fill: &ParsedFill, _reason: &str) -> Result<()> {
    // TODO: insert into fills_quarantine via ILP
    Ok(())
}
