use crate::config::Config;
use crate::db::QuestDbReader;
use crate::grpc::proto;
use chrono::{DateTime, NaiveDateTime, Utc};
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tokio_postgres::{
    types::{FromSql, ToSql},
    Error as PgError, Row,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const GET_FILLS_STREAM_BUFFER: usize = 128;
const SIDE_UNSPECIFIED: i32 = 0;
const SIDE_BUY: i32 = 1;
const SIDE_SELL: i32 = 2;

const GET_FILLS_BASE_SQL: &str = concat!(
    "SELECT time, block_time, block_number, address, coin, type, px, sz, is_buy, ",
    "start_position, is_gaining_inventory, closed_pnl, hash, oid, crossed, fee, tid, ",
    "fee_token, cloid, builder_fee, builder, local_time ",
    "FROM fills WHERE time >= $1 AND time < $2"
);
const GET_FILL_BY_HASH_SQL: &str = concat!(
    "SELECT time, block_time, block_number, address, coin, type, px, sz, is_buy, ",
    "start_position, is_gaining_inventory, closed_pnl, hash, oid, crossed, fee, tid, ",
    "fee_token, cloid, builder_fee, builder, local_time ",
    "FROM fills WHERE hash = $1 ORDER BY time ASC"
);
const GET_FILLS_BY_OID_SQL: &str = concat!(
    "SELECT time, block_time, block_number, address, coin, type, px, sz, is_buy, ",
    "start_position, is_gaining_inventory, closed_pnl, hash, oid, crossed, fee, tid, ",
    "fee_token, cloid, builder_fee, builder, local_time ",
    "FROM fills WHERE oid = $1 ORDER BY time ASC"
);

#[derive(Debug)]
struct ParsedGetFillsRequest {
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    coin: Option<String>,
    wallet: Option<String>,
    side_is_buy: Option<bool>,
    crossed_only: bool,
}

pub struct QueryService {
    config: Config,
}

impl QueryService {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[tonic::async_trait]
impl proto::historical_data_service_server::HistoricalDataService for QueryService {
    type GetFillsStream = ReceiverStream<Result<proto::GetFillsResponse, Status>>;

    async fn get_fills(
        &self,
        request: Request<proto::GetFillsRequest>,
    ) -> Result<Response<Self::GetFillsStream>, Status> {
        let request = request.into_inner();
        let parsed = parse_get_fills_request(request)?;

        let sql = build_get_fills_sql(
            parsed.coin.is_some(),
            parsed.wallet.is_some(),
            parsed.side_is_buy.is_some(),
            parsed.crossed_only,
        );

        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&parsed.start_time, &parsed.end_time];
        if let Some(ref coin) = parsed.coin {
            params.push(coin);
        }
        if let Some(ref wallet) = parsed.wallet {
            params.push(wallet);
        }
        if let Some(ref side_is_buy) = parsed.side_is_buy {
            params.push(side_is_buy);
        }

        let reader = QuestDbReader::new(&self.config.questdb);
        let mut row_stream = reader
            .query_stream(&sql, &params)
            .await
            .map_err(|err| map_query_start_error("GetFills", err))?;

        let (tx, rx) = mpsc::channel(GET_FILLS_STREAM_BUFFER);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    next_row = row_stream.next() => {
                        match next_row {
                            Some(Ok(row)) => match row_to_fill(&row) {
                                Ok(fill) => {
                                    if tx
                                        .send(Ok(proto::GetFillsResponse { fill: Some(fill) }))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Err(status) => {
                                    let _ = tx.send(Err(status)).await;
                                    return;
                                }
                            },
                            Some(Err(err)) => {
                                tracing::error!(error = %err, "GetFills row stream failed");
                                let _ = tx
                                    .send(Err(Status::unavailable(
                                        "QuestDB stream error during GetFills query",
                                    )))
                                    .await;
                                return;
                            }
                            None => return,
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_fill_by_hash(
        &self,
        request: Request<proto::GetFillByHashRequest>,
    ) -> Result<Response<proto::GetFillByHashResponse>, Status> {
        let hash = parse_get_fill_by_hash_request(request.into_inner())?;

        let reader = QuestDbReader::new(&self.config.questdb);
        let rows = reader
            .query(GET_FILL_BY_HASH_SQL, &[&hash])
            .await
            .map_err(|err| map_query_start_error("GetFillByHash", err))?;

        let fills = rows
            .iter()
            .map(row_to_fill)
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(proto::GetFillByHashResponse { fills }))
    }

    type GetFillsByOidStream = ReceiverStream<Result<proto::GetFillsByOidResponse, Status>>;

    async fn get_fills_by_oid(
        &self,
        request: Request<proto::GetFillsByOidRequest>,
    ) -> Result<Response<Self::GetFillsByOidStream>, Status> {
        let oid = parse_get_fills_by_oid_request(request.into_inner())?;
        let params: Vec<&(dyn ToSql + Sync)> = vec![&oid];

        let reader = QuestDbReader::new(&self.config.questdb);
        let mut row_stream = reader
            .query_stream(GET_FILLS_BY_OID_SQL, &params)
            .await
            .map_err(|err| map_query_start_error("GetFillsByOid", err))?;

        let (tx, rx) = mpsc::channel(GET_FILLS_STREAM_BUFFER);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    next_row = row_stream.next() => {
                        match next_row {
                            Some(Ok(row)) => match row_to_fill(&row) {
                                Ok(fill) => {
                                    if tx
                                        .send(Ok(proto::GetFillsByOidResponse { fill: Some(fill) }))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                Err(status) => {
                                    let _ = tx.send(Err(status)).await;
                                    return;
                                }
                            },
                            Some(Err(err)) => {
                                tracing::error!(error = %err, "GetFillsByOid row stream failed");
                                let _ = tx
                                    .send(Err(Status::unavailable(
                                        "QuestDB stream error during GetFillsByOid query",
                                    )))
                                    .await;
                                return;
                            }
                            None => return,
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type GetVWAPStream = ReceiverStream<Result<proto::GetVwapResponse, Status>>;

    async fn get_vwap(
        &self,
        _request: Request<proto::GetVwapRequest>,
    ) -> Result<Response<Self::GetVWAPStream>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("GetVWAP not yet implemented"))
    }

    type GetTimeBarsStream = ReceiverStream<Result<proto::GetTimeBarsResponse, Status>>;

    async fn get_time_bars(
        &self,
        _request: Request<proto::GetTimeBarsRequest>,
    ) -> Result<Response<Self::GetTimeBarsStream>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("GetTimeBars not yet implemented"))
    }

    async fn get_wallet_summary(
        &self,
        _request: Request<proto::GetWalletSummaryRequest>,
    ) -> Result<Response<proto::GetWalletSummaryResponse>, Status> {
        // TODO: implement query
        Err(Status::unimplemented(
            "GetWalletSummary not yet implemented",
        ))
    }

    async fn list_coins(
        &self,
        _request: Request<proto::ListCoinsRequest>,
    ) -> Result<Response<proto::ListCoinsResponse>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("ListCoins not yet implemented"))
    }
}

fn map_query_start_error(operation: &'static str, err: anyhow::Error) -> Status {
    tracing::error!(operation, error = %err, "failed to start query");

    if err
        .downcast_ref::<PgError>()
        .and_then(|pg_err| pg_err.as_db_error())
        .is_some()
    {
        return Status::internal(format!("{operation} query failed due to a database error"));
    }

    Status::unavailable(format!("QuestDB unavailable while starting {operation} query"))
}

fn parse_get_fills_request(
    request: proto::GetFillsRequest,
) -> Result<ParsedGetFillsRequest, Status> {
    let start_ts = require_non_zero_timestamp(request.start_time, "start_time")?;
    let end_ts = require_non_zero_timestamp(request.end_time, "end_time")?;

    let start_time = timestamp_to_naive(&start_ts, "start_time")?;
    let end_time = timestamp_to_naive(&end_ts, "end_time")?;
    if start_time >= end_time {
        return Err(Status::invalid_argument(
            "start_time must be strictly earlier than end_time",
        ));
    }

    Ok(ParsedGetFillsRequest {
        start_time,
        end_time,
        coin: normalize_optional_string(request.coin, "coin")?,
        wallet: normalize_optional_string(request.wallet, "wallet")?,
        side_is_buy: parse_side_filter(request.side)?,
        crossed_only: request.crossed_only.unwrap_or(false),
    })
}

fn parse_get_fill_by_hash_request(request: proto::GetFillByHashRequest) -> Result<String, Status> {
    normalize_required_string(request.hash, "hash")
}

fn parse_get_fills_by_oid_request(request: proto::GetFillsByOidRequest) -> Result<i64, Status> {
    require_positive_i64(request.oid, "oid")
}

fn require_non_zero_timestamp(
    timestamp: Option<Timestamp>,
    field: &'static str,
) -> Result<Timestamp, Status> {
    let timestamp =
        timestamp.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;

    if timestamp.seconds == 0 && timestamp.nanos == 0 {
        return Err(Status::invalid_argument(format!(
            "{field} must be set and non-zero"
        )));
    }

    Ok(timestamp)
}

fn timestamp_to_naive(timestamp: &Timestamp, field: &'static str) -> Result<NaiveDateTime, Status> {
    if !(0..=999_999_999).contains(&timestamp.nanos) {
        return Err(Status::invalid_argument(format!(
            "{field}.nanos must be between 0 and 999999999"
        )));
    }

    let datetime = DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is out of range")))?;

    Ok(datetime.naive_utc())
}

fn naive_to_timestamp(value: NaiveDateTime) -> Timestamp {
    let datetime = value.and_utc();
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

fn normalize_required_string(value: String, field: &'static str) -> Result<String, Status> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument(format!("{field} cannot be empty")));
    }

    Ok(trimmed.to_string())
}

fn require_positive_i64(value: i64, field: &'static str) -> Result<i64, Status> {
    if value <= 0 {
        return Err(Status::invalid_argument(format!(
            "{field} must be a positive integer"
        )));
    }

    Ok(value)
}

fn normalize_optional_string(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<String>, Status> {
    match value {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Err(Status::invalid_argument(format!(
                    "{field} cannot be empty when provided"
                )))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        None => Ok(None),
    }
}

fn parse_side_filter(side: Option<i32>) -> Result<Option<bool>, Status> {
    match side {
        None | Some(SIDE_UNSPECIFIED) => Ok(None),
        Some(SIDE_BUY) => Ok(Some(true)),
        Some(SIDE_SELL) => Ok(Some(false)),
        Some(raw) => Err(Status::invalid_argument(format!(
            "side has unknown enum value: {raw}"
        ))),
    }
}

fn build_get_fills_sql(
    has_coin: bool,
    has_wallet: bool,
    has_side: bool,
    crossed_only: bool,
) -> String {
    let mut sql = String::from(GET_FILLS_BASE_SQL);
    let mut next_param = 3;

    if has_coin {
        sql.push_str(&format!(" AND coin = ${next_param}"));
        next_param += 1;
    }

    if has_wallet {
        sql.push_str(&format!(" AND address = ${next_param}"));
        next_param += 1;
    }

    if has_side {
        sql.push_str(&format!(" AND is_buy = ${next_param}"));
    }

    if crossed_only {
        sql.push_str(" AND crossed = true");
    }

    sql.push_str(" ORDER BY time ASC");
    sql
}

fn decode_required<T>(row: &Row, column: &'static str) -> Result<T, Status>
where
    for<'a> T: FromSql<'a>,
{
    row.try_get::<_, T>(column).map_err(|err| {
        tracing::error!(column, error = %err, "failed to decode required fills column");
        Status::internal(format!("Failed to decode fill row column '{column}'"))
    })
}

fn decode_optional<T>(row: &Row, column: &'static str) -> Result<Option<T>, Status>
where
    for<'a> T: FromSql<'a>,
{
    row.try_get::<_, Option<T>>(column).map_err(|err| {
        tracing::error!(column, error = %err, "failed to decode optional fills column");
        Status::internal(format!("Failed to decode fill row column '{column}'"))
    })
}

fn row_to_fill(row: &Row) -> Result<proto::Fill, Status> {
    let time: NaiveDateTime = decode_required(row, "time")?;
    let block_time: Option<NaiveDateTime> = decode_optional(row, "block_time")?;
    let block_number: i64 = decode_required(row, "block_number")?;
    let address: String = decode_required(row, "address")?;
    let coin: String = decode_required(row, "coin")?;
    let fill_type: String = decode_required(row, "type")?;
    let px: f64 = decode_required(row, "px")?;
    let sz: f64 = decode_required(row, "sz")?;
    let is_buy: bool = decode_required(row, "is_buy")?;
    let start_position: f64 = decode_required(row, "start_position")?;
    let is_gaining_inventory: bool = decode_required(row, "is_gaining_inventory")?;
    let closed_pnl: f64 = decode_required(row, "closed_pnl")?;
    let hash: String = decode_required(row, "hash")?;
    let oid: i64 = decode_required(row, "oid")?;
    let crossed: bool = decode_required(row, "crossed")?;
    let fee: f64 = decode_required(row, "fee")?;
    let tid: i64 = decode_required(row, "tid")?;
    let fee_token: String = decode_required(row, "fee_token")?;
    let cloid: Option<String> = decode_optional(row, "cloid")?;
    let builder_fee: Option<String> = decode_optional(row, "builder_fee")?;
    let builder: Option<String> = decode_optional(row, "builder")?;
    let local_time: Option<NaiveDateTime> = decode_optional(row, "local_time")?;

    Ok(proto::Fill {
        time: Some(naive_to_timestamp(time)),
        block_time: block_time.map(naive_to_timestamp),
        block_number,
        address,
        coin,
        r#type: fill_type,
        px,
        sz,
        is_buy,
        start_position,
        is_gaining_inventory,
        closed_pnl,
        hash,
        oid,
        crossed,
        fee,
        tid,
        fee_token,
        cloid,
        builder_fee,
        builder,
        local_time: local_time.map(naive_to_timestamp),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp { seconds, nanos }
    }

    #[test]
    fn normalize_optional_string_rejects_blank_values() {
        let err = normalize_optional_string(Some("   ".to_string()), "coin")
            .expect_err("blank coin should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn normalize_optional_string_trims_values() {
        let normalized = normalize_optional_string(Some("  BTC  ".to_string()), "coin")
            .expect("coin should normalize");
        assert_eq!(normalized.as_deref(), Some("BTC"));
    }

    #[test]
    fn normalize_required_string_rejects_blank_values() {
        let err = normalize_required_string("   ".to_string(), "hash")
            .expect_err("blank hash should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn normalize_required_string_trims_values() {
        let hash =
            normalize_required_string(" 0xabc123 ".to_string(), "hash").expect("hash should normalize");
        assert_eq!(hash, "0xabc123");
    }

    #[test]
    fn require_positive_i64_rejects_non_positive_values() {
        let zero = require_positive_i64(0, "oid").expect_err("zero oid should fail");
        assert_eq!(zero.code(), tonic::Code::InvalidArgument);

        let negative = require_positive_i64(-1, "oid").expect_err("negative oid should fail");
        assert_eq!(negative.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_fill_by_hash_request_rejects_blank_hash() {
        let request = proto::GetFillByHashRequest {
            hash: "   ".to_string(),
        };

        let err = parse_get_fill_by_hash_request(request).expect_err("blank hash should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_fill_by_hash_request_trims_hash() {
        let request = proto::GetFillByHashRequest {
            hash: " 0xdeadbeef ".to_string(),
        };

        let hash = parse_get_fill_by_hash_request(request).expect("hash should parse");
        assert_eq!(hash, "0xdeadbeef");
    }

    #[test]
    fn parse_get_fills_by_oid_request_rejects_zero_oid() {
        let request = proto::GetFillsByOidRequest { oid: 0 };

        let err = parse_get_fills_by_oid_request(request).expect_err("zero oid should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_side_filter_maps_enum_values() {
        assert_eq!(parse_side_filter(None).expect("none should parse"), None);
        assert_eq!(
            parse_side_filter(Some(SIDE_UNSPECIFIED)).expect("unspecified should parse"),
            None
        );
        assert_eq!(
            parse_side_filter(Some(SIDE_BUY)).expect("buy should parse"),
            Some(true)
        );
        assert_eq!(
            parse_side_filter(Some(SIDE_SELL)).expect("sell should parse"),
            Some(false)
        );
    }

    #[test]
    fn parse_side_filter_rejects_unknown_value() {
        let err = parse_side_filter(Some(99)).expect_err("unknown side should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn build_get_fills_sql_adds_filters_and_ordering() {
        let sql = build_get_fills_sql(true, true, true, true);
        assert!(sql.contains("time >= $1 AND time < $2"));
        assert!(sql.contains("coin = $3"));
        assert!(sql.contains("address = $4"));
        assert!(sql.contains("is_buy = $5"));
        assert!(sql.contains("crossed = true"));
        assert!(sql.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn get_fill_by_hash_sql_contains_expected_clauses() {
        assert!(GET_FILL_BY_HASH_SQL.contains("WHERE hash = $1"));
        assert!(GET_FILL_BY_HASH_SQL.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn get_fills_by_oid_sql_contains_expected_clauses() {
        assert!(GET_FILLS_BY_OID_SQL.contains("WHERE oid = $1"));
        assert!(GET_FILLS_BY_OID_SQL.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn build_get_fills_sql_without_optional_filters_uses_only_range() {
        let sql = build_get_fills_sql(false, false, false, false);
        assert!(sql.contains("time >= $1 AND time < $2"));
        assert!(!sql.contains("$3"));
        assert!(!sql.contains("crossed = true"));
        assert!(sql.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn build_get_fills_sql_with_wallet_and_side_keeps_parameter_order() {
        let sql = build_get_fills_sql(false, true, true, false);
        assert!(sql.contains("address = $3"));
        assert!(sql.contains("is_buy = $4"));
        assert!(!sql.contains("crossed = true"));
    }

    #[test]
    fn parse_get_fills_request_rejects_invalid_time_order() {
        let request = proto::GetFillsRequest {
            start_time: Some(ts(10, 0)),
            end_time: Some(ts(10, 0)),
            coin: None,
            wallet: None,
            side: None,
            crossed_only: None,
        };

        let err = parse_get_fills_request(request).expect_err("start == end should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_fills_request_accepts_valid_payload() {
        let request = proto::GetFillsRequest {
            start_time: Some(ts(1_737_600_000, 123_000_000)),
            end_time: Some(ts(1_737_600_100, 0)),
            coin: Some(" BTC ".to_string()),
            wallet: Some(" 0xabc ".to_string()),
            side: Some(SIDE_BUY),
            crossed_only: Some(true),
        };

        let parsed = parse_get_fills_request(request).expect("request should parse");
        assert_eq!(parsed.coin.as_deref(), Some("BTC"));
        assert_eq!(parsed.wallet.as_deref(), Some("0xabc"));
        assert_eq!(parsed.side_is_buy, Some(true));
        assert!(parsed.crossed_only);
    }

    #[test]
    fn timestamp_conversion_round_trip_preserves_second_precision() {
        let naive = NaiveDate::from_ymd_opt(2025, 7, 27)
            .expect("valid date")
            .and_hms_nano_opt(8, 50, 10, 334_741_319)
            .expect("valid timestamp");
        let converted = naive_to_timestamp(naive);
        let round_trip = timestamp_to_naive(&converted, "time").expect("should parse");
        assert_eq!(round_trip, naive);
    }

    #[test]
    fn timestamp_conversion_rejects_invalid_nanos() {
        let err =
            timestamp_to_naive(&ts(10, -1), "start_time").expect_err("negative nanos should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
