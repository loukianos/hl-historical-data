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
const INTERVAL_UNSPECIFIED: i32 = 0;
const INTERVAL_1S: i32 = 1;
const INTERVAL_5S: i32 = 2;
const INTERVAL_30S: i32 = 3;
const INTERVAL_1M: i32 = 4;
const INTERVAL_5M: i32 = 5;
const INTERVAL_15M: i32 = 6;
const INTERVAL_30M: i32 = 7;
const INTERVAL_1H: i32 = 8;
const INTERVAL_4H: i32 = 9;
const INTERVAL_1D: i32 = 10;

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
const GET_VWAP_BASE_SQL: &str = concat!(
    "SELECT time, sum(px * sz) AS notional, sum(sz) AS volume, count() AS fill_count ",
    "FROM fills WHERE time >= $1 AND time < $2 AND coin = $3"
);
const GET_TIME_BARS_BASE_SQL: &str = concat!(
    "SELECT time, ",
    "first(px) AS open, max(px) AS high, min(px) AS low, last(px) AS close, ",
    "sum(sz) AS base_volume, sum(px * sz) AS notional_volume, count() AS trade_count, ",
    "sum(CASE WHEN is_buy = true THEN sz ELSE 0.0 END) AS buy_volume, ",
    "sum(CASE WHEN is_buy = false THEN sz ELSE 0.0 END) AS sell_volume ",
    "FROM fills WHERE time >= $1 AND time < $2 AND coin = $3 AND crossed = true"
);
const GET_WALLET_SUMMARY_PER_COIN_SQL: &str = concat!(
    "SELECT coin, ",
    "sum(sz) AS volume, ",
    "sum(px * sz) AS notional_volume, ",
    "count() AS fill_count, ",
    "sum(CASE WHEN crossed = false THEN 1 ELSE 0 END) AS maker_count, ",
    "sum(CASE WHEN crossed = true THEN 1 ELSE 0 END) AS taker_count ",
    "FROM fills ",
    "WHERE time >= $1 AND time < $2 AND address = $3 ",
    "GROUP BY coin ORDER BY fill_count DESC, coin ASC"
);
const GET_WALLET_SUMMARY_TIME_BOUNDS_SQL: &str = concat!(
    "SELECT min(time) AS first_fill_time, max(time) AS last_fill_time ",
    "FROM fills WHERE time >= $1 AND time < $2 AND address = $3"
);
const LIST_COINS_SQL: &str =
    "SELECT coin, count() AS fill_count FROM fills GROUP BY coin ORDER BY fill_count DESC";

#[derive(Debug)]
struct ParsedGetFillsRequest {
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    coin: Option<String>,
    wallet: Option<String>,
    side_is_buy: Option<bool>,
    crossed_only: bool,
}

#[derive(Debug)]
struct ParsedGetVwapRequest {
    coin: String,
    interval: SampleByInterval,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
}

#[derive(Debug)]
struct ParsedGetTimeBarsRequest {
    coin: String,
    interval: SampleByInterval,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
}

#[derive(Debug)]
struct ParsedGetWalletSummaryRequest {
    wallet: String,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleByInterval {
    S1,
    S5,
    S30,
    M1,
    M5,
    M15,
    M30,
    H1,
    H4,
    D1,
}

impl SampleByInterval {
    fn as_questdb(self) -> &'static str {
        match self {
            Self::S1 => "1s",
            Self::S5 => "5s",
            Self::S30 => "30s",
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::M30 => "30m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
        }
    }
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
        request: Request<proto::GetVwapRequest>,
    ) -> Result<Response<Self::GetVWAPStream>, Status> {
        let parsed = parse_get_vwap_request(request.into_inner())?;
        let sql = build_get_vwap_sql(parsed.interval);
        let params: Vec<&(dyn ToSql + Sync)> =
            vec![&parsed.start_time, &parsed.end_time, &parsed.coin];

        let reader = QuestDbReader::new(&self.config.questdb);
        let mut row_stream = reader
            .query_stream(&sql, &params)
            .await
            .map_err(|err| map_query_start_error("GetVWAP", err))?;

        let (tx, rx) = mpsc::channel(GET_FILLS_STREAM_BUFFER);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    next_row = row_stream.next() => {
                        match next_row {
                            Some(Ok(row)) => match row_to_vwap_point(&row) {
                                Ok(point) => {
                                    if tx
                                        .send(Ok(proto::GetVwapResponse { point: Some(point) }))
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
                                tracing::error!(error = %err, "GetVWAP row stream failed");
                                let _ = tx
                                    .send(Err(Status::unavailable(
                                        "QuestDB stream error during GetVWAP query",
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

    type GetTimeBarsStream = ReceiverStream<Result<proto::GetTimeBarsResponse, Status>>;

    async fn get_time_bars(
        &self,
        request: Request<proto::GetTimeBarsRequest>,
    ) -> Result<Response<Self::GetTimeBarsStream>, Status> {
        let parsed = parse_get_time_bars_request(request.into_inner())?;
        let sql = build_get_time_bars_sql(parsed.interval);
        let params: Vec<&(dyn ToSql + Sync)> =
            vec![&parsed.start_time, &parsed.end_time, &parsed.coin];

        let reader = QuestDbReader::new(&self.config.questdb);
        let mut row_stream = reader
            .query_stream(&sql, &params)
            .await
            .map_err(|err| map_query_start_error("GetTimeBars", err))?;

        let (tx, rx) = mpsc::channel(GET_FILLS_STREAM_BUFFER);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    next_row = row_stream.next() => {
                        match next_row {
                            Some(Ok(row)) => match row_to_time_bar(&row) {
                                Ok(bar) => {
                                    if tx
                                        .send(Ok(proto::GetTimeBarsResponse { bar: Some(bar) }))
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
                                tracing::error!(error = %err, "GetTimeBars row stream failed");
                                let _ = tx
                                    .send(Err(Status::unavailable(
                                        "QuestDB stream error during GetTimeBars query",
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

    async fn get_wallet_summary(
        &self,
        request: Request<proto::GetWalletSummaryRequest>,
    ) -> Result<Response<proto::GetWalletSummaryResponse>, Status> {
        let parsed = parse_get_wallet_summary_request(request.into_inner())?;
        let params: Vec<&(dyn ToSql + Sync)> =
            vec![&parsed.start_time, &parsed.end_time, &parsed.wallet];

        let reader = QuestDbReader::new(&self.config.questdb);
        let rows = reader
            .query(GET_WALLET_SUMMARY_PER_COIN_SQL, &params)
            .await
            .map_err(|err| map_query_start_error("GetWalletSummary", err))?;

        let per_coin = rows
            .iter()
            .map(row_to_coin_summary)
            .collect::<Result<Vec<_>, Status>>()?;

        let fill_count: i64 = per_coin.iter().map(|entry| entry.fill_count).sum();
        let unique_coins = per_coin.len() as i64;
        let total_volume: f64 = per_coin.iter().map(|entry| entry.volume).sum();
        let total_notional: f64 = per_coin.iter().map(|entry| entry.notional_volume).sum();
        let maker_count: i64 = per_coin.iter().map(|entry| entry.maker_count).sum();
        let taker_count: i64 = per_coin.iter().map(|entry| entry.taker_count).sum();
        let maker_taker_ratio = compute_maker_taker_ratio(maker_count, taker_count);

        let (first_fill_time, last_fill_time) = if per_coin.is_empty() {
            (None, None)
        } else {
            let row = reader
                .query_one(GET_WALLET_SUMMARY_TIME_BOUNDS_SQL, &params)
                .await
                .map_err(|err| map_query_start_error("GetWalletSummary", err))?;

            let first_fill_time: Option<NaiveDateTime> = decode_optional(&row, "first_fill_time")?;
            let last_fill_time: Option<NaiveDateTime> = decode_optional(&row, "last_fill_time")?;
            (first_fill_time, last_fill_time)
        };

        Ok(Response::new(proto::GetWalletSummaryResponse {
            fill_count,
            unique_coins,
            total_volume,
            total_notional,
            maker_count,
            taker_count,
            maker_taker_ratio,
            first_fill_time: first_fill_time.map(naive_to_timestamp),
            last_fill_time: last_fill_time.map(naive_to_timestamp),
            per_coin,
        }))
    }

    async fn list_coins(
        &self,
        _request: Request<proto::ListCoinsRequest>,
    ) -> Result<Response<proto::ListCoinsResponse>, Status> {
        let reader = QuestDbReader::new(&self.config.questdb);
        let rows = reader
            .query(LIST_COINS_SQL, &[])
            .await
            .map_err(|err| map_query_start_error("ListCoins", err))?;

        let coins = rows
            .iter()
            .map(row_to_coin_info)
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(proto::ListCoinsResponse { coins }))
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

    Status::unavailable(format!(
        "QuestDB unavailable while starting {operation} query"
    ))
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

fn parse_get_vwap_request(request: proto::GetVwapRequest) -> Result<ParsedGetVwapRequest, Status> {
    let start_ts = require_non_zero_timestamp(request.start_time, "start_time")?;
    let end_ts = require_non_zero_timestamp(request.end_time, "end_time")?;

    let start_time = timestamp_to_naive(&start_ts, "start_time")?;
    let end_time = timestamp_to_naive(&end_ts, "end_time")?;
    if start_time >= end_time {
        return Err(Status::invalid_argument(
            "start_time must be strictly earlier than end_time",
        ));
    }

    Ok(ParsedGetVwapRequest {
        coin: normalize_required_string(request.coin, "coin")?,
        interval: parse_interval(request.interval)?,
        start_time,
        end_time,
    })
}

fn parse_get_time_bars_request(
    request: proto::GetTimeBarsRequest,
) -> Result<ParsedGetTimeBarsRequest, Status> {
    let start_ts = require_non_zero_timestamp(request.start_time, "start_time")?;
    let end_ts = require_non_zero_timestamp(request.end_time, "end_time")?;

    let start_time = timestamp_to_naive(&start_ts, "start_time")?;
    let end_time = timestamp_to_naive(&end_ts, "end_time")?;
    if start_time >= end_time {
        return Err(Status::invalid_argument(
            "start_time must be strictly earlier than end_time",
        ));
    }

    Ok(ParsedGetTimeBarsRequest {
        coin: normalize_required_string(request.coin, "coin")?,
        interval: parse_interval(request.interval)?,
        start_time,
        end_time,
    })
}

fn parse_get_wallet_summary_request(
    request: proto::GetWalletSummaryRequest,
) -> Result<ParsedGetWalletSummaryRequest, Status> {
    let start_ts = require_non_zero_timestamp(request.start_time, "start_time")?;
    let end_ts = require_non_zero_timestamp(request.end_time, "end_time")?;

    let start_time = timestamp_to_naive(&start_ts, "start_time")?;
    let end_time = timestamp_to_naive(&end_ts, "end_time")?;
    if start_time >= end_time {
        return Err(Status::invalid_argument(
            "start_time must be strictly earlier than end_time",
        ));
    }

    Ok(ParsedGetWalletSummaryRequest {
        wallet: normalize_required_string(request.wallet, "wallet")?,
        start_time,
        end_time,
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

fn parse_interval(interval: i32) -> Result<SampleByInterval, Status> {
    match interval {
        INTERVAL_UNSPECIFIED => Err(Status::invalid_argument("interval must be specified")),
        INTERVAL_1S => Ok(SampleByInterval::S1),
        INTERVAL_5S => Ok(SampleByInterval::S5),
        INTERVAL_30S => Ok(SampleByInterval::S30),
        INTERVAL_1M => Ok(SampleByInterval::M1),
        INTERVAL_5M => Ok(SampleByInterval::M5),
        INTERVAL_15M => Ok(SampleByInterval::M15),
        INTERVAL_30M => Ok(SampleByInterval::M30),
        INTERVAL_1H => Ok(SampleByInterval::H1),
        INTERVAL_4H => Ok(SampleByInterval::H4),
        INTERVAL_1D => Ok(SampleByInterval::D1),
        raw => Err(Status::invalid_argument(format!(
            "interval has unknown enum value: {raw}"
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

fn build_get_vwap_sql(interval: SampleByInterval) -> String {
    format!(
        "{GET_VWAP_BASE_SQL} SAMPLE BY {} ORDER BY time ASC",
        interval.as_questdb()
    )
}

fn build_get_time_bars_sql(interval: SampleByInterval) -> String {
    format!(
        "{GET_TIME_BARS_BASE_SQL} SAMPLE BY {} ORDER BY time ASC",
        interval.as_questdb()
    )
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

fn i64_or_i32_as_i64<E>(
    primary: Result<i64, E>,
    fallback: impl FnOnce() -> Result<i32, E>,
) -> Result<i64, E> {
    primary.or_else(|_| fallback().map(i64::from))
}

fn decode_required_count(row: &Row, column: &'static str) -> Result<i64, Status> {
    match i64_or_i32_as_i64(row.try_get::<_, i64>(column), || {
        row.try_get::<_, i32>(column)
    }) {
        Ok(value) => Ok(value),
        Err(i64_or_i32_err) => {
            let value: f64 = row.try_get(column).map_err(|f64_err| {
                tracing::error!(
                    column,
                    i64_or_i32_error = %i64_or_i32_err,
                    f64_error = %f64_err,
                    "failed to decode required count column"
                );
                Status::internal(format!("Failed to decode row column '{column}'"))
            })?;

            if !value.is_finite() || value.fract() != 0.0 {
                tracing::error!(
                    column,
                    value,
                    "count column decoded as non-integral floating-point value"
                );
                return Err(Status::internal(format!(
                    "Failed to decode row column '{column}'"
                )));
            }

            if value < i64::MIN as f64 || value > i64::MAX as f64 {
                tracing::error!(column, value, "count column value is out of i64 range");
                return Err(Status::internal(format!(
                    "Failed to decode row column '{column}'"
                )));
            }

            Ok(value as i64)
        }
    }
}

fn row_to_coin_info(row: &Row) -> Result<proto::CoinInfo, Status> {
    let coin: String = decode_required(row, "coin")?;
    let fill_count: i64 = decode_required_count(row, "fill_count")?;

    Ok(proto::CoinInfo { coin, fill_count })
}

fn row_to_coin_summary(row: &Row) -> Result<proto::CoinSummary, Status> {
    let coin: String = decode_required(row, "coin")?;
    let volume: f64 = decode_optional(row, "volume")?.unwrap_or(0.0);
    let notional_volume: f64 = decode_optional(row, "notional_volume")?.unwrap_or(0.0);
    let fill_count: i64 = decode_required_count(row, "fill_count")?;
    let maker_count: i64 = decode_required_count(row, "maker_count")?;
    let taker_count: i64 = decode_required_count(row, "taker_count")?;

    Ok(proto::CoinSummary {
        coin,
        volume,
        notional_volume,
        fill_count,
        maker_count,
        taker_count,
        maker_taker_ratio: compute_maker_taker_ratio(maker_count, taker_count),
    })
}

fn compute_maker_taker_ratio(maker_count: i64, taker_count: i64) -> f64 {
    if taker_count == 0 {
        0.0
    } else {
        maker_count as f64 / taker_count as f64
    }
}

fn compute_vwap(notional: f64, volume: f64) -> f64 {
    if volume == 0.0 {
        0.0
    } else {
        notional / volume
    }
}

fn row_to_vwap_point(row: &Row) -> Result<proto::VwapPoint, Status> {
    let time: NaiveDateTime = decode_required(row, "time")?;
    let notional: f64 = decode_optional(row, "notional")?.unwrap_or(0.0);
    let volume: f64 = decode_optional(row, "volume")?.unwrap_or(0.0);
    let fill_count: i64 = decode_required_count(row, "fill_count")?;

    Ok(proto::VwapPoint {
        time: Some(naive_to_timestamp(time)),
        vwap: compute_vwap(notional, volume),
        volume,
        notional,
        fill_count,
    })
}

fn row_to_time_bar(row: &Row) -> Result<proto::TimeBar, Status> {
    let time: NaiveDateTime = decode_required(row, "time")?;
    let open: f64 = decode_required(row, "open")?;
    let high: f64 = decode_required(row, "high")?;
    let low: f64 = decode_required(row, "low")?;
    let close: f64 = decode_required(row, "close")?;
    let base_volume: f64 = decode_optional(row, "base_volume")?.unwrap_or(0.0);
    let notional_volume: f64 = decode_optional(row, "notional_volume")?.unwrap_or(0.0);
    let trade_count: i64 = decode_required_count(row, "trade_count")?;
    let buy_volume: f64 = decode_optional(row, "buy_volume")?.unwrap_or(0.0);
    let sell_volume: f64 = decode_optional(row, "sell_volume")?.unwrap_or(0.0);

    Ok(proto::TimeBar {
        time: Some(naive_to_timestamp(time)),
        open,
        high,
        low,
        close,
        base_volume,
        notional_volume,
        trade_count,
        buy_volume,
        sell_volume,
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
        let hash = normalize_required_string(" 0xabc123 ".to_string(), "hash")
            .expect("hash should normalize");
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
    fn list_coins_sql_is_issue_22_exact() {
        assert_eq!(
            LIST_COINS_SQL,
            "SELECT coin, count() AS fill_count FROM fills GROUP BY coin ORDER BY fill_count DESC"
        );
    }

    #[test]
    fn i64_or_i32_as_i64_prefers_i64_value() {
        let value = i64_or_i32_as_i64::<&str>(Ok(42_i64), || Ok(7_i32))
            .expect("i64 value should be preferred");
        assert_eq!(value, 42);
    }

    #[test]
    fn i64_or_i32_as_i64_falls_back_to_i32_value() {
        let value = i64_or_i32_as_i64::<&str>(Err("i64 decode failed"), || Ok(7_i32))
            .expect("i32 fallback should decode");
        assert_eq!(value, 7);
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
    fn parse_interval_maps_enum_values() {
        assert_eq!(
            parse_interval(INTERVAL_1S).expect("1s should parse"),
            SampleByInterval::S1
        );
        assert_eq!(
            parse_interval(INTERVAL_5M).expect("5m should parse"),
            SampleByInterval::M5
        );
        assert_eq!(
            parse_interval(INTERVAL_1D).expect("1d should parse"),
            SampleByInterval::D1
        );
    }

    #[test]
    fn parse_interval_rejects_unspecified_and_unknown_values() {
        let unspecified =
            parse_interval(INTERVAL_UNSPECIFIED).expect_err("unspecified should fail");
        assert_eq!(unspecified.code(), tonic::Code::InvalidArgument);

        let unknown = parse_interval(99).expect_err("unknown should fail");
        assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_vwap_request_rejects_blank_coin() {
        let request = proto::GetVwapRequest {
            coin: "   ".to_string(),
            interval: INTERVAL_1M,
            start_time: Some(ts(100, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_vwap_request(request).expect_err("blank coin should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_vwap_request_rejects_invalid_time_order() {
        let request = proto::GetVwapRequest {
            coin: "BTC".to_string(),
            interval: INTERVAL_5M,
            start_time: Some(ts(200, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_vwap_request(request).expect_err("start == end should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_vwap_request_accepts_valid_payload() {
        let request = proto::GetVwapRequest {
            coin: " BTC ".to_string(),
            interval: INTERVAL_30S,
            start_time: Some(ts(1_737_600_000, 0)),
            end_time: Some(ts(1_737_600_100, 0)),
        };

        let parsed = parse_get_vwap_request(request).expect("request should parse");
        assert_eq!(parsed.coin, "BTC");
        assert_eq!(parsed.interval, SampleByInterval::S30);
        assert!(parsed.start_time < parsed.end_time);
    }

    #[test]
    fn parse_get_time_bars_request_rejects_blank_coin() {
        let request = proto::GetTimeBarsRequest {
            coin: "   ".to_string(),
            interval: INTERVAL_1M,
            start_time: Some(ts(100, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_time_bars_request(request).expect_err("blank coin should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_time_bars_request_rejects_invalid_time_order() {
        let request = proto::GetTimeBarsRequest {
            coin: "BTC".to_string(),
            interval: INTERVAL_5M,
            start_time: Some(ts(200, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_time_bars_request(request).expect_err("start == end should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_time_bars_request_accepts_valid_payload() {
        let request = proto::GetTimeBarsRequest {
            coin: " ETH ".to_string(),
            interval: INTERVAL_5S,
            start_time: Some(ts(1_737_600_000, 0)),
            end_time: Some(ts(1_737_600_300, 0)),
        };

        let parsed = parse_get_time_bars_request(request).expect("request should parse");
        assert_eq!(parsed.coin, "ETH");
        assert_eq!(parsed.interval, SampleByInterval::S5);
        assert!(parsed.start_time < parsed.end_time);
    }

    #[test]
    fn parse_get_wallet_summary_request_rejects_blank_wallet() {
        let request = proto::GetWalletSummaryRequest {
            wallet: "   ".to_string(),
            start_time: Some(ts(100, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_wallet_summary_request(request).expect_err("blank wallet should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_wallet_summary_request_rejects_invalid_time_order() {
        let request = proto::GetWalletSummaryRequest {
            wallet: "0xabc".to_string(),
            start_time: Some(ts(200, 0)),
            end_time: Some(ts(200, 0)),
        };

        let err = parse_get_wallet_summary_request(request).expect_err("start == end should fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn parse_get_wallet_summary_request_accepts_valid_payload() {
        let request = proto::GetWalletSummaryRequest {
            wallet: " 0xabc ".to_string(),
            start_time: Some(ts(1_737_600_000, 0)),
            end_time: Some(ts(1_737_600_100, 0)),
        };

        let parsed = parse_get_wallet_summary_request(request).expect("request should parse");
        assert_eq!(parsed.wallet, "0xabc");
        assert!(parsed.start_time < parsed.end_time);
    }

    #[test]
    fn build_get_wallet_summary_per_coin_sql_has_expected_clauses() {
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("sum(sz) AS volume"));
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("sum(px * sz) AS notional_volume"));
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("count() AS fill_count"));
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("crossed = false"));
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("crossed = true"));
        assert!(
            GET_WALLET_SUMMARY_PER_COIN_SQL.contains("time >= $1 AND time < $2 AND address = $3")
        );
        assert!(GET_WALLET_SUMMARY_PER_COIN_SQL.contains("GROUP BY coin"));
    }

    #[test]
    fn build_get_wallet_summary_time_bounds_sql_has_expected_clauses() {
        assert!(GET_WALLET_SUMMARY_TIME_BOUNDS_SQL.contains("min(time) AS first_fill_time"));
        assert!(GET_WALLET_SUMMARY_TIME_BOUNDS_SQL.contains("max(time) AS last_fill_time"));
        assert!(GET_WALLET_SUMMARY_TIME_BOUNDS_SQL
            .contains("time >= $1 AND time < $2 AND address = $3"));
    }

    #[test]
    fn build_get_vwap_sql_includes_sample_by_and_ordering() {
        let sql = build_get_vwap_sql(SampleByInterval::M15);
        assert!(sql.contains("time >= $1 AND time < $2 AND coin = $3"));
        assert!(sql.contains("sum(px * sz) AS notional"));
        assert!(sql.contains("sum(sz) AS volume"));
        assert!(sql.contains("count() AS fill_count"));
        assert!(sql.contains("SAMPLE BY 15m"));
        assert!(sql.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn build_get_time_bars_sql_includes_sample_by_and_aggressor_logic() {
        let sql = build_get_time_bars_sql(SampleByInterval::S30);
        assert!(sql.contains("time >= $1 AND time < $2 AND coin = $3"));
        assert!(sql.contains("AND crossed = true"));
        assert!(sql.contains("first(px) AS open"));
        assert!(sql.contains("max(px) AS high"));
        assert!(sql.contains("min(px) AS low"));
        assert!(sql.contains("last(px) AS close"));
        assert!(sql.contains("sum(sz) AS base_volume"));
        assert!(sql.contains("sum(px * sz) AS notional_volume"));
        assert!(sql.contains("count() AS trade_count"));
        assert!(sql.contains("CASE WHEN is_buy = true THEN sz ELSE 0.0 END"));
        assert!(sql.contains("CASE WHEN is_buy = false THEN sz ELSE 0.0 END"));
        assert!(sql.contains("SAMPLE BY 30s"));
        assert!(sql.ends_with("ORDER BY time ASC"));
    }

    #[test]
    fn compute_vwap_guards_division_by_zero() {
        assert_eq!(compute_vwap(10.0, 0.0), 0.0);
    }

    #[test]
    fn compute_maker_taker_ratio_guards_division_by_zero() {
        assert_eq!(compute_maker_taker_ratio(10, 0), 0.0);
    }

    #[test]
    fn compute_maker_taker_ratio_matches_manual_division() {
        assert_eq!(compute_maker_taker_ratio(9, 3), 3.0);
    }

    #[test]
    fn compute_vwap_matches_manual_division() {
        assert_eq!(compute_vwap(105.0, 2.0), 52.5);
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
