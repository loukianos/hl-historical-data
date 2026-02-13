use crate::backfill::status::{BackfillState, SharedBackfillStatus};
use crate::config::Config;
use crate::db::QuestDbReader;
use crate::grpc::proto;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use prost_types::Timestamp;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Error as PgError, Row};
use tonic::{Request, Response, Status};

const DATE_FORMAT: &str = "%Y%m%d";
const DB_STATS_TIME_BOUNDS_SQL: &str =
    "SELECT min(time) AS min_time, max(time) AS max_time FROM fills";
const DB_STATS_QUARANTINE_COUNT_SQL: &str =
    "SELECT count() AS quarantine_count FROM fills_quarantine";
const DB_STATS_SHOW_PARTITIONS_FILLS_SQL: &str = "SHOW PARTITIONS FROM fills";
const DB_STATS_TABLE_PARTITIONS_FILLS_SQL: &str = "SELECT * FROM table_partitions('fills')";
const DB_STATS_FILLS_COUNT_SQL: &str = "SELECT count() AS fills_count FROM fills";

pub struct AdminService {
    config: Config,
    backfill_status: SharedBackfillStatus,
    ingestion_gate: Arc<Semaphore>,
}

impl AdminService {
    pub fn new(config: Config, backfill_status: SharedBackfillStatus) -> Self {
        Self {
            config,
            backfill_status,
            ingestion_gate: Arc::new(Semaphore::new(1)),
        }
    }

    async fn try_start_ingestion(&self) -> Result<OwnedSemaphorePermit, Status> {
        match self.ingestion_gate.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(_) => Err(Status::failed_precondition(
                self.already_running_message().await,
            )),
        }
    }

    async fn already_running_message(&self) -> String {
        let status = self.backfill_status.read().await;

        if status.state == BackfillState::Running {
            let current_hour = status.current_hour.as_deref().unwrap_or("unknown");
            format!(
                "backfill already running (current_hour={}, progress={}/{})",
                current_hour, status.hours_done, status.hours_total
            )
        } else {
            "backfill already running".to_string()
        }
    }
}

fn parse_yyyymmdd(value: &str, field_name: &str) -> Result<NaiveDate, Status> {
    NaiveDate::parse_from_str(value, DATE_FORMAT)
        .map_err(|_| Status::invalid_argument(format!("{} must be in YYYYMMDD format", field_name)))
}

fn validate_backfill_dates(from: &str, to: &str) -> Result<(), Status> {
    let from_date = parse_yyyymmdd(from, "from_date")?;
    let to_date = parse_yyyymmdd(to, "to_date")?;

    if to_date < from_date {
        return Err(Status::invalid_argument(
            "to_date must be greater than or equal to from_date",
        ));
    }

    Ok(())
}

fn datetime_to_timestamp(datetime: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

fn naive_to_timestamp(datetime: NaiveDateTime) -> Timestamp {
    let datetime = datetime.and_utc();
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

fn classify_db_error(operation: &'static str, err: &anyhow::Error) -> Status {
    if err
        .downcast_ref::<PgError>()
        .and_then(|pg_err| pg_err.as_db_error())
        .is_some()
    {
        return Status::internal(format!("{operation} failed due to a database error"));
    }

    Status::unavailable(format!("QuestDB unavailable while executing {operation}"))
}

fn map_db_error(operation: &'static str, err: anyhow::Error) -> Status {
    tracing::error!(operation, error = %err, "admin db stats query failed");
    classify_db_error(operation, &err)
}

fn decode_optional_naive_datetime(
    row: &Row,
    column: &'static str,
) -> Result<Option<NaiveDateTime>, Status> {
    row.try_get::<_, Option<NaiveDateTime>>(column)
        .map_err(|err| {
            tracing::error!(column, error = %err, "failed to decode optional timestamp column");
            Status::internal(format!("Failed to decode row column '{column}'"))
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

fn decode_required_count_at(row: &Row, index: usize, column: &'static str) -> Result<i64, Status> {
    match i64_or_i32_as_i64(row.try_get::<_, i64>(index), || {
        row.try_get::<_, i32>(index)
    }) {
        Ok(value) => Ok(value),
        Err(i64_or_i32_err) => {
            let value: f64 = row.try_get(index).map_err(|f64_err| {
                tracing::error!(
                    column,
                    index,
                    i64_or_i32_error = %i64_or_i32_err,
                    f64_error = %f64_err,
                    "failed to decode required count column at index"
                );
                Status::internal(format!("Failed to decode row column '{column}'"))
            })?;

            if !value.is_finite() || value.fract() != 0.0 {
                tracing::error!(
                    column,
                    index,
                    value,
                    "count column decoded as non-integral floating-point value"
                );
                return Err(Status::internal(format!(
                    "Failed to decode row column '{column}'"
                )));
            }

            if value < i64::MIN as f64 || value > i64::MAX as f64 {
                tracing::error!(
                    column,
                    index,
                    value,
                    "count column value is out of i64 range"
                );
                return Err(Status::internal(format!(
                    "Failed to decode row column '{column}'"
                )));
            }

            Ok(value as i64)
        }
    }
}

fn find_col_index(row: &Row, candidates: &[&str]) -> Option<usize> {
    row.columns().iter().position(|column| {
        candidates
            .iter()
            .any(|candidate| column.name().eq_ignore_ascii_case(candidate))
    })
}

fn decode_partition_date(row: &Row, index: usize) -> Result<String, Status> {
    if let Ok(value) = row.try_get::<_, String>(index) {
        return Ok(value);
    }

    if let Ok(value) = row.try_get::<_, Option<String>>(index) {
        if let Some(value) = value {
            return Ok(value);
        }
    }

    if let Ok(value) = row.try_get::<_, NaiveDate>(index) {
        return Ok(value.format(DATE_FORMAT).to_string());
    }

    if let Ok(value) = row.try_get::<_, Option<NaiveDate>>(index) {
        if let Some(value) = value {
            return Ok(value.format(DATE_FORMAT).to_string());
        }
    }

    if let Ok(value) = row.try_get::<_, NaiveDateTime>(index) {
        return Ok(value.date().format(DATE_FORMAT).to_string());
    }

    if let Ok(value) = row.try_get::<_, Option<NaiveDateTime>>(index) {
        if let Some(value) = value {
            return Ok(value.date().format(DATE_FORMAT).to_string());
        }
    }

    Err(Status::internal("Failed to decode partition date value"))
}

fn normalize_partition_date(raw: &str) -> String {
    let trimmed = raw.trim();

    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        return date.format(DATE_FORMAT).to_string();
    }

    if let Ok(date) = NaiveDate::parse_from_str(trimmed, DATE_FORMAT) {
        return date.format(DATE_FORMAT).to_string();
    }

    trimmed.to_string()
}

fn parse_partition_rows(rows: &[Row]) -> Result<(i64, Vec<proto::PartitionInfo>), Status> {
    if rows.is_empty() {
        return Ok((0, Vec::new()));
    }

    let date_col = find_col_index(
        &rows[0],
        &[
            "name",
            "partition",
            "partition_name",
            "partitionName",
            "minTimestamp",
            "min_timestamp",
            "minTime",
            "min_time",
            "timestamp",
            "ts",
        ],
    )
    .ok_or_else(|| {
        Status::internal("Partition metadata result missing a partition date/name column")
    })?;

    let row_count_col = find_col_index(
        &rows[0],
        &["numRows", "num_rows", "rows", "rowCount", "row_count"],
    )
    .ok_or_else(|| Status::internal("Partition metadata result missing a row-count column"))?;

    let mut fills_count = 0_i64;
    let mut partitions = Vec::with_capacity(rows.len());

    for row in rows {
        let date = normalize_partition_date(&decode_partition_date(row, date_col)?);
        let row_count = decode_required_count_at(row, row_count_col, "partition_row_count")?;

        if row_count < 0 {
            return Err(Status::internal(
                "Partition metadata row count cannot be negative",
            ));
        }

        fills_count = fills_count.checked_add(row_count).ok_or_else(|| {
            Status::internal("fills_count overflow while summing partition row counts")
        })?;

        partitions.push(proto::PartitionInfo { date, row_count });
    }

    Ok((fills_count, partitions))
}

async fn try_fetch_fills_partitions(
    reader: &QuestDbReader,
) -> Result<(i64, Vec<proto::PartitionInfo>), Status> {
    let strategies = [
        ("SHOW PARTITIONS", DB_STATS_SHOW_PARTITIONS_FILLS_SQL),
        ("table_partitions", DB_STATS_TABLE_PARTITIONS_FILLS_SQL),
    ];

    let mut last_error = Status::internal("partition metadata unavailable");

    for (strategy, sql) in strategies {
        match reader.query(sql, &[]).await {
            Ok(rows) => match parse_partition_rows(&rows) {
                Ok(parsed) => return Ok(parsed),
                Err(err) => {
                    tracing::warn!(
                        strategy,
                        error = %err,
                        "failed to parse partition metadata query result"
                    );
                    last_error = Status::internal(format!(
                        "{strategy} returned an unsupported partition metadata schema"
                    ));
                }
            },
            Err(err) => {
                tracing::warn!(strategy, error = %err, "partition metadata query failed");
                last_error = classify_db_error("GetDbStats(partitions)", &err);
            }
        }
    }

    Err(last_error)
}

#[tonic::async_trait]
impl proto::historical_data_admin_service_server::HistoricalDataAdminService for AdminService {
    async fn get_ingestion_status(
        &self,
        _request: Request<proto::GetIngestionStatusRequest>,
    ) -> Result<Response<proto::GetIngestionStatusResponse>, Status> {
        let status = self.backfill_status.read().await;
        let response = proto::GetIngestionStatusResponse {
            state: status.state_str().to_string(),
            started_at: status.started_at.map(datetime_to_timestamp),
            last_updated_at: status.last_updated_at.map(datetime_to_timestamp),
            finished_at: status.finished_at.map(datetime_to_timestamp),
            current_hour: status.current_hour.clone().unwrap_or_default(),
            hours_done: status.hours_done,
            hours_total: status.hours_total,
            rows_inserted: status.rows_inserted,
            rows_quarantined: status.rows_quarantined,
            files_missing: status.files_missing,
            files_failed: status.files_failed,
        };

        Ok(Response::new(response))
    }

    async fn trigger_backfill(
        &self,
        request: Request<proto::TriggerBackfillRequest>,
    ) -> Result<Response<proto::TriggerBackfillResponse>, Status> {
        let request = request.into_inner();
        let from = request.from_date.trim().to_string();
        let to = request.to_date.trim().to_string();

        validate_backfill_dates(&from, &to)?;

        let permit = self.try_start_ingestion().await?;

        let config = self.config.clone();
        let backfill_status = self.backfill_status.clone();
        let from_task = from.clone();
        let to_task = to.clone();

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) =
                crate::backfill::run_with_status(&config, backfill_status, &from_task, &to_task)
                    .await
            {
                tracing::error!(
                    error = %err,
                    from = %from_task,
                    to = %to_task,
                    "admin-triggered backfill failed"
                );
            }
        });

        Ok(Response::new(proto::TriggerBackfillResponse {
            accepted: true,
            message: format!("Backfill started for {}..={}", from, to),
        }))
    }

    async fn sync_to_present(
        &self,
        _request: Request<proto::SyncToPresentRequest>,
    ) -> Result<Response<proto::SyncToPresentResponse>, Status> {
        let permit = self.try_start_ingestion().await?;

        let config = self.config.clone();
        let backfill_status = self.backfill_status.clone();

        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) =
                crate::backfill::sync_to_present_with_status(&config, backfill_status).await
            {
                tracing::error!(error = %err, "admin-triggered sync-to-present failed");
            }
        });

        Ok(Response::new(proto::SyncToPresentResponse {
            accepted: true,
            message: "Sync to present started".to_string(),
        }))
    }

    async fn get_db_stats(
        &self,
        _request: Request<proto::GetDbStatsRequest>,
    ) -> Result<Response<proto::GetDbStatsResponse>, Status> {
        let reader = QuestDbReader::new(&self.config.questdb);

        let time_bounds_row = reader
            .query_one(DB_STATS_TIME_BOUNDS_SQL, &[])
            .await
            .map_err(|err| map_db_error("GetDbStats(time_bounds)", err))?;

        let min_time = decode_optional_naive_datetime(&time_bounds_row, "min_time")?;
        let max_time = decode_optional_naive_datetime(&time_bounds_row, "max_time")?;

        let quarantine_row = reader
            .query_one(DB_STATS_QUARANTINE_COUNT_SQL, &[])
            .await
            .map_err(|err| map_db_error("GetDbStats(quarantine_count)", err))?;
        let quarantine_count = decode_required_count(&quarantine_row, "quarantine_count")?;

        let (mut fills_count, partitions) = match try_fetch_fills_partitions(&reader).await {
            Ok(parsed) => parsed,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "partition metadata unavailable; falling back to count()"
                );
                let row = reader
                    .query_one(DB_STATS_FILLS_COUNT_SQL, &[])
                    .await
                    .map_err(|err| map_db_error("GetDbStats(fills_count)", err))?;
                (decode_required_count(&row, "fills_count")?, Vec::new())
            }
        };

        if partitions.is_empty() && max_time.is_some() {
            tracing::warn!(
                "partition metadata returned no partitions despite non-empty fills data; using count()"
            );
            let row = reader
                .query_one(DB_STATS_FILLS_COUNT_SQL, &[])
                .await
                .map_err(|err| map_db_error("GetDbStats(fills_count)", err))?;
            fills_count = decode_required_count(&row, "fills_count")?;
        }

        Ok(Response::new(proto::GetDbStatsResponse {
            fills_count,
            quarantine_count,
            min_time: min_time.map(naive_to_timestamp),
            max_time: max_time.map(naive_to_timestamp),
            partitions,
        }))
    }

    async fn purge_data(
        &self,
        _request: Request<proto::PurgeDataRequest>,
    ) -> Result<Response<proto::PurgeDataResponse>, Status> {
        // TODO: implement
        Err(Status::unimplemented("PurgeData not yet implemented"))
    }

    async fn re_index(
        &self,
        _request: Request<proto::ReIndexRequest>,
    ) -> Result<Response<proto::ReIndexResponse>, Status> {
        // TODO: implement
        Err(Status::unimplemented("ReIndex not yet implemented"))
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_partition_date;
    use super::AdminService;
    use crate::backfill::status::{new_shared, BackfillState};
    use crate::config::Config;
    use crate::grpc::proto;
    use chrono::{DateTime, Utc};
    use tonic::{Code, Request};

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("valid RFC3339 timestamp")
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn get_ingestion_status_returns_default_idle_payload() {
        let service = AdminService::new(Config::default(), new_shared());

        let response = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::get_ingestion_status(
            &service,
            Request::new(proto::GetIngestionStatusRequest {}),
        )
        .await
        .expect("status RPC should succeed")
        .into_inner();

        assert_eq!(response.state, "idle");
        assert!(response.started_at.is_none());
        assert!(response.last_updated_at.is_none());
        assert!(response.finished_at.is_none());
        assert_eq!(response.current_hour, "");
        assert_eq!(response.hours_done, 0);
        assert_eq!(response.hours_total, 0);
        assert_eq!(response.rows_inserted, 0);
        assert_eq!(response.rows_quarantined, 0);
        assert_eq!(response.files_missing, 0);
        assert_eq!(response.files_failed, 0);
    }

    #[tokio::test]
    async fn get_ingestion_status_maps_all_status_fields() {
        let shared_status = new_shared();
        let started_at = ts("2025-07-28T00:00:00Z");
        let last_updated_at = ts("2025-07-28T00:15:00Z");
        let finished_at = ts("2025-07-28T00:30:00Z");

        {
            let mut status = shared_status.write().await;
            status.state = BackfillState::Succeeded;
            status.started_at = Some(started_at);
            status.last_updated_at = Some(last_updated_at);
            status.finished_at = Some(finished_at);
            status.current_hour = Some("20250728/00".to_string());
            status.hours_done = 24;
            status.hours_total = 24;
            status.rows_inserted = 1000;
            status.rows_quarantined = 8;
            status.files_missing = 1;
            status.files_failed = 2;
        }

        let service = AdminService::new(Config::default(), shared_status);

        let response = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::get_ingestion_status(
            &service,
            Request::new(proto::GetIngestionStatusRequest {}),
        )
        .await
        .expect("status RPC should succeed")
        .into_inner();

        assert_eq!(response.state, "succeeded");
        assert_eq!(response.current_hour, "20250728/00");
        assert_eq!(response.hours_done, 24);
        assert_eq!(response.hours_total, 24);
        assert_eq!(response.rows_inserted, 1000);
        assert_eq!(response.rows_quarantined, 8);
        assert_eq!(response.files_missing, 1);
        assert_eq!(response.files_failed, 2);

        let started = response.started_at.expect("started_at should be set");
        let updated = response
            .last_updated_at
            .expect("last_updated_at should be set");
        let finished = response.finished_at.expect("finished_at should be set");

        assert_eq!(started.seconds, started_at.timestamp());
        assert_eq!(updated.seconds, last_updated_at.timestamp());
        assert_eq!(finished.seconds, finished_at.timestamp());
    }

    #[tokio::test]
    async fn trigger_backfill_rejects_invalid_date_format() {
        let service = AdminService::new(Config::default(), new_shared());

        let err = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::trigger_backfill(
            &service,
            Request::new(proto::TriggerBackfillRequest {
                from_date: "2025-07-28".to_string(),
                to_date: "20250728".to_string(),
            }),
        )
        .await
        .expect_err("invalid date format should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("from_date"));
    }

    #[tokio::test]
    async fn trigger_backfill_rejects_when_to_date_precedes_from_date() {
        let service = AdminService::new(Config::default(), new_shared());

        let err = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::trigger_backfill(
            &service,
            Request::new(proto::TriggerBackfillRequest {
                from_date: "20250728".to_string(),
                to_date: "20250727".to_string(),
            }),
        )
        .await
        .expect_err("invalid range should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("to_date"));
    }

    #[tokio::test]
    async fn trigger_backfill_rejects_when_already_running() {
        let service = AdminService::new(Config::default(), new_shared());
        let _permit = service
            .ingestion_gate
            .clone()
            .try_acquire_owned()
            .expect("gate should be acquirable in test setup");

        let err = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::trigger_backfill(
            &service,
            Request::new(proto::TriggerBackfillRequest {
                from_date: "20250727".to_string(),
                to_date: "20250728".to_string(),
            }),
        )
        .await
        .expect_err("concurrent trigger should fail");

        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("already running"));
    }

    #[tokio::test]
    async fn sync_to_present_rejects_when_already_running() {
        let service = AdminService::new(Config::default(), new_shared());
        let _permit = service
            .ingestion_gate
            .clone()
            .try_acquire_owned()
            .expect("gate should be acquirable in test setup");

        let err = <AdminService as proto::historical_data_admin_service_server::HistoricalDataAdminService>::sync_to_present(
            &service,
            Request::new(proto::SyncToPresentRequest {}),
        )
        .await
        .expect_err("concurrent sync should fail");

        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("already running"));
    }

    #[test]
    fn normalize_partition_date_compacts_yyyy_mm_dd() {
        assert_eq!(normalize_partition_date("2025-07-27"), "20250727");
    }

    #[test]
    fn normalize_partition_date_preserves_yyyymmdd() {
        assert_eq!(normalize_partition_date("20250727"), "20250727");
    }

    #[test]
    fn normalize_partition_date_passthrough_for_unknown_formats() {
        assert_eq!(normalize_partition_date("partition-xyz"), "partition-xyz");
    }
}
