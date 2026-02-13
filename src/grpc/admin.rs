use crate::backfill::status::{BackfillState, SharedBackfillStatus};
use crate::config::Config;
use crate::grpc::proto;
use chrono::{DateTime, NaiveDate, Utc};
use prost_types::Timestamp;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};

const DATE_FORMAT: &str = "%Y%m%d";

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
        // TODO: implement
        Err(Status::unimplemented("GetDbStats not yet implemented"))
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
}
