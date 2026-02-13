use crate::backfill::status::SharedBackfillStatus;
use crate::config::Config;
use crate::grpc::proto;
use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

pub struct AdminService {
    _config: Config,
    backfill_status: SharedBackfillStatus,
}

impl AdminService {
    pub fn new(config: Config, backfill_status: SharedBackfillStatus) -> Self {
        Self {
            _config: config,
            backfill_status,
        }
    }
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
        _request: Request<proto::TriggerBackfillRequest>,
    ) -> Result<Response<proto::TriggerBackfillResponse>, Status> {
        // TODO: implement
        Err(Status::unimplemented("TriggerBackfill not yet implemented"))
    }

    async fn sync_to_present(
        &self,
        _request: Request<proto::SyncToPresentRequest>,
    ) -> Result<Response<proto::SyncToPresentResponse>, Status> {
        // TODO: implement
        Err(Status::unimplemented("SyncToPresent not yet implemented"))
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
    use tonic::Request;

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
}
