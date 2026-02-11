use crate::backfill::status::SharedBackfillStatus;
use crate::config::Config;
use crate::grpc::server::proto;
use tonic::{Request, Response, Status};

pub struct AdminService {
    _config: Config,
    _backfill_status: SharedBackfillStatus,
}

impl AdminService {
    pub fn new(config: Config, backfill_status: SharedBackfillStatus) -> Self {
        Self {
            _config: config,
            _backfill_status: backfill_status,
        }
    }
}

#[tonic::async_trait]
impl proto::historical_data_admin_service_server::HistoricalDataAdminService for AdminService {
    async fn get_ingestion_status(
        &self,
        _request: Request<proto::GetIngestionStatusRequest>,
    ) -> Result<Response<proto::GetIngestionStatusResponse>, Status> {
        // TODO: implement
        Err(Status::unimplemented(
            "GetIngestionStatus not yet implemented",
        ))
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
