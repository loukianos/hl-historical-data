use crate::config::Config;
use crate::grpc::proto;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct QueryService {
    _config: Config,
}

impl QueryService {
    pub fn new(config: Config) -> Self {
        Self { _config: config }
    }
}

#[tonic::async_trait]
impl proto::historical_data_service_server::HistoricalDataService for QueryService {
    type GetFillsStream = ReceiverStream<Result<proto::GetFillsResponse, Status>>;

    async fn get_fills(
        &self,
        _request: Request<proto::GetFillsRequest>,
    ) -> Result<Response<Self::GetFillsStream>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("GetFills not yet implemented"))
    }

    async fn get_fill_by_hash(
        &self,
        _request: Request<proto::GetFillByHashRequest>,
    ) -> Result<Response<proto::GetFillByHashResponse>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("GetFillByHash not yet implemented"))
    }

    type GetFillsByOidStream = ReceiverStream<Result<proto::GetFillsByOidResponse, Status>>;

    async fn get_fills_by_oid(
        &self,
        _request: Request<proto::GetFillsByOidRequest>,
    ) -> Result<Response<Self::GetFillsByOidStream>, Status> {
        // TODO: implement query
        Err(Status::unimplemented("GetFillsByOid not yet implemented"))
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
