use crate::backfill::status;
use crate::config::Config;
use crate::db::QuestDbReader;
use anyhow::Result;
use tonic::transport::Server;

pub mod proto {
    tonic::include_proto!("hl_historical");
}

pub async fn run(config: Config) -> Result<()> {
    let addr = format!("{}:{}", config.grpc.host, config.grpc.port).parse()?;

    // Ensure QuestDB is reachable and tables exist
    let reader = QuestDbReader::new(&config.questdb);
    reader.ping().await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to connect to QuestDB at {}:{} - is it running? Error: {}",
            config.questdb.pg_host,
            config.questdb.pg_port,
            e
        )
    })?;
    reader.ensure_tables().await?;

    let backfill_status = status::new_shared();

    let query_service = super::queries::QueryService::new(config.clone());
    let admin_service = super::admin::AdminService::new(config.clone(), backfill_status);

    tracing::info!("gRPC server listening on {}", addr);

    Server::builder()
        .add_service(
            proto::historical_data_service_server::HistoricalDataServiceServer::new(query_service),
        )
        .add_service(
            proto::historical_data_admin_service_server::HistoricalDataAdminServiceServer::new(
                admin_service,
            ),
        )
        .serve(addr)
        .await?;

    Ok(())
}
