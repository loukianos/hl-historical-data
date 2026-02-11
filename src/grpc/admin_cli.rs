use crate::config::Config;
use crate::grpc::server::proto;
use anyhow::Result;

pub async fn run(action: crate::AdminAction, config: &Config) -> Result<()> {
    let addr = format!("http://{}:{}", config.grpc.host, config.grpc.port);

    match action {
        crate::AdminAction::Status => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client
                .get_ingestion_status(proto::GetIngestionStatusRequest {})
                .await?;
            let status = resp.into_inner();
            println!("State: {}", status.state);
            println!("Hours: {}/{}", status.hours_done, status.hours_total);
            println!("Rows inserted: {}", status.rows_inserted);
            println!("Rows quarantined: {}", status.rows_quarantined);
        }
        crate::AdminAction::Sync => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client
                .sync_to_present(proto::SyncToPresentRequest {})
                .await?;
            println!("{}", resp.into_inner().message);
        }
        crate::AdminAction::Backfill { from, to } => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client
                .trigger_backfill(proto::TriggerBackfillRequest {
                    from_date: from,
                    to_date: to,
                })
                .await?;
            println!("{}", resp.into_inner().message);
        }
        crate::AdminAction::Stats => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client.get_db_stats(proto::GetDbStatsRequest {}).await?;
            let stats = resp.into_inner();
            println!("Fills: {}", stats.fills_count);
            println!("Quarantine: {}", stats.quarantine_count);
        }
        crate::AdminAction::Purge { before } => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client
                .purge_data(proto::PurgeDataRequest {
                    before_date: before,
                })
                .await?;
            println!("{}", resp.into_inner().message);
        }
        crate::AdminAction::Reindex => {
            let mut client =
                proto::historical_data_admin_service_client::HistoricalDataAdminServiceClient::connect(addr).await?;
            let resp = client.re_index(proto::ReIndexRequest {}).await?;
            println!("{}", resp.into_inner().message);
        }
    }

    Ok(())
}
