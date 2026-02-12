use crate::backfill::status;
use crate::config::Config;
use crate::db::QuestDbReader;
use crate::grpc::proto;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use tonic::transport::Server;

pub async fn run(config: Config) -> Result<()> {
    let addr = parse_grpc_bind_addr(&config.grpc.host, config.grpc.port)?;

    // Ensure QuestDB is reachable and tables exist
    let reader = QuestDbReader::new(&config.questdb);
    reader.ping().await.with_context(|| {
        format!(
            "Failed to connect to QuestDB at {}:{} - is it running?",
            config.questdb.pg_host, config.questdb.pg_port
        )
    })?;
    reader.ensure_tables().await.with_context(|| {
        format!(
            "Connected to QuestDB but failed to ensure tables at {}:{}",
            config.questdb.pg_host, config.questdb.pg_port
        )
    })?;

    let backfill_status = status::new_shared();

    let query_service = super::queries::QueryService::new(config.clone());
    let admin_service = super::admin::AdminService::new(config.clone(), backfill_status);

    tracing::info!("Starting gRPC server on {}", addr);

    Server::builder()
        .add_service(
            proto::historical_data_service_server::HistoricalDataServiceServer::new(query_service),
        )
        .add_service(
            proto::historical_data_admin_service_server::HistoricalDataAdminServiceServer::new(
                admin_service,
            ),
        )
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .with_context(|| format!("gRPC server failed on {}", addr))?;

    Ok(())
}

fn parse_grpc_bind_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let bind_addr = format!("{host}:{port}");
    bind_addr.parse::<SocketAddr>().with_context(|| {
        format!(
            "Invalid gRPC bind address '{bind_addr}'. Set grpc.host to an IP literal (for example 127.0.0.1 or [::1]) and grpc.port to a valid TCP port"
        )
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::warn!("Failed to install SIGINT handler: {}", err);
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::warn!("Failed to install SIGTERM handler: {}", err);
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, starting graceful shutdown"),
        _ = terminate => tracing::info!("Received SIGTERM, starting graceful shutdown"),
    }
}
