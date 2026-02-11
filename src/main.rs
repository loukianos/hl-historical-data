use clap::{Parser, Subcommand};

mod backfill;
mod config;
mod db;
mod grpc;

#[derive(Parser)]
#[command(name = "hl-historical-data")]
#[command(about = "Hyperliquid historical fill data service")]
#[command(version)]
struct Cli {
    /// Path to TOML configuration file
    #[arg(short, long, global = true, default_value = "config.toml")]
    config: String,

    /// Override log filter (example: info,debug,trace)
    #[arg(long, global = true)]
    log_level: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the gRPC query server
    Serve,

    /// Backfill historical data from S3
    Backfill {
        /// Sync from last ingested hour to present
        #[arg(long, conflicts_with_all = ["from", "to"])]
        sync: bool,

        /// Start date (YYYYMMDD)
        #[arg(long, required_unless_present = "sync")]
        from: Option<String>,

        /// End date (YYYYMMDD), inclusive
        #[arg(long, required_unless_present = "sync")]
        to: Option<String>,
    },

    /// Admin operations (connects to running server)
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
}

#[derive(Subcommand)]
enum AdminAction {
    /// Show current ingestion status
    Status,
    /// Sync to present (trigger via gRPC)
    Sync,
    /// Trigger backfill for a date range (via gRPC)
    Backfill {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    /// Show database statistics
    Stats,
    /// Purge data before a date
    Purge {
        /// Delete data before this date (YYYYMMDD)
        #[arg(long)]
        before: String,
    },
    /// Re-index database tables
    Reindex,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let env_filter = match &cli.log_level {
        Some(level) => tracing_subscriber::EnvFilter::try_new(level)?,
        None => tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    };

    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let cfg = config::load(&cli.config)?;

    match cli.command {
        Commands::Serve => {
            tracing::info!(
                "Starting gRPC server on {}:{}",
                cfg.grpc.host,
                cfg.grpc.port
            );
            grpc::serve(cfg).await?;
        }
        Commands::Backfill { sync, from, to } => {
            if sync {
                tracing::info!("Syncing to present...");
                backfill::sync_to_present(&cfg).await?;
            } else {
                let from = from.expect("--from required when not using --sync");
                let to = to.expect("--to required when not using --sync");
                tracing::info!("Backfilling from {} to {}", from, to);
                backfill::run(&cfg, &from, &to).await?;
            }
        }
        Commands::Admin { action } => {
            grpc::admin_cli::run(action, &cfg).await?;
        }
    }

    Ok(())
}
