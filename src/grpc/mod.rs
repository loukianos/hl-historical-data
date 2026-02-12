pub mod admin;
pub mod admin_cli;
pub mod proto;
pub mod queries;
pub mod server;

use crate::config::Config;
use anyhow::Result;

pub async fn serve(config: Config) -> Result<()> {
    server::run(config).await
}
