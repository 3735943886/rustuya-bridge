use anyhow::Result;
use rustuyabridge::config::Cli;
use rustuyabridge::server::BridgeServer;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::load().await?;

    // Initialize logger from config (CLI/Env/JSON)
    let log_level = cli
        .log_level
        .as_deref()
        .unwrap_or(rustuyabridge::config::DEFAULT_LOG_LEVEL);
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    // Start the server
    let server = BridgeServer::new(cli);
    server.start().await
}
