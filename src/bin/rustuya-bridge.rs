use anyhow::Result;
use rustuyabridge::config::Cli;
use rustuyabridge::server::BridgeServer;
use std::io::IsTerminal as _;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::load().await?;

    // Initialize logger from config (CLI/Env/JSON)
    let log_level = cli
        .log_level
        .as_deref()
        .unwrap_or(rustuyabridge::config::DEFAULT_LOG_LEVEL);
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level));
    // Under systemd/journald (or any redirected stderr) the host already
    // stamps each line with its own (local) timestamp; env_logger's UTC
    // prefix would just duplicate it. Keep the timestamp only when stderr
    // is an interactive terminal.
    if !std::io::stderr().is_terminal() {
        builder.format_timestamp(None);
    }
    builder.init();

    // Start the server
    let mut server = BridgeServer::new(cli);
    server.setup().await?;
    server.run().await
}
