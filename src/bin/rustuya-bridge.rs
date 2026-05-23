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
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level));
    // Drop env_logger's UTC timestamp prefix when systemd/journald is
    // already stamping each line (it sets $JOURNAL_STREAM on the inherited
    // stderr — see systemd.exec(5)). Docker / piped stderr / terminals
    // don't set it, so the timestamp is preserved for those cases.
    if std::env::var_os("JOURNAL_STREAM").is_some() {
        builder.format_timestamp(None);
    }
    builder.init();

    // Start the server
    let mut server = BridgeServer::new(cli);
    server.setup().await?;
    server.run().await
}
