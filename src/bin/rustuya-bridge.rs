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

    // The topic/template/retain settings are config-file/`set_config` only;
    // warn (after logger init) about any now-ignored env so a migrating operator
    // sees a loud no-op rather than a silent one.
    for name in rustuyabridge::config::ignored_topic_env_overrides() {
        log::warn!(
            "Environment variable {name} is set but no longer honored \
             (topic/template/retain settings are managed via the config file or \
             the `set_config` command). Move it into the config file."
        );
    }

    // Start the server
    let mut server = BridgeServer::new(cli);
    server.setup().await?;
    server.run().await
}
