mod bridge;
mod config;
mod error;
mod handlers;
mod types;

use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::load().await?;

    // Initialize logger from config (CLI/Env/JSON)
    let log_level = cli.log_level.as_deref().unwrap_or("info");
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    // Maximize file descriptor limit for better performance
    rustuya::runtime::maximize_fd_limit()?;

    let (ctx, mqtt_tx_rx, save_rx, refresh_rx) = BridgeContext::new(&cli).await;

    // Start background services
    ctx.clone().spawn_state_saver(save_rx);
    ctx.clone().spawn_device_listener(refresh_rx);
    ctx.clone().spawn_mqtt_task(&cli, mqtt_tx_rx)?;

    info!("Bridge running. Press Ctrl+C to stop.");

    // Wait for termination signal
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigint.recv() => info!("Received SIGINT"),
            _ = sigterm.recv() => info!("Received SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }

    info!("Shutting down...");
    let _ = ctx.save_state().await;
    Ok(())
}
