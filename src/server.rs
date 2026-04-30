use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;

pub struct BridgeServer {
    cli: Cli,
}

impl BridgeServer {
    pub fn new(cli: Cli) -> Self {
        Self { cli }
    }

    pub async fn start(self) -> Result<()> {
        // Maximize file descriptor limit for better performance
        rustuya::runtime::maximize_fd_limit()?;

        let (ctx, mqtt_tx_rx, save_rx, refresh_rx) = BridgeContext::new(&self.cli).await;

        let cancel = tokio_util::sync::CancellationToken::new();

        // Start background services
        let state_saver_handle = ctx.clone().spawn_state_saver(save_rx, cancel.clone());
        let listener_handle = ctx
            .clone()
            .spawn_device_listener(refresh_rx, cancel.clone());
        let mqtt_handle = ctx.clone().spawn_mqtt_task(&self.cli, mqtt_tx_rx)?;

        // Publish current running config
        ctx.publish_bridge_config(&self.cli, false).await;

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
        cancel.cancel();

        // Publish clearing message and gracefully flush/close MQTT
        ctx.publish_bridge_config(&self.cli, true).await;
        ctx.shutdown_mqtt().await;

        let _ = tokio::join!(
            async {
                if let Some(handle) = mqtt_handle {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), handle).await;
                }
            },
            async {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(3), listener_handle).await;
            },
            async {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(3), state_saver_handle)
                    .await;
            }
        );

        let _ = ctx.save_state().await;
        Ok(())
    }
}
