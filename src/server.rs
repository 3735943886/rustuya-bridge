use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;

use std::sync::Arc;

pub struct BridgeServer {
    cli: Cli,
    ctx: Option<Arc<BridgeContext>>,
}

impl BridgeServer {
    pub fn new(cli: Cli) -> Self {
        Self { cli, ctx: None }
    }

    pub async fn setup(&mut self) -> Result<Arc<BridgeContext>> {
        // Maximize file descriptor limit for better performance
        rustuya::runtime::maximize_fd_limit()?;

        let (ctx, mqtt_tx_rx, save_rx, refresh_rx) = BridgeContext::new(&self.cli).await;

        // Start background services
        ctx.clone().spawn_state_saver(save_rx, ctx.cancel.clone());
        ctx.clone()
            .spawn_device_listener(refresh_rx, ctx.cancel.clone());
        ctx.clone().spawn_mqtt_task(&self.cli, mqtt_tx_rx)?;

        // Publish current running config
        ctx.publish_bridge_config(Some(&self.cli), false).await;

        self.ctx = Some(ctx.clone());
        Ok(ctx)
    }

    pub async fn run(&self) -> Result<()> {
        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Server not setup"))?;

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
        ctx.close().await;

        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(ctx) = &self.ctx {
            ctx.close().await;
        }
        Ok(())
    }
}
