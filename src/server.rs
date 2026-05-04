use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;

use std::sync::Arc;

pub struct BridgeServer {
    cli: Cli,
    ctx: Option<Arc<BridgeContext>>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BridgeServer {
    pub fn new(cli: Cli) -> Self {
        Self {
            cli,
            ctx: None,
            handles: Vec::new(),
        }
    }

    pub async fn setup(&mut self) -> Result<Arc<BridgeContext>> {
        // Maximize file descriptor limit for better performance
        rustuya::runtime::maximize_fd_limit()?;

        let (ctx, mqtt_tx_rx, save_rx, refresh_rx) = BridgeContext::new(&self.cli).await;

        // Start background services
        let h1 = ctx.clone().spawn_state_saver(save_rx, ctx.cancel.clone());
        let h2 = ctx
            .clone()
            .spawn_device_listener(refresh_rx, ctx.cancel.clone());
        let h3 = ctx.clone().spawn_mqtt_task(&self.cli, mqtt_tx_rx)?;

        // Publish current running config
        ctx.publish_bridge_config(Some(&self.cli), false).await;

        self.ctx = Some(ctx.clone());
        self.handles.push(h1);
        self.handles.push(h2);
        if let Some(h) = h3 {
            self.handles.push(h);
        }
        Ok(ctx)
    }

    pub async fn run(&mut self) -> Result<()> {
        self.ctx
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
        // close() signals all tasks and waits for them to fully complete
        self.close().await
    }

    pub async fn close(&mut self) -> Result<()> {
        // Signal all background tasks (cancel + mqtt shutdown)
        if let Some(ctx) = &self.ctx {
            ctx.close().await;
        }

        // Wait for all tasks to finish (including MQTT cleanup + PubAck flush)
        for handle in self.handles.drain(..) {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), handle).await;
        }

        Ok(())
    }
}
