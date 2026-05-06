use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;

use std::sync::Arc;

pub struct BridgeServer {
    cli: Cli,
    ctx: Option<Arc<BridgeContext>>,
    /// Handles for state_saver and device_listener - aborted on close
    background_handles: Vec<tokio::task::JoinHandle<()>>,
    /// MQTT task handle - waited on close to ensure clean disconnect
    mqtt_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BridgeServer {
    pub fn new(cli: Cli) -> Self {
        Self {
            cli,
            ctx: None,
            background_handles: Vec::new(),
            mqtt_handle: None,
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
        self.background_handles.push(h1);
        self.background_handles.push(h2);
        self.mqtt_handle = h3;
        Ok(ctx)
    }

    pub async fn run(&mut self) -> Result<()> {
        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Server not setup"))?;

        info!("Bridge running. Press Ctrl+C to stop.");

        let cancel = ctx.cancel.clone();

        let no_signals = self.cli.no_signals.unwrap_or(false);

        // Wait for termination signal or internal cancellation
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Shutdown requested internally");
            }
            _ = async {
                if no_signals {
                    futures_util::future::pending::<()>().await;
                }
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{SignalKind, signal};
                    if let (Ok(mut sigint), Ok(mut sigterm)) = (signal(SignalKind::interrupt()), signal(SignalKind::terminate())) {
                        tokio::select! {
                            _ = sigint.recv() => info!("Received SIGINT"),
                            _ = sigterm.recv() => info!("Received SIGTERM"),
                        }
                    } else {
                        // Fallback if signal binding fails
                        futures_util::future::pending::<()>().await;
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                    info!("Received Ctrl+C");
                }
            } => {}
        }

        info!("Shutting down...");
        self.close().await
    }

    pub async fn close(&mut self) -> Result<()> {
        // Signal all background tasks (drop instances, cancel, mqtt shutdown)
        if let Some(ctx) = &self.ctx {
            ctx.close().await;
        }

        // Wait for state_saver and device_listener to exit gracefully due to cancellation.
        // rustuya may block their threads; abort if they don't exit cleanly within 2 seconds.
        for mut handle in self.background_handles.drain(..) {
            if tokio::time::timeout(std::time::Duration::from_secs(2), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
            }
        }

        // Wait for MQTT task to fully flush and disconnect cleanly (up to 7s).
        // The MQTT task has its own internal 5s PubAck timeout.
        if let Some(handle) = self.mqtt_handle.take() {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(7), handle).await;
        }

        Ok(())
    }
}
