use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

pub struct BridgeServer {
    cli: Cli,
    /// Shutdown signal. `run()` selects on this; anything holding a clone
    /// (a signal handler, a language binding, an embedding application) can
    /// request a graceful shutdown without owning the `BridgeServer` itself.
    cancel: CancellationToken,
    ctx: Option<Arc<BridgeContext>>,
    /// Handles for `state_saver` and `device_listener` - aborted on close
    background_handles: Vec<tokio::task::JoinHandle<()>>,
    /// MQTT task handle - waited on close to ensure clean disconnect
    mqtt_handle: Option<tokio::task::JoinHandle<()>>,
}

impl BridgeServer {
    /// Creates a server with a freshly-allocated shutdown token. Use
    /// [`Self::with_cancel`] if you need to hold a clone of the token (e.g.
    /// to request shutdown from another thread or an FFI boundary).
    #[must_use]
    pub fn new(cli: Cli) -> Self {
        Self::with_cancel(cli, CancellationToken::new())
    }

    /// Creates a server that shuts down when `cancel` is tripped. The caller
    /// keeps a clone of `cancel` to request shutdown out-of-band — crucially,
    /// without contending for any lock that wraps the server (the running
    /// `run()` future would otherwise hold it for the whole server lifetime).
    #[must_use]
    pub fn with_cancel(cli: Cli, cancel: CancellationToken) -> Self {
        Self {
            cli,
            cancel,
            ctx: None,
            background_handles: Vec::new(),
            mqtt_handle: None,
        }
    }

    /// Returns a clone of the shutdown token. Tripping it (`.cancel()`) makes
    /// a running `run()` return and perform graceful MQTT cleanup.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Initializes the bridge context, starts background tasks (state saver, device
    /// listener, MQTT task) and publishes the running config to MQTT.
    ///
    /// # Errors
    /// Returns an error if file-descriptor limits cannot be raised, the state file
    /// directory is not writable, another bridge instance is already running, or
    /// the MQTT task fails to start.
    pub async fn setup(&mut self) -> Result<Arc<BridgeContext>> {
        // Maximize file descriptor limit for better performance
        rustuya::maximize_fd_limit()?;

        // Cap concurrent connection establishment to tame the onboarding
        // "connect storm" when a large fleet is added at once. `0` opts out
        // (unbounded). Idempotent global; set once before any device connects.
        let cc = self.cli.connect_concurrency();
        if cc > 0 {
            rustuya::set_connect_concurrency(cc);
        }

        let session_id = format!(
            "sid_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis())
        );
        self.cli.session_id = Some(session_id);

        let (ctx, mqtt_tx_rx, save_rx, refresh_rx) =
            BridgeContext::new(&self.cli, self.cancel.clone()).await?;

        ctx.check_existing_instance().await?;

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

    /// Blocks until the bridge receives a shutdown signal (SIGINT/SIGTERM, or internal
    /// cancellation), then performs a graceful shutdown.
    ///
    /// # Errors
    /// Returns an error if [`Self::setup`] has not been called, or if shutdown fails.
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
            () = cancel.cancelled() => {
                info!("Shutdown requested internally");
            }
            () = async {
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

    /// Drops the bridge context, aborts background tasks (with grace period), and waits
    /// for the MQTT task to finish flushing.
    ///
    /// # Errors
    /// Currently always returns `Ok`; reserved for future shutdown failures.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Standalone (no-broker) config writing state into `tmp`, in embedded
    /// mode (`no_signals`) so shutdown is driven purely by the cancel token —
    /// the same path the Python binding's `stop()`/`close()` use.
    fn standalone_cli(tmp: &TempDir) -> Cli {
        Cli {
            mqtt_broker: None,
            no_signals: Some(true),
            state_file: Some(tmp.path().join("state.json").to_string_lossy().into_owned()),
            ..Cli::default()
        }
    }

    /// The core of the embedded-shutdown fix: tripping the externally-held
    /// cancellation token must make a running `run()` return AND perform its
    /// graceful close — with no OS signal, and without the caller holding the
    /// server. Bounds the whole thing in a timeout so a regression (run()
    /// hanging) fails loudly instead of deadlocking the suite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_cancel_stops_run_without_signal() {
        let tmp = TempDir::new().unwrap();
        let state_path = tmp.path().join("state.json");
        let token = CancellationToken::new();
        let mut server = BridgeServer::with_cancel(standalone_cli(&tmp), token.clone());
        server.setup().await.expect("setup");

        let run = tokio::spawn(async move { server.run().await });

        // Simulate an external close()/stop() from another thread/loop.
        token.cancel();

        let res = tokio::time::timeout(Duration::from_secs(5), run).await;
        res.expect("run() did not return within 5s of external cancel")
            .expect("run task panicked")
            .expect("run() returned Err");

        // Graceful close ran end-to-end: ctx.close() → save_state() flushed
        // the state file (in a real broker setup this is also where retained
        // messages get cleared and the broker disconnect happens).
        assert!(
            state_path.exists(),
            "graceful shutdown should have flushed the state file"
        );
    }

    /// A token already tripped before `run()` starts must make `run()` return
    /// promptly rather than block — a closed server never hangs the caller.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_before_run_returns_promptly() {
        let tmp = TempDir::new().unwrap();
        let mut server = BridgeServer::new(standalone_cli(&tmp));
        let token = server.cancellation_token();
        server.setup().await.expect("setup");

        token.cancel(); // pre-cancelled

        let run = tokio::spawn(async move { server.run().await });
        let res = tokio::time::timeout(Duration::from_secs(5), run).await;
        res.expect("run() hung on an already-cancelled token")
            .expect("run task panicked")
            .expect("run() returned Err");
    }
}
