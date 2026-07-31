use crate::bridge::BridgeContext;
use crate::config::Cli;
use anyhow::Result;
use log::info;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct BridgeServer {
    /// The **pristine** CLI/env layer, kept for the lifetime of the server.
    /// Each cycle re-layers the config file and defaults over a clone of it
    /// ([`Cli::layered`]), so a restart picks up an edited config file while
    /// CLI/env keeps winning — exactly the precedence startup applied.
    ///
    /// It must be a *layer*, not a finished config: everything the caller did
    /// not explicitly set has to stay `None`, because `merge` fills only `None`
    /// fields. Hand the server a `Cli { .., ..Cli::default() }` and every field
    /// is already `Some`, so the config file can never change anything and a
    /// restart silently ignores file edits. [`Cli::from_env`] and the Python
    /// binding's kwargs layer both produce the right shape.
    base_cli: Cli,
    /// The effective config for the current cycle.
    cli: Cli,
    /// Shutdown signal. `run()` selects on this; anything holding a clone
    /// (a signal handler, a language binding, an embedding application) can
    /// request a graceful shutdown without owning the `BridgeServer` itself.
    ///
    /// This token means *stop for good*, and is never replaced — an embedder
    /// that took a clone at construction keeps a working handle across any
    /// number of restarts.
    cancel: CancellationToken,
    /// Per-cycle token, a **child** of `cancel`. The context and its background
    /// tasks run on this one, so `reconfigure` can tear the cycle down without
    /// touching the process-lifetime token, while a parent cancel still
    /// propagates down and stops everything.
    cycle: CancellationToken,
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
    ///
    /// `cli` is the **CLI/env layer** ([`Cli::from_env`], or the equivalent
    /// kwargs layer from a language binding). The server applies the config file
    /// and defaults itself, once per cycle — see [`Self::base_cli`].
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
            base_cli: cli.clone(),
            cli,
            cycle: cancel.child_token(),
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
        self.start_cycle(true).await
    }

    /// Brings one bridge cycle up: layers the config, builds a context, starts
    /// the background tasks, and publishes the running config.
    ///
    /// `first` distinguishes process startup from a restart. Only startup probes
    /// for a duplicate instance — a restart does not, and must not: we are the
    /// instance that just tore itself down, so the only thing the probe could
    /// find is our own retained sentinel, and it would spend its full
    /// ghost-detection budget confirming that nobody answers before letting us
    /// continue.
    async fn start_cycle(&mut self, first: bool) -> Result<Arc<BridgeContext>> {
        // Report panics (including those on background device-task threads)
        // with their location before the default hook aborts. Under our release
        // build (`panic = "abort"` + `strip`) a worker panic would otherwise
        // vanish with no symbols; this keeps the file:line even there.
        // Idempotent; install once here.
        //
        // These were rustuya library calls in 0.3. 0.4 owns no process-wide
        // state, so the process helpers moved into the bridge — and the
        // connect-storm cap became a `ConnectLimiter` object owned by
        // [`crate::devices::Fleet`], built from `connect_concurrency` when the
        // context is created.
        crate::devices::install_panic_logging();

        // One socket per device: the default soft limit caps the fleet well
        // below what the bridge can handle.
        crate::devices::maximize_fd_limit()?;

        // Re-layer every cycle so a restart picks up an edited config file
        // without losing CLI/env precedence.
        self.cli = self.base_cli.clone().layered().await?;

        // A fresh session id per cycle: it identifies *this* context to the
        // singleton guard, and the restarted one is genuinely a new incumbent.
        let session_id = format!(
            "sid_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis())
        );
        self.cli.session_id = Some(session_id);

        let (ctx, mqtt_tx_rx, save_rx, refresh_rx) =
            BridgeContext::new(&self.cli, self.cycle.clone()).await?;

        if first {
            ctx.check_existing_instance().await?;
        }

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
        loop {
            self.await_cycle_end().await?;
            if !self.finish_cycle().await? {
                return Ok(());
            }
        }
    }

    /// Blocks until something asks the current cycle to end: a termination
    /// signal, the process-lifetime shutdown token, or a `reconfigure` tripping
    /// the cycle token.
    ///
    /// Reports *that* the cycle ended, never *why* — the cycle token is a child
    /// of the shutdown token, so a shutdown trips both and no arm of the select
    /// can tell them apart. [`Self::finish_cycle`] makes that call.
    async fn await_cycle_end(&self) -> Result<()> {
        let ctx = self
            .ctx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Server not setup"))?;

        info!("Bridge running. Press Ctrl+C to stop.");

        let cycle = ctx.cancel.clone();
        let no_signals = self.cli.no_signals.unwrap_or(false);

        tokio::select! {
            () = cycle.cancelled() => {
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
        Ok(())
    }

    /// Tears the ended cycle down and, if it asked to be restarted, brings a new
    /// one up. Returns whether the server should keep running.
    ///
    /// A restart counts only if the context asked for one **and** the
    /// process-lifetime token is still up. An external `stop()` racing a
    /// `reconfigure` shuts down: the operator's shutdown outranks the context's
    /// request to come back.
    async fn finish_cycle(&mut self) -> Result<bool> {
        let restarting = self
            .ctx
            .as_ref()
            .is_some_and(|ctx| ctx.restart_requested.load(Ordering::Relaxed))
            && !self.cancel.is_cancelled();

        info!(
            "{}",
            if restarting {
                "Restarting to apply the new configuration..."
            } else {
                "Shutting down..."
            }
        );
        self.close().await?;

        if !restarting {
            return Ok(false);
        }

        // Fresh cycle token: the old one is spent, and every task that ran on it
        // has been awaited by `close()`.
        self.cycle = self.cancel.child_token();
        self.start_cycle(false).await?;
        info!("Restart complete; new configuration is live.");
        Ok(true)
    }

    /// The effective configuration of the current cycle — the CLI/env layer with
    /// the config file and defaults applied. An in-place restart replaces it, so
    /// this is how an embedder reads back what a `reconfigure` actually applied.
    #[must_use]
    pub fn config(&self) -> &Cli {
        &self.cli
    }

    /// Tears the current cycle down: drops the bridge context, waits out the
    /// background tasks (aborting stragglers), and lets the MQTT task flush and
    /// disconnect cleanly.
    ///
    /// Leaves the server with no context, so it is equally the end of the
    /// process and the midpoint of a restart. Nothing here touches the
    /// process-lifetime shutdown token — [`Self::run`] decides which of the two
    /// this was.
    ///
    /// # Errors
    /// Currently always returns `Ok`; reserved for future shutdown failures.
    pub async fn close(&mut self) -> Result<()> {
        // Signal all background tasks (drop instances, cancel, mqtt shutdown)
        if let Some(ctx) = self.ctx.take() {
            ctx.close().await;
        }

        // Wait for state_saver and device_listener to exit gracefully due to cancellation.
        // A task that doesn't observe the cancel within 2 seconds is aborted so a
        // restart can't stall behind it.
        for mut handle in self.background_handles.drain(..) {
            if tokio::time::timeout(std::time::Duration::from_secs(2), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
            }
        }

        // Wait for MQTT task to fully flush and disconnect cleanly (up to 7s).
        // The MQTT task has its own internal 5s PubAck timeout. On a restart this
        // matters twice over: the next cycle reconnects with the same client id,
        // so the old session must be gone before the new one dials.
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

    /// A **pristine** CLI/env layer — every field `None`, exactly what
    /// [`Cli::from_env`] produces for a bare invocation and what the Python
    /// binding builds from kwargs.
    ///
    /// Deliberately not `..Cli::default()`: that fills every field, so the
    /// config file (which `merge` can only use to fill `None`) could never
    /// change anything. A base built that way is a valid config but not a valid
    /// *layer*, and a restart over it would silently ignore file edits.
    fn pristine_cli() -> Cli {
        serde_json::from_str("{}").expect("every Cli field is Option<T>")
    }

    /// A pristine layer pointed at a config file on disk, so a restart has
    /// something to re-read. Writes `root` as the initial `mqtt_root_topic`.
    fn cli_with_config_file(tmp: &TempDir, root: &str) -> Cli {
        let config_path = tmp.path().join("config.json");
        rewrite_config(tmp, root);
        Cli {
            config: Some(config_path.to_string_lossy().into_owned()),
            no_signals: Some(true),
            state_file: Some(tmp.path().join("state.json").to_string_lossy().into_owned()),
            ..pristine_cli()
        }
    }

    /// Rewrites the config file the way an operator (or `set_config`) would.
    fn rewrite_config(tmp: &TempDir, root: &str) {
        std::fs::write(
            tmp.path().join("config.json"),
            serde_json::json!({ "mqtt_root_topic": root }).to_string(),
        )
        .unwrap();
    }

    /// `reconfigure` restarts **in this process**: the cycle ends, a new context
    /// comes up, and it is running the edited config. 0.3 could only exit and
    /// hope a supervisor restarted it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconfigure_restarts_in_place_with_the_reloaded_config() {
        let tmp = TempDir::new().unwrap();
        let token = CancellationToken::new();
        let mut server =
            BridgeServer::with_cancel(cli_with_config_file(&tmp, "before"), token.clone());
        let ctx = server.setup().await.expect("setup");
        assert_eq!(server.config().mqtt_root_topic.as_deref(), Some("before"));

        rewrite_config(&tmp, "after");
        ctx.reconfigure().await.expect("reconfigure");

        // The cycle is over, but the process is not: only the child token fired.
        assert!(ctx.cancel.is_cancelled(), "reconfigure must end the cycle");
        assert!(
            !token.is_cancelled(),
            "reconfigure must not trip the process-lifetime shutdown token"
        );

        assert!(
            server.finish_cycle().await.expect("restart"),
            "reconfigure must keep the server running, not shut it down"
        );
        assert_eq!(
            server.config().mqtt_root_topic.as_deref(),
            Some("after"),
            "the restarted cycle must be running the edited config"
        );
        assert!(
            server.ctx.is_some(),
            "a restarted server must hold a live context"
        );
    }

    /// A restart re-reads the config file but must not let it override an
    /// explicit CLI/env value — the same precedence startup applies. This is
    /// what keeping the pristine layer buys.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_keeps_cli_env_winning_over_the_config_file() {
        let tmp = TempDir::new().unwrap();
        let mut base = cli_with_config_file(&tmp, "from-file");
        base.mqtt_root_topic = Some("from-cli".into()); // as if `--mqtt-root-topic`

        let mut server = BridgeServer::new(base);
        let ctx = server.setup().await.expect("setup");
        assert_eq!(server.config().mqtt_root_topic.as_deref(), Some("from-cli"));

        rewrite_config(&tmp, "edited-in-file");
        ctx.reconfigure().await.expect("reconfigure");
        server.finish_cycle().await.expect("restart");

        assert_eq!(
            server.config().mqtt_root_topic.as_deref(),
            Some("from-cli"),
            "a config-file edit must not override an explicit CLI/env value across a restart"
        );
    }

    /// Shutdown outranks restart. If an operator stops the bridge while a
    /// `reconfigure` is in flight, it must stop — not come back up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_shutdown_beats_a_pending_reconfigure() {
        let tmp = TempDir::new().unwrap();
        let token = CancellationToken::new();
        let mut server =
            BridgeServer::with_cancel(cli_with_config_file(&tmp, "before"), token.clone());
        let ctx = server.setup().await.expect("setup");

        ctx.reconfigure().await.expect("reconfigure");
        token.cancel(); // the operator stops the bridge mid-reconfigure

        assert!(
            !server.finish_cycle().await.expect("shutdown"),
            "an external shutdown must win over a pending restart"
        );
        assert!(
            server.ctx.is_none(),
            "a shut-down server must not hold a context"
        );
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
