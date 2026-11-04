//! The bridge's MQTT side: the connection and its event loop, the single-instance
//! check, and the short-lived clients that sweep retained messages. Everything that
//! talks to `rumqttc` lives here.

use anyhow::Result;
use futures_util::StreamExt;
use log::{debug, error, info, warn};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::bridge::{BridgeContext, MQTT_CHANNEL_CAPACITY, MqttMessage, TopicVars, unix_millis};
use crate::config::Cli;
use crate::template::{compile_topic_regex, match_topic, tpl_to_wildcard};
use crate::types::{ApiResponse, BridgeRequest};

/// Client-side MQTT packet size cap (incoming and outgoing). rumqttc defaults
/// *both* to a tiny 10 KiB, which silently refuses any larger publish — a big
/// `status` reply, a multi-DP snapshot — *before it ever reaches the broker*,
/// stalling the connection on a retry loop. Raise it to a generous bound so
/// the client is never the bottleneck; the broker still enforces its own
/// limit, and `status` is paginated independently as defense in depth.
pub const MAX_MQTT_PACKET_SIZE: usize = 1024 * 1024;
pub const INITIAL_RETRY_DELAY_SECS: u64 = 10;
pub const MAX_RETRY_DELAY_SECS: u64 = 1280;
const MQTT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
// Bound concurrent commands independently of the per-command target fan-out.
const MAX_CONCURRENT_REQUESTS: usize = 100;

/// Decides whether a retained message belongs to a device being removed, from
/// the variables its topic yielded (`None` if it matched no template) and its
/// payload.
///
/// How far the payload may be trusted depends on what the topic already says,
/// because the payload check is a search for a quoted string in the whole body,
/// where a name or cid can collide with a DP key (`"1"`) or a string value
/// (`"white"`) of an unrelated device:
/// - `{id}` in the topic settles it. Names and cids are not unique, so they must
///   not be consulted: a sibling with the same name has its own topic to keep.
/// - `{name}`/`{cid}` without `{id}`: a topic miss can still be this device under
///   a name it no longer has, so look for its id, which is long enough not to
///   collide. The payload's name and cid add nothing the topic didn't already say.
/// - No identifier in the topic: the payload is all there is. Collisions remain
///   possible here, and clearing is the only way not to orphan these messages.
fn retained_belongs_to_targets(
    topic_vars: Option<&HashMap<String, String>>,
    payload: &str,
    targets: &[crate::types::ScavengerTarget],
) -> bool {
    let var = |key| topic_vars.and_then(|vars| vars.get(key));
    if let Some(id) = var("id") {
        return targets.iter().any(|t| &t.id == id);
    }
    let (name, cid) = (var("name"), var("cid"));
    let identified_by_topic = name.is_some() || cid.is_some();
    let quoted = |value: &str| payload.contains(&format!("\"{value}\""));
    targets.iter().any(|t| {
        (name.is_some() && t.name.as_ref() == name)
            || (cid.is_some() && t.cid.as_ref() == cid)
            || quoted(&t.id)
            || (!identified_by_topic
                && (t.name.as_deref().is_some_and(quoted) || t.cid.as_deref().is_some_and(quoted)))
    })
}

impl BridgeContext {
    /// Starts MQTT task for command processing and event publishing.
    ///
    /// Returns `Ok(None)` if no broker is configured.
    ///
    /// # Errors
    /// Returns an error if MQTT options cannot be constructed (e.g. invalid broker URL).
    #[allow(clippy::too_many_lines, reason = "tokio::select! event-loop driver")]
    pub fn spawn_mqtt_task(
        self: Arc<Self>,
        cli: &Cli,
        mut mqtt_tx_receiver: mpsc::Receiver<Option<MqttMessage>>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        let Some(broker_url) = &cli.mqtt_broker else {
            return Ok(None);
        };

        let (mqtt_command_topic, _) = cli.mqtt_topics();
        let client_id = cli.mqtt_client_id();
        let mqtt_options = self.create_mqtt_options(broker_url, &client_id, cli, true)?;

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 100);
        let sub_topic = tpl_to_wildcard(&mqtt_command_topic, &self.mqtt_root_topic);
        let command_topic_re = compile_topic_regex(&mqtt_command_topic);
        let config_topic =
            crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);

        // ── Seed phase (cache mode only) ──────────────────────────────────────────
        // After the first ConnAck the bridge subscribes to its own retained
        // state topic to recover prior-session snapshots, then unsubscribes
        // and calls `on_seed_complete()` once either the broker quiets (200ms
        // since last retained) or the 5s hard cap fires.
        const SEED_HARD_CAP: Duration = Duration::from_secs(5);
        const SEED_QUIET: Duration = Duration::from_millis(200);
        let seed_wildcard = self.seed_state_wildcard.clone();

        // How long after a reconnect to watch for another instance's sentinel
        // before restoring ours. Same window `check_existing_instance` observes.
        const SENTINEL_RESTORE_DELAY: Duration = Duration::from_millis(500);

        // Buffer bursts using the same bounded budget as the outbound channel.
        // One tracked driver owns all request futures; no detached task per command.
        let (request_tx, request_rx) = mpsc::channel(MQTT_CHANNEL_CAPACITY);
        let requests_ctx = self.clone();
        let stop_requests = self.cancel.clone();
        self.spawn_task(async move {
            tokio_stream::wrappers::ReceiverStream::new(request_rx)
                .take_until(stop_requests.cancelled_owned())
                .for_each_concurrent(MAX_CONCURRENT_REQUESTS, |req| {
                    let ctx = requests_ctx.clone();
                    async move {
                        let responses = crate::handlers::handle_request(ctx.clone(), req).await;
                        for response in responses {
                            ctx.publish_api_response(response).await;
                        }
                    }
                })
                .await;
        });

        let handle = tokio::spawn(async move {
            let mut outstanding_publishes = 0usize;
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            let mut next_retry: Option<Instant> = None;
            let mut seed_active = false;
            let mut seed_deadline: Option<Instant> = None;
            let mut seed_last_msg: Option<Instant> = None;
            // An unclean disconnect fires the will, which clears the sentinel, and
            // nothing else restores it until the next registry change. Until then a
            // second instance finds the root unclaimed, starts, and takes over. The
            // restore waits out the retained replay first: if another instance did
            // take over during the outage, overwriting its sentinel would make each
            // bridge see the other's and both shut down. The payload is resent
            // verbatim because a new `devices_updated_at` would announce a registry
            // change that never happened.
            let mut connected_before = false;
            let mut last_sentinel: Option<String> = None;
            let mut sentinel_restore_at: Option<Instant> = None;
            // At most one publish awaiting hand-off to rumqttc's request channel.
            // It is flushed with the non-blocking `try_publish` at the loop top, not
            // `publish().await` inside the arm: awaiting the latter blocks the
            // sibling `eventloop.poll()` arm the instant the request channel fills,
            // and that poll is what drains PubAcks to free the in-flight window — so
            // a whole-fleet QoS1 snapshot burst would self-deadlock the loop. This is
            // the same try_publish+poll interleave `clear_and_flush` relies on.
            let mut pending: Option<MqttMessage> = None;
            loop {
                // Flush the queued publish without blocking. If the request channel
                // is full, keep it pending; the poll arm below drains the in-flight
                // window and we retry next iteration. QoS by message kind: retained
                // state (per-device snapshots and status, the bridge-config sentinel)
                // → QoS1; live/transient deltas and command responses
                // → QoS0 (a fan-out emits hundreds at once and QoS1 PUBACK round-trips
                // would stall the loop; a dropped live delta self-heals via the next
                // push or the retained snapshot).
                let flushed = if let Some(msg) = &pending {
                    let retain = msg.retain && !self.retain_suppressed.load(Ordering::Relaxed);
                    let qos = if msg.retain {
                        rumqttc::QoS::AtLeastOnce
                    } else {
                        rumqttc::QoS::AtMostOnce
                    };
                    client
                        .try_publish(msg.topic.clone(), qos, retain, msg.payload.clone())
                        .is_ok()
                } else {
                    false
                };
                if flushed {
                    if let Some(msg) = &pending {
                        if msg.retain {
                            outstanding_publishes += 1;
                        }
                        if msg.topic == config_topic {
                            last_sentinel = (!msg.payload.is_empty()).then(|| msg.payload.clone());
                        }
                    }
                    pending = None;
                }
                tokio::select! {
                    () = async {
                        if let Some(deadline) = next_retry {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            futures_util::future::pending::<()>().await;
                        }
                    } => {
                        next_retry = None;
                    }
                    () = async {
                        let wakeup = match (seed_last_msg, seed_deadline) {
                            (Some(t), Some(d)) => Some((t + SEED_QUIET).min(d)),
                            (None, Some(d)) => Some(d),
                            _ => None,
                        };
                        if let Some(w) = wakeup {
                            tokio::time::sleep_until(w).await;
                        } else {
                            futures_util::future::pending::<()>().await;
                        }
                    }, if seed_active => {
                        seed_active = false;
                        seed_deadline = None;
                        seed_last_msg = None;
                        if let Some(w) = &seed_wildcard {
                            let _ = client.unsubscribe(w).await;
                        }
                        self.on_seed_complete().await;
                    }
                    () = async {
                        if let Some(deadline) = sentinel_restore_at {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            futures_util::future::pending::<()>().await;
                        }
                    } => {
                        sentinel_restore_at = None;
                        if let Some(sentinel) = &last_sentinel {
                            if client
                                .try_publish(&config_topic, rumqttc::QoS::AtLeastOnce, true, sentinel.clone())
                                .is_ok()
                            {
                                outstanding_publishes += 1;
                            } else {
                                sentinel_restore_at = Some(Instant::now() + SENTINEL_RESTORE_DELAY);
                            }
                        }
                    }
                    notification = eventloop.poll(), if next_retry.is_none() => {
                        match notification {
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                                info!("Connected to MQTT broker");
                                retry_delay = INITIAL_RETRY_DELAY_SECS;
                                if connected_before {
                                    sentinel_restore_at = Some(Instant::now() + SENTINEL_RESTORE_DELAY);
                                }
                                connected_before = true;
                                if let Err(e) = client.subscribe(&sub_topic, rumqttc::QoS::AtLeastOnce).await {
                                    error!("Subscription failed: {e}");
                                } else {
                                    info!("Subscribed to: {sub_topic}");
                                }
                                if let Err(e) = client.subscribe(&config_topic, rumqttc::QoS::AtLeastOnce).await {
                                    error!("Config subscription failed: {e}");
                                }
                                // First-connect seed bootstrap (cache mode only).
                                if let Some(w) = &seed_wildcard
                                    && !self.seed_done.load(Ordering::Acquire)
                                    && !seed_active
                                {
                                    match client.subscribe(w, rumqttc::QoS::AtLeastOnce).await {
                                        Ok(()) => {
                                            info!("Seeding cache from broker via '{w}'");
                                            seed_active = true;
                                            seed_deadline = Some(Instant::now() + SEED_HARD_CAP);
                                        }
                                        Err(e) => {
                                            error!("Seed subscribe failed: {e}; skipping seed");
                                            // Don't block snapshot publishes forever.
                                            self.on_seed_complete().await;
                                        }
                                    }
                                }
                            }
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                let payload = String::from_utf8_lossy(&p.payload);
                                debug!("MQTT Received: [{}] {payload}", p.topic);

                                // Seed-window retained delivery: feed into the cache
                                // without firing the publish path. The retain check
                                // protects us from picking up live (non-retained)
                                // events that incidentally match the wildcard.
                                if seed_active && p.retain
                                    && let Some((id, dp_opt)) = self.match_seed_topic(&p.topic)
                                {
                                    if let Some(dps) = crate::payload::parse_seed_dps(
                                        &payload,
                                        dp_opt.as_deref(),
                                        self.mqtt_payload_template.as_deref(),
                                    ) && let Some(cache) = &self.cache
                                    {
                                        cache.fill_missing(&id, &dps);
                                        seed_last_msg = Some(Instant::now());
                                    }
                                    continue;
                                }

                                if p.topic == config_topic {
                                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&payload)
                                        && let Some(sid) = val.get("session_id").and_then(Value::as_str)
                                        && Some(sid) != self.cli.session_id.as_deref() {
                                        error!("Another bridge instance took over (session_id: {sid}). Shutting down.");
                                        self.cancel.cancel();
                                        return;
                                    }
                                    continue;
                                }

                                let vars = match_topic(&p.topic, &mqtt_command_topic, command_topic_re.as_ref()).unwrap_or_default();
                                let req_val = crate::payload::parse_mqtt_payload(&payload, &vars);

                                let requests = match req_val {
                                    Value::Array(arr) => arr
                                        .into_iter()
                                        .filter_map(|v| serde_json::from_value::<BridgeRequest>(v).ok())
                                        .collect::<Vec<_>>(),
                                    other => serde_json::from_value::<BridgeRequest>(other)
                                        .map(|req| vec![req])
                                        .unwrap_or_default(),
                                };

                                for req in requests {
                                    let action = req.action_name().to_owned();
                                    if let Err(err) = request_tx.try_send(req) {
                                        warn!("MQTT request queue unavailable: {err}");
                                        self.publish_api_response(ApiResponse::error("Bridge is busy; retry the request").with_action(action)).await;
                                    }
                                }
                            }
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_))) => {
                                outstanding_publishes = outstanding_publishes.saturating_sub(1);
                            }
                            Err(e) => {
                                sentinel_restore_at = None;
                                error!("MQTT Error: {e}. Retrying in {retry_delay}s...");
                                let jitter = u64::try_from(unix_millis() % 1000).unwrap_or(0);
                                next_retry = Some(Instant::now() + Duration::from_secs(retry_delay) + Duration::from_millis(jitter));
                                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_SECS);
                            }
                            _ => {}
                        }
                    }
                    msg_opt = async {
                        tokio::select! {
                            biased;
                            () = self.mqtt_shutdown.cancelled() => None,
                            msg = mqtt_tx_receiver.recv(), if pending.is_none() => msg,
                        }
                    } => {
                        if let Some(Some(msg)) = msg_opt {
                            debug!("MQTT Publish queued: [{}] {}", msg.topic, msg.payload);
                            // Hand off to the non-blocking flush at the loop top.
                            // Gating this arm on `pending.is_none()` backpressures
                            // into `mqtt_tx` (cap 4096) rather than blocking here on
                            // a full request channel — which would starve the poll
                            // arm that frees it.
                            pending = Some(msg);
                        } else {
                                debug!("MQTT shutdown signal received, clearing and disconnecting...");

                                // Queue the clear while polling, then wait for *all* queued
                                // QoS1 publishes, including the clear, to be acknowledged.
                                // A prior publish's ACK must never stand in for this clear.
                                // Stop producers and preserve FIFO for all accepted messages.
                                // The config clear must be the final retained publish.
                                mqtt_tx_receiver.close();
                                let mut outbound = std::collections::VecDeque::new();
                                if let Some(msg) = pending.take() {
                                    outbound.push_back(msg);
                                }
                                while let Ok(msg) = mqtt_tx_receiver.try_recv() {
                                    if let Some(msg) = msg { outbound.push_back(msg); }
                                }
                                let flush = async {
                                    let mut clear_queued = false;
                                    loop {
                                        while let Some(msg) = outbound.front() {
                                            let qos = if msg.retain { rumqttc::QoS::AtLeastOnce } else { rumqttc::QoS::AtMostOnce };
                                            let retain = msg.retain && !self.retain_suppressed.load(Ordering::Relaxed);
                                            if client.try_publish(&msg.topic, qos, retain, msg.payload.clone()).is_err() { break; }
                                            if msg.retain { outstanding_publishes += 1; }
                                            outbound.pop_front();
                                        }
                                        if outbound.is_empty() && !clear_queued && client.try_publish(
                                            &config_topic, rumqttc::QoS::AtLeastOnce, true, ""
                                        ).is_ok() {
                                            outstanding_publishes += 1;
                                            clear_queued = true;
                                        }
                                        if clear_queued && outstanding_publishes == 0 {
                                            return Ok::<(), rumqttc::ConnectionError>(());
                                        }
                                        if let rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)) = eventloop.poll().await? {
                                            outstanding_publishes = outstanding_publishes.saturating_sub(1);
                                        }
                                    }
                                };
                                match tokio::time::timeout(MQTT_FLUSH_TIMEOUT, flush).await {
                                    Ok(Ok(())) => debug!("MQTT retained clear acknowledged"),
                                    result => warn!("MQTT shutdown flush failed: {result:?}"),
                                }

                                // 4. Disconnect, then drain the eventloop until rumqttc surfaces
                                // an error (its signal that the connection is fully torn down).
                                // No inner timeout: bounded externally by the 7s timeout that
                                // `BridgeServer::close` wraps around this task's JoinHandle, so a
                                // broker that never closes the socket cannot hang shutdown.
                                let _ = client.disconnect().await;
                                loop {
                                    if eventloop.poll().await.is_err() {
                                        break;
                                    }
                                }
                                debug!("MQTT eventloop terminated");
                                return;
                        }
                    }
                }
            }
        });

        Ok(Some(handle))
    }

    /// Renders the topic the liveness probe publishes its `status` ping to,
    /// using the same `replace_vars` translator as the rest of the codebase.
    /// The action can ride *in the topic* (e.g. `{root}/command/{action}/{id}/{dp}`),
    /// so `action` = "status"; every other variable gets a non-empty placeholder
    /// because the command regex captures `[^/]+` per level — an empty segment
    /// would never match the incumbent's subscription.
    pub(crate) fn probe_command_topic(&self) -> String {
        self.replace_vars(
            self.cli
                .mqtt_command_topic
                .as_deref()
                .unwrap_or(crate::config::DEFAULT_MQTT_COMMAND_TOPIC),
            &TopicVars {
                id: "bridge",
                name: Some("bridge"),
                cid: Some("bridge"),
                level: Some("bridge"),
                event_type: Some("bridge"),
                action: Some("status"),
                dp: Some("bridge"),
                ..Default::default()
            },
        )
    }

    /// Renders the topic the liveness probe listens on for the incumbent's
    /// `status` reply — the exact topic `publish_api_response` sends a
    /// bridge-level OK response to (message topic, id/name = "bridge", level
    /// "response"), so subscribe == publish for any template.
    pub(crate) fn probe_response_topic(&self) -> String {
        self.replace_vars(
            self.mqtt_message_topic
                .as_deref()
                .unwrap_or(crate::config::DEFAULT_MQTT_MESSAGE_TOPIC),
            &TopicVars {
                id: "bridge",
                name: Some("bridge"),
                level: Some("response"),
                ..Default::default()
            },
        )
    }

    /// Detects whether another *live* bridge instance owns this root topic.
    ///
    /// First observes the retained bridge-config sentinel for 500ms. If none is
    /// present the path is clear and we start immediately. If a sentinel exists
    /// it may belong to a live instance *or* be a stale ghost left by an unclean
    /// exit whose LWT never fired — e.g. power loss with a co-located broker: the
    /// broker dies before it can publish the will, then restores the stale
    /// retained config on reboot, and mere-presence detection would deadlock
    /// every restart. So we probe liveness instead: ping the `status` action
    /// repeatedly and watch the bridge response topic. A live instance answers
    /// within milliseconds; a ghost never does. The probe is biased generously
    /// toward "alive" (many retries over a wide window, returning the instant any
    /// response arrives) so a momentarily-disconnected incumbent is never
    /// mistaken for a ghost — only sustained silence proceeds. The config sid
    /// guard in the main MQTT loop remains the backstop if two live instances
    /// ever slip past this gate.
    ///
    /// # Errors
    /// Returns an error if the MQTT subscription fails or if a live duplicate answers.
    pub async fn check_existing_instance(&self) -> Result<()> {
        let Some(broker_url) = &self.cli.mqtt_broker else {
            return Ok(());
        };
        let client_id = format!("{}_check", self.cli.mqtt_client_id());
        let opts = self.create_mqtt_options(broker_url, &client_id, &self.cli, false)?;
        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 10);
        let config_topic =
            crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
        client
            .subscribe(&config_topic, rumqttc::QoS::AtLeastOnce)
            .await?;

        // Phase 1: observe the retained config sentinel.
        let mut sentinel_seen = false;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, eventloop.poll()).await {
            if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = event
                && p.topic == config_topic
            {
                let payload = String::from_utf8_lossy(&p.payload);
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&payload)
                    && val.get("session_id").is_some()
                {
                    sentinel_seen = true;
                    break;
                }
            }
        }

        // No sentinel → no prior instance → start clean (fast path, no probe).
        if !sentinel_seen {
            let _ = client.disconnect().await;
            return Ok(());
        }

        // Phase 2: liveness probe. `PROBE_ATTEMPTS * PROBE_INTERVAL` is the hard
        // cap, which also bounds power-loss recovery latency — kept under the
        // ~45s LWT fallback it supersedes. A live instance answers on the first
        // ping (ms), so this budget is only ever fully spent against a ghost.
        const PROBE_ATTEMPTS: u32 = 12;
        const PROBE_INTERVAL: Duration = Duration::from_secs(2);

        let cmd_topic = self.probe_command_topic();
        // The reply lands wherever the main loop publishes a bridge-level
        // response. Responses are non-retained, so a dead instance's old reply
        // can't masquerade as liveness — no nonce needed.
        let resp_topic = self.probe_response_topic();
        client
            .subscribe(&resp_topic, rumqttc::QoS::AtLeastOnce)
            .await?;
        let ping = serde_json::json!({ "action": "status" }).to_string();

        for _ in 0..PROBE_ATTEMPTS {
            let _ = client
                .publish(
                    &cmd_topic,
                    rumqttc::QoS::AtLeastOnce,
                    false,
                    ping.as_bytes(),
                )
                .await;
            let attempt_deadline = tokio::time::Instant::now() + PROBE_INTERVAL;
            loop {
                match tokio::time::timeout_at(attempt_deadline, eventloop.poll()).await {
                    Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))))
                        if p.topic == resp_topic =>
                    {
                        let _ = client.disconnect().await;
                        anyhow::bail!(
                            "Duplicate instance detected. Another bridge is already running."
                        );
                    }
                    // Other events / transient poll errors: keep listening until
                    // this attempt's window elapses, then re-ping.
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }

        warn!(
            "Bridge config retained but no instance answered `status` in {}s; \
             treating it as a stale ghost (unclean exit / power loss) and starting.",
            PROBE_ATTEMPTS * PROBE_INTERVAL.as_secs() as u32
        );
        let _ = client.disconnect().await;
        Ok(())
    }

    /// Creates MQTT options from broker URL
    pub(crate) fn create_mqtt_options(
        &self,
        broker_url: &str,
        client_id: &str,
        cli: &Cli,
        with_lwt: bool,
    ) -> Result<rumqttc::MqttOptions> {
        let normalized = if broker_url.contains("://") {
            broker_url.to_owned()
        } else {
            format!("mqtt://{broker_url}")
        };
        let url = url::Url::parse(&normalized)?;
        anyhow::ensure!(
            matches!(url.scheme(), "mqtt" | "tcp" | "mqtts" | "ssl"),
            "Unsupported MQTT URL scheme"
        );
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("Missing host in broker URL"))?;
        let is_ssl = matches!(url.scheme(), "mqtts" | "ssl");
        let port = url.port().unwrap_or(if is_ssl { 8883 } else { 1883 });
        let mut opts = rumqttc::MqttOptions::new(client_id, host.trim_matches(['[', ']']), port);
        if let Some(user) = &cli.mqtt_user {
            opts.set_credentials(user, cli.mqtt_password.as_deref().unwrap_or(""));
        } else if !url.username().is_empty() {
            opts.set_credentials(url.username(), url.password().unwrap_or(""));
        }
        if is_ssl {
            opts.set_transport(rumqttc::Transport::tls_with_default_config());
        }

        opts.set_keep_alive(Duration::from_secs(30));
        // rumqttc caps both directions at 10 KiB by default — far too small for
        // a fleet `status` reply or a large snapshot. Lift the client cap so it
        // isn't the bottleneck (the broker still enforces its own).
        opts.set_max_packet_size(MAX_MQTT_PACKET_SIZE, MAX_MQTT_PACKET_SIZE);

        if with_lwt {
            // Set Last Will and Testament (LWT) to clear the config on abnormal termination
            opts.set_last_will(rumqttc::LastWill {
                topic: crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic),
                message: bytes::Bytes::from(""),
                qos: rumqttc::QoS::AtLeastOnce,
                retain: true,
            });
        }

        Ok(opts)
    }

    /// Publishes an empty retained (clear) for each topic and drives the
    /// eventloop until the broker has **acknowledged** them all. Interleaves
    /// non-blocking `try_publish` with `poll` so a bounded request channel can
    /// never deadlock (awaiting `publish()` inside a poll loop blocks the loop
    /// the moment the channel fills).
    ///
    /// Counts incoming `PubAck`s, not the outgoing writes: rumqttc buffers its
    /// network writes, so an `Outgoing::Publish` only means "encoded into the
    /// write buffer". Disconnecting right after the last one (the eventloop
    /// stops being polled, so the buffer is never flushed and the DISCONNECT
    /// never even leaves) drops that whole still-buffered tail — which is why a
    /// 112/112 "sent" count left ~80 retained alive on the broker. A `PubAck`
    /// can only come back after the broker actually received the clear, so
    /// waiting for them forces the flush and confirms delivery. Returns the
    /// acked count; bounded by a per-poll stall timeout.
    async fn clear_and_flush(
        client: &rumqttc::AsyncClient,
        eventloop: &mut rumqttc::EventLoop,
        topics: &[String],
    ) -> usize {
        let mut queued = 0usize; // handed to the client
        let mut acked = 0usize; // confirmed received by the broker
        while acked < topics.len() {
            // Enqueue as many clears as the request channel will take right now.
            while queued < topics.len()
                && client
                    .try_publish(topics[queued].as_str(), rumqttc::QoS::AtLeastOnce, true, "")
                    .is_ok()
            {
                queued += 1;
            }
            // Drive the eventloop; a PubAck means the broker stored the clear.
            match tokio::time::timeout(Duration::from_secs(10), eventloop.poll()).await {
                Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))) => acked += 1,
                Ok(Ok(_)) => {}
                other => {
                    debug!(
                        "clear_and_flush stop after {acked}/{} acked: {other:?}",
                        topics.len()
                    );
                    break; // connection error or stall
                }
            }
        }
        acked
    }

    /// Spawns a background task to clear retained messages for specific devices
    #[allow(clippy::too_many_lines, reason = "tokio::select! event-loop driver")]
    pub async fn spawn_retain_scavenger(&self, targets: Vec<crate::types::ScavengerTarget>) {
        if targets.is_empty() {
            return;
        }

        let mut rx = {
            let mut lock = self.scavenger_tx.lock().await;
            if let Some(tx) = &*lock
                && tx.send(targets.clone()).is_ok()
            {
                return;
            }
            let (tx, rx) = mpsc::unbounded_channel();
            *lock = Some(tx);
            rx
        };

        let Some(broker_url) = self.cli.mqtt_broker.clone() else {
            return;
        };
        let client_id = format!("{}_scavenger_{}", self.cli.mqtt_client_id(), unix_millis());
        let mqtt_options = match self.create_mqtt_options(&broker_url, &client_id, &self.cli, false)
        {
            Ok(opts) => opts,
            Err(e) => {
                error!("Failed to create MQTT options for scavenger: {e}");
                return;
            }
        };

        // Determine wildcard subscriptions based on templates
        let mut subs = HashSet::new();
        let templates = vec![
            self.mqtt_event_topic.clone(),
            self.mqtt_message_topic.clone().unwrap_or_default(),
        ];

        let root_topic = self.mqtt_root_topic.clone();

        for tpl in &templates {
            if tpl.is_empty() {
                continue;
            }
            subs.insert(tpl_to_wildcard(tpl, &root_topic));
        }

        // The event topic is stored already `{root}`-substituted (from
        // `cli.mqtt_topics()`), but the message topic is stored raw. Substitute
        // `{root}` here too: `compile_topic_regex`/`match_topic` only recognize
        // `TOPIC_VARS` (which excludes `root`), so a raw `{root}` would be
        // escaped literally and the regex could never match a real message
        // topic — leaving the precise topic-based match dead (it worked only via
        // the payload id-injection fallback).
        let event_tpl = templates[0].clone();
        let msg_tpl = templates[1].replace("{root}", &root_topic);
        let event_re = compile_topic_regex(&event_tpl);
        let msg_re = compile_topic_regex(&msg_tpl);
        let scavenger_timeout_secs = self.cli.scavenger_timeout_secs();

        let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &root_topic);

        // Shared with the sender side (the coalesce-or-spawn block above) so the
        // task can atomically retire the coalescing channel when it stops — see
        // the exit handling at the bottom of the loop.
        let scav_tx = Arc::clone(&self.scavenger_tx);

        self.spawn_task(async move {
            let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 64);

            // Loop-until-dry. A single collect window ends on an idle gap, but
            // the broker delivers retained only once per SUBSCRIBE — so a bursty
            // or slow replay (a large fleet, or a cold/loaded broker) that falls
            // silent for > scavenger_timeout_secs mid-stream strands every
            // retained the one-shot pass never saw, with no second chance. Each
            // pass re-subscribes (re-triggering replay of whatever is *still*
            // retained, i.e. only what the prior pass missed) and clears what it
            // matches; we stop once a pass clears nothing (proving the broker is
            // clean) or hit the cap. This makes scavenging independent of replay
            // timing instead of betting the whole sweep on one uninterrupted run.
            //
            // MAX_PASSES is a runaway backstop, NOT a tuned value — the real exit
            // is the dry pass above. It's generous on purpose: the number of
            // passes needed grows with fleet size, because each pass only collects
            // as much of the gappy retained replay as arrives before a
            // > scavenger_timeout_secs gap. Measured against tuyamock fleets (each
            // device leaves ~2 retained: an event/state snapshot + an error/{id}
            // errorCode topic): 1000 devices converged in ~4 passes, 2500 in ~9.
            // 30 keeps ~3x headroom over the largest measured case for slower
            // brokers / bigger fleets while staying finite. Overshoot is cheap — a
            // needless pass costs at most one scavenger_timeout_secs of idle, and
            // a dry pass ends the loop the instant the broker is actually clean —
            // so don't "optimize" this down to a tight number; that just
            // reintroduces the early-give-up bug at scale.
            const MAX_PASSES: u32 = 30;
            let mut active_targets = targets;
            let mut closed = false;
            for pass in 1..=MAX_PASSES {
                for sub in &subs {
                    if let Err(e) = client.subscribe(sub, rumqttc::QoS::AtLeastOnce).await {
                        error!("Scavenger subscription failed for {sub}: {e}");
                    } else {
                        debug!("Scavenger subscribed to {sub} (pass {pass})");
                    }
                }

                // Collect every retained message during the receive window, then
                // match at the end. Retained is delivered once (at subscribe), but
                // coalesced targets (a burst of `remove`s forwarded via `rx`)
                // accumulate over time — matching per-message-as-it-arrives would
                // miss devices whose `remove` landed after their retained was
                // already received and discarded. Collecting and clearing are kept
                // separate so our PUBACKs stay prompt (no broker throttle) and the
                // bounded request channel can't fill and deadlock mid-receive.
                let mut seen: Vec<(String, bytes::Bytes)> = Vec::new();
                let mut deadline =
                    tokio::time::Instant::now() + Duration::from_secs(scavenger_timeout_secs);

                loop {
                    tokio::select! {
                        // Biased: process new-target arrivals before the deadline branch
                        // so a simultaneous send + expiry can't lose the extension.
                        biased;
                        new_targets_opt = rx.recv() => {
                            if let Some(new_targets) = new_targets_opt {
                                let count = new_targets.len();
                                active_targets.extend(new_targets);
                                deadline = tokio::time::Instant::now() + Duration::from_secs(scavenger_timeout_secs);
                                debug!("Scavenger added {count} new targets, extended timeout");
                            } else {
                                closed = true;
                                break;
                            }
                        }
                        () = tokio::time::sleep_until(deadline) => break,
                        notification = eventloop.poll() => {
                            match notification {
                                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                    if !p.retain || p.topic == config_topic {
                                        continue;
                                    }
                                    seen.push((p.topic.clone(), p.payload.clone()));
                                    // Keep the window open while retained keep arriving.
                                    deadline = tokio::time::Instant::now() + Duration::from_secs(scavenger_timeout_secs);
                                }
                                Err(_) => break, // Connection error
                                _ => {}
                            }
                        }
                    }
                }

                // Match the full set against the final target list, then clear.
                let mut to_clear: Vec<String> = Vec::new();
                for (topic, payload) in &seen {
                    let vars = match_topic(topic, &event_tpl, event_re.as_ref())
                        .or_else(|| match_topic(topic, &msg_tpl, msg_re.as_ref()));
                    let payload = String::from_utf8_lossy(payload);
                    let matched = retained_belongs_to_targets(vars.as_ref(), &payload, &active_targets);
                    if matched {
                        to_clear.push(topic.clone());
                    }
                }
                to_clear.sort();
                to_clear.dedup();
                debug!(
                    "Scavenger pass {pass} collected={:?} to_clear={to_clear:?}",
                    seen.iter().map(|(t, _)| t).collect::<Vec<_>>()
                );
                let matched = to_clear.len();
                let acked = Self::clear_and_flush(&client, &mut eventloop, &to_clear).await;
                info!(
                    "Scavenger pass {pass}: collected {} retained, matched {matched} of {} targets, cleared {acked}",
                    seen.len(),
                    active_targets.len()
                );

                // An empty pass (nothing left to clear) proves the broker is
                // clean — only then are we done. A pass that *did* clear may have
                // been cut short by an idle gap with more still retained, so it
                // earns another sweep. Bail too if the target channel closed.
                if to_clear.is_empty() || closed {
                    // Retire the coalescing channel atomically against a racing
                    // spawn_retain_scavenger. It sends under this same lock, so
                    // there are only two orderings: it enqueued a late target
                    // before we lock (we drain and sweep once more, keeping the
                    // channel), or it locks after us and finds `None` (and spawns
                    // a fresh scavenger). Without this, a target sent during the
                    // final `disconnect().await` below would land in a channel
                    // this task never reads again — orphaning that retained.
                    let mut lock = scav_tx.lock().await;
                    let mut late = Vec::new();
                    while let Ok(t) = rx.try_recv() {
                        late.extend(t);
                    }
                    if late.is_empty() || closed {
                        *lock = None;
                        break;
                    }
                    drop(lock);
                    active_targets.extend(late);
                    // Late arrivals: fall through to another pass to sweep them.
                }
            }
            // Reached only when MAX_PASSES is exhausted with retained still
            // matching (very large fleet / slow broker) — the dry-exit path above
            // already cleared the tx under the lock. Clear it here too so future
            // callers spawn a fresh scavenger, and warn if a late target will go
            // unswept.
            {
                let mut lock = scav_tx.lock().await;
                let mut leftover = 0usize;
                while let Ok(t) = rx.try_recv() {
                    leftover += t.len();
                }
                if leftover > 0 {
                    warn!(
                        "Scavenger stopped at MAX_PASSES with {leftover} late target(s) unswept; their retained may be orphaned"
                    );
                }
                *lock = None;
            }
            let _ = client.disconnect().await;
        });
    }

    /// Clears **every** retained message under the bridge's event and message
    /// topic wildcards, regardless of device. Connects a transient MQTT client
    /// (separate from the main one), drains retained messages until
    /// `scavenger_timeout_secs` of idle, and returns the number of topics
    /// cleared. Best-effort: returns `0` on no broker or connection failure.
    ///
    /// Unlike [`Self::spawn_retain_scavenger`] (which matches a removed-device
    /// target set), this is unconditional and awaited inline — used by
    /// `reconfigure` to wipe old-scheme retained snapshots before a restart.
    pub async fn purge_all_retained(&self) -> usize {
        let Some(broker_url) = self.cli.mqtt_broker.clone() else {
            return 0;
        };
        let client_id = format!("{}_purge_{}", self.cli.mqtt_client_id(), unix_millis());
        let mqtt_options = match self.create_mqtt_options(&broker_url, &client_id, &self.cli, false)
        {
            Ok(opts) => opts,
            Err(e) => {
                error!("Failed to create MQTT options for purge: {e}");
                return 0;
            }
        };

        let root_topic = self.mqtt_root_topic.clone();
        let mut subs = HashSet::new();
        for tpl in [
            Some(self.mqtt_event_topic.as_str()),
            self.mqtt_message_topic.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if !tpl.is_empty() {
                subs.insert(tpl_to_wildcard(tpl, &root_topic));
            }
        }
        let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &root_topic);
        let timeout = self.cli.scavenger_timeout_secs();

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 64);
        for sub in &subs {
            if let Err(e) = client.subscribe(sub, rumqttc::QoS::AtLeastOnce).await {
                error!("Purge subscription failed for {sub}: {e}");
            } else {
                debug!("Purge subscribed to {sub}");
            }
        }

        // Phase 1: collect every retained topic. We don't clear inline here —
        // see `clear_and_flush` for why awaiting a publish inside the receive
        // loop deadlocks once the request channel fills.
        let mut topics: HashSet<String> = HashSet::new();
        let mut deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => break,
                notification = eventloop.poll() => {
                    match notification {
                        Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                            // Skip non-retained, the bridge-config topic, and our
                            // own empty clears echoed back to us.
                            if !p.retain || p.payload.is_empty() || p.topic == config_topic {
                                continue;
                            }
                            topics.insert(p.topic.clone());
                            // Keep the window open while retained keep arriving.
                            deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
                        }
                        Err(e) => {
                            warn!("Purge connection error: {e}");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        // Phase 2: clear every collected topic, flushing without blocking.
        let topics: Vec<String> = topics.into_iter().collect();
        debug!("Purge topics={topics:?}");
        let acked = Self::clear_and_flush(&client, &mut eventloop, &topics).await;
        info!(
            "Purge: collected {} retained, cleared {acked}",
            topics.len()
        );
        let _ = client.disconnect().await;
        acked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scav_target(
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
    ) -> crate::types::ScavengerTarget {
        crate::types::ScavengerTarget {
            id: id.into(),
            name: name.map(Into::into),
            cid: cid.map(Into::into),
        }
    }

    fn topic_vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn scavenge_id_in_topic_decides_alone() {
        let target = [scav_target("dev-a", Some("kitchen"), Some("c1"))];
        let own = topic_vars(&[("id", "dev-a")]);
        assert!(retained_belongs_to_targets(Some(&own), "{}", &target));
        let sibling = topic_vars(&[("id", "dev-b"), ("name", "kitchen"), ("cid", "c1")]);
        assert!(!retained_belongs_to_targets(
            Some(&sibling),
            r#"{"id":"dev-a","name":"kitchen","cid":"c1"}"#,
            &target
        ));
    }

    #[test]
    fn scavenge_name_or_cid_in_topic_falls_back_to_payload_id_only() {
        let target = [scav_target("dev-a", Some("kitchen"), Some("c1"))];
        for vars in [
            topic_vars(&[("name", "kitchen")]),
            topic_vars(&[("cid", "c1")]),
        ] {
            assert!(retained_belongs_to_targets(Some(&vars), "{}", &target));
        }
        for vars in [
            topic_vars(&[("name", "hall")]),
            topic_vars(&[("cid", "c2")]),
        ] {
            assert!(!retained_belongs_to_targets(
                Some(&vars),
                r#"{"21":"kitchen","22":"c1"}"#,
                &target
            ));
            // The same device under a name it had before a rename.
            assert!(retained_belongs_to_targets(
                Some(&vars),
                r#"{"id":"dev-a"}"#,
                &target
            ));
        }
    }

    #[test]
    fn scavenge_without_identifier_in_topic_relies_on_payload() {
        let target = [scav_target("dev-a", Some("kitchen"), Some("c1"))];
        let literal = topic_vars(&[("type", "state")]);
        for vars in [None, Some(&literal)] {
            for payload in [
                r#"{"id":"dev-a"}"#,
                r#"{"name":"kitchen"}"#,
                r#"{"cid":"c1"}"#,
            ] {
                assert!(
                    retained_belongs_to_targets(vars, payload, &target),
                    "{payload}"
                );
            }
            // Quoted match only: an id inside a longer string is someone else's.
            assert!(!retained_belongs_to_targets(
                vars,
                r#"{"id":"dev-ab"}"#,
                &target
            ));
        }
    }
}
