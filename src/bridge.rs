use anyhow::Result;
use futures_util::StreamExt;
use log::{debug, error, info, trace, warn};
use regex::Regex;
use rustuya::{Device, DeviceBuilder};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;

use crate::config::{Cli, DeviceConfig, load_state};
use crate::error::BridgeError;
use crate::types::{ApiResponse, BridgeRequest};

pub const MQTT_CHANNEL_CAPACITY: usize = 100;
pub const INITIAL_RETRY_DELAY_SECS: u64 = 10;
pub const MAX_RETRY_DELAY_SECS: u64 = 1280;
pub const LISTENER_TIMEOUT_SECS: u64 = 300;
pub const REQUEST_TIMEOUT_SECS: u64 = 15;
pub const SCAVENGER_TIMEOUT_SECS: u64 = 1;

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

#[derive(Debug, Clone)]
pub struct MqttMessage {
    pub topic: String,
    pub payload: String,
    pub retain: bool,
}

#[derive(Default)]
struct TopicVars<'a> {
    id: &'a str,
    name: Option<&'a str>,
    cid: Option<&'a str>,
    level: Option<&'a str>,
    event_type: Option<&'a str>,
    dp: Option<&'a str>,
    val: Option<&'a Value>,
    dps_str: Option<&'a str>,
}

#[derive(Default)]
pub struct BridgeState {
    pub configs: HashMap<String, DeviceConfig>,
    pub instances: HashMap<String, Device>,
    /// Mapping from (`parent_id`, `cid`) to sub-device ID
    pub cid_map: HashMap<(String, String), String>,
    /// Mapping from name to device IDs
    pub name_map: HashMap<String, Vec<String>>,
}

pub struct BridgeContext {
    pub cli: Cli,
    pub mqtt_tx: Option<mpsc::Sender<Option<MqttMessage>>>,
    pub mqtt_root_topic: String,
    pub mqtt_event_topic: String,
    pub mqtt_retain: bool,
    pub mqtt_message_topic: Option<String>,
    pub mqtt_payload_template: Option<String>,
    pub mqtt_scanner_topic: Option<String>,
    pub state_file: String,
    pub save_debounce_secs: u64,
    pub state: RwLock<BridgeState>,
    pub save_tx: mpsc::Sender<()>,
    pub refresh_tx: mpsc::Sender<()>,
    pub scavenger_tx:
        tokio::sync::Mutex<Option<mpsc::UnboundedSender<Vec<crate::types::ScavengerTarget>>>>,
    pub cancel: tokio_util::sync::CancellationToken,
}

/// Topic-template variable keys recognised by [`tpl_to_wildcard`], [`compile_topic_regex`],
/// and [`BridgeContext::replace_vars`]. The `value`, `dps`, `timestamp`, and `root` keys are
/// handled separately because they require values rather than wildcard substitution.
const TOPIC_WILDCARD_KEYS: &[&str] = &["id", "name", "dp", "action", "cid", "type", "level"];

/// Walks a template string of the form `"foo/{id}/bar"`, calling `substitute(key, &mut out)`
/// for each `{key}` placeholder. The callback returns `true` to consume the placeholder or
/// `false` to leave it untouched (the literal `{key}` is then emitted).
fn render_template<F>(template: &str, mut substitute: F) -> String
where
    F: FnMut(&str, &mut String) -> bool,
{
    let mut res = String::with_capacity(template.len() + 32);
    let mut last = 0;
    while let Some(start) = template[last..].find('{') {
        let actual_start = last + start;
        res.push_str(&template[last..actual_start]);
        let Some(end) = template[actual_start..].find('}') else {
            break;
        };
        let actual_end = actual_start + end;
        let key = &template[actual_start + 1..actual_end];
        if substitute(key, &mut res) {
            last = actual_end + 1;
        } else {
            res.push('{');
            last = actual_start + 1;
        }
    }
    res.push_str(&template[last..]);
    res
}

/// Converts MQTT template to wildcard for subscription
#[must_use]
pub fn tpl_to_wildcard(template: &str, root_topic: &str) -> String {
    render_template(template, |key, out| match key {
        "root" => {
            out.push_str(root_topic);
            true
        }
        k if TOPIC_WILDCARD_KEYS.contains(&k) => {
            out.push('+');
            true
        }
        _ => false,
    })
}

/// Compiles a template into a Regex if it contains variables
#[must_use]
pub fn compile_topic_regex(template: &str) -> Option<Regex> {
    if !template.contains('{') {
        return None;
    }

    // Convert template to regex. E.g. "tuya/{id}/command" -> "^tuya/(?P<id>[^/]+)/command$"
    let mut pattern = regex::escape(template);
    for key in TOPIC_WILDCARD_KEYS {
        pattern = pattern.replace(&format!(r"\{{{key}\}}"), &format!(r"(?P<{key}>[^/]+)"));
    }

    Regex::new(&format!("^{pattern}$")).ok()
}

/// Matches MQTT topic against precompiled regex or template
#[must_use]
pub fn match_topic(
    topic: &str,
    template: &str,
    re: Option<&Regex>,
) -> Option<HashMap<String, String>> {
    if let Some(re) = re {
        let caps = re.captures(topic)?;
        let mut vars = HashMap::new();
        for name in re.capture_names().flatten() {
            if let Some(m) = caps.name(name) {
                vars.insert(name.to_string(), m.as_str().to_string());
            }
        }
        Some(vars)
    } else {
        (topic == template).then(HashMap::new)
    }
}
async fn verify_write_permission(state_file: &str) -> Result<()> {
    let path = Path::new(state_file);

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            anyhow::anyhow!(
                "Cannot create directory for state file ({}): {e}",
                parent.display()
            )
        })?;
    }

    let test_path = path.with_extension("test_write");

    tokio::fs::write(&test_path, b"").await.map_err(|e| {
        anyhow::anyhow!(
            "No write permission for state file ({state_file}). Check directory permissions: {e}"
        )
    })?;

    let _ = tokio::fs::remove_file(&test_path).await;
    Ok(())
}

impl BridgeContext {
    /// Constructs a new bridge context. Verifies write permission to the state
    /// file, then loads existing device configs and prepares background channels.
    ///
    /// # Errors
    /// Returns an error if the state file directory is not writable.
    pub async fn new(
        cli: &Cli,
    ) -> Result<(
        Arc<Self>,
        mpsc::Receiver<Option<MqttMessage>>,
        mpsc::Receiver<()>,
        mpsc::Receiver<()>,
    )> {
        verify_write_permission(cli.state_file()).await?;

        let (_, mqtt_event_topic) = cli.mqtt_topics();
        let initial_configs = load_state(cli.state_file()).await;

        let mut initial_instances = HashMap::new();
        let mut initial_name_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut initial_cid_map = HashMap::new();

        for (id, cfg) in &initial_configs {
            if let Some(cid) = &cfg.cid
                && let Some(parent_id) = &cfg.parent_id
            {
                initial_cid_map.insert((parent_id.clone(), cid.clone()), id.clone());
            } else if let Some(key) = &cfg.key {
                let dev = DeviceBuilder::new(id, key.as_bytes().to_vec())
                    .address(cfg.ip.as_deref().unwrap_or("Auto"))
                    .version(
                        cfg.version
                            .as_deref()
                            .and_then(|s| s.parse::<rustuya::Version>().ok())
                            .unwrap_or_default(),
                    )
                    .nowait(true)
                    .run();
                initial_instances.insert(id.clone(), dev);
            } else {
                error!("Device {id} is invalid: missing key or parent info");
            }

            if let Some(name) = &cfg.name {
                initial_name_map
                    .entry(name.clone())
                    .or_default()
                    .push(id.clone());
            }
        }

        let (mqtt_tx_sender, mqtt_tx_receiver) = mpsc::channel(MQTT_CHANNEL_CAPACITY);
        let (save_tx, save_rx) = mpsc::channel(1);
        let (refresh_tx, refresh_rx) = mpsc::channel(1);

        let ctx = Arc::new(Self {
            cli: cli.clone(),
            mqtt_tx: cli.mqtt_broker.is_some().then_some(mqtt_tx_sender),
            mqtt_root_topic: cli
                .mqtt_root_topic
                .clone()
                .unwrap_or_else(|| crate::config::DEFAULT_MQTT_ROOT_TOPIC.to_string()),
            mqtt_event_topic,
            mqtt_retain: cli.mqtt_retain(),
            mqtt_message_topic: cli.mqtt_message_topic.clone(),
            mqtt_payload_template: cli.mqtt_payload_template.clone(),
            mqtt_scanner_topic: cli.mqtt_scanner_topic.clone(),
            state_file: cli.state_file().to_string(),
            save_debounce_secs: cli.save_debounce_secs(),
            state: RwLock::new(BridgeState {
                configs: initial_configs,
                instances: initial_instances,
                cid_map: initial_cid_map,
                name_map: initial_name_map,
            }),
            save_tx,
            refresh_tx,
            scavenger_tx: tokio::sync::Mutex::new(None),
            cancel: tokio_util::sync::CancellationToken::new(),
        });

        Ok((ctx, mqtt_tx_receiver, save_rx, refresh_rx))
    }

    /// Starts state saver task with debounce
    pub fn spawn_state_saver(
        self: Arc<Self>,
        mut save_rx: mpsc::Receiver<()>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    res = save_rx.recv() => {
                        if res.is_none() { break; }
                        tokio::select! {
                            () = cancel.cancelled() => {
                                while save_rx.try_recv().is_ok() {} // Drain pending requests
                                if let Err(e) = self.save_state().await {
                                    error!("Save failed during shutdown: {e}");
                                }
                                break;
                            }
                            () = tokio::time::sleep(Duration::from_secs(self.save_debounce_secs)) => {
                                while save_rx.try_recv().is_ok() {} // Drain pending requests
                                if let Err(e) = self.save_state().await {
                                    error!("Save failed: {e}");
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Starts device event listener task
    pub fn spawn_device_listener(
        self: Arc<Self>,
        mut refresh_rx: mpsc::Receiver<()>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let instances = {
                    let state = self.state.read().await;
                    state.instances.values().cloned().collect::<Vec<_>>()
                };

                if instances.is_empty() {
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        res = refresh_rx.recv() => {
                            if res.is_none() { return; }
                            continue;
                        }
                    }
                }

                let mut stream = rustuya::device::unified_listener(instances.clone());
                debug!("Started unified listener for {} devices", instances.len());
                loop {
                    tokio::select! {
                        () = cancel.cancelled() => return,
                        res = tokio::time::timeout(Duration::from_secs(LISTENER_TIMEOUT_SECS), stream.next()) => {
                            match res {
                                Ok(Some(event_res)) => {
                                    if let Ok(event) = event_res {
                                        let Some(payload_str) = event.message.payload_as_string() else {
                                            warn!("Non-UTF8 payload from {}", event.device_id);
                                            continue;
                                        };
                                        trace!("Raw event from {}: {payload_str}", event.device_id);
                                        let payload: Value = serde_json::from_str(&payload_str)
                                            .unwrap_or_else(|_| Value::String(payload_str.clone()));
                                        let (target_id, name, cid, exists) = self.resolve_event_target(&event.device_id, &payload).await;

                                // Check for 'dps' at root or inside 'data' (for sub-devices/gateways)
                                let payload_obj = payload.as_object();

                                // Check for error messages
                                if payload_obj.is_some_and(|o| o.contains_key("errorCode") || o.contains_key("errorMsg")) {
                                    if let Some(n) = name.as_ref() {
                                        info!("Device {target_id} ({n}) reported error: {payload}");
                                    } else {
                                        info!("Device {target_id} reported error: {payload}");
                                    }
                                    // Update last_error_code in device config
                                    let error_code = payload.get("errorCode")
                                        .and_then(Value::as_u64)
                                        .and_then(|v| u32::try_from(v).ok());
                                    if let Some(code) = error_code {
                                        let mut state = self.state.write().await;
                                        if let Some(cfg) = state.configs.get_mut(&target_id) {
                                            cfg.last_error_code = Some(code);
                                        }
                                    }
                                    self.publish_device_message(&target_id, name.as_deref(), cid.as_deref(), "error", payload, self.mqtt_retain).await;
                                    continue;
                                }

                                let root_dps = payload_obj.and_then(|o| o.get("dps"));
                                let data_dps = payload_obj.and_then(|o| o.get("data"))
                                    .and_then(|d| d.as_object())
                                    .and_then(|do_| do_.get("dps"));

                                let is_passive = root_dps.is_none() && data_dps.is_none();
                                let mut dps = root_dps.or(data_dps).cloned().unwrap_or_else(|| {
                                    if let Some(obj) = payload.as_object()
                                        && let Some(data) = obj.get("data") {
                                        let mut data_val = data.clone();
                                        if let Some(data_obj) = data_val.as_object_mut()
                                            && let Some(inner) = data_obj.get("data").cloned()
                                            && let Some(inner_obj) = inner.as_object() {
                                            for (k, v) in inner_obj {
                                                data_obj.entry(k.clone()).or_insert(v.clone());
                                            }
                                            data_obj.remove("data");
                                        }
                                        data_val
                                    } else {
                                        payload.clone()
                                    }
                                });

                                // If target is parent (gateway) but CID was present, include CID in dps for visibility
                                if target_id == event.device_id && let Some(c) = &cid
                                    && let Some(obj) = dps.as_object_mut() {
                                    obj.insert("cid".to_string(), Value::String(c.clone()));
                                }

                                        self.publish_device_event(target_id, name, cid, dps, is_passive, exists).await;
                                    }
                                }
                                Ok(None) => {
                                    warn!("Device listener stream ended");
                                    break;
                                }
                                Err(_) => {
                                    info!("Device listener timeout after {LISTENER_TIMEOUT_SECS}s (no events received)");
                                }
                            }
                        }
                        _ = refresh_rx.recv() => break, // Refresh listener on device changes
                    }
                }
            }
        })
    }

    /// Resolves target device ID and name from raw device event and payload
    async fn resolve_event_target(
        &self,
        parent_id: &str,
        payload: &Value,
    ) -> (String, Option<String>, Option<String>, bool) {
        // Extract CID (sub-device) from payload
        let cid = payload
            .as_object()
            .and_then(|o| {
                // Check for root "cid" or nested "data" -> "cid" (as seen in tuya2mqtt.py)
                o.get("cid").or_else(|| {
                    o.get("data")
                        .and_then(|d| d.as_object())
                        .and_then(|do_| do_.get("cid"))
                })
            })
            .and_then(|c| c.as_str())
            .map(ToString::to_string);

        let (target_id, name, exists) = {
            let state = self.state.read().await;
            let target_id = cid.as_ref().map_or_else(
                || parent_id.to_string(),
                |c| {
                    state
                        .cid_map
                        .get(&(parent_id.to_string(), c.clone()))
                        .cloned()
                        .unwrap_or_else(|| parent_id.to_string())
                },
            );
            let (name, exists) = state
                .configs
                .get(&target_id)
                .map_or((None, false), |c| (c.name.clone(), true));
            drop(state);
            (target_id, name, exists)
        };

        (target_id, name, cid, exists)
    }

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

        let handle = tokio::spawn(async move {
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            let mut next_retry: Option<Instant> = None;
            loop {
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
                    notification = eventloop.poll(), if next_retry.is_none() => {
                        match notification {
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                                info!("Connected to MQTT broker");
                                retry_delay = INITIAL_RETRY_DELAY_SECS;
                                if let Err(e) = client.subscribe(&sub_topic, rumqttc::QoS::AtLeastOnce).await {
                                    error!("Subscription failed: {e}");
                                } else {
                                    info!("Subscribed to: {sub_topic}");
                                }
                                if let Err(e) = client.subscribe(&config_topic, rumqttc::QoS::AtLeastOnce).await {
                                    error!("Config subscription failed: {e}");
                                }
                            }
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                let payload = String::from_utf8_lossy(&p.payload);
                                debug!("MQTT Received: [{}] {payload}", p.topic);

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
                                let req_val = Self::parse_mqtt_payload(&payload, &vars);

                                let requests = if let Some(arr) = req_val.as_array() {
                                    arr.iter()
                                        .filter_map(|v| serde_json::from_value::<BridgeRequest>(v.clone()).ok())
                                        .collect::<Vec<_>>()
                                } else if let Ok(req) = serde_json::from_value::<BridgeRequest>(req_val) {
                                    vec![req]
                                } else {
                                    vec![]
                                };

                                for req in requests {
                                    let ctx_h = self.clone();
                                    tokio::spawn(async move {
                                        let res = crate::handlers::handle_request(ctx_h.clone(), req).await;
                                        // Suppress response for successful set/get actions to avoid redundancy
                                        if res.status != crate::types::Status::Ok || !matches!(res.action.as_deref(), Some("set" | "get")) {
                                            ctx_h.publish_api_response(res).await;
                                        }
                                    });
                                }
                            }
                            Err(e) => {
                                error!("MQTT Error: {e}. Retrying in {retry_delay}s...");
                                let jitter = u64::try_from(unix_millis() % 1000).unwrap_or(0);
                                next_retry = Some(Instant::now() + Duration::from_secs(retry_delay) + Duration::from_millis(jitter));
                                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_SECS);
                            }
                            _ => {}
                        }
                    }
                    msg_opt = mqtt_tx_receiver.recv() => {
                        if let Some(Some(msg)) = msg_opt {
                            debug!("MQTT Publish: [{}] {}", msg.topic, msg.payload);
                            if let Err(e) = client.publish(msg.topic, rumqttc::QoS::AtLeastOnce, msg.retain, msg.payload).await {
                                error!("Publish failed: {e}");
                            }
                        } else {
                                debug!("MQTT shutdown signal received, clearing and disconnecting...");

                                // Clear bridge config retain topic
                                let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
                                let expected_pubacks = 1;
                                let _ = client.publish(config_topic, rumqttc::QoS::AtLeastOnce, true, "").await;

                                debug!("Waiting for {expected_pubacks} PubAcks before disconnecting...");

                                // 3. Poll eventloop until all PubAcks are received (with timeout)
                                let mut received_pubacks: usize = 0;
                                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                                loop {
                                    if received_pubacks >= expected_pubacks {
                                        break;
                                    }
                                    if tokio::time::Instant::now() >= deadline {
                                        warn!("Timed out waiting for PubAcks ({received_pubacks}/{expected_pubacks} received)");
                                        break;
                                    }
                                    match tokio::time::timeout_at(deadline, eventloop.poll()).await {
                                        Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))) => {
                                            received_pubacks += 1;
                                        }
                                        Ok(Err(e)) => {
                                            warn!("MQTT error while waiting for PubAck: {e}");
                                            break;
                                        }
                                        Err(_) => {
                                            warn!("Timed out waiting for PubAcks");
                                            break;
                                        }
                                        _ => {}
                                    }
                                }

                                debug!("PubAck flush done ({received_pubacks}/{expected_pubacks}).");

                                // 4. Disconnect
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

    pub async fn publish_bridge_config(&self, cli: Option<&crate::config::Cli>, clear: bool) {
        let topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
        let payload = if clear {
            String::new()
        } else {
            cli.map_or_else(
                || "{}".to_string(),
                |c| serde_json::to_string(c).unwrap_or_else(|_| "{}".to_string()),
            )
        };
        self.try_send_mqtt(Some(MqttMessage {
            topic,
            payload,
            retain: true,
        }))
        .await;
    }

    pub async fn shutdown_mqtt(&self) {
        if let Some(tx) = &self.mqtt_tx {
            let _ = tx.send(None).await;
        }
    }

    /// Signals MQTT to shutdown (synchronous version for Drop)
    pub fn signal_shutdown_mqtt(&self) {
        if let Some(tx) = &self.mqtt_tx {
            let _ = tx.try_send(None);
        }
    }

    /// Fully closes the bridge context and cleans up resources
    pub async fn close(&self) {
        info!("Closing bridge context...");

        // Drop all Device instances first so the rustuya library stops its
        // internal background scanning/reconnection tasks before we proceed.
        {
            let mut state = self.state.write().await;
            state.instances.clear();
        }

        self.cancel.cancel();
        self.shutdown_mqtt().await;
        let _ = self.save_state().await;
    }

    async fn try_send_mqtt(&self, msg: Option<MqttMessage>) {
        if let Some(tx) = &self.mqtt_tx
            && tokio::time::timeout(Duration::from_millis(500), tx.send(msg))
                .await
                .is_err()
        {
            warn!("MQTT queue full, dropping message");
        }
    }

    /// Briefly subscribes to the bridge config topic and returns an error if another
    /// bridge instance is detected (its retained config is observed within 500ms).
    ///
    /// # Errors
    /// Returns an error if the MQTT subscription fails or if a duplicate instance is detected.
    pub async fn check_existing_instance(&self) -> Result<()> {
        let Some(broker_url) = &self.cli.mqtt_broker else {
            return Ok(());
        };
        let client_id = format!("{}_check", self.cli.mqtt_client_id());
        let opts = self.create_mqtt_options(broker_url, &client_id, &self.cli, false)?;
        let (client, mut eventloop) = rumqttc::AsyncClient::new(opts, 10);
        let topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
        client.subscribe(&topic, rumqttc::QoS::AtLeastOnce).await?;

        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, eventloop.poll()).await {
            if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = event {
                let payload = String::from_utf8_lossy(&p.payload);
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&payload)
                    && val.get("session_id").is_some()
                {
                    anyhow::bail!(
                        "Duplicate instance detected. Another bridge is already running."
                    );
                }
            }
        }
        let _ = client.disconnect().await;
        Ok(())
    }

    /// Creates MQTT options from broker URL
    fn create_mqtt_options(
        &self,
        broker_url: &str,
        client_id: &str,
        cli: &Cli,
        with_lwt: bool,
    ) -> Result<rumqttc::MqttOptions> {
        let mut opts = if broker_url.contains("://") {
            let url = url::Url::parse(broker_url)?;
            let host = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing host in broker URL"))?;
            let is_ssl = matches!(url.scheme(), "mqtts" | "ssl");
            let port = url.port().unwrap_or(if is_ssl { 8883 } else { 1883 });

            let mut opts = rumqttc::MqttOptions::new(client_id, host, port);

            // Priority: Cli fields > URL components
            if let Some(user) = &cli.mqtt_user {
                opts.set_credentials(user, cli.mqtt_password.as_deref().unwrap_or(""));
            } else if !url.username().is_empty() {
                opts.set_credentials(url.username(), url.password().unwrap_or(""));
            }

            if is_ssl {
                opts.set_transport(rumqttc::Transport::tls(Vec::new(), None, None));
            }
            opts
        } else {
            let parts: Vec<&str> = broker_url.split(':').collect();
            let host = parts[0];
            let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1883);
            let mut opts = rumqttc::MqttOptions::new(client_id, host, port);
            if let Some(user) = &cli.mqtt_user {
                opts.set_credentials(user, cli.mqtt_password.as_deref().unwrap_or(""));
            }
            opts
        };

        opts.set_keep_alive(Duration::from_secs(30));

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

    /// Parses MQTT payload into a [`BridgeRequest`] value, merging topic variables.
    fn parse_mqtt_payload(payload: &str, vars: &HashMap<String, String>) -> Value {
        let mut val = serde_json::from_str::<Value>(payload)
            .unwrap_or_else(|_| Value::String(payload.to_string()));

        // If it's an array, merge topic variables into each element
        if let Some(arr) = val.as_array_mut() {
            for item in arr {
                if let Some(obj) = item.as_object_mut() {
                    for (k, v) in vars {
                        if !obj.contains_key(k) {
                            obj.insert(k.clone(), Value::String(v.clone()));
                        }
                    }
                    // Heuristic for 'set' action inside array items
                    if obj.get("action").and_then(Value::as_str) == Some("set") {
                        Self::apply_set_heuristic(obj);
                    }
                }
            }
            return val;
        }

        // If it's not an object (and not an array), wrap it so we can merge topic variables
        if !val.is_object() {
            let mut obj = serde_json::Map::new();
            if let Some(dp) = vars.get("dp") {
                // If we have a {dp} in topic, the payload is the value for that DP
                let mut dps = serde_json::Map::new();
                dps.insert(dp.clone(), val);
                obj.insert("dps".to_string(), Value::Object(dps));
            } else {
                // Otherwise, keep it as a generic payload field
                obj.insert("payload".to_string(), val);
            }
            val = Value::Object(obj);
        }

        if let Some(obj) = val.as_object_mut() {
            // Merge topic variables (id, name, cid, action, dp etc)
            for (k, v) in vars {
                if !obj.contains_key(k) {
                    obj.insert(k.clone(), Value::String(v.clone()));
                }
            }

            // Heuristic for tuya2mqtt compatibility and simple set commands:
            // If action is 'set', ensure we have a 'dps' object
            if obj.get("action").and_then(Value::as_str) == Some("set") {
                Self::apply_set_heuristic(obj);
            }
        }
        val
    }

    /// Helper to apply 'set' command heuristic to an object
    fn apply_set_heuristic(obj: &mut serde_json::Map<String, Value>) {
        if !obj.contains_key("dps") && !obj.contains_key("data") {
            let mut dps = obj.clone();
            dps.remove("id");
            dps.remove("name");
            dps.remove("cid");
            dps.remove("action");
            dps.remove("dp");
            dps.remove("payload");
            obj.insert("dps".to_string(), Value::Object(dps));
        }
    }

    /// Atomically saves device configuration to state file
    ///
    /// # Errors
    /// Returns an error if the state file cannot be serialized, written, or atomically renamed.
    pub async fn save_state(&self) -> Result<()> {
        let json = {
            let state = self.state.read().await;
            serde_json::to_string_pretty(&state.configs)?
        };

        let path = Path::new(&self.state_file);
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_path = path.with_extension("tmp");
        tokio::fs::write(&tmp_path, json).await?;

        if let Err(e) = tokio::fs::rename(&tmp_path, path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            anyhow::bail!("Failed to commit state file {}: {}", self.state_file, e);
        }

        info!("State persisted to {}", self.state_file);
        Ok(())
    }

    /// Triggers a debounced state save
    pub fn request_save(&self) {
        let _ = self.save_tx.try_send(());
    }

    /// Triggers a refresh of the device listener
    pub fn request_refresh(&self) {
        let _ = self.refresh_tx.try_send(());
    }

    /// Finds device IDs by ID or Name (ID has priority)
    pub async fn find_device_ids(
        &self,
        id: Option<Vec<String>>,
        name: Option<Vec<String>>,
    ) -> Vec<String> {
        let state = self.state.read().await;

        // 1. Match by ID
        if let Some(ids) = id {
            return ids
                .into_iter()
                .filter(|id_val| state.configs.contains_key(id_val))
                .collect();
        }

        // 2. Match by Name
        if let Some(names) = name {
            let mut results = Vec::new();
            for name_val in names {
                if let Some(ids) = state.name_map.get(&name_val) {
                    results.extend(ids.clone());
                }
            }
            results.sort();
            results.dedup();
            return results;
        }

        vec![]
    }

    /// Generates MQTT topics and payloads based on templates
    #[allow(
        clippy::literal_string_with_formatting_args,
        reason = "MQTT topic template uses literal `{dp}`/`{value}` placeholders, not format args"
    )]
    fn generate_device_templates(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: &Value,
        is_passive: bool,
    ) -> Vec<(String, String)> {
        let dps_str = dps.to_string();
        let event_type = if is_passive { "passive" } else { "active" };

        let replace_vars_local = |s: &str, dp: Option<&str>, val: Option<&Value>| {
            self.replace_vars(
                s,
                &TopicVars {
                    id,
                    name,
                    cid,
                    event_type: Some(event_type),
                    dp,
                    val,
                    dps_str: Some(&dps_str),
                    ..Default::default()
                },
            )
        };

        let mut templates = Vec::new();
        let tpl = &self.mqtt_event_topic;

        if tpl.contains("{dp}") || tpl.contains("{value}") {
            if let Some(dps_obj) = dps.as_object() {
                for (dp, val) in dps_obj {
                    let topic = replace_vars_local(tpl, Some(dp), Some(val));
                    let p_tpl = self.mqtt_payload_template.as_deref().unwrap_or("{value}");
                    let payload = replace_vars_local(p_tpl, Some(dp), Some(val));
                    templates.push((topic, payload));
                }
            }
        } else {
            let topic = replace_vars_local(tpl, None, None);
            let p_tpl = self.mqtt_payload_template.as_deref().unwrap_or("{value}");
            let payload = replace_vars_local(p_tpl, None, None);
            templates.push((topic, payload));
        }

        templates
    }

    /// Publishes device event using MQTT templates
    pub async fn publish_device_event(
        &self,
        id: String,
        name: Option<String>,
        cid: Option<String>,
        dps: Value,
        is_passive: bool,
        exists: bool,
    ) {
        if dps.is_null() || dps.as_str().is_some_and(str::is_empty) {
            return;
        }

        debug!("Device Event: [{id}] {dps}");

        if self.mqtt_tx.is_some() {
            // Check if device still exists before publishing to avoid race conditions during removal
            if !exists {
                return;
            }

            let templates = self.generate_device_templates(
                &id,
                name.as_deref(),
                cid.as_deref(),
                &dps,
                is_passive,
            );
            if templates.is_empty() {
                return;
            }

            for (topic, payload) in templates {
                let mut retain = self.mqtt_retain;
                if retain && !topic.contains(&id) && !payload.contains(&id) {
                    warn!(
                        "Ignoring retain for event from '{id}' because '{id}' is missing from both topic and payload"
                    );
                    retain = false;
                }
                self.try_send_mqtt(Some(MqttMessage {
                    topic,
                    payload,
                    retain,
                }))
                .await;
            }
        }
    }

    /// Publishes a message for a specific device (e.g. error messages)
    pub async fn publish_device_message(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        level: &str,
        mut payload: Value,
        retain: bool,
    ) {
        if self.mqtt_tx.is_some() {
            let topic = self.replace_vars(
                self.mqtt_message_topic.as_deref().unwrap_or_default(),
                &TopicVars {
                    id,
                    name,
                    cid,
                    level: Some(level),
                    ..Default::default()
                },
            );

            if let Some(obj) = payload.as_object_mut() {
                obj.insert("id".to_string(), id.into());
                if let Some(n) = name {
                    obj.insert("name".to_string(), n.into());
                }
                if let Some(c) = cid {
                    obj.insert("cid".to_string(), c.into());
                }
            }

            let payload_str = payload.to_string();
            let mut final_retain = retain;
            if final_retain && !topic.contains(id) && !payload_str.contains(id) {
                warn!(
                    "Ignoring retain for message from '{id}' because '{id}' is missing from both topic and payload"
                );
                final_retain = false;
            }
            self.try_send_mqtt(Some(MqttMessage {
                topic,
                payload: payload_str,
                retain: final_retain,
            }))
            .await;
        }
    }

    /// Publishes an [`ApiResponse`] to the appropriate topic
    pub async fn publish_api_response(&self, response: ApiResponse) {
        let level = match response.status {
            crate::types::Status::Ok => "response",
            crate::types::Status::Error => "error",
        };
        let id = response.id.as_deref().unwrap_or("bridge");
        let name = (id == "bridge").then_some("bridge");

        match serde_json::to_value(&response) {
            Ok(payload) => {
                self.publish_device_message(id, name, None, level, payload, false)
                    .await;
            }
            Err(e) => {
                error!("Failed to serialize ApiResponse: {e}");
            }
        }
    }

    /// Helper to replace template variables in a string
    fn replace_vars(&self, template: &str, vars: &TopicVars<'_>) -> String {
        use std::fmt::Write;

        render_template(template, |key, out| match key {
            "root" => {
                out.push_str(&self.mqtt_root_topic);
                true
            }
            "id" => {
                out.push_str(vars.id);
                true
            }
            "name" => {
                out.push_str(vars.name.unwrap_or(""));
                true
            }
            "cid" => {
                out.push_str(vars.cid.unwrap_or(""));
                true
            }
            "level" => vars.level.is_some_and(|v| {
                out.push_str(v);
                true
            }),
            "type" => vars.event_type.is_some_and(|v| {
                out.push_str(v);
                true
            }),
            "dp" => vars.dp.is_some_and(|v| {
                out.push_str(v);
                true
            }),
            "value" => {
                if let Some(val) = vars.val {
                    let _ = write!(out, "{val}");
                    true
                } else if let Some(dps_str) = vars.dps_str {
                    out.push_str(dps_str);
                    true
                } else {
                    false
                }
            }
            "dps" => vars.dps_str.is_some_and(|s| {
                out.push_str(s);
                true
            }),
            "timestamp" => {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let _ = write!(out, "{ts}");
                true
            }
            _ => false,
        })
    }

    /// Publishes bridge-level messages (e.g. scanner results)
    pub async fn publish_scanner_event(&self, payload: Value) {
        if payload.is_null() || payload.as_str().is_some_and(str::is_empty) {
            return;
        }
        debug!("Scanner Event: {payload}");
        if self.mqtt_tx.is_some() {
            let topic = if let Some(topic) = &self.mqtt_scanner_topic {
                self.replace_vars(
                    topic,
                    &TopicVars {
                        id: "bridge",
                        name: Some("bridge"),
                        ..Default::default()
                    },
                )
            } else {
                self.replace_vars(
                    self.mqtt_message_topic.as_deref().unwrap_or_default(),
                    &TopicVars {
                        id: "bridge",
                        name: Some("bridge"),
                        level: Some("scanner"),
                        ..Default::default()
                    },
                )
            };
            self.try_send_mqtt(Some(MqttMessage {
                topic,
                payload: payload.to_string(),
                retain: false,
            }))
            .await;
        }
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

        let event_tpl = templates[0].clone();
        let msg_tpl = templates[1].clone();
        let event_re = compile_topic_regex(&event_tpl);
        let msg_re = compile_topic_regex(&msg_tpl);

        tokio::spawn(async move {
            let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);

            for sub in &subs {
                if let Err(e) = client.subscribe(sub, rumqttc::QoS::AtLeastOnce).await {
                    error!("Scavenger subscription failed for {sub}: {e}");
                } else {
                    debug!("Scavenger subscribed to {sub}");
                }
            }

            let mut active_targets = targets;
            let mut deadline =
                tokio::time::Instant::now() + Duration::from_secs(SCAVENGER_TIMEOUT_SECS);

            loop {
                tokio::select! {
                    new_targets_opt = rx.recv() => {
                        if let Some(new_targets) = new_targets_opt {
                            let count = new_targets.len();
                            active_targets.extend(new_targets);
                            deadline = tokio::time::Instant::now() + Duration::from_secs(SCAVENGER_TIMEOUT_SECS);
                            debug!("Scavenger added {count} new targets, extended timeout");
                        } else {
                            break;
                        }
                    }
                    () = tokio::time::sleep_until(deadline) => {
                        break;
                    }
                    notification = eventloop.poll() => {
                        match notification {
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                if !p.retain {
                                    continue;
                                }

                                let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &root_topic);
                                if p.topic == config_topic {
                                    continue;
                                }

                                let mut should_clear = false;

                                // 1. Precise Topic Match
                                let vars = match_topic(&p.topic, &event_tpl, event_re.as_ref())
                                    .or_else(|| match_topic(&p.topic, &msg_tpl, msg_re.as_ref()));

                                if let Some(vars) = vars {
                                    let v_id = vars.get("id");
                                    let v_name = vars.get("name");
                                    let v_chan = vars.get("cid");

                                    should_clear = active_targets.iter().any(|t| {
                                        let id_hit = v_id.is_some_and(|v| v == &t.id);
                                        let cid_hit = v_chan.is_some() && t.cid.as_ref() == v_chan;
                                        let name_hit = v_name.is_some() && t.name.as_ref() == v_name;
                                        id_hit || cid_hit || name_hit
                                    });
                                }

                                // 2. Exact Payload Match
                                if !should_clear {
                                    let payload = String::from_utf8_lossy(&p.payload);
                                    should_clear = active_targets.iter().any(|t| {
                                        // Exact JSON string value match
                                        let id_match = payload.contains(&format!("\"{}\"", t.id));
                                        let cid_match = t.cid.as_ref().is_some_and(|cid| payload.contains(&format!("\"{cid}\"")));
                                        let name_match = t.name.as_ref().is_some_and(|name| payload.contains(&format!("\"{name}\"")));
                                        id_match || cid_match || name_match
                                    });
                                }

                                if should_clear {
                                    debug!("Scavenger clearing retained message: {}", p.topic);
                                    let _ = client
                                        .publish(&p.topic, rumqttc::QoS::AtLeastOnce, true, "")
                                        .await;
                                }
                            }
                            Err(_) => break, // Connection error
                            _ => {}
                        }
                    }
                }
            }
            debug!("Scavenger finished for {} devices", active_targets.len());
            let _ = client.disconnect().await;
        });
    }

    /// Normalizes [`DeviceConfig`] fields and applies aliases
    fn normalize_config(cfg: &mut DeviceConfig) {
        let normalize = |opt: &mut Option<String>| {
            if let Some(s) = opt
                && (s.is_empty() || s == "Auto")
            {
                *opt = None;
            }
        };

        normalize(&mut cfg.name);
        normalize(&mut cfg.ip);
        normalize(&mut cfg.key);
        normalize(&mut cfg.version);
        normalize(&mut cfg.cid);
        normalize(&mut cfg.parent_id);

        // Validate IP: If it's a public IP, ignore it and use Auto
        if let Some(ip_str) = &cfg.ip
            && let Ok(ip) = ip_str.parse::<std::net::IpAddr>()
        {
            let is_private = match ip {
                std::net::IpAddr::V4(v4) => {
                    v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified()
                }
                std::net::IpAddr::V6(v6) => {
                    v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xff00 == 0xfe00)
                }
            };
            if !is_private {
                cfg.ip = None;
            }
        }
    }

    /// Adds or updates a device configuration
    ///
    /// # Errors
    /// Returns [`BridgeError::InvalidRequest`] when the device has neither
    /// `(cid + parent_id)` nor `key`.
    pub async fn add_device(&self, mut cfg: DeviceConfig) -> Result<ApiResponse, BridgeError> {
        Self::normalize_config(&mut cfg);

        let id = cfg.id.clone();
        let name = cfg.name.clone();

        {
            let mut state = self.state.write().await;

            // Clean up old mappings if device already exists
            let old_mapping = state
                .configs
                .get(&id)
                .map(|cfg| (cfg.name.clone(), cfg.cid.clone(), cfg.parent_id.clone()));

            if let Some((old_name, old_cid, old_parent)) = old_mapping {
                if let Some(n) = old_name
                    && let Some(ids) = state.name_map.get_mut(&n)
                {
                    ids.retain(|x| x != &id);
                    if ids.is_empty() {
                        state.name_map.remove(&n);
                    }
                }
                if let Some(cid) = old_cid
                    && let Some(parent) = old_parent
                {
                    state.cid_map.remove(&(parent, cid));
                }
            }

            if let Some(cid) = &cfg.cid
                && let Some(parent_id) = &cfg.parent_id
            {
                // Register as sub-device
                state
                    .cid_map
                    .insert((parent_id.clone(), cid.clone()), id.clone());
                state.instances.remove(&id);
            } else if let Some(key) = &cfg.key {
                // Register as direct device
                let dev = DeviceBuilder::new(&id, key.as_bytes().to_vec())
                    .address(cfg.ip.as_deref().unwrap_or("Auto"))
                    .version(
                        cfg.version
                            .as_deref()
                            .and_then(|s| s.parse::<rustuya::Version>().ok())
                            .unwrap_or_default(),
                    )
                    .nowait(true)
                    .run();
                state.instances.insert(id.clone(), dev);
            } else {
                return Err(BridgeError::InvalidRequest(
                    "Device must have either (cid & parent_id) for sub-device or (key) for direct device"
                        .to_string(),
                ));
            }

            state.configs.insert(id.clone(), cfg);
            if let Some(n) = name {
                state.name_map.entry(n).or_default().push(id.clone());
            }
        }

        info!("Device registered/updated: {id}");
        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("add", id))
    }

    /// Removes devices and their sub-devices
    ///
    /// # Errors
    /// Returns [`BridgeError::NoMatchingDevices`] if no devices match the given selectors.
    pub async fn remove_device(
        &self,
        id: Option<Vec<String>>,
        name: Option<Vec<String>>,
    ) -> Result<ApiResponse, BridgeError> {
        let mut targets = self.get_targets(id, name).await?;

        // Cascade removal to sub-devices
        let sub_targets: Vec<String> = {
            let state = self.state.read().await;
            state
                .configs
                .iter()
                .filter_map(|(sub_id, cfg)| {
                    cfg.parent_id
                        .as_ref()
                        .and_then(|p_id| targets.contains(p_id).then(|| sub_id.clone()))
                })
                .collect()
        };
        targets.extend(sub_targets);
        targets.sort();
        targets.dedup();

        let mut scavenger_targets = Vec::new();
        {
            let mut state = self.state.write().await;

            for target_id in &targets {
                if let Some(cfg) = state.configs.remove(target_id) {
                    scavenger_targets.push(crate::types::ScavengerTarget {
                        id: target_id.clone(),
                        name: cfg.name.clone(),
                        cid: cfg.cid.clone(),
                    });

                    if let Some(n) = cfg.name
                        && let Some(ids) = state.name_map.get_mut(&n)
                    {
                        ids.retain(|x| x != target_id);
                        if ids.is_empty() {
                            state.name_map.remove(&n);
                        }
                    }
                    if let Some(cid) = cfg.cid
                        && let Some(parent_id) = cfg.parent_id
                    {
                        state.cid_map.remove(&(parent_id, cid));
                    }
                } else {
                    scavenger_targets.push(crate::types::ScavengerTarget {
                        id: target_id.clone(),
                        name: None,
                        cid: None,
                    });
                }
                state.instances.remove(target_id);
            }
        }

        // Send refresh signal to kill the listener stream and drop physical connections
        self.request_refresh();

        // Scavenge retained messages for removed devices
        self.spawn_retain_scavenger(scavenger_targets).await;

        info!("Devices removed: {}", targets.join(", "));
        self.request_save();
        Ok(ApiResponse::ok("remove", targets.join(",")))
    }

    /// Clears all registered devices and scavenges their retained MQTT messages.
    ///
    /// # Errors
    /// Currently always returns `Ok`; reserved for future failure modes.
    pub async fn clear_devices(&self) -> Result<ApiResponse, BridgeError> {
        let scavenger_targets: Vec<crate::types::ScavengerTarget> = {
            let mut state = self.state.write().await;

            let targets = state
                .configs
                .values()
                .map(|cfg| crate::types::ScavengerTarget {
                    id: cfg.id.clone(),
                    name: cfg.name.clone(),
                    cid: cfg.cid.clone(),
                })
                .collect();

            state.configs.clear();
            state.instances.clear();
            state.cid_map.clear();
            state.name_map.clear();
            targets
        };

        info!("All devices cleared");

        // Send refresh signal to kill the listener stream and drop physical connections
        self.request_refresh();

        // Scavenge all retained messages
        self.spawn_retain_scavenger(scavenger_targets).await;

        self.request_save();
        Ok(ApiResponse::ok("clear", "all"))
    }

    /// Returns current bridge status and registered devices
    pub async fn get_bridge_status(&self) -> ApiResponse {
        let state = self.state.read().await;
        let mut devices = serde_json::Map::new();
        for (id, cfg) in &state.configs {
            if let Ok(mut dev_val) = serde_json::to_value(cfg) {
                if let Some(obj) = dev_val.as_object_mut() {
                    let status = Self::determine_device_status(cfg, &state.instances);
                    obj.insert("status".to_string(), Value::String(status));
                }
                devices.insert(id.clone(), dev_val);
            }
        }
        drop(state);

        ApiResponse::ok_action("status").with_extra("devices", Value::Object(devices))
    }

    fn determine_device_status(cfg: &DeviceConfig, instances: &HashMap<String, Device>) -> String {
        if cfg.cid.is_some() {
            // Sub-device: check if parent instance exists
            cfg.parent_id
                .as_ref()
                .map_or("invalid subdevice", |p_id| {
                    if instances.contains_key(p_id) {
                        "subdevice"
                    } else {
                        "no parent"
                    }
                })
                .to_string()
        } else if instances.contains_key(&cfg.id) {
            // Direct device: use last_error_code if present, else "online"
            cfg.last_error_code
                .map_or_else(|| "online".to_string(), |code| code.to_string())
        } else {
            "offline".to_string()
        }
    }

    /// Resolves target device IDs and returns [`BridgeError::NoMatchingDevices`] if empty.
    ///
    /// # Errors
    /// Returns [`BridgeError::NoMatchingDevices`] when no IDs match the selectors.
    pub async fn get_targets(
        &self,
        id: Option<Vec<String>>,
        name: Option<Vec<String>>,
    ) -> Result<Vec<String>, BridgeError> {
        let targets = self.find_device_ids(id, name).await;
        if targets.is_empty() {
            Err(BridgeError::NoMatchingDevices)
        } else {
            Ok(targets)
        }
    }

    /// Resolves actual CID for a device (priority: `request_cid` > `config_cid`)
    pub async fn resolve_cid(&self, id: &str, request_cid: Option<String>) -> Option<String> {
        if request_cid.is_some() {
            return request_cid;
        }
        let state = self.state.read().await;
        state.configs.get(id).and_then(|c| c.cid.clone())
    }

    /// Gets a connected device instance, supporting sub-devices via parent.
    ///
    /// # Errors
    /// Returns [`BridgeError::DeviceNotFound`] if neither the device nor its
    /// parent (for sub-devices) is currently registered.
    pub async fn get_connected_device(&self, id: &str) -> Result<Device, BridgeError> {
        let parent_lookup = {
            let state = self.state.read().await;
            if let Some(dev) = state.instances.get(id) {
                return Ok(dev.clone());
            }
            // Try lookup via parent for sub-devices
            state.configs.get(id).and_then(|cfg| {
                cfg.parent_id
                    .as_ref()
                    .map(|p_id| (p_id.clone(), state.instances.get(p_id).cloned()))
            })
        };

        if let Some((parent_id, dev)) = parent_lookup {
            return dev.ok_or_else(|| {
                BridgeError::DeviceNotFound(format!(
                    "Parent device '{parent_id}' for subdevice '{id}' not found"
                ))
            });
        }

        Err(BridgeError::DeviceNotFound(id.to_string()))
    }
}

impl Drop for BridgeContext {
    fn drop(&mut self) {
        self.signal_shutdown_mqtt();
    }
}
