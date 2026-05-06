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
    /// Mapping from (parent_id, cid) to sub-device ID
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

/// Converts MQTT template to wildcard for subscription
/// Converts MQTT template to wildcard for subscription
pub fn tpl_to_wildcard(template: &str, root_topic: &str) -> String {
    template
        .replace("{root}", root_topic)
        .replace("{id}", "+")
        .replace("{name}", "+")
        .replace("{dp}", "+")
        .replace("{action}", "+")
        .replace("{cid}", "+")
        .replace("{type}", "+")
        .replace("{level}", "+")
}

/// Compiles a template into a Regex if it contains variables
pub fn compile_topic_regex(template: &str) -> Option<Regex> {
    if !template.contains('{') {
        return None;
    }

    // Convert template to regex. E.g. "tuya/{id}/command" -> "^tuya/(?P<id>[^/]+)/command$"
    let mut pattern = regex::escape(template);
    for key in ["id", "name", "dp", "action", "cid", "type", "level"] {
        pattern = pattern.replace(&format!(r"\{{{}\}}", key), &format!(r"(?P<{}>[^/]+)", key));
    }

    Regex::new(&format!("^{}$", pattern)).ok()
}

/// Matches MQTT topic against precompiled regex or template
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

impl BridgeContext {
    pub async fn new(
        cli: &Cli,
    ) -> (
        Arc<Self>,
        mpsc::Receiver<Option<MqttMessage>>,
        mpsc::Receiver<()>,
        mpsc::Receiver<()>,
    ) {
        let (_, mqtt_event_topic) = cli.get_mqtt_topics();
        let initial_configs = load_state(&cli.get_state_file()).await;

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
                    .version(cfg.version.as_deref().unwrap_or("Auto"))
                    .nowait(true)
                    .run();
                initial_instances.insert(id.clone(), dev);
            } else {
                error!("Device {} is invalid: missing key or parent info", id);
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
            mqtt_retain: cli.get_mqtt_retain(),
            mqtt_message_topic: cli.mqtt_message_topic.clone(),
            mqtt_payload_template: cli.mqtt_payload_template.clone(),
            mqtt_scanner_topic: cli.mqtt_scanner_topic.clone(),
            state_file: cli.get_state_file(),
            save_debounce_secs: cli.get_save_debounce_secs(),
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

        (ctx, mqtt_tx_receiver, save_rx, refresh_rx)
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
                    _ = cancel.cancelled() => break,
                    res = save_rx.recv() => {
                        if res.is_none() { break; }
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                while save_rx.try_recv().is_ok() {} // Drain pending requests
                                if let Err(e) = self.save_state().await {
                                    error!("Save failed during shutdown: {}", e);
                                }
                                break;
                            }
                            _ = tokio::time::sleep(Duration::from_secs(self.save_debounce_secs)) => {
                                while save_rx.try_recv().is_ok() {} // Drain pending requests
                                if let Err(e) = self.save_state().await {
                                    error!("Save failed: {}", e);
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
                        _ = cancel.cancelled() => return,
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
                        _ = cancel.cancelled() => return,
                        res = tokio::time::timeout(Duration::from_secs(LISTENER_TIMEOUT_SECS), stream.next()) => {
                            match res {
                                Ok(Some(event_res)) => {
                                    if let Ok(event) = event_res {
                                        let payload_str = event.message.payload_as_string().unwrap();
                                        trace!("Raw event from {}: {}", event.device_id, payload_str);
                                        let payload: Value = serde_json::from_str(&payload_str).unwrap_or(Value::String(payload_str.clone()));
                                        let (target_id, name, cid, exists) = self.resolve_event_target(&event.device_id, &payload).await;

                                // Check for 'dps' at root or inside 'data' (for sub-devices/gateways)
                                let payload_obj = payload.as_object();

                                // Check for error messages
                                if payload_obj.is_some_and(|o| o.contains_key("errorCode") || o.contains_key("errorMsg")) {
                                    if let Some(ref n) = name {
                                        info!("Device {} ({}) reported error: {}", target_id, n, payload);
                                    } else {
                                        info!("Device {} reported error: {}", target_id, payload);
                                    }
                                    // Update last_error_code in device config
                                    let error_code = payload.get("errorCode").and_then(|v| v.as_u64()).map(|v| v as u32);
                                    if let Some(code) = error_code {
                                        let mut state = self.state.write().await;
                                        if let Some(cfg) = state.configs.get_mut(&target_id) {
                                            cfg.last_error_code = Some(code);
                                        }
                                    }
                                    self.publish_device_message(&target_id, name.as_deref(), cid.as_deref(), "error", payload).await;
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
                                    info!("Device listener timeout after {}s (no events received)", LISTENER_TIMEOUT_SECS);
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
            .map(|s| s.to_string());

        let state = self.state.read().await;

        // Use sub-device info if CID is registered, else fall back to parent
        let target_id = if let Some(c) = &cid {
            state
                .cid_map
                .get(&(parent_id.to_string(), c.clone()))
                .cloned()
                .unwrap_or_else(|| parent_id.to_string())
        } else {
            parent_id.to_string()
        };

        let cfg = state.configs.get(&target_id);
        let name = cfg.and_then(|c| c.name.clone());
        let exists = cfg.is_some();

        (target_id, name, cid, exists)
    }

    /// Starts MQTT task for command processing and event publishing
    pub fn spawn_mqtt_task(
        self: Arc<Self>,
        cli: &Cli,
        mut mqtt_tx_receiver: mpsc::Receiver<Option<MqttMessage>>,
    ) -> Result<Option<tokio::task::JoinHandle<()>>> {
        let broker_url = match &cli.mqtt_broker {
            Some(url) => url,
            None => return Ok(None),
        };

        let (mqtt_command_topic, _) = cli.get_mqtt_topics();
        let client_id = cli.get_mqtt_client_id();
        let mqtt_options = self.create_mqtt_options(broker_url, &client_id, cli, true)?;

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 100);
        let sub_topic = tpl_to_wildcard(&mqtt_command_topic, &self.mqtt_root_topic);
        let command_topic_re = compile_topic_regex(&mqtt_command_topic);

        let handle = tokio::spawn(async move {
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            let mut next_retry: Option<Instant> = None;
            loop {
                tokio::select! {
                    _ = async {
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
                                    error!("Subscription failed: {}", e);
                                } else {
                                    info!("Subscribed to: {}", sub_topic);
                                }
                            }
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                let payload = String::from_utf8_lossy(&p.payload);
                                debug!("MQTT Received: [{}] {}", p.topic, payload);
                                let vars = match_topic(&p.topic, &mqtt_command_topic, command_topic_re.as_ref()).unwrap_or_default();
                                let req_val = self.parse_mqtt_payload(&payload, vars);

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
                                error!("MQTT Error: {}. Retrying in {}s...", e, retry_delay);
                                next_retry = Some(Instant::now() + Duration::from_secs(retry_delay));
                                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_SECS);
                            }
                            _ => {}
                        }
                    }
                    msg_opt = mqtt_tx_receiver.recv() => {
                        match msg_opt {
                            Some(Some(msg)) => {
                                debug!("MQTT Publish: [{}] {}", msg.topic, msg.payload);
                                if let Err(e) = client.publish(msg.topic, rumqttc::QoS::AtLeastOnce, msg.retain, msg.payload).await {
                                    error!("Publish failed: {}", e);
                                }
                            }
                            Some(None) | None => {
                                debug!("MQTT shutdown signal received, clearing and disconnecting...");

                                // Clear bridge config retain topic
                                let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
                                let expected_pubacks = 1;
                                let _ = client.publish(config_topic, rumqttc::QoS::AtLeastOnce, true, "").await;

                                debug!("Waiting for {} PubAcks before disconnecting...", expected_pubacks);

                                // 3. Poll eventloop until all PubAcks are received (with timeout)
                                let mut received_pubacks: usize = 0;
                                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                                loop {
                                    if received_pubacks >= expected_pubacks {
                                        break;
                                    }
                                    if tokio::time::Instant::now() >= deadline {
                                        warn!("Timed out waiting for PubAcks ({}/{} received)", received_pubacks, expected_pubacks);
                                        break;
                                    }
                                    match tokio::time::timeout_at(deadline, eventloop.poll()).await {
                                        Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))) => {
                                            received_pubacks += 1;
                                        }
                                        Ok(Err(e)) => {
                                            warn!("MQTT error while waiting for PubAck: {}", e);
                                            break;
                                        }
                                        Err(_) => {
                                            warn!("Timed out waiting for PubAcks");
                                            break;
                                        }
                                        _ => {}
                                    }
                                }

                                debug!("PubAck flush done ({}/{}).", received_pubacks, expected_pubacks);

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
            }
        });

        Ok(Some(handle))
    }

    pub async fn publish_bridge_config(&self, cli: Option<&crate::config::Cli>, clear: bool) {
        let topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
        let payload = if clear {
            String::new()
        } else {
            cli.map(|c| serde_json::to_string(c).unwrap_or_else(|_| "{}".to_string()))
                .unwrap_or_else(|| "{}".to_string())
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

    /// Parses MQTT payload into a BridgeRequest value, merging topic variables.
    fn parse_mqtt_payload(&self, payload: &str, vars: HashMap<String, String>) -> Value {
        let mut val =
            serde_json::from_str::<Value>(payload).unwrap_or(Value::String(payload.to_string()));

        // If it's an array, merge topic variables into each element
        if let Some(arr) = val.as_array_mut() {
            for item in arr {
                if let Some(obj) = item.as_object_mut() {
                    for (k, v) in &vars {
                        if !obj.contains_key(k) {
                            obj.insert(k.clone(), Value::String(v.clone()));
                        }
                    }
                    // Heuristic for 'set' action inside array items
                    if obj.get("action").and_then(|a| a.as_str()) == Some("set") {
                        self.apply_set_heuristic(obj);
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
            for (k, v) in &vars {
                if !obj.contains_key(k) {
                    obj.insert(k.clone(), Value::String(v.clone()));
                }
            }

            // Heuristic for tuya2mqtt compatibility and simple set commands:
            // If action is 'set', ensure we have a 'dps' object
            if obj.get("action").and_then(|a| a.as_str()) == Some("set") {
                self.apply_set_heuristic(obj);
            }
        }
        val
    }

    /// Helper to apply 'set' command heuristic to an object
    fn apply_set_heuristic(&self, obj: &mut serde_json::Map<String, Value>) {
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
    fn generate_device_templates(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: Value,
        is_passive: bool,
    ) -> Vec<(String, String)> {
        let dps_str = dps.to_string();
        let event_type = if is_passive { "passive" } else { "active" };

        let replace_vars_local = |s: &str, dp: Option<&str>, val: Option<&Value>| {
            self.replace_vars(
                s,
                TopicVars {
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
        if dps.is_null() || dps.as_str().is_some_and(|s| s.is_empty()) {
            return;
        }

        debug!("Device Event: [{}] {}", id, dps);

        if self.mqtt_tx.is_some() {
            // Check if device still exists before publishing to avoid race conditions during removal
            if !exists {
                return;
            }

            let templates = self.generate_device_templates(
                &id,
                name.as_deref(),
                cid.as_deref(),
                dps,
                is_passive,
            );
            if templates.is_empty() {
                return;
            }

            for (topic, payload) in templates {
                let mut retain = self.mqtt_retain;
                if retain && !topic.contains(&id) && !payload.contains(&id) {
                    warn!(
                        "Ignoring retain for event from '{}' because '{}' is missing from both topic and payload",
                        id, id
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
    ) {
        if self.mqtt_tx.is_some() {
            let topic = self.replace_vars(
                &self.mqtt_message_topic.clone().unwrap_or_default(),
                TopicVars {
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
            let mut retain = self.mqtt_retain && level == "error";
            if retain && id != "bridge" && !topic.contains(id) && !payload_str.contains(id) {
                warn!(
                    "Ignoring retain for message from '{}' because '{}' is missing from both topic and payload",
                    id, id
                );
                retain = false;
            }
            self.try_send_mqtt(Some(MqttMessage {
                topic,
                payload: payload_str,
                retain,
            }))
            .await;
        }
    }

    /// Publishes an ApiResponse to the appropriate topic
    pub async fn publish_api_response(&self, response: ApiResponse) {
        let level = match response.status {
            crate::types::Status::Ok => "response",
            crate::types::Status::Error => "error",
        };
        let id = response.id.as_deref().unwrap_or("bridge");
        let name = (id == "bridge").then_some("bridge");

        match serde_json::to_value(&response) {
            Ok(payload) => {
                self.publish_device_message(id, name, None, level, payload)
                    .await;
            }
            Err(e) => {
                error!("Failed to serialize ApiResponse: {}", e);
            }
        }
    }

    /// Helper to replace template variables in a string
    fn replace_vars(&self, template: &str, vars: TopicVars) -> String {
        let mut res = template
            .replace("{root}", &self.mqtt_root_topic)
            .replace("{id}", vars.id)
            .replace("{name}", vars.name.unwrap_or(""))
            .replace("{cid}", vars.cid.unwrap_or(""));

        if let Some(l) = vars.level {
            res = res.replace("{level}", l);
        }
        if let Some(t) = vars.event_type {
            res = res.replace("{type}", t);
        }
        if let Some(d) = vars.dp {
            res = res.replace("{dp}", d);
        }
        if let Some(v) = vars.val {
            res = res.replace("{value}", &v.to_string());
        } else if let Some(ds) = vars.dps_str {
            res = res.replace("{value}", ds);
        }
        if let Some(ds) = vars.dps_str {
            res = res.replace("{dps}", ds);
        }
        res = res.replace(
            "{timestamp}",
            &std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_string(),
        );
        res
    }

    /// Publishes bridge-level messages (e.g. scanner results)
    pub async fn publish_scanner_event(&self, payload: Value) {
        if payload.is_null() || payload.as_str().is_some_and(|s| s.is_empty()) {
            return;
        }
        debug!("Scanner Event: {}", payload);
        if self.mqtt_tx.is_some() {
            let topic = if let Some(topic) = &self.mqtt_scanner_topic {
                self.replace_vars(
                    topic,
                    TopicVars {
                        id: "bridge",
                        name: Some("bridge"),
                        ..Default::default()
                    },
                )
            } else {
                self.replace_vars(
                    self.mqtt_message_topic.as_deref().unwrap_or_default(),
                    TopicVars {
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
    pub async fn spawn_retain_scavenger(&self, targets: Vec<crate::types::ScavengerTarget>) {
        if targets.is_empty() {
            return;
        }

        let mut lock = self.scavenger_tx.lock().await;
        if let Some(tx) = &*lock
            && tx.send(targets.clone()).is_ok()
        {
            return;
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        *lock = Some(tx);

        let broker_url = match &self.cli.mqtt_broker {
            Some(url) => url.clone(),
            None => return,
        };
        let client_id = format!(
            "{}_scavenger_{}",
            self.cli.get_mqtt_client_id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let mqtt_options = match self.create_mqtt_options(&broker_url, &client_id, &self.cli, false)
        {
            Ok(opts) => opts,
            Err(e) => {
                error!("Failed to create MQTT options for scavenger: {}", e);
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
                    error!("Scavenger subscription failed for {}: {}", sub, e);
                } else {
                    debug!("Scavenger subscribed to {}", sub);
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
                            debug!("Scavenger added {} new targets, extended timeout", count);
                        } else {
                            break;
                        }
                    }
                    _ = tokio::time::sleep_until(deadline) => {
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
                                    let ext_id = vars.get("id");
                                    let ext_name = vars.get("name");
                                    let ext_cid = vars.get("cid");

                                    should_clear = active_targets.iter().any(|t| {
                                        let match_id = ext_id.is_some_and(|id| id == &t.id);
                                        let match_cid = ext_cid.is_some() && t.cid.as_ref() == ext_cid;
                                        let match_name = ext_name.is_some() && t.name.as_ref() == ext_name;
                                        match_id || match_cid || match_name
                                    });
                                }

                                // 2. Exact Payload Match
                                if !should_clear {
                                    let payload = String::from_utf8_lossy(&p.payload);
                                    should_clear = active_targets.iter().any(|t| {
                                        // Exact JSON string value match
                                        let id_match = payload.contains(&format!("\"{}\"", t.id));
                                        let cid_match = t.cid.as_ref().is_some_and(|cid| payload.contains(&format!("\"{}\"", cid)));
                                        let name_match = t.name.as_ref().is_some_and(|name| payload.contains(&format!("\"{}\"", name)));
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

    /// Normalizes DeviceConfig fields and applies aliases
    fn normalize_config(&self, cfg: &mut DeviceConfig) {
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
    pub async fn add_device(&self, mut cfg: DeviceConfig) -> Result<ApiResponse, BridgeError> {
        self.normalize_config(&mut cfg);

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
                    .version(cfg.version.as_deref().unwrap_or("Auto"))
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

        info!("Device registered/updated: {}", id);
        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("add", id))
    }

    /// Removes devices and their sub-devices
    pub async fn remove_device(
        &self,
        id: Option<Vec<String>>,
        name: Option<Vec<String>>,
    ) -> Result<ApiResponse, BridgeError> {
        let mut targets = self.get_targets(id, name).await?;

        // Cascade removal to sub-devices
        {
            let state = self.state.read().await;
            let sub_targets: Vec<String> = state
                .configs
                .iter()
                .filter_map(|(sub_id, cfg)| {
                    cfg.parent_id
                        .as_ref()
                        .and_then(|p_id| targets.contains(p_id).then(|| sub_id.clone()))
                })
                .collect();
            targets.extend(sub_targets);
        }
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
                    let status = self.determine_device_status(cfg, &state.instances);
                    obj.insert("status".to_string(), Value::String(status));
                }
                devices.insert(id.clone(), dev_val);
            }
        }

        let mut resp = ApiResponse::ok("status", "");
        resp.id = None;
        resp.with_extra("devices", Value::Object(devices))
    }

    fn determine_device_status(
        &self,
        cfg: &DeviceConfig,
        instances: &HashMap<String, Device>,
    ) -> String {
        if cfg.cid.is_some() {
            // Sub-device: check if parent instance exists
            cfg.parent_id
                .as_ref()
                .map(|p_id| {
                    if instances.contains_key(p_id) {
                        "subdevice"
                    } else {
                        "no parent"
                    }
                })
                .unwrap_or("invalid subdevice")
                .to_string()
        } else if instances.contains_key(&cfg.id) {
            // Direct device: use last_error_code if present, else "online"
            if let Some(code) = cfg.last_error_code {
                code.to_string()
            } else {
                "online".to_string()
            }
        } else {
            "offline".to_string()
        }
    }

    /// Resolves target device IDs and returns NoMatchingDevices error if empty
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

    /// Resolves actual CID for a device (priority: request_cid > config_cid)
    pub async fn resolve_cid(&self, id: &str, request_cid: Option<String>) -> Option<String> {
        if request_cid.is_some() {
            return request_cid;
        }
        let state = self.state.read().await;
        state.configs.get(id).and_then(|c| c.cid.clone())
    }

    /// Gets a connected device instance, supporting sub-devices via parent
    pub async fn get_connected_device(&self, id: &str) -> Result<Device, BridgeError> {
        let state = self.state.read().await;

        if let Some(dev) = state.instances.get(id) {
            return Ok(dev.clone());
        }

        // Try lookup via parent for sub-devices
        if let Some(cfg) = state.configs.get(id)
            && let Some(parent_id) = &cfg.parent_id
        {
            if let Some(parent_dev) = state.instances.get(parent_id) {
                return Ok(parent_dev.clone());
            } else {
                return Err(BridgeError::DeviceNotFound(format!(
                    "Parent device '{}' for subdevice '{}' not found",
                    parent_id, id
                )));
            }
        }

        Err(BridgeError::DeviceNotFound(id.to_string()))
    }
}

impl Drop for BridgeContext {
    fn drop(&mut self) {
        self.signal_shutdown_mqtt();
    }
}
