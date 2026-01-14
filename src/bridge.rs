use anyhow::Result;
use futures_util::StreamExt;
use log::{debug, error, info, trace};
use regex::Regex;
use rustuya::{Device, DeviceBuilder};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

use crate::config::{Cli, DeviceConfig, load_state};
use crate::error::BridgeError;
use crate::types::{ApiResponse, BridgeRequest};

pub const MQTT_CHANNEL_CAPACITY: usize = 100;
pub const INITIAL_RETRY_DELAY_SECS: u64 = 10;
pub const MAX_RETRY_DELAY_SECS: u64 = 1280;

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
    pub mqtt_tx: Option<mpsc::Sender<MqttMessage>>,
    pub mqtt_event_topic: String,
    pub mqtt_retain: bool,
    pub mqtt_topic_template: Option<String>,
    pub mqtt_message_topic_template: Option<String>,
    pub mqtt_payload_template: Option<String>,
    pub mqtt_scanner_topic: Option<String>,
    pub state_file: String,
    pub save_debounce_secs: u64,
    pub state: RwLock<BridgeState>,
    /// Published topics per device (for clearing retained messages)
    pub published_topics: RwLock<HashMap<String, HashSet<String>>>,
    pub save_tx: mpsc::Sender<()>,
    pub refresh_tx: mpsc::Sender<()>,
}

/// Converts MQTT template to wildcard for subscription
pub fn tpl_to_wildcard(template: &str) -> String {
    template
        .replace("{id}", "+")
        .replace("{name}", "+")
        .replace("{dp}", "+")
        .replace("{action}", "+")
}

/// Matches MQTT topic against template and extracts variables
pub fn match_topic(topic: &str, template: &str) -> Option<HashMap<String, String>> {
    if !template.contains('{') {
        return (topic == template).then(HashMap::new);
    }

    // Convert template to regex. E.g. "tuya/{id}/command" -> "^tuya/(?P<id>[^/]+)/command$"
    let mut pattern = regex::escape(template);
    for key in ["id", "name", "dp", "action"] {
        pattern = pattern.replace(&format!(r"\{{{}\}}", key), &format!(r"(?P<{}>[^/]+)", key));
    }

    let re = Regex::new(&format!("^{}$", pattern)).ok()?;
    let caps = re.captures(topic)?;

    let mut vars = HashMap::new();
    for name in re.capture_names().flatten() {
        if let Some(m) = caps.name(name) {
            vars.insert(name.to_string(), m.as_str().to_string());
        }
    }
    Some(vars)
}

impl BridgeContext {
    pub async fn new(
        cli: &Cli,
    ) -> (
        Arc<Self>,
        mpsc::Receiver<MqttMessage>,
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
            mqtt_tx: cli.mqtt_broker.is_some().then_some(mqtt_tx_sender),
            mqtt_event_topic,
            mqtt_retain: cli.get_mqtt_retain(),
            mqtt_topic_template: cli.mqtt_topic_template.clone(),
            mqtt_message_topic_template: cli.mqtt_message_topic_template.clone(),
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
            published_topics: RwLock::new(HashMap::new()),
            save_tx,
            refresh_tx,
        });

        (ctx, mqtt_tx_receiver, save_rx, refresh_rx)
    }

    /// Starts state saver task with debounce
    pub fn spawn_state_saver(self: Arc<Self>, mut save_rx: mpsc::Receiver<()>) {
        tokio::spawn(async move {
            while save_rx.recv().await.is_some() {
                tokio::time::sleep(Duration::from_secs(self.save_debounce_secs)).await;
                while save_rx.try_recv().is_ok() {} // Drain pending requests
                if let Err(e) = self.save_state().await {
                    error!("Save failed: {}", e);
                }
            }
        });
    }

    /// Starts device event listener task
    pub fn spawn_device_listener(self: Arc<Self>, mut refresh_rx: mpsc::Receiver<()>) {
        tokio::spawn(async move {
            loop {
                let instances = {
                    let state = self.state.read().await;
                    state.instances.values().cloned().collect::<Vec<_>>()
                };

                if instances.is_empty() {
                    if refresh_rx.recv().await.is_none() {
                        break;
                    }
                    continue;
                }

                let mut stream = rustuya::device::unified_listener(instances.clone());
                debug!("Started unified listener for {} devices", instances.len());
                loop {
                    tokio::select! {
                        Some(event_res) = stream.next() => {
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
                                    self.publish_device_message(&target_id, name.as_deref(), cid.as_deref(), "error", payload).await;
                                    continue;
                                }

                                let root_dps = payload_obj.and_then(|o| o.get("dps"));
                                let data_dps = payload_obj.and_then(|o| o.get("data"))
                                    .and_then(|d| d.as_object())
                                    .and_then(|do_| do_.get("dps"));

                                let is_passive = root_dps.is_none() && data_dps.is_none();
                                let mut dps = root_dps.or(data_dps).cloned().unwrap_or(payload);

                                // If target is parent (gateway) but CID was present, include CID in dps for visibility
                                if target_id == event.device_id && let Some(c) = &cid
                                    && let Some(obj) = dps.as_object_mut() {
                                    obj.insert("cid".to_string(), Value::String(c.clone()));
                                }

                                self.publish_device_event(target_id, name, cid, dps, is_passive, exists).await;
                            }
                        }
                        _ = refresh_rx.recv() => break, // Refresh listener on device changes
                    }
                }
            }
        });
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
        mut mqtt_tx_receiver: mpsc::Receiver<MqttMessage>,
    ) -> Result<()> {
        let broker_url = match &cli.mqtt_broker {
            Some(url) => url,
            None => return Ok(()),
        };

        let (mqtt_command_topic, _) = cli.get_mqtt_topics();
        let client_id = cli.mqtt_client_id.as_deref().unwrap_or("rustuya-bridge");
        let mqtt_options = self.create_mqtt_options(broker_url, client_id)?;

        let (client, mut eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);
        let sub_topic = tpl_to_wildcard(&mqtt_command_topic);

        tokio::spawn(async move {
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            loop {
                tokio::select! {
                    notification = eventloop.poll() => {
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
                                let vars = match_topic(&p.topic, &mqtt_command_topic).unwrap_or_default();
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
                                        ctx_h.publish_api_response(res).await;
                                    });
                                }
                            }
                            Err(e) => {
                                error!("MQTT Error: {}. Retrying in {}s...", e, retry_delay);
                                tokio::time::sleep(Duration::from_secs(retry_delay)).await;
                                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY_SECS);
                            }
                            _ => {}
                        }
                    }
                    Some(msg) = mqtt_tx_receiver.recv() => {
                        debug!("MQTT Publish: [{}] {}", msg.topic, msg.payload);
                        if let Err(e) = client.publish(msg.topic, rumqttc::QoS::AtLeastOnce, msg.retain, msg.payload).await {
                            error!("Publish failed: {}", e);
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Creates MQTT options from broker URL
    fn create_mqtt_options(
        &self,
        broker_url: &str,
        client_id: &str,
    ) -> Result<rumqttc::MqttOptions> {
        let mut opts = if broker_url.contains("://") {
            let url = url::Url::parse(broker_url)?;
            let host = url
                .host_str()
                .ok_or_else(|| anyhow::anyhow!("Missing host in broker URL"))?;
            let is_ssl = matches!(url.scheme(), "mqtts" | "ssl");
            let port = url.port().unwrap_or(if is_ssl { 8883 } else { 1883 });

            let mut opts = rumqttc::MqttOptions::new(client_id, host, port);
            if !url.username().is_empty() {
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
            rumqttc::MqttOptions::new(client_id, host, port)
        };

        opts.set_keep_alive(Duration::from_secs(5));
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

        // Default payload: if no template, merge id/name into dps (backward compatibility/default)
        let default_payload = if let Some(tpl) = &self.mqtt_payload_template {
            replace_vars_local(tpl, None, None)
        } else {
            let mut payload_obj = dps.clone();
            if let Some(obj) = payload_obj.as_object_mut() {
                obj.insert("id".to_string(), id.into());
                if let Some(n) = name {
                    obj.insert("name".to_string(), n.into());
                }
            }
            payload_obj.to_string()
        };

        let mut templates = Vec::new();

        // Only add default topic if mqtt_topic_template is NOT present,
        // or if it is present but evaluates to a different topic.
        let default_topic = self.mqtt_event_topic.replace("{type}", event_type);

        if let Some(tpl) = &self.mqtt_topic_template {
            let extra_topic = replace_vars_local(tpl, None, None);

            if tpl.contains("{dp}") || tpl.contains("{value}") {
                // If template is per-DP, we still want the main event topic
                templates.push((default_topic, default_payload));

                if let Some(dps_obj) = dps.as_object() {
                    for (dp, val) in dps_obj {
                        let topic = replace_vars_local(tpl, Some(dp), None);
                        let payload = self
                            .mqtt_payload_template
                            .as_ref()
                            .map(|p_tpl| replace_vars_local(p_tpl, Some(dp), Some(val)))
                            .unwrap_or_else(|| val.to_string());
                        templates.push((topic, payload));
                    }
                }
            } else {
                // If it's a general template
                let payload = self
                    .mqtt_payload_template
                    .as_ref()
                    .map(|p_tpl| replace_vars_local(p_tpl, None, None))
                    .unwrap_or_else(|| {
                        let mut p_obj = dps.clone();
                        if let Some(obj) = p_obj.as_object_mut() {
                            obj.insert("id".to_string(), id.into());
                            obj.insert("name".to_string(), name.into());
                        }
                        p_obj.to_string()
                    });

                // Avoid duplicate if topics are the same
                if extra_topic != default_topic {
                    templates.push((default_topic, default_payload));
                }
                templates.push((extra_topic, payload));
            }
        } else {
            // No extra template, just use default
            templates.push((default_topic, default_payload));
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

        if let Some(tx) = &self.mqtt_tx {
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

            if self.mqtt_retain {
                let mut topics = self.published_topics.write().await;
                let device_topics = topics.entry(id.clone()).or_default();
                for (topic, _) in &templates {
                    device_topics.insert(topic.clone());
                }
            }

            for (topic, payload) in templates {
                let _ = tx
                    .send(MqttMessage {
                        topic,
                        payload,
                        retain: self.mqtt_retain,
                    })
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
        if let Some(tx) = &self.mqtt_tx {
            let topic = if let Some(tpl) = &self.mqtt_message_topic_template {
                self.replace_vars(
                    tpl,
                    TopicVars {
                        id,
                        name,
                        cid,
                        level: Some(level),
                        ..Default::default()
                    },
                )
            } else {
                let root_topic = self
                    .mqtt_event_topic
                    .replace("/{type}", "")
                    .replace("/event", "")
                    .trim_end_matches('/')
                    .to_string();
                format!("{}/{}/{}", root_topic, level, id)
            };

            if let Some(obj) = payload.as_object_mut() {
                obj.insert("id".to_string(), id.into());
                if let Some(n) = name {
                    obj.insert("name".to_string(), n.into());
                }
                if let Some(c) = cid {
                    obj.insert("cid".to_string(), c.into());
                }
            }

            let _ = tx
                .send(MqttMessage {
                    topic,
                    payload: payload.to_string(),
                    retain: false,
                })
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
        let root_topic = self
            .mqtt_event_topic
            .replace("/{type}", "")
            .replace("/event", "")
            .trim_end_matches('/')
            .to_string();

        let mut res = template
            .replace("{root}", &root_topic)
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
        }
        if let Some(ds) = vars.dps_str {
            res = res.replace("{dps}", ds);
        }
        res
    }

    /// Publishes bridge-level messages (e.g. scanner results)
    pub async fn publish_scanner_event(&self, payload: Value) {
        if payload.is_null() || payload.as_str().is_some_and(|s| s.is_empty()) {
            return;
        }
        debug!("Scanner Event: {}", payload);
        if let Some(tx) = &self.mqtt_tx {
            let topic = if let Some(topic) = &self.mqtt_scanner_topic {
                self.replace_vars(
                    topic,
                    TopicVars {
                        id: "bridge",
                        name: Some("bridge"),
                        ..Default::default()
                    },
                )
            } else if let Some(tpl) = &self.mqtt_message_topic_template {
                self.replace_vars(
                    tpl,
                    TopicVars {
                        id: "bridge",
                        name: Some("bridge"),
                        level: Some("scanner"),
                        ..Default::default()
                    },
                )
            } else {
                let root_topic = self
                    .mqtt_event_topic
                    .replace("/{type}", "")
                    .replace("/event", "")
                    .trim_end_matches('/')
                    .to_string();
                format!("{}/scanner", root_topic)
            };
            let _ = tx
                .send(MqttMessage {
                    topic: topic.clone(),
                    payload: payload.to_string(),
                    retain: false, // Scanner events usually don't need retain
                })
                .await;
        }
    }

    /// Clears all retained messages for a device
    pub async fn clear_retained_messages(&self, id: &str) {
        if let Some(tx) = &self.mqtt_tx {
            let topics = {
                let mut published = self.published_topics.write().await;
                published.remove(id)
            };

            if let Some(topics) = topics {
                for topic in topics {
                    let _ = tx
                        .send(MqttMessage {
                            topic,
                            payload: "".to_string(), // Empty payload clears retain
                            retain: true,
                        })
                        .await;
                }
            }
        }
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

        {
            let mut state = self.state.write().await;

            for target_id in &targets {
                if let Some(cfg) = state.configs.remove(target_id) {
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
                }
                state.instances.remove(target_id);
            }
        }

        // Clear retained messages AFTER dropping all bridge locks
        for target_id in &targets {
            self.clear_retained_messages(target_id).await;
        }

        info!("Devices removed: {}", targets.join(", "));
        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("remove", targets.join(",")))
    }

    /// Clears all devices and configurations
    pub async fn clear_devices(&self) -> Result<ApiResponse, BridgeError> {
        let ids: Vec<String> = {
            let mut state = self.state.write().await;

            let ids = state.configs.keys().cloned().collect();

            state.configs.clear();
            state.instances.clear();
            state.cid_map.clear();
            state.name_map.clear();
            ids
        };

        info!("All devices cleared");
        // Clear all retained messages after clearing configs
        for id in ids {
            self.clear_retained_messages(&id).await;
        }

        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("clear", "all"))
    }

    /// Returns current bridge status and registered devices
    pub async fn get_bridge_status(&self) -> ApiResponse {
        let state = self.state.read().await;

        let devices: Vec<Value> = state
            .configs
            .values()
            .map(|cfg| {
                serde_json::to_value(cfg)
                    .map(|mut dev_val| {
                        if let Some(obj) = dev_val.as_object_mut() {
                            let status = self.determine_device_status(cfg, &state.instances);
                            obj.insert("status".to_string(), Value::String(status));
                        }
                        dev_val
                    })
                    .unwrap_or_else(|_| Value::Null)
            })
            .filter(|v| !v.is_null())
            .collect();

        ApiResponse::ok("status", "bridge").with_extra("devices", Value::Array(devices))
    }

    fn determine_device_status(
        &self,
        cfg: &DeviceConfig,
        instances: &HashMap<String, Device>,
    ) -> String {
        if cfg.cid.is_some() {
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
            "online".to_string()
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
