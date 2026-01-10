use anyhow::Result;
use futures_util::StreamExt;
use log::{error, info};
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

pub struct BridgeContext {
    pub mqtt_tx: Option<mpsc::Sender<MqttMessage>>,
    pub mqtt_event_topic: String,
    pub mqtt_retain: bool,
    pub mqtt_topic_template: Option<String>,
    pub mqtt_passive_topic_template: Option<String>,
    pub mqtt_message_topic_template: Option<String>,
    pub mqtt_payload_template: Option<String>,
    pub state_file: String,
    pub save_debounce_secs: u64,
    pub configs: RwLock<HashMap<String, DeviceConfig>>,
    pub instances: RwLock<HashMap<String, Device>>,
    /// Mapping from (parent_id, cid) to sub-device ID
    pub cid_map: RwLock<HashMap<(String, String), String>>,
    /// Mapping from name to device IDs
    pub name_map: RwLock<HashMap<String, Vec<String>>>,
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
        return if topic == template {
            Some(HashMap::new())
        } else {
            None
        };
    }

    // Convert template to regex. E.g. "tuya/{id}/command" -> "^tuya/(?P<id>[^/]+)/command$"
    let mut pattern = regex::escape(template);
    pattern = pattern.replace(r"\{id\}", r"(?P<id>[^/]+)");
    pattern = pattern.replace(r"\{name\}", r"(?P<name>[^/]+)");
    pattern = pattern.replace(r"\{dp\}", r"(?P<dp>[^/]+)");
    pattern = pattern.replace(r"\{action\}", r"(?P<action>[^/]+)");

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
        let (_mqtt_command_topic, mqtt_event_topic) = cli.get_mqtt_topics();
        let state_file = cli.get_state_file();
        let save_debounce_secs = cli.get_save_debounce_secs();
        let mqtt_retain = cli.get_mqtt_retain();

        let initial_configs = load_state(&state_file).await;
        let mut initial_instances = HashMap::new();
        let mut initial_name_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut initial_cid_map = HashMap::new();

        for (id, cfg) in &initial_configs {
            if let Some(cid) = &cfg.cid
                && let Some(parent_id) = &cfg.parent_id
            {
                initial_cid_map.insert((parent_id.clone(), cid.clone()), id.clone());
            } else {
                let dev = DeviceBuilder::new(id, cfg.key.as_bytes().to_vec())
                    .address(&cfg.ip)
                    .version(&*cfg.version)
                    .nowait(true)
                    .run();
                initial_instances.insert(id.clone(), dev);
            }

            if let Some(name) = &cfg.name {
                initial_name_map
                    .entry(name.clone())
                    .or_default()
                    .push(id.clone());
            }
        }

        let (mqtt_tx_sender, mqtt_tx_receiver) =
            mpsc::channel::<MqttMessage>(MQTT_CHANNEL_CAPACITY);
        let (save_tx, save_rx) = mpsc::channel::<()>(1);
        let (refresh_tx, refresh_rx) = mpsc::channel::<()>(1);

        let ctx = Arc::new(Self {
            mqtt_tx: if cli.mqtt_broker.is_some() {
                Some(mqtt_tx_sender)
            } else {
                None
            },
            mqtt_event_topic,
            mqtt_retain,
            mqtt_topic_template: cli.mqtt_topic_template.clone(),
            mqtt_passive_topic_template: cli.mqtt_passive_topic_template.clone(),
            mqtt_message_topic_template: cli.mqtt_message_topic_template.clone(),
            mqtt_payload_template: cli.mqtt_payload_template.clone(),
            state_file,
            save_debounce_secs,
            configs: RwLock::new(initial_configs),
            instances: RwLock::new(initial_instances),
            cid_map: RwLock::new(initial_cid_map),
            name_map: RwLock::new(initial_name_map),
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
                    let inst = self.instances.read().await;
                    inst.values().cloned().collect::<Vec<_>>()
                };

                if instances.is_empty() {
                    if refresh_rx.recv().await.is_none() {
                        break;
                    }
                    continue;
                }

                let mut stream = rustuya::device::unified_listener(instances);
                loop {
                    tokio::select! {
                        Some(event_res) = stream.next() => {
                            if let Ok(event) = event_res
                                && let Some(payload_str) = event.message.payload_as_string()
                            {
                                let payload: Value = serde_json::from_str(&payload_str).unwrap_or(Value::String(payload_str.to_string()));

                                // Extract CID (sub-device) from payload
                                let cid = payload.as_object()
                                    .and_then(|o| o.get("cid"))
                                    .and_then(|c| c.as_str());

                                let (device_id, name, is_passive) = {
                                    let configs = self.configs.read().await;
                                    let cid_map = self.cid_map.read().await;

                                    // Use sub-device info if CID is registered, else fall back to parent
                                    let target_id = if let Some(c) = cid {
                                        cid_map.get(&(event.device_id.clone(), c.to_string()))
                                            .cloned()
                                            .unwrap_or_else(|| event.device_id.clone())
                                    } else {
                                        event.device_id.clone()
                                    };

                                    let name = configs.get(&target_id)
                                        .and_then(|c| c.name.clone())
                                        .unwrap_or_else(|| target_id.clone());

                                    // Passive events typically have 'dps' but no 'data'
                                    let is_passive = payload.as_object().is_some_and(|o| {
                                        o.contains_key("dps") && !o.contains_key("data")
                                    });
                                    (target_id, name, is_passive)
                                };

                                let dps = payload.as_object().and_then(|o| o.get("dps")).cloned().unwrap_or(payload);
                                self.publish_device_event(device_id, name, dps, is_passive).await;
                            }
                        }
                        _ = refresh_rx.recv() => break, // Refresh listener on device changes
                    }
                }
            }
        });
    }

    /// Starts MQTT task for command processing and event publishing
    pub async fn spawn_mqtt_task(
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
        let mut mqtt_options =
            if broker_url.starts_with("mqtt://") || broker_url.starts_with("tcp://") {
                let url = url::Url::parse(broker_url)?;
                let host = url.host_str().unwrap_or("localhost");
                let port = url.port().unwrap_or(1883);
                rumqttc::MqttOptions::new(client_id, host, port)
            } else {
                anyhow::bail!("Invalid broker URL: {}", broker_url);
            };
        mqtt_options.set_keep_alive(Duration::from_secs(5));

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
                                retry_delay = INITIAL_RETRY_DELAY_SECS; // Reset on success
                                if let Err(e) = client.subscribe(&sub_topic, rumqttc::QoS::AtLeastOnce).await {
                                    error!("Initial subscription failed: {}", e);
                                } else {
                                    info!("Subscribed to: {}", sub_topic);
                                }
                            }
                            Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(p))) => {
                                let topic = p.topic;
                                let payload = String::from_utf8_lossy(&p.payload).to_string();
                                let vars = match_topic(&topic, &mqtt_command_topic).unwrap_or_default();
                                let req_val = self.parse_mqtt_payload(&payload, vars);

                                if let Ok(req) = serde_json::from_value::<BridgeRequest>(req_val) {
                                    let ctx_h = self.clone();
                                    tokio::spawn(async move { crate::handlers::handle_request(ctx_h, req).await; });
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
                        if let Err(e) = client.publish(msg.topic, rumqttc::QoS::AtLeastOnce, msg.retain, msg.payload).await {
                            error!("Publish failed: {}", e);
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Parses MQTT payload into a BridgeRequest value, merging topic variables.
    fn parse_mqtt_payload(&self, payload: &str, vars: HashMap<String, String>) -> Value {
        match serde_json::from_str::<Value>(payload) {
            Ok(mut val) => {
                if let Some(obj) = val.as_object_mut() {
                    for (k, v) in vars {
                        if !obj.contains_key(&k) {
                            obj.insert(k, Value::String(v));
                        }
                    }
                }
                val
            }
            Err(_) => {
                // Simple payload heuristic for non-JSON or partial payloads
                let mut obj = serde_json::Map::new();
                for (k, v) in vars {
                    obj.insert(k, Value::String(v));
                }
                if let Some(dp) = obj
                    .get("dp")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                {
                    let val = serde_json::from_str::<Value>(payload)
                        .unwrap_or(Value::String(payload.to_string()));
                    let mut dps = serde_json::Map::new();
                    dps.insert(dp, val);
                    obj.insert("action".to_string(), "set".into());
                    obj.insert("dps".to_string(), Value::Object(dps));
                }
                Value::Object(obj)
            }
        }
    }

    /// Atomically saves device configuration to state file
    pub async fn save_state(&self) -> Result<()> {
        let json = {
            let configs = self.configs.read().await;
            serde_json::to_string_pretty(&*configs)?
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
        // 1. Match by ID
        if let Some(ids) = id {
            let configs = self.configs.read().await;
            return ids
                .into_iter()
                .filter(|id_val| configs.contains_key(id_val))
                .collect();
        }

        // 2. Match by Name
        if let Some(names) = name {
            let name_map = self.name_map.read().await;
            let mut results = Vec::new();
            for name_val in names {
                if let Some(ids) = name_map.get(&name_val) {
                    results.extend(ids.clone());
                }
            }
            results.sort();
            results.dedup();
            return results;
        }

        vec![]
    }

    /// Publishes device event using MQTT templates
    pub async fn publish_device_event(
        &self,
        id: String,
        name: String,
        dps: Value,
        is_passive: bool,
    ) {
        // Prepare default payload
        let mut payload_obj = dps.clone();
        if let Some(obj) = payload_obj.as_object_mut() {
            obj.insert("id".to_string(), Value::String(id.clone()));
            obj.insert("name".to_string(), Value::String(name.clone()));
        }
        let payload_str = payload_obj.to_string();

        if let Some(tx) = &self.mqtt_tx {
            // Check if device still exists before publishing to avoid race conditions during removal
            if !self.configs.read().await.contains_key(&id) {
                return;
            }

            let mut templates = Vec::new();

            // Always publish to default event topic
            templates.push((self.mqtt_event_topic.clone(), payload_str.clone()));

            // Select template based on event type
            let template = if is_passive {
                self.mqtt_passive_topic_template.as_deref()
            } else {
                self.mqtt_topic_template.as_deref()
            };

            if let Some(tpl) = template {
                if tpl.contains("{dp}") || tpl.contains("{value}") {
                    // Single DP per message mode
                    if let Some(dps_obj) = dps.as_object() {
                        for (dp, val) in dps_obj {
                            let topic = tpl
                                .replace("{id}", &id)
                                .replace("{name}", &name)
                                .replace("{dp}", dp);

                            let payload = if let Some(p_tpl) = &self.mqtt_payload_template {
                                p_tpl
                                    .replace("{value}", &val.to_string())
                                    .replace("{id}", &id)
                                    .replace("{name}", &name)
                                    .replace("{dp}", dp)
                            } else {
                                val.to_string()
                            };
                            templates.push((topic, payload));
                        }
                    }
                } else {
                    // All DPs in one message mode
                    let topic = tpl.replace("{id}", &id).replace("{name}", &name);
                    templates.push((topic, payload_str));
                }
            }

            for (topic, payload) in templates {
                if self.mqtt_retain {
                    let mut topics = self.published_topics.write().await;
                    topics.entry(id.clone()).or_default().insert(topic.clone());
                }

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

    /// Publishes bridge-level messages (e.g. scanner results)
    pub async fn publish_scanner_event(&self, payload: Value) {
        if let Some(tx) = &self.mqtt_tx {
            let topic = if let Some(tpl) = &self.mqtt_message_topic_template {
                tpl.replace("{level}", "scanner")
                    .replace("{id}", "bridge")
                    .replace("{name}", "bridge")
            } else {
                format!("{}/scanner", self.mqtt_event_topic.replace("/event", ""))
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
            let mut published = self.published_topics.write().await;
            if let Some(topics) = published.remove(id) {
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

    /// Adds or updates a device configuration
    pub async fn add_device(&self, cfg: DeviceConfig) -> Result<ApiResponse, BridgeError> {
        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;
        let mut cid_map = self.cid_map.write().await;
        let mut name_map = self.name_map.write().await;

        let id = cfg.id.clone();
        let name = cfg.name.clone();

        // Clean up old mappings if device already exists
        if let Some(old_cfg) = configs.get(&id) {
            if let Some(old_name) = &old_cfg.name
                && let Some(ids) = name_map.get_mut(old_name)
            {
                ids.retain(|x| x != &id);
                if ids.is_empty() {
                    name_map.remove(old_name);
                }
            }
            if let Some(old_cid) = &old_cfg.cid
                && let Some(old_parent) = &old_cfg.parent_id
            {
                cid_map.remove(&(old_parent.clone(), old_cid.clone()));
            }
        }

        if let Some(cid) = &cfg.cid
            && let Some(parent_id) = &cfg.parent_id
        {
            // Register as sub-device
            cid_map.insert((parent_id.clone(), cid.clone()), id.clone());
            instances.remove(&id);
        } else {
            // Register as direct device
            let dev = DeviceBuilder::new(&id, cfg.key.as_bytes().to_vec())
                .address(&cfg.ip)
                .version(&*cfg.version)
                .nowait(true)
                .run();
            instances.insert(id.clone(), dev);
        }

        configs.insert(id.clone(), cfg);
        if let Some(n) = name {
            name_map.entry(n).or_default().push(id.clone());
        }

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
        let mut targets = self.find_device_ids(id, name).await;
        if targets.is_empty() {
            return Err(BridgeError::NoMatchingDevices);
        }

        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;
        let mut cid_map = self.cid_map.write().await;
        let mut name_map = self.name_map.write().await;

        // Cascade removal to sub-devices
        let mut sub_targets = Vec::new();
        for target_id in &targets {
            for (sub_id, cfg) in configs.iter() {
                if let Some(p_id) = &cfg.parent_id
                    && p_id == target_id
                {
                    sub_targets.push(sub_id.clone());
                }
            }
        }
        targets.extend(sub_targets);
        targets.sort();
        targets.dedup();

        for target_id in &targets {
            if let Some(cfg) = configs.remove(target_id) {
                if let Some(n) = cfg.name
                    && let Some(ids) = name_map.get_mut(&n)
                {
                    ids.retain(|x| x != target_id);
                    if ids.is_empty() {
                        name_map.remove(&n);
                    }
                }
                if let Some(cid) = cfg.cid
                    && let Some(parent_id) = cfg.parent_id
                {
                    cid_map.remove(&(parent_id, cid));
                }
            }
            instances.remove(target_id);
            // Clear retained messages AFTER removing from configs to ensure no new events are published
            self.clear_retained_messages(target_id).await;
        }

        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("remove", targets.join(",")))
    }

    /// Clears all devices and configurations
    pub async fn clear_devices(&self) -> Result<ApiResponse, BridgeError> {
        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;
        let mut cid_map = self.cid_map.write().await;
        let mut name_map = self.name_map.write().await;

        let ids: Vec<String> = configs.keys().cloned().collect();

        configs.clear();
        instances.clear();
        cid_map.clear();
        name_map.clear();

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
        let configs = self.configs.read().await;
        let instances = self.instances.read().await;
        let mut devices = Vec::new();
        for cfg in configs.values() {
            let mut dev_val = serde_json::to_value(cfg).unwrap();
            if let Some(obj) = dev_val.as_object_mut() {
                let status = if cfg.cid.is_some() {
                    if let Some(parent_id) = &cfg.parent_id {
                        if instances.contains_key(parent_id) {
                            "subdevice"
                        } else {
                            "no parent"
                        }
                    } else {
                        "invalid subdevice"
                    }
                } else if instances.contains_key(&cfg.id) {
                    "online"
                } else {
                    "offline"
                };
                obj.insert("status".to_string(), Value::String(status.to_string()));
            }
            devices.push(dev_val);
        }
        ApiResponse::ok("status", "bridge").with_extra("devices", Value::Array(devices))
    }

    /// Gets a connected device instance, supporting sub-devices via parent
    pub async fn get_connected_device(&self, id: &str) -> Result<Device, BridgeError> {
        let instances = self.instances.read().await;
        if let Some(dev) = instances.get(id) {
            return Ok(dev.clone());
        }

        // Try lookup via parent for sub-devices
        let configs = self.configs.read().await;
        if let Some(cfg) = configs.get(id)
            && let Some(parent_id) = &cfg.parent_id
        {
            if let Some(parent_dev) = instances.get(parent_id) {
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
