use anyhow::{Context as _, Result};
use clap::Parser;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

pub const DEFAULT_STATE_FILE: &str = "rustuya.json";
pub const DEFAULT_SAVE_DEBOUNCE_SECS: u64 = 30;

#[derive(Parser, Debug, Serialize, Deserialize, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Config file path (JSON)
    #[arg(short = 'C', long, env = "CONFIG")]
    pub config: Option<String>,

    /// MQTT Broker address (e.g., mqtt://localhost:1883, mqtts://localhost:8883, tcp://localhost:1883, or localhost)
    #[arg(short = 'm', long, env = "MQTT_BROKER")]
    pub mqtt_broker: Option<String>,

    /// MQTT Root topic (used as prefix for command/event topics)
    #[arg(long, env = "MQTT_ROOT_TOPIC")]
    pub mqtt_root_topic: Option<String>,

    /// MQTT Command topic (defaults to {root}/command)
    #[arg(long, env = "MQTT_COMMAND_TOPIC")]
    pub mqtt_command_topic: Option<String>,

    /// MQTT Event topic (defaults to {root_topic}/event)
    #[arg(long, env = "MQTT_EVENT_TOPIC")]
    pub mqtt_event_topic: Option<String>,

    /// MQTT Client ID
    #[arg(long, env = "MQTT_CLIENT_ID")]
    pub mqtt_client_id: Option<String>,

    /// MQTT Topic template for messages/errors
    #[arg(long, env = "MQTT_MESSAGE_TOPIC")]
    pub mqtt_message_topic: Option<String>,

    /// MQTT Payload template for device events (e.g. "{\"val\": {value}}")
    #[arg(long, env = "MQTT_PAYLOAD_TEMPLATE")]
    pub mqtt_payload_template: Option<String>,

    /// MQTT Topic for scanner results
    #[arg(long, env = "MQTT_SCANNER_TOPIC")]
    pub mqtt_scanner_topic: Option<String>,

    /// MQTT Retain flag for device status updates
    #[arg(long, env = "MQTT_RETAIN")]
    pub mqtt_retain: Option<bool>,

    /// State file path to persist registered devices
    #[arg(short = 's', long, env = "STATE_FILE")]
    pub state_file: Option<String>,

    /// Seconds to wait before saving state file (debounce)
    #[arg(long, env = "SAVE_DEBOUNCE_SECS")]
    pub save_debounce_secs: Option<u64>,

    /// Log level (error, warn, info, debug, trace)
    #[arg(short = 'l', long, env = "LOG_LEVEL")]
    pub log_level: Option<String>,
}

impl Cli {
    pub async fn load() -> Result<Self> {
        let mut cli = Self::parse();

        if let Some(config_path) = cli.config.clone() {
            let path = Path::new(&config_path);
            if path.exists() {
                let content = tokio::fs::read_to_string(path)
                    .await
                    .with_context(|| format!("Failed to read config file: {config_path}"))?;
                let file_cli: Self = serde_json::from_str(&content)
                    .with_context(|| format!("Failed to parse config file: {config_path}"))?;
                cli.merge(file_cli);
                info!("Merged configuration from {config_path}");
            } else if std::env::var("CONFIG").is_ok_and(|v| v == config_path) {
                // If it came from ENV and doesn't exist, just skip it without error
                info!("Config file {config_path} not found, skipping (using defaults/env)");
            } else {
                // If it was explicitly provided via CLI, it should probably be an error
                anyhow::bail!("Config file not found: {config_path}");
            }
        }

        Ok(cli)
    }

    fn merge(&mut self, other: Self) {
        let Self {
            config: _,
            mqtt_broker,
            mqtt_root_topic,
            mqtt_command_topic,
            mqtt_event_topic,
            mqtt_client_id,
            mqtt_message_topic,
            mqtt_payload_template,
            mqtt_scanner_topic,
            mqtt_retain,
            state_file,
            save_debounce_secs,
            log_level,
        } = other;

        macro_rules! merge_field {
            ($field:ident) => {
                if self.$field.is_none() {
                    self.$field = $field;
                }
            };
        }

        merge_field!(mqtt_broker);
        merge_field!(mqtt_root_topic);
        merge_field!(mqtt_command_topic);
        merge_field!(mqtt_event_topic);
        merge_field!(mqtt_client_id);
        merge_field!(mqtt_message_topic);
        merge_field!(mqtt_payload_template);
        merge_field!(mqtt_scanner_topic);
        merge_field!(mqtt_retain);
        merge_field!(state_file);
        merge_field!(save_debounce_secs);
        merge_field!(log_level);
    }

    pub fn get_mqtt_topics(&self) -> (String, String) {
        let root_topic = self.mqtt_root_topic.as_deref().unwrap_or("rustuya");
        let mqtt_command_topic = self.mqtt_command_topic.as_deref().map_or_else(
            || format!("{root_topic}/command"),
            |t| t.replace("{root}", root_topic),
        );
        let mqtt_event_topic = self.mqtt_event_topic.as_deref().map_or_else(
            || format!("{root_topic}/event/{{type}}"),
            |t| t.replace("{root}", root_topic),
        );
        (mqtt_command_topic, mqtt_event_topic)
    }

    pub fn get_state_file(&self) -> String {
        self.state_file
            .as_deref()
            .unwrap_or(DEFAULT_STATE_FILE)
            .to_string()
    }

    pub fn get_save_debounce_secs(&self) -> u64 {
        self.save_debounce_secs
            .unwrap_or(DEFAULT_SAVE_DEBOUNCE_SECS)
    }

    pub fn get_mqtt_retain(&self) -> bool {
        self.mqtt_retain.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceConfig {
    #[serde(alias = "devId", alias = "device_id", default, skip_serializing)]
    pub id: String,
    #[serde(skip_serializing_if = "is_empty_option")]
    pub name: Option<String>,
    #[serde(
        skip_serializing_if = "is_empty_option",
        alias = "ipAddress",
        alias = "address"
    )]
    pub ip: Option<String>,
    #[serde(
        skip_serializing_if = "is_empty_option",
        alias = "localKey",
        alias = "localkey",
        alias = "local_key"
    )]
    pub key: Option<String>,
    #[serde(
        skip_serializing_if = "is_empty_option",
        alias = "protocol",
        alias = "ver"
    )]
    pub version: Option<String>,
    #[serde(
        skip_serializing_if = "is_empty_option",
        alias = "node_id",
        alias = "nodeId"
    )]
    pub cid: Option<String>,
    #[serde(
        skip_serializing_if = "is_empty_option",
        alias = "parent",
        alias = "parentId",
        alias = "gatewayId"
    )]
    pub parent_id: Option<String>,
}

#[allow(clippy::ref_option)]
fn is_empty_option(opt: &Option<String>) -> bool {
    opt.as_ref().is_none_or(|s| s.is_empty() || s == "Auto")
}

pub async fn load_state(path: &str) -> HashMap<String, DeviceConfig> {
    let path_obj = Path::new(path);
    if !path_obj.exists() {
        return HashMap::new();
    }

    let devices_res: Result<HashMap<String, DeviceConfig>> = async {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StateFormat {
            Map(HashMap<String, DeviceConfig>),
            List(Vec<DeviceConfig>),
        }

        let content = tokio::fs::read_to_string(path_obj).await?;
        let format: StateFormat = serde_json::from_str(&content)?;

        let devices = match format {
            StateFormat::Map(mut map) => {
                for (k, v) in &mut map {
                    if v.id.is_empty() {
                        v.id.clone_from(k);
                    }
                }
                map
            }
            StateFormat::List(list) => list
                .into_iter()
                .map(|d| (d.id.clone(), d))
                .collect::<HashMap<_, _>>(),
        };
        Ok(devices)
    }
    .await;

    match devices_res {
        Ok(devices) => {
            info!("Loaded {} devices from {path:?}", devices.len());
            devices
        }
        Err(e) => {
            error!("Failed to load state file {path:?}: {e}");
            HashMap::new()
        }
    }
}
