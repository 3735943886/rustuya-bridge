use anyhow::{Context as _, Result};
use clap::Parser;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

pub const DEFAULT_STATE_FILE: &str = "rustuya.json";
pub const DEFAULT_SAVE_DEBOUNCE_SECS: u64 = 30;
pub const DEFAULT_MQTT_ROOT_TOPIC: &str = "rustuya";
pub const DEFAULT_MQTT_COMMAND_TOPIC: &str = "{root}/command";
pub const DEFAULT_MQTT_EVENT_TOPIC: &str = "{root}/event/{type}/{id}";
pub const DEFAULT_MQTT_MESSAGE_TOPIC: &str = "{root}/{level}/{id}";
pub const DEFAULT_MQTT_PAYLOAD_TEMPLATE: &str = "{value}";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_MQTT_CLIENT_ID: &str = "{root}-bridge";
pub const DEFAULT_MQTT_SCANNER_TOPIC: &str = "{root}/scanner";
pub const BRIDGE_CONFIG_TOPIC: &str = "{root}/bridge/config";

#[derive(Parser, Debug, Serialize, Deserialize, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Config file path (JSON)
    #[arg(short = 'C', long, env = "CONFIG")]
    pub config: Option<String>,

    /// MQTT Broker address (e.g., `mqtt://localhost:1883`, `mqtts://localhost:8883`, `tcp://localhost:1883`, or `localhost`)
    #[arg(short = 'm', long, env = "MQTT_BROKER")]
    pub mqtt_broker: Option<String>,

    /// MQTT User
    #[arg(long, env = "MQTT_USER")]
    pub mqtt_user: Option<String>,

    /// MQTT Password
    #[arg(long, env = "MQTT_PASSWORD")]
    pub mqtt_password: Option<String>,

    /// MQTT Root topic (defaults to rustuya)
    #[arg(long, env = "MQTT_ROOT_TOPIC")]
    pub mqtt_root_topic: Option<String>,

    /// MQTT Command topic (defaults to {root}/command)
    #[arg(long, env = "MQTT_COMMAND_TOPIC")]
    pub mqtt_command_topic: Option<String>,

    /// MQTT Event topic (defaults to {root}/event/{type})
    #[arg(long, env = "MQTT_EVENT_TOPIC")]
    pub mqtt_event_topic: Option<String>,

    /// MQTT Client ID
    #[arg(long, env = "MQTT_CLIENT_ID")]
    pub mqtt_client_id: Option<String>,

    /// MQTT Topic template for messages/errors (defaults to {root}/{level}/{id})
    #[arg(long, env = "MQTT_MESSAGE_TOPIC")]
    pub mqtt_message_topic: Option<String>,

    /// MQTT Payload template for device events (e.g. "{\"val\": {value}}", defaults to {value})
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

    /// Disable internal signal handling (for library use)
    #[arg(long, env = "NO_SIGNALS")]
    pub no_signals: Option<bool>,

    /// Session ID generated at runtime to prevent duplicate execution
    #[serde(skip_deserializing)]
    #[arg(skip)]
    pub session_id: Option<String>,
}

impl Default for Cli {
    /// Defaults used as the last layer of [`Cli::load`]. Fields without a meaningful
    /// default (`mqtt_broker`, `mqtt_user`, `mqtt_password`, `config`, `session_id`)
    /// stay `None`.
    fn default() -> Self {
        Self {
            config: None,
            mqtt_broker: None,
            mqtt_user: None,
            mqtt_password: None,
            mqtt_root_topic: Some(DEFAULT_MQTT_ROOT_TOPIC.into()),
            mqtt_command_topic: Some(DEFAULT_MQTT_COMMAND_TOPIC.into()),
            mqtt_event_topic: Some(DEFAULT_MQTT_EVENT_TOPIC.into()),
            mqtt_client_id: Some(DEFAULT_MQTT_CLIENT_ID.into()),
            mqtt_message_topic: Some(DEFAULT_MQTT_MESSAGE_TOPIC.into()),
            mqtt_payload_template: Some(DEFAULT_MQTT_PAYLOAD_TEMPLATE.into()),
            mqtt_scanner_topic: Some(DEFAULT_MQTT_SCANNER_TOPIC.into()),
            mqtt_retain: Some(false),
            state_file: Some(DEFAULT_STATE_FILE.into()),
            save_debounce_secs: Some(DEFAULT_SAVE_DEBOUNCE_SECS),
            log_level: Some(DEFAULT_LOG_LEVEL.into()),
            no_signals: Some(false),
            session_id: None,
        }
    }
}

impl Cli {
    /// Parses CLI/env arguments and merges any JSON config file specified by `--config`.
    /// If the file does not exist, the current settings are written there for future runs.
    ///
    /// Resolution priority (highest first): CLI/env > config file > [`Cli::default`].
    ///
    /// # Errors
    /// Returns an error if the config file cannot be read, parsed, or written.
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
            } else {
                info!(
                    "Config file {config_path} not found, creating a new one from current settings"
                );

                cli.merge(Self::default());

                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }

                let content = serde_json::to_string_pretty(&cli)
                    .with_context(|| "Failed to serialize configuration")?;
                tokio::fs::write(path, content)
                    .await
                    .with_context(|| format!("Failed to write config file: {config_path}"))?;
                info!("Saved configuration to {config_path}");
            }
        }

        cli.merge(Self::default());
        Ok(cli)
    }

    /// Fills `None` fields in `self` from `other`. Existing `Some` values are preserved.
    fn merge(&mut self, other: Self) {
        let Self {
            config: _,
            mqtt_broker,
            mqtt_user,
            mqtt_password,
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
            no_signals,
            session_id,
        } = other;
        macro_rules! fill {
            ($($f:ident),* $(,)?) => { $(self.$f = self.$f.take().or($f);)* };
        }
        fill!(
            mqtt_broker,
            mqtt_user,
            mqtt_password,
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
            no_signals,
            session_id,
        );
    }

    /// Returns `(command_topic, event_topic)` with `{root}` substituted.
    #[must_use]
    pub fn mqtt_topics(&self) -> (String, String) {
        let root = self
            .mqtt_root_topic
            .as_deref()
            .unwrap_or(DEFAULT_MQTT_ROOT_TOPIC);
        let command = self
            .mqtt_command_topic
            .as_deref()
            .unwrap_or(DEFAULT_MQTT_COMMAND_TOPIC);
        let event = self
            .mqtt_event_topic
            .as_deref()
            .unwrap_or(DEFAULT_MQTT_EVENT_TOPIC);
        (
            command.replace("{root}", root),
            event.replace("{root}", root),
        )
    }

    /// Returns the MQTT client ID with `{root}` substituted.
    #[must_use]
    pub fn mqtt_client_id(&self) -> String {
        let root = self
            .mqtt_root_topic
            .as_deref()
            .unwrap_or(DEFAULT_MQTT_ROOT_TOPIC);
        self.mqtt_client_id
            .as_deref()
            .unwrap_or(DEFAULT_MQTT_CLIENT_ID)
            .replace("{root}", root)
    }

    #[must_use]
    pub fn state_file(&self) -> &str {
        self.state_file.as_deref().unwrap_or(DEFAULT_STATE_FILE)
    }

    #[must_use]
    pub fn save_debounce_secs(&self) -> u64 {
        self.save_debounce_secs
            .unwrap_or(DEFAULT_SAVE_DEBOUNCE_SECS)
    }

    #[must_use]
    pub fn mqtt_retain(&self) -> bool {
        self.mqtt_retain.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceConfig {
    #[serde(alias = "devId", alias = "device_id", default)]
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
    /// Last error code received from the device. Not persisted to state file.
    #[serde(skip)]
    pub last_error_code: Option<u32>,
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
