use anyhow::{Context as _, Result};
use clap::Parser;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

pub const DEFAULT_STATE_FILE: &str = "rustuya.json";
pub const DEFAULT_SAVE_DEBOUNCE_SECS: u64 = 30;
pub const DEFAULT_SCAVENGER_TIMEOUT_SECS: u64 = 1;
/// Default cap on devices establishing a connection concurrently. Bounds the
/// onboarding "connect storm" when a large fleet is added at once.
pub const DEFAULT_CONNECT_CONCURRENCY: usize = 128;
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
    // Never serialize credentials: the running Cli is published to the retained
    // `{root}/bridge/config` topic, which any broker client with read access can
    // see. `skip_serializing` keeps input (CLI/env/config file) working while
    // keeping the secret off the wire. NOTE: credentials embedded inline in the
    // broker URL (`mqtt://user:pass@host`) still leak — prefer these flags/env.
    #[arg(long, env = "MQTT_USER")]
    #[serde(skip_serializing)]
    pub mqtt_user: Option<String>,

    /// MQTT Password
    #[arg(long, env = "MQTT_PASSWORD")]
    #[serde(skip_serializing)]
    pub mqtt_password: Option<String>,

    /// MQTT Root topic (defaults to rustuya)
    #[arg(long, env = "MQTT_ROOT_TOPIC")]
    pub mqtt_root_topic: Option<String>,

    /// MQTT Command topic (defaults to {root}/command).
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_command_topic: Option<String>,

    /// MQTT Event topic (defaults to {root}/event/{type}).
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_event_topic: Option<String>,

    /// MQTT Client ID
    #[arg(long, env = "MQTT_CLIENT_ID")]
    pub mqtt_client_id: Option<String>,

    /// MQTT Topic template for messages/errors (defaults to {root}/{level}/{id}).
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_message_topic: Option<String>,

    /// MQTT Payload template for device events (e.g. "{\"val\": {value}}", defaults to {value}).
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_payload_template: Option<String>,

    /// MQTT Topic for scanner results.
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_scanner_topic: Option<String>,

    /// MQTT Retain flag for device status updates.
    /// Config-file/`set_config` only — no CLI flag or env (single-source policy).
    #[arg(skip)]
    pub mqtt_retain: Option<bool>,

    /// State file path to persist registered devices
    #[arg(short = 's', long, env = "STATE_FILE")]
    pub state_file: Option<String>,

    /// Seconds to wait before saving state file (debounce)
    #[arg(long, env = "SAVE_DEBOUNCE_SECS")]
    pub save_debounce_secs: Option<u64>,

    /// Seconds the retain scavenger waits for retained messages before exiting.
    /// Raise this on slow brokers if retained device state isn't being cleared
    /// after `remove`/`clear`.
    #[arg(long, env = "SCAVENGER_TIMEOUT_SECS")]
    pub scavenger_timeout_secs: Option<u64>,

    /// Max devices establishing a connection concurrently (the handshake cap).
    /// Tames the onboarding "connect storm" when many devices are added at
    /// once: once connected a device is cheap (idle + heartbeat), so only the
    /// expensive establishment phase is bounded. `0` disables the cap
    /// (unbounded — the historical behavior).
    #[arg(long, env = "CONNECT_CONCURRENCY")]
    pub connect_concurrency: Option<usize>,

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
            scavenger_timeout_secs: Some(DEFAULT_SCAVENGER_TIMEOUT_SECS),
            connect_concurrency: Some(DEFAULT_CONNECT_CONCURRENCY),
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
    /// Resolution priority (highest first): CLI/env > config file > config-relative
    /// default (for `state_file` only, see [`Cli::resolve_default_state_file`]) >
    /// [`Cli::default`].
    ///
    /// # Errors
    /// Returns an error if the config file cannot be read, parsed, or written.
    pub async fn load() -> Result<Self> {
        let mut cli = Self::parse();
        cli.apply_config_file().await?;
        cli.resolve_default_state_file();
        cli.merge(Self::default());
        Ok(cli)
    }

    /// Resolves `state_file` against the directory of `--config` when both apply:
    ///   - `state_file` is unset → defaults to `<config-dir>/rustuya.json`.
    ///   - `state_file` is set but **relative** (`"mystate.json"`,
    ///     `"data/state.json"`, etc.) → reinterpreted as
    ///     `<config-dir>/<state_file>`.
    ///   - `state_file` is **absolute** → left untouched.
    ///
    /// Without this hook, a CWD-relative path would surprise anyone launching
    /// the bridge from a different directory than its config lives in. With
    /// `--config` provided, the user almost certainly means "next to the
    /// config", not "wherever I happened to `cd` first".
    pub fn resolve_default_state_file(&mut self) {
        let Some(cfg) = &self.config else { return };
        let Some(parent) = Path::new(cfg).parent() else {
            return;
        };
        if parent.as_os_str().is_empty() {
            return;
        }
        match &self.state_file {
            None => {
                self.state_file = Some(
                    parent
                        .join(DEFAULT_STATE_FILE)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            Some(p) if !Path::new(p).is_absolute() => {
                self.state_file =
                    Some(parent.join(p).to_string_lossy().into_owned());
            }
            Some(_) => {} // absolute path — respect as-is
        }
    }

    /// Reads the JSON config file at `self.config` and merges it into `self`.
    /// If the file does not exist, the current settings (merged with [`Cli::default`])
    /// are written there for future runs. No-op when `self.config` is `None`.
    ///
    /// The caller is expected to run `self.merge(Self::default())` afterward so any
    /// field still `None` falls back to [`Cli::default`].
    ///
    /// # Errors
    /// Returns an error if the config file cannot be read, parsed, or written.
    pub async fn apply_config_file(&mut self) -> Result<()> {
        let Some(config_path) = self.config.clone() else {
            return Ok(());
        };
        let path = Path::new(&config_path);
        if path.exists() {
            let content = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("Failed to read config file: {config_path}"))?;
            let file_cli: Self = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {config_path}"))?;
            self.merge(file_cli);
            info!("Merged configuration from {config_path}");
        } else {
            info!("Config file {config_path} not found, creating a new one from current settings");

            // Resolve state_file relative to the new config's directory BEFORE
            // merging defaults, so the written file captures the resolved path
            // explicitly instead of the literal CWD-relative "rustuya.json".
            self.resolve_default_state_file();
            self.merge(Self::default());

            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }

            let content = serde_json::to_string_pretty(self)
                .with_context(|| "Failed to serialize configuration")?;
            tokio::fs::write(path, content)
                .await
                .with_context(|| format!("Failed to write config file: {config_path}"))?;
            info!("Saved configuration to {config_path}");
        }
        Ok(())
    }

    /// Fills `None` fields in `self` from `other`. Existing `Some` values are preserved.
    pub fn merge(&mut self, other: Self) {
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
            scavenger_timeout_secs,
            connect_concurrency,
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
            scavenger_timeout_secs,
            connect_concurrency,
            log_level,
            no_signals,
            session_id,
        );
    }

    /// Returns `(command_topic, event_topic)` with `{root}` substituted.
    #[allow(clippy::literal_string_with_formatting_args)]
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
    #[allow(clippy::literal_string_with_formatting_args)]
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
    pub fn scavenger_timeout_secs(&self) -> u64 {
        self.scavenger_timeout_secs
            .unwrap_or(DEFAULT_SCAVENGER_TIMEOUT_SECS)
    }

    /// Connection-establishment concurrency cap (`0` = unbounded).
    #[must_use]
    pub fn connect_concurrency(&self) -> usize {
        self.connect_concurrency
            .unwrap_or(DEFAULT_CONNECT_CONCURRENCY)
    }

    #[must_use]
    pub fn mqtt_retain(&self) -> bool {
        self.mqtt_retain.unwrap_or(false)
    }
}

/// Env vars for the topic/template/retain fields that are now managed solely via
/// the config file and the `set_config` command (single-source policy). They are
/// intentionally NOT wired as CLI/env args, so if one is set in the environment
/// it is silently ignored — [`ignored_topic_env_overrides`] surfaces that.
pub const MANAGED_TOPIC_ENV_VARS: [&str; 6] = [
    "MQTT_COMMAND_TOPIC",
    "MQTT_EVENT_TOPIC",
    "MQTT_MESSAGE_TOPIC",
    "MQTT_SCANNER_TOPIC",
    "MQTT_PAYLOAD_TEMPLATE",
    "MQTT_RETAIN",
];

/// Returns the subset of [`MANAGED_TOPIC_ENV_VARS`] currently present in the
/// environment. These are no longer honored (see the field docs on [`Cli`]); the
/// binary logs a warning for each so an operator migrating from the old
/// env-driven setup sees a loud no-op instead of a silent one.
#[must_use]
pub fn ignored_topic_env_overrides() -> Vec<&'static str> {
    MANAGED_TOPIC_ENV_VARS
        .into_iter()
        .filter(|name| std::env::var_os(name).is_some())
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── #8: scavenger_timeout_secs plumbing ──────────────────────────────

    #[test]
    fn scavenger_timeout_secs_returns_default_when_unset() {
        let cli = Cli {
            scavenger_timeout_secs: None,
            ..Cli::default()
        };
        assert_eq!(cli.scavenger_timeout_secs(), DEFAULT_SCAVENGER_TIMEOUT_SECS);
    }

    #[test]
    fn scavenger_timeout_secs_returns_override_when_set() {
        let cli = Cli {
            scavenger_timeout_secs: Some(7),
            ..Cli::default()
        };
        assert_eq!(cli.scavenger_timeout_secs(), 7);
    }

    #[test]
    fn scavenger_timeout_secs_round_trips_through_config_json() {
        // The bridge persists/reloads Cli via serde_json. Make sure the new
        // field survives that round-trip (catches a missing serde attribute).
        let cli = Cli {
            scavenger_timeout_secs: Some(42),
            ..Cli::default()
        };
        let json = serde_json::to_string(&cli).expect("serialize");
        let back: Cli = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.scavenger_timeout_secs(), 42);
    }

    #[test]
    fn serialized_cli_omits_credentials_but_keeps_session_id() {
        // The running Cli is published to the retained bridge/config topic, so
        // credentials must not serialize. session_id must survive (duplicate-
        // instance detection reads it back off that topic).
        let cli = Cli {
            mqtt_user: Some("secretuser".into()),
            mqtt_password: Some("S3cret-pw".into()),
            session_id: Some("sid_123".into()),
            ..Cli::default()
        };
        let json = serde_json::to_string(&cli).expect("serialize");
        assert!(!json.contains("mqtt_user"), "mqtt_user key leaked: {json}");
        assert!(
            !json.contains("mqtt_password"),
            "mqtt_password key leaked: {json}"
        );
        assert!(!json.contains("secretuser"), "user value leaked: {json}");
        assert!(!json.contains("S3cret-pw"), "password value leaked: {json}");
        assert!(json.contains("session_id"), "session_id missing: {json}");

        // Input still works: skip_serializing must not break deserialization.
        let back: Cli =
            serde_json::from_str(r#"{"mqtt_password":"p","mqtt_user":"u"}"#).expect("deserialize");
        assert_eq!(back.mqtt_password.as_deref(), Some("p"));
        assert_eq!(back.mqtt_user.as_deref(), Some("u"));
    }

    #[test]
    fn merge_preserves_scavenger_timeout_secs_when_set() {
        // `merge` only fills None fields from `other` — an explicit Some
        // must survive.
        let mut a = Cli {
            scavenger_timeout_secs: Some(10),
            ..Cli::default()
        };
        let b = Cli {
            scavenger_timeout_secs: Some(99),
            ..Cli::default()
        };
        a.merge(b);
        assert_eq!(a.scavenger_timeout_secs(), 10);
    }

    #[test]
    fn merge_fills_scavenger_timeout_secs_from_other_when_none() {
        let mut a = Cli {
            scavenger_timeout_secs: None,
            ..Cli::default()
        };
        let b = Cli {
            scavenger_timeout_secs: Some(99),
            ..Cli::default()
        };
        a.merge(b);
        assert_eq!(a.scavenger_timeout_secs(), 99);
    }

    // ── resolve_default_state_file ──────────────────────────────────────

    fn make_cli(config: Option<&str>, state_file: Option<&str>) -> Cli {
        Cli {
            config: config.map(String::from),
            state_file: state_file.map(String::from),
            ..Cli::default()
        }
    }

    #[test]
    fn resolve_state_file_no_op_without_config() {
        let mut cli = make_cli(None, None);
        cli.resolve_default_state_file();
        assert_eq!(cli.state_file, None);
    }

    #[test]
    fn resolve_state_file_defaults_to_config_dir_when_unset() {
        let mut cli = make_cli(Some("/etc/rustuya/config.json"), None);
        cli.resolve_default_state_file();
        assert_eq!(
            cli.state_file.as_deref(),
            Some("/etc/rustuya/rustuya.json")
        );
    }

    #[test]
    fn resolve_state_file_rewrites_relative_path_against_config_dir() {
        // User wrote just a filename in config.json — they almost certainly
        // mean "next to the config", not "wherever I cd-ed first".
        let mut cli = make_cli(Some("/etc/rustuya/config.json"), Some("mystate.json"));
        cli.resolve_default_state_file();
        assert_eq!(
            cli.state_file.as_deref(),
            Some("/etc/rustuya/mystate.json")
        );
    }

    #[test]
    fn resolve_state_file_rewrites_multi_segment_relative_path() {
        let mut cli = make_cli(
            Some("/etc/rustuya/config.json"),
            Some("data/state.json"),
        );
        cli.resolve_default_state_file();
        assert_eq!(
            cli.state_file.as_deref(),
            Some("/etc/rustuya/data/state.json")
        );
    }

    #[test]
    fn resolve_state_file_leaves_absolute_path_untouched() {
        let mut cli = make_cli(
            Some("/etc/rustuya/config.json"),
            Some("/var/lib/foo/state.json"),
        );
        cli.resolve_default_state_file();
        assert_eq!(
            cli.state_file.as_deref(),
            Some("/var/lib/foo/state.json")
        );
    }

    // ── topic fields: no CLI flag / env (single-source policy) ──────────

    #[test]
    fn topic_fields_are_not_cli_flags() {
        // The 6 managed fields use `#[arg(skip)]`, so passing them as flags must
        // be rejected as unknown args — proving they're config-file/`set_config`
        // only.
        for flag in [
            "--mqtt-command-topic",
            "--mqtt-event-topic",
            "--mqtt-message-topic",
            "--mqtt-scanner-topic",
            "--mqtt-payload-template",
            "--mqtt-retain",
        ] {
            let res = Cli::try_parse_from(["rustuyabridge", flag, "x"]);
            assert!(res.is_err(), "{flag} should not be a recognized CLI flag");
        }
    }

    #[test]
    fn topic_fields_still_round_trip_through_config_json() {
        // env/CLI removal must not touch serde: config-file load is the source.
        let cli = Cli {
            mqtt_command_topic: Some("{root}/cmd".into()),
            mqtt_event_topic: Some("{root}/ev/{id}".into()),
            mqtt_message_topic: Some("{root}/msg/{id}".into()),
            mqtt_scanner_topic: Some("{root}/scan".into()),
            mqtt_payload_template: Some(r#"{"v":{value}}"#.into()),
            mqtt_retain: Some(true),
            ..Cli::default()
        };
        let json = serde_json::to_string(&cli).expect("serialize");
        let back: Cli = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.mqtt_command_topic.as_deref(), Some("{root}/cmd"));
        assert_eq!(back.mqtt_event_topic.as_deref(), Some("{root}/ev/{id}"));
        assert_eq!(back.mqtt_message_topic.as_deref(), Some("{root}/msg/{id}"));
        assert_eq!(back.mqtt_scanner_topic.as_deref(), Some("{root}/scan"));
        assert_eq!(back.mqtt_payload_template.as_deref(), Some(r#"{"v":{value}}"#));
        assert_eq!(back.mqtt_retain, Some(true));
    }

    #[test]
    fn managed_env_var_list_is_stable() {
        // The boot warning iterates these names; keep them in sync with the
        // fields above so a renamed field can't silently drop its warning.
        assert_eq!(
            MANAGED_TOPIC_ENV_VARS,
            [
                "MQTT_COMMAND_TOPIC",
                "MQTT_EVENT_TOPIC",
                "MQTT_MESSAGE_TOPIC",
                "MQTT_SCANNER_TOPIC",
                "MQTT_PAYLOAD_TEMPLATE",
                "MQTT_RETAIN",
            ]
        );
    }

    #[test]
    fn resolve_state_file_no_op_for_bare_config_filename() {
        // `--config config.json` has no parent dir → no config-dir context to
        // attach to; fall through to Cli::default behaviour.
        let mut cli = make_cli(Some("config.json"), None);
        cli.resolve_default_state_file();
        assert_eq!(cli.state_file, None);
    }
}
