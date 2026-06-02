use anyhow::Result;
use futures_util::StreamExt;
use log::{debug, error, info, trace, warn};
use regex::Regex;
use rustuya::{Device, DeviceBuilder};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;

use crate::config::{Cli, DeviceConfig, load_state};
use crate::dps_cache::DpsCache;
use crate::error::BridgeError;
use crate::types::{ApiResponse, BridgeRequest};
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex as StdMutex;

// 200 (not 100) because cache mode doubles per-active-event push count
// (active delta + state snapshot). At 100, a sustained 50-msg/sec active
// burst against a slow broker could trip `try_send_mqtt`'s 500ms timeout.
pub const MQTT_CHANNEL_CAPACITY: usize = 200;
pub const INITIAL_RETRY_DELAY_SECS: u64 = 10;
pub const MAX_RETRY_DELAY_SECS: u64 = 1280;
pub const LISTENER_TIMEOUT_SECS: u64 = 300;
pub const REQUEST_TIMEOUT_SECS: u64 = 15;

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
    /// Identifier placeholders present in the (event_topic, payload_template) pair.
    /// Drives the retain gate in [`Self::publish_device_event`].
    pub event_identifiers: IdentifierSet,
    pub state_file: String,
    pub save_debounce_secs: u64,
    pub state: RwLock<BridgeState>,
    pub save_tx: mpsc::Sender<()>,
    pub refresh_tx: mpsc::Sender<()>,
    pub scavenger_tx:
        tokio::sync::Mutex<Option<mpsc::UnboundedSender<Vec<crate::types::ScavengerTarget>>>>,
    /// Count of MQTT messages dropped because the outbound channel stayed
    /// full past `try_send_mqtt`'s timeout. Exposed in `status` responses.
    pub mqtt_drop_count: AtomicU64,
    pub cancel: tokio_util::sync::CancellationToken,

    /// Per-device merged DPS cache. `Some` only when `mqtt_retain=true` and the
    /// event topic carries `{type}` (cache mode). `None` in pass-through mode (pass-through).
    pub cache: Option<Arc<DpsCache>>,

    /// Set when the broker-retained seed phase has completed (cache mode only). While
    /// `false`, cache-driven snapshot publishes are deferred and the
    /// device id is recorded in `seed_pending` for a single flush at seed end.
    pub seed_done: AtomicBool,

    /// Device ids whose cache changed during the seed window. Drained once at
    /// seed end to publish a single state snapshot per device, avoiding the
    /// noisy "republish everything" pattern.
    pub seed_pending: StdMutex<BTreeSet<String>>,

    /// MQTT wildcard topic used to recover cached state from the broker on
    /// startup (cache mode only, default payload template only). Derived from the event
    /// topic with `{type}=passive` and other placeholders as `+`.
    pub seed_state_wildcard: Option<String>,

    /// Regex matched against incoming retained messages on the seed wildcard
    /// to extract `{id}` and (optionally) `{dp}` from the topic.
    pub seed_state_regex: Option<Regex>,
}

/// Topic-template variable keys recognised by [`tpl_to_wildcard`], [`compile_topic_regex`],
/// and [`BridgeContext::replace_vars`]. The `value`, `dps`, `timestamp`, and `root` keys are
/// handled separately because they require values rather than wildcard substitution.
const TOPIC_WILDCARD_KEYS: &[&str] = &["id", "name", "dp", "action", "cid", "type", "level"];

/// Records which identifier placeholders (`{id}`, `{name}`, `{cid}`) a set of
/// topic/payload templates references. Used to decide whether `retain=true` is
/// safe: a retained message must carry an identifier so the scavenger can find
/// and clear it when the device is later removed.
///
/// `{id}` is treated as always-satisfied because every registered device has an
/// id; `{name}` and `{cid}` only satisfy retain when the device actually has a
/// non-empty value for that field.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdentifierSet {
    pub id: bool,
    pub name: bool,
    pub cid: bool,
}

impl IdentifierSet {
    /// Union of identifiers referenced across all given templates.
    #[must_use]
    pub fn from_templates<'a, I: IntoIterator<Item = &'a str>>(templates: I) -> Self {
        let mut set = Self::default();
        for tpl in templates {
            if tpl.contains("{id}") {
                set.id = true;
            }
            if tpl.contains("{name}") {
                set.name = true;
            }
            if tpl.contains("{cid}") {
                set.cid = true;
            }
        }
        set
    }

    /// `true` when the templates reference no identifier — retain is structurally
    /// impossible to scavenge regardless of the device.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        !self.id && !self.name && !self.cid
    }

    /// `true` when at least one referenced identifier has a usable value for the
    /// given device, so the scavenger can locate and clear the retained message.
    #[must_use]
    pub fn satisfied_by(self, name: Option<&str>, cid: Option<&str>) -> bool {
        self.id
            || (self.name && name.is_some_and(|s| !s.is_empty()))
            || (self.cid && cid.is_some_and(|s| !s.is_empty()))
    }
}

/// Walks a template string and substitutes `{key}` placeholders.
///
/// It calls `substitute(key, &mut out)` for each placeholder. The callback returns
/// `true` to consume the placeholder or `false` to leave it untouched
/// (the literal `{key}` is then emitted).
pub fn render_template<F>(template: &str, mut substitute: F) -> String
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

/// Parses a broker-retained seed message payload into a `dps` map. Only
/// handles the default `{value}` payload template: multi-DP mode payloads
/// are the full dps JSON object; single-DP mode payloads are the per-DP
/// value (the `dp` is recovered from the topic). Returns `None` on malformed
/// payloads or templates this parser can't reverse — caller drops silently.
fn parse_seed_payload(
    payload: &str,
    dp: Option<&str>,
) -> Option<serde_json::Map<String, Value>> {
    let v: Value = serde_json::from_str(payload).ok()?;
    match dp {
        Some(dp_key) => {
            let mut m = serde_json::Map::new();
            m.insert(dp_key.to_string(), v);
            Some(m)
        }
        None => v.as_object().cloned(),
    }
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
    /// The `cancel` token is owned by the caller (e.g. [`crate::server::BridgeServer`]
    /// or a language binding) so shutdown can be requested from outside this
    /// context — `run()` selects on it. Pass a fresh `CancellationToken::new()`
    /// if you have no external handle to wire up.
    ///
    /// # Errors
    /// Returns an error if the state file directory is not writable.
    pub async fn new(
        cli: &Cli,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(
        Arc<Self>,
        mpsc::Receiver<Option<MqttMessage>>,
        mpsc::Receiver<()>,
        mpsc::Receiver<()>,
    )> {
        verify_write_permission(cli.state_file()).await?;

        if cli.mqtt_broker.is_none() {
            warn!(
                "No --mqtt-broker configured; running in debug/standalone mode (devices are tracked locally but no MQTT publish/subscribe)"
            );
        }

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
                    .build();
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

        let payload_tpl = cli
            .mqtt_payload_template
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_MQTT_PAYLOAD_TEMPLATE);
        let event_identifiers =
            IdentifierSet::from_templates([mqtt_event_topic.as_str(), payload_tpl]);

        if cli.mqtt_retain() && event_identifiers.is_empty() {
            warn!(
                "mqtt_retain=true but neither event_topic ('{mqtt_event_topic}') nor payload_template ('{payload_tpl}') references an identifier ({{id}}, {{name}}, or {{cid}}); retained device events would be unreachable for the scavenger and will be dropped"
            );
        }

        // cache mode (mqtt_retain=true) splits each device update across a
        // no-retain delta and a retained snapshot. MQTT semantics keep us
        // safe on the reload path regardless of templates: a no-retain
        // publish doesn't overwrite the retained value, so a re-subscribing
        // client always receives only the latest snapshot, never a stale
        // active. Spurious re-fires on reconnect — the primary bug we
        // wanted to kill — are gone unconditionally.
        //
        // The only residual concern is *live* double-fire: a subscriber on
        // the active+snapshot topic gets two messages per active event and
        // can't tell which is which unless `{type}` appears in the topic
        // (separate subscriptions) or the payload (filter via value_json).
        // State entities are idempotent and don't care; event automations
        // need the distinction. We warn rather than downgrade because
        // mqtt_retain=true is the user's explicit opt-in, and a fleet with
        // no event automations is perfectly happy with neither {type}.
        let type_distinguishable =
            mqtt_event_topic.contains("{type}") || payload_tpl.contains("{type}");
        if cli.mqtt_retain() && !type_distinguishable {
            warn!(
                "mqtt_retain=true but {{type}} is absent from both mqtt_event_topic ('{mqtt_event_topic}') and mqtt_payload_template ('{payload_tpl}'). State recovery on reconnect works, but live event automations subscribed to the snapshot topic may double-fire because active deltas and retained snapshots collide. Add {{type}} to either template to let consumers filter."
            );
        }
        let effective_retain = cli.mqtt_retain();

        let cache = effective_retain.then(|| Arc::new(DpsCache::new()));

        // The seed phase parses broker-retained snapshots into the cache.
        // Parsing only works when payload_template is the default `{value}`
        // (so the published payload IS the dps dict, or the per-DP value in
        // single-DP mode). For custom templates we can't generally reverse
        // the wrapper, so seed is disabled and the bridge will overwrite
        // broker retained on first publish per device. The user is warned
        // once at startup.
        let seed_supported = effective_retain
            && matches!(
                cli.mqtt_payload_template.as_deref(),
                None | Some("{value}")
            );
        if effective_retain && !seed_supported {
            warn!(
                "Custom mqtt_payload_template ('{payload_tpl}') is incompatible with the broker seed phase; the bridge will overwrite broker retained on first publish per device"
            );
        }

        let root_topic_str = cli
            .mqtt_root_topic
            .clone()
            .unwrap_or_else(|| crate::config::DEFAULT_MQTT_ROOT_TOPIC.to_string());

        let (seed_state_wildcard, seed_state_regex) = if seed_supported {
            let tpl = mqtt_event_topic.replace("{type}", "passive");
            let wildcard = tpl_to_wildcard(&tpl, &root_topic_str);
            let regex = compile_topic_regex(&tpl);
            (Some(wildcard), regex)
        } else {
            (None, None)
        };

        // If we can't seed (pass-through mode or unsupported template), pre-flip seed_done so
        // publish_device_event doesn't defer snapshots indefinitely.
        let initial_seed_done = !seed_supported;

        let ctx = Arc::new(Self {
            cli: cli.clone(),
            mqtt_tx: cli.mqtt_broker.is_some().then_some(mqtt_tx_sender),
            mqtt_root_topic: root_topic_str,
            mqtt_event_topic,
            mqtt_retain: effective_retain,
            mqtt_message_topic: cli.mqtt_message_topic.clone(),
            mqtt_payload_template: cli.mqtt_payload_template.clone(),
            mqtt_scanner_topic: cli.mqtt_scanner_topic.clone(),
            event_identifiers,
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
            mqtt_drop_count: AtomicU64::new(0),
            cancel,
            cache,
            seed_done: AtomicBool::new(initial_seed_done),
            seed_pending: StdMutex::new(BTreeSet::new()),
            seed_state_wildcard,
            seed_state_regex,
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
                                Ok(Some(Ok(event))) => {
                                    let Some(payload_str) = event.message.payload_as_string() else {
                                        warn!("Non-UTF8 payload from {}", event.device_id);
                                        continue;
                                    };
                                    self.handle_device_event(&event.device_id, payload_str).await;
                                }
                                Ok(Some(Err(_))) => {}
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

    /// Processes one device event: parses payload, branches on error vs DPS,
    /// updates per-device error state, and dispatches to the appropriate
    /// `publish_*` method. Extracted from the listener loop to keep the
    /// `tokio::select!` driver shallow.
    async fn handle_device_event(&self, device_id: &str, payload_str: String) {
        trace!("Raw event from {device_id}: {payload_str}");
        let payload: Value = serde_json::from_str(&payload_str)
            .unwrap_or_else(|_| Value::String(payload_str.clone()));
        let (target_id, name, cid, exists) =
            self.resolve_event_target(device_id, &payload).await;
        let payload_obj = payload.as_object();

        // Error path: log, persist last_error_code, publish as message.
        if payload_obj.is_some_and(|o| o.contains_key("errorCode") || o.contains_key("errorMsg"))
        {
            if let Some(n) = name.as_ref() {
                info!("Device {target_id} ({n}) reported error: {payload}");
            } else {
                info!("Device {target_id} reported error: {payload}");
            }
            let error_code = payload
                .get("errorCode")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok());
            if let Some(code) = error_code {
                let mut state = self.state.write().await;
                if let Some(cfg) = state.configs.get_mut(&target_id) {
                    cfg.last_error_code = Some(code);
                }
            }
            self.publish_device_message(
                &target_id,
                name.as_deref(),
                cid.as_deref(),
                "error",
                payload,
                self.mqtt_retain,
            )
            .await;
            return;
        }

        // Locate DPS either at the root or nested under "data" (sub-device/gateway).
        let root_dps = payload_obj.and_then(|o| o.get("dps"));
        let data_dps = payload_obj
            .and_then(|o| o.get("data"))
            .and_then(|d| d.as_object())
            .and_then(|do_| do_.get("dps"));

        let is_passive = root_dps.is_none() && data_dps.is_none();
        let mut dps = root_dps.or(data_dps).cloned().unwrap_or_else(|| {
            // Passive path: synthesise DPS from "data" (flattening nested "data.data"),
            // falling back to the raw payload.
            if let Some(obj) = payload.as_object()
                && let Some(data) = obj.get("data")
            {
                let mut data_val = data.clone();
                if let Some(data_obj) = data_val.as_object_mut()
                    && let Some(inner) = data_obj.get("data").cloned()
                    && let Some(inner_obj) = inner.as_object()
                {
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

        // Gateway with sub-device CID: surface the CID in DPS for downstream consumers.
        if target_id == device_id
            && let Some(c) = &cid
            && let Some(obj) = dps.as_object_mut()
        {
            obj.insert("cid".to_string(), Value::String(c.clone()));
        }

        self.publish_device_event(target_id, name, cid, dps, is_passive, exists)
            .await;
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

        // ── Seed phase (cache mode only) ──────────────────────────────────────────
        // After the first ConnAck the bridge subscribes to its own retained
        // state topic to recover prior-session snapshots, then unsubscribes
        // and calls `on_seed_complete()` once either the broker quiets (200ms
        // since last retained) or the 5s hard cap fires.
        const SEED_HARD_CAP: Duration = Duration::from_secs(5);
        const SEED_QUIET: Duration = Duration::from_millis(200);
        let seed_wildcard = self.seed_state_wildcard.clone();

        let handle = tokio::spawn(async move {
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            let mut next_retry: Option<Instant> = None;
            let mut seed_active = false;
            let mut seed_deadline: Option<Instant> = None;
            let mut seed_last_msg: Option<Instant> = None;
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
                                    if let Some(dps) =
                                        parse_seed_payload(&payload, dp_opt.as_deref())
                                        && let Some(cache) = &self.cache
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
                                let req_val = Self::parse_mqtt_payload(&payload, &vars);

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
                                    let ctx_h = self.clone();
                                    tokio::spawn(async move {
                                        let res = crate::handlers::handle_request(ctx_h.clone(), req).await;
                                        // Suppress response for successful set/get actions to avoid
                                        // redundancy — unless the response carries fan-out info
                                        // (`matched` extra), which the caller needs to see.
                                        let suppress = res.status == crate::types::Status::Ok
                                            && matches!(res.action.as_deref(), Some("set" | "get"))
                                            && !res.extra.contains_key("matched");
                                        if !suppress {
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
            let n = self.mqtt_drop_count.fetch_add(1, Ordering::Relaxed) + 1;
            error!("MQTT queue full, dropping message (cumulative drops: {n})");
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
    pub fn parse_mqtt_payload(payload: &str, vars: &HashMap<String, String>) -> Value {
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

    /// Helper to apply 'set' command heuristic to an object.
    ///
    /// Treats every top-level field that is *not* a known `BridgeRequest`
    /// field as a DP id/value pair. The deny-list must cover every field used
    /// by any `BridgeRequest` variant (plus topic-merged keys like `dp` and the
    /// scalar-wrap key `payload`) — adding a new field to `BridgeRequest`
    /// without updating this list will silently turn it into a DP write.
    fn apply_set_heuristic(obj: &mut serde_json::Map<String, Value>) {
        const RESERVED_FIELDS: &[&str] = &[
            // BridgeRequest variants' fields:
            "action", "id", "name", "key", "ip", "version", "cid", "parent_id",
            "cmd", "data", "dps",
            // Topic-merged / scalar-wrap synthetic keys:
            "dp", "payload",
        ];
        if !obj.contains_key("dps") && !obj.contains_key("data") {
            let mut dps = obj.clone();
            for f in RESERVED_FIELDS {
                dps.remove(*f);
            }
            obj.insert("dps".to_string(), Value::Object(dps));
        }
    }

    /// Atomically saves device configuration to state file
    ///
    /// # Errors
    /// Returns an error if the state file cannot be serialized, written, or atomically renamed.
    pub async fn save_state(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;

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

        // Atomic write: write + fsync temp file, rename, then fsync parent
        // dir so the rename itself survives an unclean shutdown.
        let tmp_path = path.with_extension("tmp");
        {
            let mut f = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)
                .await?;
            f.write_all(json.as_bytes()).await?;
            f.sync_all().await?;
        }

        if let Err(e) = tokio::fs::rename(&tmp_path, path).await {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            anyhow::bail!("Failed to commit state file {}: {}", self.state_file, e);
        }

        // Fsync the directory entry (Unix). On Windows this is a no-op and
        // `File::open` on a directory fails — silently ignore.
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
            && let Ok(dir) = tokio::fs::File::open(parent).await
        {
            let _ = dir.sync_all().await;
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
        // Both selectors provided: id wins, name is silently ignored. Log so
        // operators tracking down "why didn't my name lookup fire" can spot it.
        if id.is_some() && name.is_some() {
            debug!(
                "find_device_ids: both `id` and `name` provided; `id` takes priority and `name` is ignored"
            );
        }

        let state = self.state.read().await;

        // 1. Match by ID
        if let Some(ids) = id {
            // Empty strings in the id list are a common symptom of "I wanted
            // name-based lookup but the topic merged an empty {id} segment in".
            // `find_device_ids` short-circuits on `id` regardless, so an empty
            // string can't fall through to the name path — warn loudly.
            if ids.iter().any(String::is_empty) {
                warn!(
                    "find_device_ids: `id` selector contains an empty string; the lookup will short-circuit to id-only and fail. If you want name-based lookup with an {{id}} placeholder in the topic, send `\"id\": null` (not `\"\"`) in the payload — see docs/internals.md §10.1"
                );
            }
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

    /// Renders the configured event topic / payload templates for a device
    /// update and pushes each rendered (topic, payload) onto the MQTT outbound
    /// channel. Single low-level publish helper used by both pass-through
    /// and cache-mode active/snapshot publishes; retain is decided by the caller.
    async fn publish_event_templates(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: &Value,
        is_passive: bool,
        retain: bool,
    ) {
        let templates = self.generate_device_templates(id, name, cid, dps, is_passive);
        if templates.is_empty() {
            return;
        }
        for (topic, payload) in templates {
            self.try_send_mqtt(Some(MqttMessage {
                topic,
                payload,
                retain,
            }))
            .await;
        }
    }

    /// Publishes a device event.
    ///
    /// pass-through mode (`mqtt_retain=false`): pass-through — render templates with the
    /// incoming `is_passive` and publish a single MQTT message without retain.
    /// This preserves the historical bridge behavior for retain-off users.
    ///
    /// cache mode (`mqtt_retain=true`): two parallel routes.
    /// 1. *Direct route* (active only): publish the incoming delta on
    ///    `{type}=active`, no retain — for HA event automations etc.
    /// 2. *Cache route*: merge the incoming DPs into the in-memory cache; if
    ///    any value actually changed AND the seed phase has finished, publish
    ///    the full merged snapshot on `{type}=passive` retained. During the
    ///    seed window the device id is queued in `seed_pending` for a single
    ///    deferred flush — this is what avoids overwriting the broker's
    ///    full-state snapshot with a partial passive (e.g. a battery-only
    ///    report).
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

        if self.mqtt_tx.is_none() || !exists {
            return;
        }

        let name_opt = name.as_deref();
        let cid_opt = cid.as_deref();

        // pass-through. mqtt_retain=false here either by user choice or by
        // the {type}-missing downgrade in `BridgeContext::new`.
        if !self.mqtt_retain {
            self.publish_event_templates(&id, name_opt, cid_opt, &dps, is_passive, false)
                .await;
            return;
        }

        // cache-mode direct route: active delta to {type}=active, no retain.
        if !is_passive {
            self.publish_event_templates(&id, name_opt, cid_opt, &dps, false, false)
                .await;
        }

        // cache-mode cache route: merge then publish snapshot (gated by seed_done).
        let Some(cache) = &self.cache else { return };
        let Some(dps_obj) = dps.as_object() else { return };
        let changed = cache.merge(&id, dps_obj);
        if changed.is_empty() {
            return;
        }

        if !self.seed_done.load(Ordering::Acquire) {
            // Defer to seed-end flush; dedup by device id.
            self.seed_pending.lock().unwrap().insert(id.clone());
            return;
        }

        // Pass `changed` so single-DP mode only republishes the DPs that
        // actually changed — otherwise a one-DP update would also resend
        // the retained snapshots for every other cached DP on this device.
        self.publish_snapshot(&id, name_opt, cid_opt, Some(&changed))
            .await;
    }

    /// Match an incoming topic against the seed wildcard regex and return
    /// `(id, optional_dp)`. Returns `None` if not in cache-mode seed-enabled mode or
    /// the topic doesn't match.
    fn match_seed_topic(&self, topic: &str) -> Option<(String, Option<String>)> {
        let regex = self.seed_state_regex.as_ref()?;
        let captures = regex.captures(topic)?;
        let id = captures.name("id")?.as_str().to_string();
        let dp = captures.name("dp").map(|m| m.as_str().to_string());
        Some((id, dp))
    }

    /// Drives the seed-phase transition: flip `seed_done`, drain `seed_pending`,
    /// and publish one snapshot per device whose cache changed during the seed
    /// window. No-op if seed_done was already true (e.g. pass-through bypass).
    pub async fn on_seed_complete(self: &Arc<Self>) {
        if self.seed_done.swap(true, Ordering::AcqRel) {
            return;
        }
        let pending: Vec<String> = {
            let mut guard = self.seed_pending.lock().unwrap();
            std::mem::take(&mut *guard).into_iter().collect()
        };
        info!(
            "Seed phase complete; flushing {} pending device snapshot(s)",
            pending.len()
        );
        for id in pending {
            let (name, cid) = {
                let state = self.state.read().await;
                state
                    .configs
                    .get(&id)
                    .map(|c| (c.name.clone(), c.cid.clone()))
                    .unwrap_or((None, None))
            };
            // Seed flush: pass None so the full cache is re-asserted as
            // initial sync (we don't track per-DP dirty during seed).
            self.publish_snapshot(&id, name.as_deref(), cid.as_deref(), None)
                .await;
        }
    }

    /// Publishes the cached snapshot for one device on the `{type}=passive`
    /// topic with retain. `only_keys` narrows what gets published in
    /// single-DP mode (one topic per DP) so an event that changed DP 1
    /// doesn't republish a still-current DP 2's retained — pass
    /// `Some(&changed)` from the per-event path. Seed-end flush passes
    /// `None` to (re)assert the full cache as an initial sync.
    /// In multi-DP mode (one topic per device, full DPS dict in payload)
    /// the filter is meaningless and ignored: a single message always
    /// carries the full snapshot.
    ///
    /// Applies the same identifier-set retain gate as historical events —
    /// if the template references `{name}`/`{cid}` and this device lacks
    /// them, retain is stripped so the scavenger can still reach the
    /// message later.
    pub(crate) async fn publish_snapshot(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        only_keys: Option<&[String]>,
    ) {
        let Some(cache) = &self.cache else { return };
        let Some(full_snap) = cache.snapshot(id) else { return };

        let event_tpl = &self.mqtt_event_topic;
        let is_single_dp = event_tpl.contains("{dp}") || event_tpl.contains("{value}");

        let snap = match (is_single_dp, only_keys) {
            (true, Some(keys)) => {
                let mut filtered = std::collections::BTreeMap::new();
                for k in keys {
                    if let Some(v) = full_snap.get(k) {
                        filtered.insert(k.clone(), v.clone());
                    }
                }
                if filtered.is_empty() {
                    return;
                }
                filtered
            }
            _ => full_snap,
        };

        let snap_value =
            Value::Object(snap.into_iter().collect::<serde_json::Map<String, Value>>());

        let retain = self.event_identifiers.satisfied_by(name, cid);
        if !retain {
            debug!(
                "Stripping retain for snapshot from '{id}': no template identifier resolvable (name={name:?}, cid={cid:?})"
            );
        }
        self.publish_event_templates(id, name, cid, &snap_value, true, retain)
            .await;
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

            // Force the payload into an object so we can always inject `id` —
            // the scavenger's payload-fallback match (`"<id>"`) relies on this
            // and is the reason `message_topic` doesn't need its own
            // `IdentifierSet` retain gate (unlike `event_topic`, which is
            // gated in `publish_device_event`).
            if !payload.is_object() {
                let inner = std::mem::replace(
                    &mut payload,
                    Value::Object(serde_json::Map::new()),
                );
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert("payload".to_string(), inner);
                }
            }
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("id".to_string(), id.into());
                if let Some(n) = name {
                    obj.insert("name".to_string(), n.into());
                }
                if let Some(c) = cid {
                    obj.insert("cid".to_string(), c.into());
                }
            }
            self.try_send_mqtt(Some(MqttMessage {
                topic,
                payload: payload.to_string(),
                retain,
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
            "level" => {
                out.push_str(vars.level.unwrap_or(""));
                true
            }
            "type" => {
                out.push_str(vars.event_type.unwrap_or(""));
                true
            }
            "dp" => {
                out.push_str(vars.dp.unwrap_or(""));
                true
            }
            "value" => {
                if let Some(val) = vars.val {
                    let _ = write!(out, "{val}");
                } else if let Some(dps_str) = vars.dps_str {
                    out.push_str(dps_str);
                }
                true
            }
            "dps" => {
                out.push_str(vars.dps_str.unwrap_or(""));
                true
            }
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
            let topic = self.mqtt_scanner_topic.as_deref().map_or_else(
                || {
                    self.replace_vars(
                        self.mqtt_message_topic.as_deref().unwrap_or_default(),
                        &TopicVars {
                            id: "bridge",
                            name: Some("bridge"),
                            level: Some("scanner"),
                            ..Default::default()
                        },
                    )
                },
                |topic| {
                    self.replace_vars(
                        topic,
                        &TopicVars {
                            id: "bridge",
                            name: Some("bridge"),
                            ..Default::default()
                        },
                    )
                },
            );
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
        let scavenger_timeout_secs = self.cli.scavenger_timeout_secs();

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

        // Tracks whether the listener needs to be rebuilt. Direct-device
        // additions/removals/key changes flip it; pure metadata changes
        // (name, sub-device cid_map) do not.
        let mut listener_changed = false;

        {
            let mut state = self.state.write().await;

            // Snapshot old config (key/ip/version) to decide whether the
            // direct-device branch needs to rebuild its `Device` and refresh
            // the listener.
            let old_direct = state.configs.get(&id).map(|c| {
                (
                    c.cid.is_some() && c.parent_id.is_some(),
                    c.key.clone(),
                    c.ip.clone(),
                    c.version.clone(),
                )
            });

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
                // Register as sub-device. If the old shape was a direct device,
                // the listener loses an instance and must refresh.
                if old_direct.as_ref().is_some_and(|(was_sub, ..)| !was_sub) {
                    listener_changed = true;
                }
                state
                    .cid_map
                    .insert((parent_id.clone(), cid.clone()), id.clone());
                state.instances.remove(&id);
            } else if let Some(key) = &cfg.key {
                // Register as direct device. Rebuild the Device only when key,
                // ip, version, or shape changed; otherwise keep the existing
                // connection and avoid the listener refresh.
                let unchanged = matches!(
                    &old_direct,
                    Some((false, Some(old_key), old_ip, old_ver))
                        if old_key == key
                            && old_ip == &cfg.ip
                            && old_ver == &cfg.version
                            && state.instances.contains_key(&id)
                );
                if !unchanged {
                    let dev = DeviceBuilder::new(&id, key.as_bytes().to_vec())
                        .address(cfg.ip.as_deref().unwrap_or("Auto"))
                        .version(
                            cfg.version
                                .as_deref()
                                .and_then(|s| s.parse::<rustuya::Version>().ok())
                                .unwrap_or_default(),
                        )
                        .nowait(true)
                        .build();
                    state.instances.insert(id.clone(), dev);
                    listener_changed = true;
                }
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
        if listener_changed {
            self.request_refresh();
        }
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
        // Capture caller intent before consuming the selectors so we can decide
        // whether to annotate the response with `matched` fan-out info.
        let by_name = id.is_none() && name.is_some();
        let direct_matches = self.get_targets(id, name).await?;
        let name_matched = direct_matches.len();
        let mut targets = direct_matches.clone();

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

        // Track whether the unified listener needs to rebuild. Removing a
        // sub-device doesn't affect the listener (subs aren't in `instances`),
        // so only direct-device removals flip this flag.
        let mut listener_changed = false;
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
                if state.instances.remove(target_id).is_some() {
                    listener_changed = true;
                }
            }
        }

        // Only refresh when a direct device was actually evicted — sub-device
        // removals don't appear in the listener stream.
        if listener_changed {
            self.request_refresh();
        }

        // Drop any cached DPS snapshot for removed devices (cache mode only —
        // pass-through mode has no cache). Done before scavenger so we don't republish a
        // snapshot in a race with the cleanup.
        if let Some(cache) = &self.cache {
            for target_id in &targets {
                cache.remove(target_id);
            }
        }

        // Scavenge retained messages for removed devices
        self.spawn_retain_scavenger(scavenger_targets).await;

        info!("Devices removed: {}", targets.join(", "));
        self.request_save();

        let mut res = ApiResponse::ok("remove", targets.join(","));
        // Annotate fan-out only for name-based lookups that resolved to
        // multiple *directly-matched* devices. Cascaded sub-devices are not
        // counted — the caller's name didn't match those.
        if by_name && name_matched > 1 {
            res = res
                .with_extra("matched", Value::from(name_matched as u64))
                .with_extra("targets", Value::from(direct_matches));
        }
        Ok(res)
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

        // Drop the full cache-mode cache alongside config wipe.
        if let Some(cache) = &self.cache {
            for t in &scavenger_targets {
                cache.remove(&t.id);
            }
        }

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

        let mqtt_drop_count = self.mqtt_drop_count.load(Ordering::Relaxed);
        ApiResponse::ok_action("status")
            .with_extra("devices", Value::Object(devices))
            .with_extra("mqtt_drop_count", Value::from(mqtt_drop_count))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── IdentifierSet ──────────────────────────────────────────────────────────

    #[test]
    fn identifier_set_detects_each_placeholder() {
        let s = IdentifierSet::from_templates(["{root}/event/{id}"]);
        assert!(s.id && !s.name && !s.cid);

        let s = IdentifierSet::from_templates(["{root}/event/{name}"]);
        assert!(!s.id && s.name && !s.cid);

        let s = IdentifierSet::from_templates(["{root}/event/{cid}"]);
        assert!(!s.id && !s.name && s.cid);
    }

    #[test]
    fn identifier_set_unions_across_multiple_templates() {
        let s = IdentifierSet::from_templates(["{root}/event/{name}", r#"{"id":"{id}"}"#]);
        assert!(s.id && s.name && !s.cid);
    }

    #[test]
    fn identifier_set_is_empty_when_no_identifier_referenced() {
        let s = IdentifierSet::from_templates(["{root}/event", "{value}"]);
        assert!(s.is_empty());
    }

    #[test]
    fn identifier_set_satisfied_by_id_always() {
        let s = IdentifierSet { id: true, ..Default::default() };
        assert!(s.satisfied_by(None, None));
        assert!(s.satisfied_by(Some(""), None));
    }

    #[test]
    fn identifier_set_satisfied_by_name_requires_nonempty() {
        let s = IdentifierSet { name: true, ..Default::default() };
        assert!(!s.satisfied_by(None, None));
        assert!(!s.satisfied_by(Some(""), None));
        assert!(s.satisfied_by(Some("kitchen"), None));
    }

    #[test]
    fn identifier_set_satisfied_by_cid_requires_nonempty() {
        let s = IdentifierSet { cid: true, ..Default::default() };
        assert!(!s.satisfied_by(None, None));
        assert!(s.satisfied_by(None, Some("sub-1")));
    }

    // ── tpl_to_wildcard ────────────────────────────────────────────────────────

    #[test]
    fn tpl_to_wildcard_substitutes_root_and_wildcards_variables() {
        assert_eq!(
            tpl_to_wildcard("{root}/event/{type}/{id}", "rustuya"),
            "rustuya/event/+/+"
        );
    }

    #[test]
    fn tpl_to_wildcard_preserves_unknown_keys_literally() {
        assert_eq!(
            tpl_to_wildcard("{root}/{value}/x", "rustuya"),
            "rustuya/{value}/x"
        );
    }

    // ── compile_topic_regex / match_topic ─────────────────────────────────────

    #[test]
    fn compile_topic_regex_returns_none_for_literal_template() {
        assert!(compile_topic_regex("rustuya/command").is_none());
    }

    #[test]
    fn match_topic_extracts_variables() {
        let tpl = "rustuya/event/{type}/{id}";
        let re = compile_topic_regex(tpl).unwrap();
        let vars = match_topic("rustuya/event/active/dev-1", tpl, Some(&re)).unwrap();
        assert_eq!(vars.get("type").map(String::as_str), Some("active"));
        assert_eq!(vars.get("id").map(String::as_str), Some("dev-1"));
    }

    #[test]
    fn match_topic_rejects_mismatch() {
        let tpl = "rustuya/event/{id}";
        let re = compile_topic_regex(tpl).unwrap();
        assert!(match_topic("rustuya/other/dev-1", tpl, Some(&re)).is_none());
    }

    #[test]
    fn match_topic_literal_template_exact_match() {
        assert!(match_topic("rustuya/command", "rustuya/command", None).is_some());
        assert!(match_topic("rustuya/command/x", "rustuya/command", None).is_none());
    }

    // ── render_template ────────────────────────────────────────────────────────

    #[test]
    fn render_template_substitutes_known_keys_and_keeps_unknown() {
        let mut vars = HashMap::new();
        vars.insert("a".to_string(), "1".to_string());
        let out = render_template("x={a},y={b}", |key, out| {
            vars.get(key).is_some_and(|v| {
                out.push_str(v);
                true
            })
        });
        assert_eq!(out, "x=1,y={b}");
    }

    // ── parse_mqtt_payload ────────────────────────────────────────────────────

    #[test]
    fn parse_payload_merges_topic_vars_into_object() {
        let vars: HashMap<String, String> =
            [("id".into(), "dev-1".into())].into_iter().collect();
        let val = BridgeContext::parse_mqtt_payload(r#"{"action":"get"}"#, &vars);
        assert_eq!(val.get("id").and_then(|v| v.as_str()), Some("dev-1"));
        assert_eq!(val.get("action").and_then(|v| v.as_str()), Some("get"));
    }

    #[test]
    fn parse_payload_topic_vars_do_not_overwrite_payload_keys() {
        let vars: HashMap<String, String> =
            [("id".into(), "from-topic".into())].into_iter().collect();
        let val = BridgeContext::parse_mqtt_payload(r#"{"id":"from-payload"}"#, &vars);
        assert_eq!(
            val.get("id").and_then(|v| v.as_str()),
            Some("from-payload")
        );
    }

    #[test]
    fn parse_payload_wraps_scalar_with_dp_var_into_dps() {
        let vars: HashMap<String, String> = [("dp".into(), "1".into())].into_iter().collect();
        let val = BridgeContext::parse_mqtt_payload("true", &vars);
        assert_eq!(val.get("dps").and_then(|v| v.get("1")), Some(&json!(true)));
    }

    #[test]
    fn parse_payload_set_heuristic_promotes_loose_fields_into_dps() {
        let vars: HashMap<String, String> = HashMap::new();
        let val = BridgeContext::parse_mqtt_payload(
            r#"{"action":"set","id":"dev-1","1":true,"2":50}"#,
            &vars,
        );
        let dps = val.get("dps").and_then(|v| v.as_object()).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        assert_eq!(dps.get("2"), Some(&json!(50)));
        // Selector/action fields must not be smuggled into dps.
        assert!(!dps.contains_key("id"));
        assert!(!dps.contains_key("action"));
    }

    #[test]
    fn parse_payload_set_heuristic_excludes_all_bridge_request_fields() {
        // Regression guard for the extended deny-list: when the heuristic runs
        // (no explicit `dps` / `data`), every BridgeRequest top-level field
        // (key/ip/version/cid/parent_id/cmd + the existing core ones) must be
        // stripped from auto-built dps, so a typo'd/misplaced reserved field
        // can't silently become a DP write.
        let vars: HashMap<String, String> = HashMap::new();
        let val = BridgeContext::parse_mqtt_payload(
            r#"{"action":"set","id":"dev","name":"n","key":"k","ip":"1.2.3.4",
                "version":"3.3","cid":"c","parent_id":"p","cmd":7,
                "dp":"99","payload":"x","1":true}"#,
            &vars,
        );
        let dps = val.get("dps").and_then(|v| v.as_object()).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        for reserved in [
            "action", "id", "name", "key", "ip", "version", "cid", "parent_id",
            "cmd", "dp", "payload", "dps",
        ] {
            assert!(
                !dps.contains_key(reserved),
                "reserved field `{reserved}` leaked into auto-built dps"
            );
        }
    }

    #[test]
    fn parse_payload_set_heuristic_skips_when_data_present() {
        // Documented behavior: `data` opts out of the auto-wrap, so the
        // payload is passed through unchanged (no `dps` synthesized).
        let vars: HashMap<String, String> = HashMap::new();
        let val = BridgeContext::parse_mqtt_payload(
            r#"{"action":"set","id":"dev","data":{"x":1}}"#,
            &vars,
        );
        assert!(val.get("dps").is_none());
        assert_eq!(val.get("data"), Some(&json!({"x": 1})));
    }

// ── Async / context-bound behavior tests ─────────────────────────────────
// Tests below construct a full BridgeContext over a tempdir so they exercise
// add_device, publish_device_event, etc. end-to-end with a captured MQTT
// receiver. Kept in a separate module so the heavy setup helper doesn't
// leak into the pure-function test module above.
#[cfg(test)]
mod context_tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    /// Constructs a BridgeContext writing state to a fresh tempdir, with the
    /// MQTT channel wired (so `publish_*` actually sends) but no real broker.
    /// `customize` can override Cli fields like event_topic / payload_template.
    async fn make_ctx(
        customize: impl FnOnce(&mut Cli),
    ) -> (
        Arc<BridgeContext>,
        TempDir,
        mpsc::Receiver<Option<MqttMessage>>,
        mpsc::Receiver<()>,
        mpsc::Receiver<()>,
    ) {
        let tmp = TempDir::new().expect("create tempdir");
        let state_path = tmp.path().join("state.json");
        // `mqtt_tx` is wired iff mqtt_broker is Some — value never dialed.
        let mut cli = Cli {
            mqtt_broker: Some("mqtt://noop.invalid:1883".to_string()),
            state_file: Some(state_path.to_string_lossy().into_owned()),
            ..Cli::default()
        };
        customize(&mut cli);
        let (ctx, mqtt_rx, save_rx, refresh_rx) =
            BridgeContext::new(&cli, tokio_util::sync::CancellationToken::new())
                .await
                .expect("new context");
        (ctx, tmp, mqtt_rx, save_rx, refresh_rx)
    }

    fn direct_device(id: &str, key: &str, ip: Option<&str>) -> DeviceConfig {
        DeviceConfig {
            id: id.into(),
            name: None,
            ip: ip.map(str::to_string),
            key: Some(key.into()),
            version: None,
            cid: None,
            parent_id: None,
            last_error_code: None,
        }
    }

    // ── #10: add_device idempotency ───────────────────────────────────────

    #[tokio::test]
    async fn add_device_skips_refresh_when_re_added_identically() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(direct_device("dev-1", "0123456789abcdef", Some("10.0.0.1")))
            .await
            .expect("first add");
        assert_eq!(
            refresh_rx.try_recv(),
            Ok(()),
            "first add must trigger refresh"
        );

        ctx.add_device(direct_device("dev-1", "0123456789abcdef", Some("10.0.0.1")))
            .await
            .expect("idempotent re-add");
        assert!(
            refresh_rx.try_recv().is_err(),
            "identical re-add must not trigger refresh"
        );
    }

    #[tokio::test]
    async fn add_device_refreshes_when_key_changes() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(direct_device("dev-1", "aaaaaaaaaaaaaaaa", None))
            .await
            .unwrap();
        let _ = refresh_rx.try_recv();

        ctx.add_device(direct_device("dev-1", "bbbbbbbbbbbbbbbb", None))
            .await
            .unwrap();
        assert_eq!(
            refresh_rx.try_recv(),
            Ok(()),
            "key change must trigger refresh"
        );
    }

    #[tokio::test]
    async fn add_device_refreshes_when_ip_changes() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(direct_device("dev-1", "0123456789abcdef", Some("10.0.0.1")))
            .await
            .unwrap();
        let _ = refresh_rx.try_recv();

        ctx.add_device(direct_device("dev-1", "0123456789abcdef", Some("10.0.0.2")))
            .await
            .unwrap();
        assert_eq!(
            refresh_rx.try_recv(),
            Ok(()),
            "ip change must trigger refresh"
        );
    }

    #[tokio::test]
    async fn add_device_refreshes_when_shape_changes_direct_to_sub() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(direct_device("dev-1", "0123456789abcdef", None))
            .await
            .unwrap();
        let _ = refresh_rx.try_recv();

        // Re-add as sub-device — direct instance must drop, listener must refresh.
        ctx.add_device(DeviceConfig {
            id: "dev-1".into(),
            name: None,
            ip: None,
            key: None,
            version: None,
            cid: Some("sub-1".into()),
            parent_id: Some("gateway-A".into()),
            last_error_code: None,
        })
        .await
        .unwrap();
        assert_eq!(
            refresh_rx.try_recv(),
            Ok(()),
            "direct→sub conversion must trigger refresh"
        );
    }

    // ── #2: optional template vars fall back to empty string ──────────────

    /// Collects all `MqttMessage`s sent within a short window from the captured
    /// receiver — `publish_device_event` returns immediately after queuing, so
    /// a tiny yield is enough.
    async fn drain_mqtt(rx: &mut mpsc::Receiver<Option<MqttMessage>>) -> Vec<MqttMessage> {
        // Yield once so any pending sends land in the channel.
        tokio::task::yield_now().await;
        let mut out = Vec::new();
        while let Ok(Some(msg)) = rx.try_recv() {
            out.push(msg);
        }
        out
    }

    #[tokio::test]
    async fn publish_event_multi_dp_renders_missing_dp_as_empty() {
        // Multi-DP mode (event_topic has no {dp}/{value}) + a payload template
        // that references {dp} → {dp} must render as empty string, not the
        // literal `{dp}` placeholder.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_event_topic = Some("{root}/event/{id}".to_string());
            cli.mqtt_payload_template = Some(r#"{"dp":"{dp}","val":{value}}"#.to_string());
        })
        .await;

        ctx.publish_device_event(
            "dev-1".to_string(),
            None,
            None,
            json!({"1": true, "2": 50}),
            false,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1, "multi-DP mode emits one message");
        assert_eq!(msgs[0].topic, "rustuya/event/dev-1");
        assert!(
            !msgs[0].payload.contains("{dp}"),
            "literal {{dp}} must not leak into payload, got: {}",
            msgs[0].payload
        );
        // Concrete shape: `{"dp":"","val":{"1":true,"2":50}}` (key order
        // depends on serde_json::Map's preservation, so check fragments).
        assert!(msgs[0].payload.contains(r#""dp":"""#));
    }

    #[tokio::test]
    async fn publish_event_single_dp_renders_dp_and_value_per_dp() {
        // Sanity: with {dp} in event_topic, single-DP mode fires and emits one
        // message per dp with both {dp} and {value} resolved.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_event_topic = Some("{root}/event/{id}/{dp}".to_string());
            cli.mqtt_payload_template = Some(r#"{"v":{value}}"#.to_string());
        })
        .await;

        ctx.publish_device_event(
            "dev-1".to_string(),
            None,
            None,
            json!({"1": true, "2": 50}),
            false,
            true,
        )
        .await;

        let mut msgs = drain_mqtt(&mut mqtt_rx).await;
        msgs.sort_by(|a, b| a.topic.cmp(&b.topic));
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].topic, "rustuya/event/dev-1/1");
        assert_eq!(msgs[0].payload, r#"{"v":true}"#);
        assert_eq!(msgs[1].topic, "rustuya/event/dev-1/2");
        assert_eq!(msgs[1].payload, r#"{"v":50}"#);
    }

    #[tokio::test]
    async fn publish_event_empty_string_for_missing_name_in_topic() {
        // {name} in event_topic with a device that has no name → must render
        // as empty (not the literal `{name}`), producing `rustuya/event//1`.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_event_topic = Some("{root}/event/{name}/{dp}".to_string());
            cli.mqtt_payload_template = Some("{value}".to_string());
        })
        .await;

        ctx.publish_device_event(
            "dev-1".to_string(),
            None,
            None,
            json!({"1": true}),
            false,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].topic, "rustuya/event//1");
    }

    // ── #6: publish_device_message wraps non-object payloads ──────────────

    #[tokio::test]
    async fn publish_message_wraps_non_object_payload_and_injects_id() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;

        // Pass a plain JSON string — exercises the wrap+inject path.
        ctx.publish_device_message(
            "dev-1",
            None,
            None,
            "error",
            Value::String("kaboom".to_string()),
            false,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&msgs[0].payload).unwrap();
        let obj = parsed.as_object().unwrap();
        assert_eq!(obj.get("id").and_then(|v| v.as_str()), Some("dev-1"));
        // Original payload is preserved under the `payload` field.
        assert_eq!(
            obj.get("payload").and_then(|v| v.as_str()),
            Some("kaboom")
        );
    }

    // ── #7: mqtt_drop_count is exposed in status response ─────────────────

    // ── B1/B2: remove_device matched annotation + selective refresh ──────

    fn sub_device(id: &str, parent_id: &str, cid: &str) -> DeviceConfig {
        DeviceConfig {
            id: id.into(),
            name: None,
            ip: None,
            key: None,
            version: None,
            cid: Some(cid.into()),
            parent_id: Some(parent_id.into()),
            last_error_code: None,
        }
    }

    fn named_direct(id: &str, name: &str) -> DeviceConfig {
        DeviceConfig {
            id: id.into(),
            name: Some(name.into()),
            ip: None,
            key: Some("0123456789abcdef".into()),
            version: None,
            cid: None,
            parent_id: None,
            last_error_code: None,
        }
    }

    #[tokio::test]
    async fn remove_by_name_with_cascade_reports_name_match_count_not_total() {
        // Regression for the matched-count semantic: removing a single
        // gateway by name that cascades to sub-devices must report
        // `matched: 1` (or omit it), NOT the total removed including subs.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("gw-1", "kitchen-gateway"))
            .await
            .unwrap();
        for cid in ["s1", "s2", "s3"] {
            ctx.add_device(sub_device(&format!("sub-{cid}"), "gw-1", cid))
                .await
                .unwrap();
        }

        let res = ctx
            .remove_device(None, Some(vec!["kitchen-gateway".into()]))
            .await
            .expect("remove by name");

        // Single name match → no `matched` annotation should be added.
        assert!(
            !res.extra.contains_key("matched"),
            "single name match must not be annotated as fan-out, got: {:?}",
            res.extra
        );
        // But all 4 devices (gateway + 3 subs) should appear in the comma-joined id.
        let removed: Vec<&str> = res.id.as_deref().unwrap_or("").split(',').collect();
        assert_eq!(removed.len(), 4, "cascade should remove gateway + 3 subs");
    }

    #[tokio::test]
    async fn remove_by_name_with_real_fanout_reports_only_name_matched_count() {
        // Two devices share a name → name lookup fans out to both. Each is a
        // direct device (no subs). matched should be exactly 2.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("dev-a", "kitchen")).await.unwrap();
        ctx.add_device(named_direct("dev-b", "kitchen")).await.unwrap();

        let res = ctx
            .remove_device(None, Some(vec!["kitchen".into()]))
            .await
            .unwrap();

        assert_eq!(
            res.extra.get("matched").and_then(|v| v.as_u64()),
            Some(2),
            "two name matches must report matched: 2"
        );
        let targets = res
            .extra
            .get("targets")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str()).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(targets.len(), 2);
    }

    #[tokio::test]
    async fn remove_sub_device_only_skips_listener_refresh() {
        // Removing a sub-device alone must NOT trigger refresh — subs aren't
        // in the listener stream, so killing the parent's TCP connection is
        // gratuitous churn.
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("gw-1", "gw")).await.unwrap();
        ctx.add_device(sub_device("sub-A", "gw-1", "cid-A"))
            .await
            .unwrap();
        // Drain refresh signals from the adds.
        while refresh_rx.try_recv().is_ok() {}

        ctx.remove_device(Some(vec!["sub-A".into()]), None)
            .await
            .unwrap();

        assert!(
            refresh_rx.try_recv().is_err(),
            "removing a sub-device only must not refresh the listener"
        );
    }

    #[tokio::test]
    async fn remove_direct_device_triggers_listener_refresh() {
        // Sanity-check the inverse — direct-device removal MUST refresh so
        // the unified listener stops trying to read from a dropped Device.
        let (ctx, _tmp, _mqtt_rx, _save_rx, mut refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("gw-1", "gw")).await.unwrap();
        while refresh_rx.try_recv().is_ok() {}

        ctx.remove_device(Some(vec!["gw-1".into()]), None)
            .await
            .unwrap();

        assert_eq!(
            refresh_rx.try_recv(),
            Ok(()),
            "direct-device removal must refresh"
        );
    }

    #[tokio::test]
    async fn status_response_includes_mqtt_drop_count() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;
        // Simulate a drop having happened.
        ctx.mqtt_drop_count.store(3, Ordering::Relaxed);

        let res = ctx.get_bridge_status().await;
        let v = serde_json::to_value(&res).unwrap();
        assert_eq!(
            v.get("mqtt_drop_count").and_then(|n| n.as_u64()),
            Some(3),
            "status must surface the cumulative MQTT drop count"
        );
    }

    // ── cache mode bring-up ────────────────────────────────────────────────────

    #[tokio::test]
    async fn passthrough_mode_skips_cache_when_retain_off() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(false);
        })
        .await;
        assert!(!ctx.mqtt_retain);
        assert!(ctx.cache.is_none(), "pass-through mode must not allocate a cache");
    }

    #[tokio::test]
    async fn cache_mode_allocates_cache_with_type_in_topic() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            // Default event topic contains {type}, so cache mode stays enabled.
        })
        .await;
        assert!(ctx.mqtt_retain);
        assert!(ctx.cache.is_some(), "cache mode must allocate a cache");
    }

    #[tokio::test]
    async fn cache_mode_stays_active_when_type_missing_from_both() {
        // No-{type} setup is opt-in for users whose fleet has no event
        // automations — state recovery via retained snapshot still works
        // (no-retain active never overwrites the retained passive). Bridge
        // logs a WARN about potential live double-fire but honors the
        // explicit mqtt_retain=true.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{id}".into());
            cli.mqtt_payload_template = Some("{value}".into());
        })
        .await;
        assert!(ctx.mqtt_retain, "mqtt_retain=true must stay honored");
        assert!(ctx.cache.is_some());
    }

    #[tokio::test]
    async fn cache_mode_stays_active_when_type_in_payload_only() {
        // {type} in payload template suffices — subscribers can filter on
        // `value_json.type` even if topic doesn't separate active/passive.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{id}/{dp}".into());
            cli.mqtt_payload_template =
                Some("{\"type\":\"{type}\",\"value\":{value}}".into());
        })
        .await;
        assert!(
            ctx.mqtt_retain,
            "{{type}} in payload template alone must keep cache mode active"
        );
        assert!(ctx.cache.is_some());
    }

    // ── cache-mode publish routing ──────────────────────────────────────────────────

    async fn add_named_direct(ctx: &Arc<BridgeContext>, id: &str) {
        ctx.add_device(DeviceConfig {
            id: id.into(),
            name: Some(format!("name-{id}")),
            ip: Some("10.0.0.1".into()),
            key: Some("0123456789abcdef".into()),
            version: None,
            cid: None,
            parent_id: None,
            last_error_code: None,
        })
        .await
        .expect("add device");
    }

    #[tokio::test]
    async fn cache_mode_active_publishes_delta_then_snapshot_after_seed() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await; // any add-time publishes

        // Pretend the seed phase finished so snapshots aren't deferred.
        ctx.seed_done.store(true, Ordering::Release);

        let dps = serde_json::json!({"1": true});
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            dps,
            /* is_passive */ false,
            /* exists */ true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(
            msgs.len(),
            2,
            "active in cache mode must produce a delta + a snapshot publish"
        );
        let active = msgs.iter().find(|m| !m.retain).expect("active no-retain");
        let snapshot = msgs.iter().find(|m| m.retain).expect("snapshot retained");
        assert!(active.topic.contains("/active/"));
        assert!(snapshot.topic.contains("/passive/"));
    }

    #[tokio::test]
    async fn cache_mode_passive_publishes_only_snapshot_after_seed() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        // Passive arrives as a raw JSON object (no `dps` key). The synthesised
        // dps becomes this object, which goes straight into the cache.
        let payload = serde_json::json!({"battery": 80});
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            payload,
            /* is_passive */ true,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1, "passive in cache mode must produce snapshot only");
        assert!(msgs[0].retain, "snapshot must be retained");
        assert!(msgs[0].topic.contains("/passive/"));
    }

    #[tokio::test]
    async fn cache_mode_partial_passive_preserves_other_cached_keys() {
        // The bug this whole redesign fixes: a battery-only passive update
        // must not wipe the previously-known state DPs from the retained
        // snapshot. The cache merges, so the snapshot we publish still has
        // both keys.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true, "2": 50}),
            true, // passive periodic status report
            true,
        )
        .await;
        drain_mqtt(&mut mqtt_rx).await;

        // Battery-only partial follow-up
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"battery": 80}),
            true,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&msgs[0].payload).unwrap();
        assert_eq!(payload.get("1"), Some(&serde_json::json!(true)));
        assert_eq!(payload.get("2"), Some(&serde_json::json!(50)));
        assert_eq!(payload.get("battery"), Some(&serde_json::json!(80)));
    }

    #[tokio::test]
    async fn cache_mode_snapshot_publish_deferred_during_seed() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        // seed_done stays false — simulating the seed window.

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true}),
            false, // active
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        // Active delta still fires (HA event automations); snapshot is deferred.
        assert_eq!(msgs.len(), 1, "only active delta during seed");
        assert!(!msgs[0].retain);
        assert!(msgs[0].topic.contains("/active/"));

        assert!(
            ctx.seed_pending.lock().unwrap().contains("dev-1"),
            "device should be queued for the seed-end flush"
        );
    }

    #[tokio::test]
    async fn cache_mode_dedupes_unchanged_values() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true}),
            true,
            true,
        )
        .await;
        let first = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(first.len(), 1, "first passive publishes snapshot");

        // Same value again — cache.merge returns no changed keys, no snapshot.
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true}),
            true,
            true,
        )
        .await;
        let second = drain_mqtt(&mut mqtt_rx).await;
        assert!(
            second.is_empty(),
            "same-value passive must dedupe to no publish"
        );
    }

    #[tokio::test]
    async fn passthrough_mode_publishes_single_message() {
        // Sanity: pass-through mode retains the historical one-publish-per-event shape.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(false);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true}),
            false,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(msgs.len(), 1, "pass-through must publish exactly one message");
        assert!(!msgs[0].retain, "pass-through never retains");
        assert!(msgs[0].topic.contains("/active/"));
    }

    // ── cache-mode seed phase ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn cache_mode_default_template_arms_seed_wildcard() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        assert!(
            ctx.seed_state_wildcard.is_some(),
            "default template + cache mode must produce a seed wildcard"
        );
        assert!(ctx.seed_state_regex.is_some());
        assert!(
            !ctx.seed_done.load(Ordering::Acquire),
            "seed_done starts false to gate snapshots"
        );
    }

    #[tokio::test]
    async fn cache_mode_custom_template_disables_seed() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_payload_template = Some("{\"v\":{value}}".into());
        })
        .await;
        assert!(
            ctx.seed_state_wildcard.is_none(),
            "custom template must skip seed wildcard"
        );
        assert!(
            ctx.seed_done.load(Ordering::Acquire),
            "seed_done must be pre-flipped so snapshots aren't deferred"
        );
    }

    #[tokio::test]
    async fn match_seed_topic_extracts_id_in_multi_dp_mode() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{type}/{id}".into());
        })
        .await;
        let (id, dp) = ctx
            .match_seed_topic("rustuya/event/passive/dev-1")
            .expect("topic should match");
        assert_eq!(id, "dev-1");
        assert!(dp.is_none(), "multi-DP mode has no {{dp}}");
    }

    #[tokio::test]
    async fn match_seed_topic_extracts_id_and_dp_in_single_dp_mode() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{type}/{id}/{dp}".into());
        })
        .await;
        let (id, dp) = ctx
            .match_seed_topic("rustuya/event/passive/dev-1/42")
            .expect("topic should match");
        assert_eq!(id, "dev-1");
        assert_eq!(dp.as_deref(), Some("42"));
    }

    #[test]
    fn parse_seed_payload_multi_dp_uses_full_object() {
        let dps = parse_seed_payload(r#"{"1": true, "2": 50}"#, None).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        assert_eq!(dps.get("2"), Some(&json!(50)));
    }

    #[test]
    fn parse_seed_payload_single_dp_wraps_value_under_dp_key() {
        let dps = parse_seed_payload("true", Some("13")).unwrap();
        assert_eq!(dps.get("13"), Some(&json!(true)));
    }

    #[test]
    fn parse_seed_payload_returns_none_on_garbage() {
        assert!(parse_seed_payload("not json", None).is_none());
        assert!(parse_seed_payload("", Some("1")).is_none());
    }

    #[test]
    fn parse_seed_payload_multi_dp_rejects_scalar_payload() {
        // Multi-DP mode expects an object; a scalar means custom template.
        assert!(parse_seed_payload("true", None).is_none());
        assert!(parse_seed_payload(r#""string""#, None).is_none());
    }

    #[tokio::test]
    async fn on_seed_complete_flips_flag_and_flushes_pending() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;

        // Simulate an active arriving during the seed window — snapshot
        // publish is deferred, device gets queued in seed_pending.
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            json!({"1": true}),
            false,
            true,
        )
        .await;
        let pre = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(pre.len(), 1, "only active delta during seed");
        assert!(ctx.seed_pending.lock().unwrap().contains("dev-1"));

        // Seed completes: flag flips, pending drains, snapshot publishes.
        ctx.on_seed_complete().await;
        assert!(ctx.seed_done.load(Ordering::Acquire));
        assert!(ctx.seed_pending.lock().unwrap().is_empty());

        let post = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(post.len(), 1, "flush publishes one snapshot");
        assert!(post[0].retain, "flush snapshot is retained");
        assert!(post[0].topic.contains("/passive/"));
    }

    #[tokio::test]
    async fn on_seed_complete_is_idempotent() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        ctx.on_seed_complete().await;
        // Second call must be a no-op — seed_done already true.
        ctx.on_seed_complete().await;
        assert!(ctx.seed_done.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn cache_mode_runtime_added_device_publishes_immediately_after_seed() {
        // Devices added after seed_done flips should not be silently queued
        // for an already-finished flush — their first cache change must
        // publish right away.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        ctx.seed_done.store(true, Ordering::Release);

        add_named_direct(&ctx, "late-1").await;
        drain_mqtt(&mut mqtt_rx).await;

        ctx.publish_device_event(
            "late-1".into(),
            Some("name-late-1".into()),
            None,
            json!({"1": true}),
            true,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert_eq!(
            msgs.len(),
            1,
            "runtime-added device must publish snapshot on first cache change"
        );
        assert!(msgs[0].retain);
        assert!(
            ctx.seed_pending.lock().unwrap().is_empty(),
            "must not queue when seed already done"
        );
    }

    #[tokio::test]
    async fn cache_mode_single_dp_mode_publishes_snapshot_per_dp() {
        // Single-DP topic template → each DP gets its own snapshot message.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{type}/{id}/{dp}".into());
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            json!({"1": true, "2": 50}),
            true,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        let snapshots: Vec<_> = msgs.iter().filter(|m| m.retain).collect();
        assert_eq!(
            snapshots.len(),
            2,
            "single-DP snapshot publishes one retained message per DP"
        );
        let topics: Vec<&str> = snapshots.iter().map(|m| m.topic.as_str()).collect();
        assert!(topics.iter().any(|t| t.ends_with("/1")));
        assert!(topics.iter().any(|t| t.ends_with("/2")));
    }

    #[tokio::test]
    async fn cache_mode_single_dp_publishes_only_changed_dps() {
        // Regression for a user-reported bug: after the cache is populated
        // with multiple DPs, an event changing one DP should NOT also
        // republish snapshots for unchanged DPs in single-DP topic mode.
        // Each per-DP topic already has the correct retained from prior
        // snapshots — re-emitting them as type=passive on every unrelated
        // event spuriously surfaces "events" for DPs that didn't change.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{type}/{id}/{dp}".into());
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        // Seed cache with two DPs, drain the resulting publishes.
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            json!({"1": true, "2": 50}),
            true,
            true,
        )
        .await;
        drain_mqtt(&mut mqtt_rx).await;

        // Now change only DP 1.
        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            json!({"1": false}),
            false,
            true,
        )
        .await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        let snapshots: Vec<_> = msgs.iter().filter(|m| m.retain).collect();
        assert_eq!(
            snapshots.len(),
            1,
            "single-DP mode must only republish the DP that changed, not all cached DPs"
        );
        assert!(
            snapshots[0].topic.ends_with("/1"),
            "snapshot must be for the changed DP only (got '{}')",
            snapshots[0].topic
        );
    }

    #[tokio::test]
    async fn cache_mode_remove_drops_cached_snapshot() {
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        ctx.publish_device_event(
            "dev-1".into(),
            Some("name-dev-1".into()),
            None,
            serde_json::json!({"1": true}),
            true,
            true,
        )
        .await;
        let cache = ctx.cache.as_ref().expect("cache exists");
        assert!(cache.snapshot("dev-1").is_some());

        ctx.remove_device(Some(vec!["dev-1".into()]), None)
            .await
            .unwrap();
        assert!(
            cache.snapshot("dev-1").is_none(),
            "cache must drop on removal"
        );
    }
}

    #[test]
    fn parse_payload_array_merges_vars_per_item() {
        let vars: HashMap<String, String> =
            [("id".into(), "dev-1".into())].into_iter().collect();
        let val = BridgeContext::parse_mqtt_payload(
            r#"[{"action":"get"},{"action":"status","id":"override"}]"#,
            &vars,
        );
        let arr = val.as_array().unwrap();
        assert_eq!(arr[0].get("id").and_then(|v| v.as_str()), Some("dev-1"));
        assert_eq!(arr[1].get("id").and_then(|v| v.as_str()), Some("override"));
    }
}
