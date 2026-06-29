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
use crate::template::{compile_topic_regex, match_topic, render_template, tpl_to_wildcard};
use crate::types::{ApiResponse, BridgeRequest};
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex as StdMutex;

// Sized to absorb a whole-fleet operation burst without shedding. A
// name-addressed `set` (or cascade) to N devices makes every one of them ack
// AND push its state change at once — in cache mode that's ~3 messages each
// (api response + active delta + state snapshot), so a 500-device fan-out
// transiently queues ~1500 messages. A small buffer (was 200) drops most of
// them via `try_send_mqtt`'s 500ms timeout even though the broker would drain
// them within a second. 4096 holds the full transient for fleets into the low
// thousands while staying bounded (~600 KB worst case) so a genuinely wedged
// broker still can't grow memory without limit.
pub const MQTT_CHANNEL_CAPACITY: usize = 4096;
/// Default devices per `status` response page. ~50 device records stay well
/// under restrictive broker packet limits (~6 KB) so the default `status`
/// never stalls the connection, regardless of fleet size.
pub const STATUS_DEFAULT_PAGE: usize = 50;
/// Hard cap on a `status` page even when a larger `limit` is requested —
/// bounds the worst-case response size for capable brokers.
pub const STATUS_MAX_PAGE: usize = 500;
/// Client-side MQTT packet size cap (incoming and outgoing). rumqttc defaults
/// *both* to a tiny 10 KiB, which silently refuses any larger publish — a big
/// `status` reply, a multi-DP snapshot — *before it ever reaches the broker*,
/// stalling the connection on a retry loop. Raise it to a generous bound so
/// the client is never the bottleneck; the broker still enforces its own
/// limit, and `status` is paginated independently as defense in depth.
pub const MAX_MQTT_PACKET_SIZE: usize = 1024 * 1024;
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
    /// Drives BOTH the MQTT retain flag and the publish QoS — they are not
    /// independent. The outbound loop (`spawn_mqtt_task`) maps `retain` → QoS:
    /// `retain=true → QoS1 (AtLeastOnce)`, `retain=false → QoS0 (AtMostOnce)`.
    /// There is no other QoS knob; this field is the single source of truth.
    ///
    /// Per message kind (see the `retain:` call sites):
    ///   retain=true  → QoS1 : state snapshots (`{type}=state`, cache mode only),
    ///                         bridge-config sentinel. Durable state a re-subscriber
    ///                         must recover, so PUBACK-acknowledged delivery is worth it.
    ///   retain=false → QoS0 : active/passive deltas, error/connect notices,
    ///                         API/command responses. Live & transient — a drop
    ///                         self-heals via the next push or the retained snapshot.
    ///
    /// Why live = QoS0 (do NOT blanket-revert to QoS1): a whole-fleet fan-out emits
    /// hundreds of these at once; per-message PUBACK round-trips + rumqttc's in-flight
    /// window (100) stalled the event loop and starved the device actors (~60% reset
    /// on a 500-device name fan-out). The non-blocking `try_publish` in the loop is the
    /// structural burst-safety fix; QoS0 on the live path is defense-in-depth that keeps
    /// that path's protocol load low. NOTE: the one kind with a real delivery-guarantee
    /// argument is command *responses* — if that ever matters, scope QoS1 to responses
    /// specifically rather than reverting the whole `else` branch (it re-adds N PUBACKs
    /// to the fan-out path).
    pub retain: bool,
}

#[derive(Default)]
struct TopicVars<'a> {
    id: &'a str,
    name: Option<&'a str>,
    cid: Option<&'a str>,
    level: Option<&'a str>,
    event_type: Option<&'a str>,
    action: Option<&'a str>,
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

    /// When set, the outbound publish loop forces `retain=false` on every
    /// message regardless of its `MqttMessage::retain` flag. Latched on by the
    /// `reconfigure` action so device events arriving during/after the
    /// old-scheme retained purge can't recreate orphaned retained snapshots.
    /// Never cleared — the process exits shortly after it's set.
    pub retain_suppressed: AtomicBool,

    /// Per-device merged DPS cache. `Some` whenever `mqtt_retain=true` (cache
    /// mode); `None` in pass-through mode (`mqtt_retain=false`). Note `{type}`
    /// absence does *not* gate this — it only triggers a startup WARN (deltas
    /// and the state snapshot then collide on one topic), never a downgrade.
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
    /// topic with `{type}=state` (the retained-snapshot type) and other
    /// placeholders as `+`.
    pub seed_state_wildcard: Option<String>,

    /// Regex matched against incoming retained messages on the seed wildcard
    /// to extract `{id}` and (optionally) `{dp}` from the topic.
    pub seed_state_regex: Option<Regex>,
}

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

// Template / payload helpers moved into dedicated modules — see
// [`crate::template`] (forward substitution, topic ↔ wildcard / regex,
// `match_topic`) and [`crate::payload`] (inbound command parsing, reverse
// template parsing, seed-phase DPS extraction).

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

/// Atomically writes `bytes` to `path`: write + fsync a temp file, rename it
/// over the target, then fsync the parent dir so the rename itself survives an
/// unclean shutdown. Shared by the state file and the config file persistence.
///
/// # Errors
/// Returns an error if the temp file cannot be created/written/synced or the
/// rename fails (the temp file is cleaned up on rename failure).
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }

    let tmp_path = path.with_extension("tmp");
    {
        let mut f = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;
        f.write_all(bytes).await?;
        f.sync_all().await?;
    }

    if let Err(e) = tokio::fs::rename(&tmp_path, path).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        anyhow::bail!("Failed to commit file {}: {}", path.display(), e);
    }

    // Fsync the directory entry (Unix). On Windows this is a no-op and
    // `File::open` on a directory fails — silently ignore.
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Ok(dir) = tokio::fs::File::open(parent).await
    {
        let _ = dir.sync_all().await;
    }

    Ok(())
}

/// Validates a topic/template string supplied to `set_config`: it must be
/// non-empty and contain no MQTT wildcards (`+` or `#`). Wildcards in publish
/// topics (event/message/scanner) are illegal MQTT; the command (subscribe)
/// topic could legally use them, but they are rejected there too for a uniform,
/// footgun-free rule (a stray `#` would silently widen the command subscription).
fn validate_topic_template(name: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if value.contains('+') || value.contains('#') {
        return Err(format!(
            "{name} must not contain MQTT wildcards ('+' or '#')"
        ));
    }
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

        // cache mode (mqtt_retain=true) splits each device update across
        // no-retain raw deltas (`{type}=active` / `{type}=passive`) and a
        // retained merged snapshot (`{type}=state`). MQTT semantics keep us
        // safe on the reload path regardless of templates: a no-retain
        // publish doesn't overwrite the retained value, so a re-subscribing
        // client always receives only the latest `state` snapshot, never a
        // stale delta. Spurious re-fires on reconnect — the primary bug we
        // wanted to kill — are gone unconditionally.
        //
        // The only residual concern is *live* double-fire: when `{type}` is
        // absent all three kinds (active delta, passive delta, state
        // snapshot) land on one topic, so a live subscriber gets every delta
        // *and* every snapshot and can't tell them apart. With `{type}` in
        // the topic each kind is its own topic (subscribe to just what you
        // want); in the payload, consumers filter via value_json.type.
        // State entities are idempotent and don't care; event automations
        // need the distinction. We warn rather than downgrade because
        // mqtt_retain=true is the user's explicit opt-in, and a fleet with
        // no event automations is perfectly happy with neither {type}.
        let type_distinguishable =
            mqtt_event_topic.contains("{type}") || payload_tpl.contains("{type}");
        if cli.mqtt_retain() && !type_distinguishable {
            warn!(
                "mqtt_retain=true but {{type}} is absent from both mqtt_event_topic ('{mqtt_event_topic}') and mqtt_payload_template ('{payload_tpl}'). State recovery on reconnect works, but live subscribers can't separate active/passive deltas from the retained state snapshot — all three collide on one topic. Add {{type}} to either template to let consumers filter."
            );
        }
        let effective_retain = cli.mqtt_retain();

        let cache = effective_retain.then(|| Arc::new(DpsCache::new()));

        // The seed phase parses broker-retained snapshots into the cache.
        // Default `{value}` is handled directly; any other template is run
        // through the JSON-tree reverse parser in [`crate::payload`]
        // (sentinel + parallel walk), which works for any JSON-shaped
        // template. Only text-style templates (e.g. `v={value};ts={timestamp}`)
        // remain unparseable — those disable seed entirely and the bridge
        // will overwrite broker retained on first publish per device.
        let seed_supported = effective_retain
            && match cli.mqtt_payload_template.as_deref() {
                None | Some("{value}") => true,
                Some(tpl) => crate::payload::validate_payload_template(tpl).is_ok(),
            };
        if effective_retain
            && !seed_supported
            && let Some(tpl) = cli.mqtt_payload_template.as_deref()
            && let Err(why) = crate::payload::validate_payload_template(tpl)
        {
            warn!(
                "Seed phase disabled: {why}. The bridge will overwrite broker retained on first publish per device."
            );
        }

        let root_topic_str = cli
            .mqtt_root_topic
            .clone()
            .unwrap_or_else(|| crate::config::DEFAULT_MQTT_ROOT_TOPIC.to_string());

        let (seed_state_wildcard, seed_state_regex) = if seed_supported {
            let tpl = mqtt_event_topic.replace("{type}", "state");
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
            retain_suppressed: AtomicBool::new(false),
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

        // Locate DPS either nested under "data" or at the root.
        //
        // Active vs passive is determined by the wrapper shape, which maps to
        // the underlying Tuya cmd:
        //   Active  — device-initiated push (cmd 8 / DP_STATUS, similar).
        //             Payload: {"data": {"dps": {...}}, "protocol": ..., "t": ...}
        //   Passive — DP_QUERY response (cmd 16), periodic status reports, or
        //             other non-push events.
        //             Payload: {"dps": {...}}  (no `data` wrapper)
        //
        // The presence of `data.dps` is what distinguishes active. When
        // `data` is present its `dps` is the canonical source; when only the
        // root `dps` exists, that is the source. If neither is present we
        // fall through to a synthesis path that uses `data` directly (older
        // sub-device firmwares occasionally emit periodic status this way).
        let data_dps = payload_obj
            .and_then(|o| o.get("data"))
            .and_then(|d| d.as_object())
            .and_then(|do_| do_.get("dps"));
        let root_dps = payload_obj.and_then(|o| o.get("dps"));

        let is_passive = data_dps.is_none();
        let mut dps = data_dps.or(root_dps).cloned().unwrap_or_else(|| {
            // Fallback: no `dps` field anywhere. Synthesise from `data`
            // (flattening nested `data.data`) so downstream sees a uniform
            // shape. Always treated as passive (no `data.dps` push).
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
                // state (per-device snapshots, the bridge-config sentinel) → QoS1;
                // live/transient deltas, error/connect notices, and command responses
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
                                    let ctx_h = self.clone();
                                    tokio::spawn(async move {
                                        // Each affected target answers with its own response
                                        // (per-id), addressed to its own topic. An empty Vec
                                        // means the handler suppressed it (single successful
                                        // set/get).
                                        let responses = crate::handlers::handle_request(ctx_h.clone(), req).await;
                                        for res in responses {
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
                    msg_opt = mqtt_tx_receiver.recv(), if pending.is_none() => {
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

    /// Serializes the running config for the `{root}/bridge/config` topic,
    /// injecting the bridge `version` (a build constant, not a `Cli` field, so
    /// it never round-trips through the config file). Credentials are stripped
    /// by `#[serde(skip_serializing)]` on the `Cli` fields themselves.
    fn build_bridge_config_payload(cli: &crate::config::Cli) -> String {
        let mut v =
            serde_json::to_value(cli).unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "version".to_string(),
                Value::String(env!("CARGO_PKG_VERSION").to_string()),
            );
        }
        serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string())
    }

    pub async fn publish_bridge_config(&self, cli: Option<&crate::config::Cli>, clear: bool) {
        let topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt_root_topic);
        let payload = if clear {
            String::new()
        } else {
            cli.map_or_else(|| "{}".to_string(), Self::build_bridge_config_payload)
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

    /// Renders the topic the liveness probe publishes its `status` ping to,
    /// using the same `replace_vars` translator as the rest of the codebase.
    /// The action can ride *in the topic* (e.g. `{root}/command/{action}/{id}/{dp}`),
    /// so `action` = "status"; every other variable gets a non-empty placeholder
    /// because the command regex captures `[^/]+` per level — an empty segment
    /// would never match the incumbent's subscription.
    fn probe_command_topic(&self) -> String {
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
    fn probe_response_topic(&self) -> String {
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
                // Load the platform's native root certificates so `mqtts://`
                // validates against public CAs (hosted brokers). Passing an
                // empty CA Vec to `Transport::tls` builds an empty root store
                // and fails every handshake with `NoValidCertInChain`.
                opts.set_transport(rumqttc::Transport::tls_with_default_config());
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

    // `parse_mqtt_payload` and `apply_set_heuristic` live in
    // [`crate::payload`] — they're pure functions with no `BridgeContext`
    // state. The MQTT command loop calls them via `crate::payload::`.

    /// Atomically saves device configuration to state file
    ///
    /// # Errors
    /// Returns an error if the state file cannot be serialized, written, or atomically renamed.
    pub async fn save_state(&self) -> Result<()> {
        let json = {
            let state = self.state.read().await;
            serde_json::to_string_pretty(&state.configs)?
        };

        write_atomic(Path::new(&self.state_file), json.as_bytes()).await?;

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
        event_type: &str,
    ) -> Vec<(String, String)> {
        let dps_str = dps.to_string();

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
    /// channel. Single low-level publish helper used by pass-through publishes
    /// and by both cache-mode routes (raw delta and state snapshot); the
    /// caller picks the `{type}` value (`active`/`passive`/`state`) and retain.
    async fn publish_event_templates(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: &Value,
        event_type: &str,
        retain: bool,
    ) {
        let templates = self.generate_device_templates(id, name, cid, dps, event_type);
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
    /// 1. *Direct route* (active AND passive): publish the incoming raw delta,
    ///    no retain, on `{type}=active` (active) or `{type}=passive` (passive).
    ///    Fires unconditionally — even for a passive that matches the cache, so
    ///    a `get`/status readback is still observable, and even during the seed
    ///    window. This is the live "something arrived" signal.
    /// 2. *Cache route*: merge the incoming DPs into the in-memory cache; if
    ///    any value actually changed AND the seed phase has finished, publish
    ///    the full merged snapshot on `{type}=state` retained. During the
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
        let delta_type = if is_passive { "passive" } else { "active" };

        // pass-through. mqtt_retain=false here either by user choice or by
        // the {type}-missing downgrade in `BridgeContext::new`.
        if !self.mqtt_retain {
            self.publish_event_templates(&id, name_opt, cid_opt, &dps, delta_type, false)
                .await;
            return;
        }

        // cache-mode direct route: raw incoming delta, no retain, for BOTH
        // active and passive (active → {type}=active, passive → {type}=passive).
        // Fires unconditionally — even for a passive whose values all match the
        // cache (so a `get`/status readback is observable) and even during the
        // seed window. The retained `{type}=state` snapshot below is what
        // dedupes; this live signal never does.
        self.publish_event_templates(&id, name_opt, cid_opt, &dps, delta_type, false)
            .await;

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

    /// Publishes the cached snapshot for one device on the `{type}=state`
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
        self.publish_event_templates(id, name, cid, &snap_value, "state", retain)
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
            "action" => {
                out.push_str(vars.action.unwrap_or(""));
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
                    debug!("clear_and_flush stop after {acked}/{} acked: {other:?}", topics.len());
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

        let event_tpl = templates[0].clone();
        let msg_tpl = templates[1].clone();
        let event_re = compile_topic_regex(&event_tpl);
        let msg_re = compile_topic_regex(&msg_tpl);
        let scavenger_timeout_secs = self.cli.scavenger_timeout_secs();

        let config_topic = crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &root_topic);

        tokio::spawn(async move {
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
                    // 1. Precise topic match.
                    let vars = match_topic(topic, &event_tpl, event_re.as_ref())
                        .or_else(|| match_topic(topic, &msg_tpl, msg_re.as_ref()));
                    let mut matched = false;
                    if let Some(vars) = vars {
                        let v_id = vars.get("id");
                        let v_name = vars.get("name");
                        let v_chan = vars.get("cid");
                        matched = active_targets.iter().any(|t| {
                            let id_hit = v_id.is_some_and(|v| v == &t.id);
                            let cid_hit = v_chan.is_some() && t.cid.as_ref() == v_chan;
                            let name_hit = v_name.is_some() && t.name.as_ref() == v_name;
                            id_hit || cid_hit || name_hit
                        });
                    }
                    // 2. Exact payload (quoted-id) fallback.
                    if !matched {
                        let payload = String::from_utf8_lossy(payload);
                        matched = active_targets.iter().any(|t| {
                            let id_match = payload.contains(&format!("\"{}\"", t.id));
                            let cid_match = t.cid.as_ref().is_some_and(|cid| payload.contains(&format!("\"{cid}\"")));
                            let name_match = t.name.as_ref().is_some_and(|name| payload.contains(&format!("\"{name}\"")));
                            id_match || cid_match || name_match
                        });
                    }
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
                    break;
                }
            }
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

    /// Removes devices and their sub-devices, returning the ids actually removed
    /// (the matched devices plus every cascaded sub-device).
    ///
    /// # Errors
    /// Returns [`BridgeError::NoMatchingDevices`] if no devices match the given selectors.
    pub async fn remove_device(
        &self,
        id: Option<Vec<String>>,
        name: Option<Vec<String>>,
    ) -> Result<Vec<String>, BridgeError> {
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

        // Return the removed ids (parent + cascaded subs); the handler emits one
        // per-id `remove` response so each lands on its own response topic.
        Ok(targets)
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
        let mqtt_options =
            match self.create_mqtt_options(&broker_url, &client_id, &self.cli, false) {
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
        info!("Purge: collected {} retained, cleared {acked}", topics.len());
        let _ = client.disconnect().await;
        acked
    }

    /// Applies a `set_config` request: validates the provided topic / template /
    /// retain fields, patches them into the config file (the single source for
    /// these settings — they have no CLI/env path), and reports which fields
    /// changed. All changes take effect on the next `reconfigure`/restart, since
    /// the bridge reads these only at startup; with `apply = true` a
    /// `reconfigure` is chained here so the response can carry the diff.
    ///
    /// # Errors
    /// Returns [`BridgeError::InvalidRequest`] when no config file is configured,
    /// the file can't be read / parsed / written, or a value fails validation.
    #[allow(
        clippy::too_many_arguments,
        reason = "one optional per settable config field; a struct would just be unpacked here"
    )]
    pub async fn set_config(
        &self,
        mqtt_command_topic: Option<String>,
        mqtt_event_topic: Option<String>,
        mqtt_message_topic: Option<String>,
        mqtt_scanner_topic: Option<String>,
        mqtt_payload_template: Option<String>,
        mqtt_retain: Option<bool>,
        apply: bool,
    ) -> Result<ApiResponse, BridgeError> {
        // The config file is the only place these settings persist to, and the
        // restart re-reads it — without one there is nothing to apply.
        let Some(config_path) = self.cli.config.clone() else {
            return Err(BridgeError::InvalidRequest(
                "set_config requires a config file (start the bridge with --config)".into(),
            ));
        };

        // Validate up front so a bad value never reaches the file.
        for (name, val) in [
            ("mqtt_command_topic", mqtt_command_topic.as_deref()),
            ("mqtt_event_topic", mqtt_event_topic.as_deref()),
            ("mqtt_message_topic", mqtt_message_topic.as_deref()),
            ("mqtt_scanner_topic", mqtt_scanner_topic.as_deref()),
        ] {
            if let Some(t) = val {
                validate_topic_template(name, t).map_err(BridgeError::InvalidRequest)?;
            }
        }
        if let Some(tpl) = mqtt_payload_template.as_deref() {
            if tpl.trim().is_empty() {
                return Err(BridgeError::InvalidRequest(
                    "mqtt_payload_template must not be empty".into(),
                ));
            }
            crate::payload::validate_payload_template(tpl).map_err(|e| {
                BridgeError::InvalidRequest(format!("mqtt_payload_template: {e}"))
            })?;
        }

        // Read the current file (source of truth). Diff is computed against the
        // *effective* values (file merged with defaults) so an omitted field
        // reads as its default rather than a spurious change.
        let content = tokio::fs::read_to_string(&config_path).await.map_err(|e| {
            BridgeError::InvalidRequest(format!("cannot read config file {config_path}: {e}"))
        })?;
        let mut file_cli: Cli = serde_json::from_str(&content).map_err(|e| {
            BridgeError::InvalidRequest(format!("cannot parse config file {config_path}: {e}"))
        })?;
        let mut effective = file_cli.clone();
        effective.merge(Cli::default());

        let mut changed = serde_json::Map::new();
        macro_rules! patch_str {
            ($field:ident, $name:literal) => {
                if let Some(new_val) = &$field {
                    let old_val = effective.$field.clone().unwrap_or_default();
                    if *new_val != old_val {
                        changed.insert(
                            $name.to_string(),
                            serde_json::json!({ "from": old_val, "to": new_val }),
                        );
                        file_cli.$field = Some(new_val.clone());
                    }
                }
            };
        }
        patch_str!(mqtt_command_topic, "mqtt_command_topic");
        patch_str!(mqtt_event_topic, "mqtt_event_topic");
        patch_str!(mqtt_message_topic, "mqtt_message_topic");
        patch_str!(mqtt_scanner_topic, "mqtt_scanner_topic");
        patch_str!(mqtt_payload_template, "mqtt_payload_template");
        if let Some(new_retain) = mqtt_retain {
            let old_retain = effective.mqtt_retain.unwrap_or(false);
            if new_retain != old_retain {
                changed.insert(
                    "mqtt_retain".to_string(),
                    serde_json::json!({ "from": old_retain, "to": new_retain }),
                );
                file_cli.mqtt_retain = Some(new_retain);
            }
        }

        let reconfigure_required = !changed.is_empty();
        if reconfigure_required {
            let json = serde_json::to_string_pretty(&file_cli)
                .map_err(BridgeError::SerializationError)?;
            write_atomic(Path::new(&config_path), json.as_bytes())
                .await
                .map_err(|e| {
                    BridgeError::InvalidRequest(format!(
                        "cannot write config file {config_path}: {e}"
                    ))
                })?;
            info!(
                "set_config: patched {} field(s) in {config_path}; reconfigure required to apply.",
                changed.len()
            );
        } else {
            info!("set_config: no effective changes; config file left untouched.");
        }

        let mut resp = ApiResponse::ok_action("set_config")
            .with_extra("changed", Value::Object(changed))
            .with_extra("reconfigure_required", reconfigure_required);

        if apply && reconfigure_required {
            // Writes the file first (above) so the chained reconfigure's
            // scavenge check sees the new scheme on disk and purges old-scheme
            // retained where needed. Same delivery path as the `reconfigure`
            // command: this response is published before the restart completes.
            self.do_reconfigure().await;
            resp = resp.with_extra("reconfigure", "triggered");
        }

        Ok(resp)
    }

    /// Re-applies a configuration change by clearing old-scheme retained
    /// messages and restarting the bridge.
    ///
    /// The bridge reads its config only at startup, so any change to the
    /// topic/payload templates or `mqtt_retain` needs a restart to take
    /// effect. `reconfigure` performs that restart cleanly *while the old
    /// (in-memory) config is still live*, so it can scavenge the retained
    /// snapshots published under the old scheme before they become orphans.
    ///
    /// It **always** restarts (relying on a process supervisor to bring the
    /// bridge back); device registrations are preserved on disk. When
    /// `mqtt_retain` is off there are no bridge-published retained snapshots to
    /// clear, so it warns but still restarts — letting "edit config →
    /// reconfigure" be a uniform habit. The retained purge is skipped when the
    /// scavenge-relevant config on disk is unchanged (see
    /// [`Self::scavenge_config_unchanged`]), so a casual reconfigure on an
    /// unchanged scheme is a plain restart, not a needless state wipe.
    ///
    /// # Errors
    /// Currently always returns `Ok`; reserved for future failure modes.
    pub async fn reconfigure(&self) -> Result<ApiResponse, BridgeError> {
        self.do_reconfigure().await;

        // Bridge-level action (not an all-devices op like `clear`), so the
        // response carries no device id and lands on the bridge response topic.
        Ok(ApiResponse::ok_action("reconfigure"))
    }

    /// Performs the config-reapply work: clears old-scheme retained messages
    /// (when the scavenge-relevant config changed) and trips the cancellation
    /// token for a clean restart. Returns nothing so callers can shape their own
    /// response — used by the `reconfigure` command and by `set_config`'s
    /// `apply` path.
    async fn do_reconfigure(&self) {
        let supervised = std::env::var_os("INVOCATION_ID").is_some()
            || std::env::var_os("JOURNAL_STREAM").is_some();

        info!("reconfigure: re-applying config via clean restart.");
        if supervised {
            info!("reconfigure: process supervisor detected; expecting an automatic restart.");
        } else {
            warn!(
                "reconfigure: no process supervisor detected (no INVOCATION_ID/JOURNAL_STREAM). \
                 The bridge will exit and must be restarted manually to apply the new config."
            );
        }

        if self.cli.mqtt_broker.is_none() {
            warn!(
                "reconfigure: no MQTT broker configured — skipping retained cleanup; \
                 restarting only."
            );
        } else if self.scavenge_config_unchanged().await {
            // The retained topics depend only on the root/event/message
            // templates and the retain flag; if none of those changed on disk,
            // the existing retained snapshots are still valid under the
            // restarted scheme — clearing them would needlessly blank state.
            info!(
                "reconfigure: scavenge-relevant config (broker, root/event/message topic, \
                 retain) is unchanged on disk; restarting without retained cleanup. Edit the \
                 config first if you intended a change."
            );
        } else {
            // Latch retain off for the rest of this process's life so device
            // events arriving during/after the purge can't recreate old-scheme
            // retained snapshots. Live (non-retained) delivery continues.
            self.retain_suppressed.store(true, Ordering::Relaxed);
            if !self.mqtt_retain {
                warn!(
                    "reconfigure: mqtt_retain is off — clearing any stale retained from a prior \
                     retain=on run."
                );
            }
            let cleared = self.purge_all_retained().await;
            info!("reconfigure: cleared {cleared} retained message(s) under the old scheme.");
        }

        info!("reconfigure: shutting down for restart.");
        self.cancel.cancel();
    }

    /// Returns `true` when the scavenge-relevant config on disk (broker, root,
    /// event topic, message topic, and `mqtt_retain` — *not* the payload
    /// template, which doesn't relocate retained topics) matches the running
    /// in-memory config, i.e. a restart won't move or orphan any retained
    /// messages.
    ///
    /// Errs toward `false` (→ purge): no `--config` file, an unreadable or
    /// unparseable file, or any differing field all return `false`. A CLI/env
    /// override of one of these fields makes the file value diverge from the
    /// in-memory value, so it also returns `false` — the safe direction, since
    /// a wrongly-skipped purge would leave permanent orphans while a needless
    /// purge merely re-syncs state after restart.
    async fn scavenge_config_unchanged(&self) -> bool {
        let Some(config_path) = self.cli.config.as_deref() else {
            return false;
        };
        let Ok(content) = tokio::fs::read_to_string(config_path).await else {
            return false;
        };
        let Ok(mut file_cli) = serde_json::from_str::<Cli>(&content) else {
            return false;
        };
        // Resolve omitted fields to the same defaults the runtime config did,
        // so an absent field reads as its default rather than a spurious diff.
        file_cli.merge(Cli::default());
        let (_, file_event) = file_cli.mqtt_topics();
        let file_root = file_cli
            .mqtt_root_topic
            .as_deref()
            .unwrap_or(crate::config::DEFAULT_MQTT_ROOT_TOPIC);
        // The broker is part of "where retained live": switching brokers leaves
        // the old broker's retained orphaned (the purge runs against the old,
        // in-memory broker before the restart), so a broker change must purge.
        file_cli.mqtt_broker == self.cli.mqtt_broker
            && file_event == self.mqtt_event_topic
            && file_root == self.mqtt_root_topic
            && file_cli.mqtt_message_topic == self.mqtt_message_topic
            && file_cli.mqtt_retain() == self.mqtt_retain
    }

    /// Returns current bridge status and a **page** of registered devices.
    ///
    /// The device list is paginated so the response stays within MQTT broker
    /// packet limits at fleet scale — serializing all of a 1000-device fleet
    /// into one message produces ~127 KB, which exceeds many brokers' max
    /// packet size and stalls the connection. `device_count` is always the
    /// full total; `devices` is the `[offset, offset+limit)` window (ids
    /// sorted for stable paging). `limit` defaults to
    /// [`STATUS_DEFAULT_PAGE`] and is capped at [`STATUS_MAX_PAGE`].
    pub async fn get_bridge_status(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> ApiResponse {
        let off = offset.unwrap_or(0);
        let lim = limit.unwrap_or(STATUS_DEFAULT_PAGE).min(STATUS_MAX_PAGE);

        let state = self.state.read().await;
        let total = state.configs.len();
        let mut ids: Vec<&String> = state.configs.keys().collect();
        ids.sort();

        let mut devices = serde_json::Map::new();
        for id in ids.into_iter().skip(off).take(lim) {
            let cfg = &state.configs[id];
            if let Ok(mut dev_val) = serde_json::to_value(cfg) {
                if let Some(obj) = dev_val.as_object_mut() {
                    let status = Self::determine_device_status(cfg, &state.instances);
                    obj.insert("status".to_string(), Value::String(status));
                }
                devices.insert(id.clone(), dev_val);
            }
        }
        drop(state);

        let returned = devices.len();
        let mqtt_drop_count = self.mqtt_drop_count.load(Ordering::Relaxed);
        ApiResponse::ok_action("status")
            .with_extra("devices", Value::Object(devices))
            .with_extra("device_count", Value::from(total))
            .with_extra("offset", Value::from(off))
            .with_extra("limit", Value::from(lim))
            .with_extra("returned", Value::from(returned))
            .with_extra("has_more", Value::from(off + returned < total))
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

    // ── bridge/config payload ──────────────────────────────────────────────────

    #[test]
    fn bridge_config_payload_injects_version_and_hides_password() {
        let cli = crate::config::Cli {
            mqtt_password: Some("S3cret-pw".into()),
            session_id: Some("sid_1".into()),
            ..crate::config::Cli::default()
        };
        let json = BridgeContext::build_bridge_config_payload(&cli);
        assert!(
            json.contains(&format!(r#""version":"{}""#, env!("CARGO_PKG_VERSION"))),
            "version not injected: {json}"
        );
        assert!(json.contains("session_id"), "session_id missing: {json}");
        assert!(
            !json.contains("S3cret-pw"),
            "password leaked into bridge/config: {json}"
        );
    }

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

    // template / payload pure-function tests moved alongside their subjects
    // in `crate::template::tests` and `crate::payload::tests`.

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

    // ── B1/B2: remove_device removed-id reporting + selective refresh ──────

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
    async fn remove_by_name_with_cascade_returns_all_removed_ids() {
        // Removing a single gateway by name that cascades to sub-devices must
        // return every removed id (gateway + 3 subs) so the handler can answer
        // one per-id `remove` response each.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("gw-1", "kitchen-gateway"))
            .await
            .unwrap();
        for cid in ["s1", "s2", "s3"] {
            ctx.add_device(sub_device(&format!("sub-{cid}"), "gw-1", cid))
                .await
                .unwrap();
        }

        let removed = ctx
            .remove_device(None, Some(vec!["kitchen-gateway".into()]))
            .await
            .expect("remove by name");

        assert_eq!(removed.len(), 4, "cascade should remove gateway + 3 subs");
        assert!(removed.contains(&"gw-1".to_string()));
        assert!(removed.contains(&"sub-s2".to_string()));
    }

    #[tokio::test]
    async fn remove_by_name_with_real_fanout_returns_each_matched_id() {
        // Two devices share a name → name lookup fans out to both. Each is a
        // direct device (no subs); both ids are returned individually.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;

        ctx.add_device(named_direct("dev-a", "kitchen")).await.unwrap();
        ctx.add_device(named_direct("dev-b", "kitchen")).await.unwrap();

        let mut removed = ctx
            .remove_device(None, Some(vec!["kitchen".into()]))
            .await
            .unwrap();
        removed.sort();

        assert_eq!(removed, vec!["dev-a".to_string(), "dev-b".to_string()]);
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

        let res = ctx.get_bridge_status(None, None).await;
        let v = serde_json::to_value(&res).unwrap();
        assert_eq!(
            v.get("mqtt_drop_count").and_then(|n| n.as_u64()),
            Some(3),
            "status must surface the cumulative MQTT drop count"
        );
    }

    #[tokio::test]
    async fn status_paginates_large_fleet() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;
        {
            let mut state = ctx.state.write().await;
            for i in 0..60 {
                let id = format!("dev{i:03}");
                state.configs.insert(
                    id.clone(),
                    serde_json::from_value(serde_json::json!({"id": id, "key": "k"})).unwrap(),
                );
            }
        }

        // Default: one capped page, but the full count is always reported.
        let v = serde_json::to_value(ctx.get_bridge_status(None, None).await).unwrap();
        assert_eq!(v["device_count"], 60);
        assert_eq!(v["returned"], STATUS_DEFAULT_PAGE);
        assert_eq!(v["devices"].as_object().unwrap().len(), STATUS_DEFAULT_PAGE);
        assert_eq!(v["has_more"], true);

        // Second page drains the remainder.
        let v2 = serde_json::to_value(ctx.get_bridge_status(Some(50), None).await).unwrap();
        assert_eq!(v2["returned"], 10);
        assert_eq!(v2["has_more"], false);

        // An over-large limit is capped, never honored verbatim.
        let v3 = serde_json::to_value(ctx.get_bridge_status(Some(0), Some(100_000)).await).unwrap();
        assert_eq!(v3["limit"], STATUS_MAX_PAGE as u64);
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
        assert!(snapshot.topic.contains("/state/"));
    }

    #[tokio::test]
    async fn cache_mode_passive_publishes_delta_then_snapshot_after_seed() {
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
        assert_eq!(
            msgs.len(),
            2,
            "passive in cache mode produces a no-retain delta + a retained snapshot"
        );
        let delta = msgs.iter().find(|m| !m.retain).expect("passive delta no-retain");
        let snapshot = msgs.iter().find(|m| m.retain).expect("snapshot retained");
        assert!(delta.topic.contains("/passive/"), "delta on {{type}}=passive");
        assert!(snapshot.topic.contains("/state/"), "snapshot on {{type}}=state");
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
        // Battery-only passive → a no-retain delta (battery only) + the merged
        // retained snapshot. The snapshot is what must still carry DPs 1 and 2.
        let snapshot = msgs.iter().find(|m| m.retain).expect("merged snapshot retained");
        let payload: serde_json::Value = serde_json::from_str(&snapshot.payload).unwrap();
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
    async fn cache_mode_dedupes_unchanged_snapshot_but_always_emits_delta() {
        // The retained `state` snapshot dedupes on unchanged values, but the
        // no-retain delta always fires so a repeated `get`/status readback is
        // still observable (the whole reason passive gets a live delta).
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
        assert_eq!(first.len(), 2, "first passive: delta + snapshot");
        assert_eq!(first.iter().filter(|m| m.retain).count(), 1, "one retained snapshot");

        // Same value again — cache.merge returns no changed keys → no snapshot,
        // but the no-retain delta still publishes.
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
        assert_eq!(second.len(), 1, "same-value passive still emits the delta");
        assert!(!second[0].retain, "no retained snapshot on unchanged value");
        assert!(second[0].topic.contains("/passive/"));
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

    // (The historical "custom template disables seed" assertion was retired
    // — JSON-shaped templates are now seed-parseable. The retained-shape
    // gate lives in `payload::validate_payload_template`; coverage
    // for both the supported and unsupported cases is in two new tests
    // below, near the other ☆ seed setup.)

    #[tokio::test]
    async fn match_seed_topic_extracts_id_in_multi_dp_mode() {
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_event_topic = Some("{root}/event/{type}/{id}".into());
        })
        .await;
        let (id, dp) = ctx
            .match_seed_topic("rustuya/event/state/dev-1")
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
            .match_seed_topic("rustuya/event/state/dev-1/42")
            .expect("topic should match");
        assert_eq!(id, "dev-1");
        assert_eq!(dp.as_deref(), Some("42"));
    }

    // parse_seed_payload-equivalent unit tests live in
    // `crate::payload::tests` (default + custom-template scenarios).

    #[tokio::test]
    async fn cache_mode_custom_template_with_parseable_shape_enables_seed() {
        // A JSON-shaped custom template is now seed-parseable — the
        // historical "default template only" restriction is gone.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_payload_template =
                Some(r#"{"type":"{type}","value":{value}}"#.into());
        })
        .await;
        assert!(
            ctx.seed_state_wildcard.is_some(),
            "JSON-shaped template must arm the seed wildcard"
        );
        assert!(
            !ctx.seed_done.load(Ordering::Acquire),
            "seed_done must start false so the seed phase runs"
        );
    }

    #[tokio::test]
    async fn cache_mode_unparseable_template_skips_seed() {
        // Text-style template (e.g. `key=val` style) can't be sentinelled
        // into valid JSON → seed disabled, seed_done pre-flipped.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
            cli.mqtt_payload_template = Some("v={value};ts={timestamp}".into());
        })
        .await;
        assert!(ctx.seed_state_wildcard.is_none());
        assert!(ctx.seed_done.load(Ordering::Acquire));
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
        assert!(post[0].topic.contains("/state/"));
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
        // passive → no-retain delta + retained snapshot.
        let snaps: Vec<_> = msgs.iter().filter(|m| m.retain).collect();
        assert_eq!(
            snaps.len(),
            1,
            "runtime-added device must publish snapshot on first cache change"
        );
        assert!(snaps[0].topic.contains("/state/"));
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
        // snapshots — re-emitting them on {type}=state on every unrelated
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

    // ── active/passive detection ───────────────────────────────────────────

    #[tokio::test]
    async fn handle_event_classifies_data_dps_wrapper_as_active() {
        // User-confirmed wire shape: device-initiated DP_STATUS (cmd 8)
        // wraps the dps under `data`. Bridge must classify as active and
        // publish to {type}=active.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        let payload = r#"{"data":{"dps":{"1":false,"2":false}},"protocol":4,"t":1780376055}"#;
        ctx.handle_device_event("dev-1", payload.into()).await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert!(
            msgs.iter().any(|m| m.topic.contains("/active/")),
            "data.dps wrapper must produce an active publish (got topics: {:?})",
            msgs.iter().map(|m| &m.topic).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn handle_event_classifies_root_dps_as_passive() {
        // User-confirmed wire shape: DP_QUERY response (cmd 16) puts dps
        // at the root with no `data` wrapper. Classified as passive: a
        // no-retain passive delta plus a retained state snapshot — never active.
        let (ctx, _tmp, mut mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.mqtt_retain = Some(true);
        })
        .await;
        add_named_direct(&ctx, "dev-1").await;
        drain_mqtt(&mut mqtt_rx).await;
        ctx.seed_done.store(true, Ordering::Release);

        let payload = r#"{"dps":{"1":false,"14":"off","2":false,"7":0,"8":0}}"#;
        ctx.handle_device_event("dev-1", payload.into()).await;

        let msgs = drain_mqtt(&mut mqtt_rx).await;
        assert!(
            !msgs.iter().any(|m| m.topic.contains("/active/")),
            "root dps without data wrapper must NOT produce active publish"
        );
        assert!(
            msgs.iter().any(|m| m.topic.contains("/passive/") && !m.retain),
            "passive must produce a no-retain delta"
        );
        assert!(
            msgs.iter().any(|m| m.topic.contains("/state/") && m.retain),
            "passive must also produce a retained state snapshot"
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

    // ── reconfigure ─────────────────────────────────────────────────────────

    #[test]
    fn reconfigure_action_deserializes() {
        let req: BridgeRequest =
            serde_json::from_value(json!({ "action": "reconfigure" })).expect("parse reconfigure");
        assert!(matches!(req, BridgeRequest::Reconfigure));
        assert_eq!(req.action_name(), "reconfigure");
    }

    #[tokio::test]
    async fn reconfigure_without_broker_still_restarts() {
        // No broker → purge is skipped (nothing to clear), but reconfigure must
        // still always trigger shutdown ("edit config → reconfigure" is a
        // uniform habit, broker or not).
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) =
            make_ctx(|cli| cli.mqtt_broker = None).await;

        assert!(!ctx.cancel.is_cancelled());

        let res = ctx.reconfigure().await.expect("reconfigure ok");

        assert_eq!(res.action.as_deref(), Some("reconfigure"));
        assert!(
            res.id.is_none(),
            "reconfigure is a bridge-level action — no device id (lands on the bridge topic), not \"all\""
        );
        assert!(
            ctx.cancel.is_cancelled(),
            "reconfigure must always trigger shutdown, even with no broker"
        );
    }

    #[tokio::test]
    async fn scavenge_config_unchanged_false_without_config_file() {
        // No --config path → can't prove the scheme is unchanged → purge.
        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|_| {}).await;
        assert!(!ctx.scavenge_config_unchanged().await);
    }

    #[tokio::test]
    async fn scavenge_config_unchanged_detects_match_and_change() {
        let cfg_dir = TempDir::new().expect("cfg tempdir");
        let cfg_path = cfg_dir.path().join("config.json");
        let broker = "mqtt://noop.invalid:1883";
        let matching = format!(
            r#"{{"mqtt_broker":"{broker}","mqtt_event_topic":"{{root}}/ev/{{id}}","mqtt_retain":true}}"#
        );
        tokio::fs::write(&cfg_path, &matching)
            .await
            .expect("write config");
        let cfg_str = cfg_path.to_string_lossy().into_owned();

        let (ctx, _tmp, _mqtt_rx, _save_rx, _refresh_rx) = make_ctx(|cli| {
            cli.config = Some(cfg_str.clone());
            cli.mqtt_broker = Some(broker.into());
            cli.mqtt_event_topic = Some("{root}/ev/{id}".into());
            cli.mqtt_retain = Some(true);
        })
        .await;

        // File matches in-memory (broker + root/event/message topic + retain) → skip purge.
        assert!(
            ctx.scavenge_config_unchanged().await,
            "matching config must read as unchanged"
        );

        // Relocate the event topic on disk → a restart would orphan retained → purge.
        tokio::fs::write(
            &cfg_path,
            r#"{"mqtt_broker":"mqtt://noop.invalid:1883","mqtt_event_topic":"{root}/NEW/{id}","mqtt_retain":true}"#,
        )
        .await
        .expect("rewrite config");
        assert!(
            !ctx.scavenge_config_unchanged().await,
            "an event-topic change must be detected"
        );

        // Point at a different broker → the old broker's retained would orphan → purge.
        tokio::fs::write(
            &cfg_path,
            r#"{"mqtt_broker":"mqtt://other.invalid:1883","mqtt_event_topic":"{root}/ev/{id}","mqtt_retain":true}"#,
        )
        .await
        .expect("rewrite config");
        assert!(
            !ctx.scavenge_config_unchanged().await,
            "a broker change must be detected"
        );
    }

    /// The liveness probe's `status` ping must survive any command-topic
    /// template: the rendered topic has to (a) match what the incumbent
    /// subscribes to and (b) parse back to a `Status` request — including
    /// templates that carry the action in the topic (`{action}`) or that would
    /// otherwise render an empty segment. Guards the two regressions found
    /// while building the probe.
    #[tokio::test]
    async fn liveness_probe_command_topic_round_trips_across_templates() {
        let templates = [
            "{root}/command",
            "{root}/command/{action}",
            "{root}/command/{id}",
            "{root}/command/{cid}",
            "{root}/command/{action}/{id}/{dp}",
        ];
        for tmpl in templates {
            let (ctx, _tmp, _m, _s, _r) =
                make_ctx(|cli| cli.mqtt_command_topic = Some((*tmpl).to_string())).await;

            // Incumbent side, exactly as `spawn_mqtt_task` builds it: the
            // root-substituted template drives both the regex and the match.
            let (cmd_tmpl, _) = ctx.cli.mqtt_topics();
            let re = compile_topic_regex(&cmd_tmpl);

            let probe_topic = ctx.probe_command_topic();

            // No empty levels — the command regex captures `[^/]+` per level.
            assert!(
                !probe_topic.split('/').any(str::is_empty),
                "probe topic {probe_topic:?} has an empty segment for {tmpl:?}"
            );

            let vars = match_topic(&probe_topic, &cmd_tmpl, re.as_ref()).unwrap_or_else(|| {
                panic!("probe topic {probe_topic:?} must match the subscription for {tmpl:?}")
            });

            let req_val = crate::payload::parse_mqtt_payload(r#"{"action":"status"}"#, &vars);
            let req: BridgeRequest = serde_json::from_value(req_val)
                .unwrap_or_else(|e| panic!("must parse to a request for {tmpl:?}: {e}"));
            assert!(
                matches!(req, BridgeRequest::Status { .. }),
                "probe must parse to Status for {tmpl:?}, got {:?}",
                req.action_name()
            );
        }
    }

    /// The probe listens on the exact topic the incumbent publishes its
    /// `status` reply to. Drives the real publish path (`get_bridge_status` →
    /// `publish_api_response`) and asserts the captured topic equals what the
    /// probe subscribes to — so the two halves can't silently drift apart.
    #[tokio::test]
    async fn liveness_probe_response_topic_matches_status_publish() {
        for msg_tmpl in ["{root}/{level}/{id}", "{root}/{level}/{id}/{name}"] {
            let (ctx, _tmp, mut mqtt_rx, _s, _r) =
                make_ctx(|cli| cli.mqtt_message_topic = Some((*msg_tmpl).to_string())).await;

            let resp = ctx.get_bridge_status(None, None).await;
            ctx.publish_api_response(resp).await;

            let published = mqtt_rx
                .recv()
                .await
                .expect("channel open")
                .expect("a publish, not a shutdown signal");
            assert_eq!(
                ctx.probe_response_topic(),
                published.topic,
                "probe must subscribe where status replies land, for {msg_tmpl:?}"
            );
        }
    }

    // ── set_config ──────────────────────────────────────────────────────

    /// Builds a context whose `--config` points at a temp file seeded with
    /// `file_json`. Broker is left `None` so a chained `do_reconfigure` skips the
    /// (network-bound) retained purge and just trips the cancel token.
    async fn make_ctx_with_config(file_json: &str) -> (Arc<BridgeContext>, TempDir, String) {
        let tmp = TempDir::new().expect("create tempdir");
        let cfg_path = tmp.path().join("config.json");
        tokio::fs::write(&cfg_path, file_json)
            .await
            .expect("write config");
        let cfg_str = cfg_path.to_string_lossy().into_owned();
        let cli = Cli {
            config: Some(cfg_str.clone()),
            state_file: Some(tmp.path().join("state.json").to_string_lossy().into_owned()),
            ..Cli::default()
        };
        let (ctx, _mqtt_rx, _save_rx, _refresh_rx) =
            BridgeContext::new(&cli, tokio_util::sync::CancellationToken::new())
                .await
                .expect("new context");
        (ctx, tmp, cfg_str)
    }

    #[tokio::test]
    async fn set_config_patches_file_and_reports_change() {
        let (ctx, _tmp, cfg) =
            make_ctx_with_config(r#"{"mqtt_event_topic":"{root}/event/{type}/{id}"}"#).await;

        let resp = ctx
            .set_config(None, Some("{root}/ev/{id}".into()), None, None, None, None, false)
            .await
            .expect("set_config ok");

        assert_eq!(resp.status, crate::types::Status::Ok);
        assert_eq!(
            resp.extra.get("reconfigure_required"),
            Some(&Value::Bool(true))
        );
        let changed = resp
            .extra
            .get("changed")
            .and_then(Value::as_object)
            .expect("changed object");
        assert!(changed.contains_key("mqtt_event_topic"));

        // The new value is committed to disk.
        let content = tokio::fs::read_to_string(&cfg).await.expect("read back");
        let written: Cli = serde_json::from_str(&content).expect("parse back");
        assert_eq!(written.mqtt_event_topic.as_deref(), Some("{root}/ev/{id}"));
    }

    #[tokio::test]
    async fn set_config_noop_when_value_unchanged() {
        let (ctx, _tmp, cfg) =
            make_ctx_with_config(r#"{"mqtt_event_topic":"{root}/ev/{id}"}"#).await;
        let before = tokio::fs::read_to_string(&cfg).await.unwrap();

        let resp = ctx
            .set_config(None, Some("{root}/ev/{id}".into()), None, None, None, None, false)
            .await
            .expect("ok");

        assert_eq!(
            resp.extra.get("reconfigure_required"),
            Some(&Value::Bool(false))
        );
        assert!(
            resp.extra
                .get("changed")
                .and_then(Value::as_object)
                .unwrap()
                .is_empty()
        );
        // No effective change → file is left byte-for-byte untouched.
        assert_eq!(before, tokio::fs::read_to_string(&cfg).await.unwrap());
    }

    #[tokio::test]
    async fn set_config_rejects_wildcards() {
        let (ctx, _tmp, _cfg) = make_ctx_with_config("{}").await;
        let err = ctx
            .set_config(Some("{root}/cmd/#".into()), None, None, None, None, None, false)
            .await
            .unwrap_err();
        assert!(matches!(err, BridgeError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn set_config_requires_a_config_file() {
        let tmp = TempDir::new().unwrap();
        let cli = Cli {
            config: None,
            state_file: Some(tmp.path().join("state.json").to_string_lossy().into_owned()),
            ..Cli::default()
        };
        let (ctx, _m, _s, _r) =
            BridgeContext::new(&cli, tokio_util::sync::CancellationToken::new())
                .await
                .unwrap();
        let err = ctx
            .set_config(None, Some("{root}/ev/{id}".into()), None, None, None, None, false)
            .await
            .unwrap_err();
        assert!(matches!(err, BridgeError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn set_config_apply_triggers_reconfigure() {
        let (ctx, _tmp, _cfg) =
            make_ctx_with_config(r#"{"mqtt_event_topic":"{root}/event/{type}/{id}"}"#).await;
        assert!(!ctx.cancel.is_cancelled());

        let resp = ctx
            .set_config(None, Some("{root}/ev/{id}".into()), None, None, None, None, true)
            .await
            .expect("ok");

        assert_eq!(
            resp.extra.get("reconfigure"),
            Some(&Value::String("triggered".into()))
        );
        assert!(
            ctx.cancel.is_cancelled(),
            "apply=true with a real change must trip reconfigure"
        );
    }
}
}
