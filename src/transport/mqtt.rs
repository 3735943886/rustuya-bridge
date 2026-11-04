//! The bridge's MQTT side: the connection and its event loop, the single-instance
//! check, and the short-lived clients that sweep retained messages. Everything that
//! talks to `rumqttc` lives here.

use anyhow::Result;
use log::{debug, error, info, warn};
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::bridge::{BridgeContext, unix_millis};
use crate::config::Cli;
use crate::outbound::{DeltaKind, Outbound};
use crate::template::{compile_topic_regex, match_topic, render_template, tpl_to_wildcard};
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
const MQTT_SEND_TIMEOUT: Duration = Duration::from_millis(500);
const MQTT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
// One log line per this many dropped messages.
const DROP_LOG_INTERVAL: u64 = 100;

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
    ///   retain=true  → QoS1 : state snapshots (`{type}=state`) and device error/connect
    ///                         status (both cache mode only), bridge-config sentinel.
    ///                         Durable state a re-subscriber must recover, so
    ///                         PUBACK-acknowledged delivery is worth it.
    ///   retain=false → QoS0 : active/passive deltas, API/command responses, scanner
    ///                         results, and device status in pass-through mode. Live &
    ///                         transient — a drop self-heals via the next push or the
    ///                         retained snapshot.
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
pub(crate) struct TopicVars<'a> {
    pub(crate) id: &'a str,
    pub(crate) name: Option<&'a str>,
    pub(crate) cid: Option<&'a str>,
    pub(crate) level: Option<&'a str>,
    pub(crate) event_type: Option<&'a str>,
    pub(crate) action: Option<&'a str>,
    pub(crate) dp: Option<&'a str>,
    pub(crate) val: Option<&'a Value>,
    pub(crate) dps_str: Option<&'a str>,
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

/// The bridge's MQTT side: how its output is laid out on the broker, from the
/// configuration, and the state of the connection that carries it.
pub struct MqttTransport {
    /// To the connection loop; `None` without a broker.
    pub tx: Option<mpsc::Sender<Option<MqttMessage>>>,
    pub(crate) shutdown: tokio_util::sync::CancellationToken,
    pub scavenger_tx:
        Arc<tokio::sync::Mutex<Option<mpsc::UnboundedSender<Vec<crate::types::ScavengerTarget>>>>>,
    /// Count of MQTT messages dropped because the outbound channel stayed
    /// full past `try_send_mqtt`'s timeout. Exposed in `status` responses.
    pub drop_count: AtomicU64,
    /// When set, the outbound publish loop forces `retain=false` on every
    /// message regardless of its `MqttMessage::retain` flag. Latched on by the
    /// `reconfigure` action so device events arriving during/after the
    /// old-scheme retained purge can't recreate orphaned retained snapshots.
    /// Never cleared — the process exits shortly after it's set.
    pub retain_suppressed: AtomicBool,
    pub root_topic: String,
    pub event_topic: String,
    pub retain: bool,
    pub message_topic: Option<String>,
    pub payload_template: Option<String>,
    pub scanner_topic: Option<String>,
    /// Identifiers the event topic and payload templates reference: the scavenger
    /// can only find a retained message through one of them.
    pub event_identifiers: IdentifierSet,
    pub seed_state_wildcard: Option<String>,
    pub seed_state_regex: Option<Regex>,
}

impl MqttTransport {
    pub(crate) fn from_cli(cli: &Cli, tx: mpsc::Sender<Option<MqttMessage>>) -> Self {
        let (_, mqtt_event_topic) = cli.mqtt_topics();
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

        Self {
            tx: cli.mqtt_broker.is_some().then_some(tx),
            shutdown: tokio_util::sync::CancellationToken::new(),
            scavenger_tx: Arc::new(tokio::sync::Mutex::new(None)),
            drop_count: AtomicU64::new(0),
            retain_suppressed: AtomicBool::new(false),
            root_topic: root_topic_str,
            event_topic: mqtt_event_topic,
            retain: effective_retain,
            message_topic: cli.mqtt_message_topic.clone(),
            payload_template: cli.mqtt_payload_template.clone(),
            scanner_topic: cli.mqtt_scanner_topic.clone(),
            event_identifiers,
            seed_state_wildcard,
            seed_state_regex,
        }
    }
}

/// The seed phase (cache mode only). After the first `ConnAck` the bridge subscribes
/// to its own retained state topic to recover prior-session snapshots, then
/// unsubscribes and calls `on_seed_complete()` once either the broker quiets
/// (`SEED_QUIET` since the last retained message) or `SEED_HARD_CAP` fires.
struct SeedWindow {
    deadline: Instant,
    last_retained: Option<Instant>,
}

impl SeedWindow {
    const SEED_HARD_CAP: Duration = Duration::from_secs(5);
    const SEED_QUIET: Duration = Duration::from_millis(200);

    fn opening_now() -> Self {
        Self {
            deadline: Instant::now() + Self::SEED_HARD_CAP,
            last_retained: None,
        }
    }

    fn wakeup(&self) -> Instant {
        self.last_retained.map_or(self.deadline, |last| {
            (last + Self::SEED_QUIET).min(self.deadline)
        })
    }
}

/// Waits for `deadline`, or forever if there is none: an arm of `select!` that is
/// only live while its timer is set.
async fn sleep_until_set(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => futures_util::future::pending().await,
    }
}

/// The session on a config-topic payload, if it belongs to another instance.
fn foreign_session(payload: &str, own: Option<&str>) -> Option<String> {
    let value = serde_json::from_str::<Value>(payload).ok()?;
    let session = value.get("session_id")?.as_str()?;
    (Some(session) != own).then(|| session.to_owned())
}

/// The requests in a message on the command topic. A malformed request, on its own
/// or as an element of an array, is dropped.
fn parse_commands(
    topic: &str,
    payload: &str,
    template: &str,
    template_re: Option<&Regex>,
) -> Vec<BridgeRequest> {
    let vars = match_topic(topic, template, template_re).unwrap_or_default();
    match crate::payload::parse_mqtt_payload(payload, &vars) {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|item| serde_json::from_value(item).ok())
            .collect(),
        other => serde_json::from_value(other)
            .map(|req| vec![req])
            .unwrap_or_default(),
    }
}

impl BridgeContext {
    /// Sends an event to the broker. Rendering happens here on the sender's task,
    /// so the loop only moves finished messages.
    pub(crate) async fn send_over_mqtt(&self, event: &Outbound) {
        if self.mqtt.tx.is_none() {
            return;
        }
        for msg in self.render(event) {
            self.try_send_mqtt(Some(msg)).await;
        }
    }

    /// The messages an event becomes under the configured templates.
    fn render(&self, event: &Outbound) -> Vec<MqttMessage> {
        match event {
            Outbound::DeviceDelta {
                id,
                name,
                cid,
                dps,
                kind,
            } => {
                let event_type = match kind {
                    DeltaKind::Active => "active",
                    DeltaKind::Passive => "passive",
                };
                self.render_event(id, name.as_deref(), cid.as_deref(), dps, event_type, false)
            }
            Outbound::DeviceState {
                id,
                name,
                cid,
                dps,
                changed,
            } => self.render_state(id, name.as_deref(), cid.as_deref(), dps, changed.as_deref()),
            Outbound::DeviceStatus {
                id,
                name,
                cid,
                payload,
            } => vec![self.render_message(
                id,
                name.as_deref(),
                cid.as_deref(),
                "error",
                payload.clone(),
                self.mqtt.retain,
            )],
            Outbound::Response { response, name } => {
                self.render_response(response, name.as_deref())
            }
            Outbound::ScanResult(payload) => vec![self.render_scan(payload)],
            Outbound::BridgeInfo => vec![MqttMessage {
                topic: crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt.root_topic),
                payload: Self::build_bridge_config_payload(&self.cli),
                retain: true,
            }],
        }
    }

    fn render_event(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: &Value,
        event_type: &str,
        retain: bool,
    ) -> Vec<MqttMessage> {
        self.generate_device_templates(id, name, cid, dps, event_type)
            .into_iter()
            .map(|(topic, payload)| MqttMessage {
                topic,
                payload,
                retain,
            })
            .collect()
    }

    /// A device's snapshot on the `{type}=state` topic. With one topic per DP,
    /// `changed` narrows it so an event that changed DP 1 doesn't republish a
    /// still-current DP 2's retained. With one topic per device the filter means
    /// nothing: a single message always carries the full snapshot.
    ///
    /// Applies the identifier-set retain gate: if the template references
    /// `{name}`/`{cid}` and this device lacks them, retain is stripped so the
    /// scavenger can still reach the message later.
    fn render_state(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        dps: &Value,
        changed: Option<&[String]>,
    ) -> Vec<MqttMessage> {
        let tpl = &self.mqtt.event_topic;
        let one_topic_per_dp = tpl.contains("{dp}") || tpl.contains("{value}");
        let narrowed = match (one_topic_per_dp, changed, dps.as_object()) {
            (true, Some(keys), Some(all)) => {
                let filtered: serde_json::Map<String, Value> = keys
                    .iter()
                    .filter_map(|k| all.get(k).map(|v| (k.clone(), v.clone())))
                    .collect();
                if filtered.is_empty() {
                    return Vec::new();
                }
                Some(Value::Object(filtered))
            }
            _ => None,
        };

        let retain = self.mqtt.event_identifiers.satisfied_by(name, cid);
        if !retain {
            debug!(
                "Stripping retain for snapshot from '{id}': no template identifier resolvable (name={name:?}, cid={cid:?})"
            );
        }
        self.render_event(
            id,
            name,
            cid,
            narrowed.as_ref().unwrap_or(dps),
            "state",
            retain,
        )
    }

    /// A response goes out on the message topic, under the device's name when that
    /// topic uses one. The name then also joins the payload; without it the payload
    /// stays as it was.
    fn render_response(&self, response: &ApiResponse, name: Option<&str>) -> Vec<MqttMessage> {
        let level = match response.status {
            crate::types::Status::Ok => "response",
            crate::types::Status::Error => "error",
        };
        let id = response.id.as_deref().unwrap_or("bridge");
        let name = if id == "bridge" {
            Some("bridge")
        } else if self
            .mqtt
            .message_topic
            .as_deref()
            .is_some_and(|topic| topic.contains("{name}"))
        {
            name
        } else {
            None
        };
        match serde_json::to_value(response) {
            Ok(payload) => vec![self.render_message(id, name, None, level, payload, false)],
            Err(e) => {
                error!("Failed to serialize ApiResponse: {e}");
                Vec::new()
            }
        }
    }

    /// A message on the message topic for a specific device.
    fn render_message(
        &self,
        id: &str,
        name: Option<&str>,
        cid: Option<&str>,
        level: &str,
        mut payload: Value,
        retain: bool,
    ) -> MqttMessage {
        {
            let topic = self.replace_vars(
                self.mqtt.message_topic.as_deref().unwrap_or_default(),
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
                let inner = std::mem::replace(&mut payload, Value::Object(serde_json::Map::new()));
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
            MqttMessage {
                topic,
                payload: payload.to_string(),
                retain,
            }
        }
    }

    fn render_scan(&self, payload: &Value) -> MqttMessage {
        let topic = self.mqtt.scanner_topic.as_deref().map_or_else(
            || {
                self.replace_vars(
                    self.mqtt.message_topic.as_deref().unwrap_or_default(),
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
        MqttMessage {
            topic,
            payload: payload.to_string(),
            retain: false,
        }
    }

    /// Generates MQTT topics and payloads based on templates
    #[allow(
        clippy::literal_string_with_formatting_args,
        reason = "MQTT topic template uses literal `{dp}`/`{value}` placeholders, not format args"
    )]
    pub(crate) fn generate_device_templates(
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
        let tpl = &self.mqtt.event_topic;

        if tpl.contains("{dp}") || tpl.contains("{value}") {
            if let Some(dps_obj) = dps.as_object() {
                for (dp, val) in dps_obj {
                    let topic = replace_vars_local(tpl, Some(dp), Some(val));
                    let p_tpl = self.mqtt.payload_template.as_deref().unwrap_or("{value}");
                    let payload = replace_vars_local(p_tpl, Some(dp), Some(val));
                    templates.push((topic, payload));
                }
            }
        } else {
            let topic = replace_vars_local(tpl, None, None);
            let p_tpl = self.mqtt.payload_template.as_deref().unwrap_or("{value}");
            let payload = replace_vars_local(p_tpl, None, None);
            templates.push((topic, payload));
        }

        templates
    }

    /// Helper to replace template variables in a string
    pub(crate) fn replace_vars(&self, template: &str, vars: &TopicVars<'_>) -> String {
        use std::fmt::Write;

        render_template(template, |key, out| match key {
            "root" => {
                out.push_str(&self.mqtt.root_topic);
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

    /// Match an incoming topic against the seed wildcard regex and return
    /// `(id, optional_dp)`. Returns `None` if not in cache-mode seed-enabled mode or
    /// the topic doesn't match.
    pub(crate) fn match_seed_topic(&self, topic: &str) -> Option<(String, Option<String>)> {
        let regex = self.mqtt.seed_state_regex.as_ref()?;
        let captures = regex.captures(topic)?;
        let id = captures.name("id")?.as_str().to_string();
        let dp = captures.name("dp").map(|m| m.as_str().to_string());
        Some((id, dp))
    }

    /// Serializes the running config for the `{root}/bridge/config` topic,
    /// injecting the bridge `version` (a build constant, not a `Cli` field, so
    /// it never round-trips through the config file) and `devices_updated_at`
    /// (last time the device registry changed — see `spawn_state_saver`, which
    /// republishes this payload on the same debounce that flushes a registry
    /// mutation to disk). Credentials are stripped by
    /// `#[serde(skip_serializing)]` on the `Cli` fields themselves.
    pub(crate) fn build_bridge_config_payload(cli: &crate::config::Cli) -> String {
        let mut v =
            serde_json::to_value(cli).unwrap_or_else(|_| Value::Object(serde_json::Map::new()));
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "version".to_string(),
                Value::String(env!("CARGO_PKG_VERSION").to_string()),
            );
            obj.insert(
                "devices_updated_at".to_string(),
                Value::from(u64::try_from(unix_millis()).unwrap_or(u64::MAX)),
            );
        }
        serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string())
    }

    pub(crate) async fn try_send_mqtt(&self, msg: Option<MqttMessage>) {
        if let Some(tx) = &self.mqtt.tx {
            match tokio::time::timeout(MQTT_SEND_TIMEOUT, tx.send(msg)).await {
                Ok(Ok(())) => {}
                result => {
                    let n = self.mqtt.drop_count.fetch_add(1, Ordering::Relaxed) + 1;
                    // Under overload this runs for every message; logging each one
                    // would add to the load, and through Python would wait on the GIL.
                    if n == 1 || n.is_multiple_of(DROP_LOG_INTERVAL) {
                        error!("MQTT message not delivered ({result:?}; cumulative drops: {n})");
                    }
                }
            }
        }
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
        let sub_topic = tpl_to_wildcard(&mqtt_command_topic, &self.mqtt.root_topic);
        let command_topic_re = compile_topic_regex(&mqtt_command_topic);
        let config_topic =
            crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt.root_topic);

        let seed_wildcard = self.mqtt.seed_state_wildcard.clone();

        // How long after a reconnect to watch for another instance's sentinel
        // before restoring ours. Same window `check_existing_instance` observes.
        const SENTINEL_RESTORE_DELAY: Duration = Duration::from_millis(500);

        let request_tx = self.spawn_request_driver();

        let handle = tokio::spawn(async move {
            let mut outstanding_publishes = 0usize;
            let mut retry_delay = INITIAL_RETRY_DELAY_SECS;
            let mut next_retry: Option<Instant> = None;
            let mut seed: Option<SeedWindow> = None;
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
                    let (qos, retain) = self.publish_params(msg);
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
                    () = sleep_until_set(next_retry) => {
                        next_retry = None;
                    }
                    () = sleep_until_set(seed.as_ref().map(SeedWindow::wakeup)) => {
                        seed = None;
                        if let Some(w) = &seed_wildcard {
                            let _ = client.unsubscribe(w).await;
                        }
                        self.on_seed_complete().await;
                    }
                    () = sleep_until_set(sentinel_restore_at) => {
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
                                    && seed.is_none()
                                {
                                    match client.subscribe(w, rumqttc::QoS::AtLeastOnce).await {
                                        Ok(()) => {
                                            info!("Seeding cache from broker via '{w}'");
                                            seed = Some(SeedWindow::opening_now());
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
                                if let Some(window) = &mut seed && p.retain
                                    && let Some((id, dp)) = self.match_seed_topic(&p.topic)
                                {
                                    if self.seed_cache(&id, dp.as_deref(), &payload) {
                                        window.last_retained = Some(Instant::now());
                                    }
                                    continue;
                                }

                                if p.topic == config_topic {
                                    if let Some(sid) = foreign_session(&payload, self.cli.session_id.as_deref()) {
                                        error!("Another bridge instance took over (session_id: {sid}). Shutting down.");
                                        self.cancel.cancel();
                                        return;
                                    }
                                    continue;
                                }

                                let requests = parse_commands(&p.topic, &payload, &mqtt_command_topic, command_topic_re.as_ref());
                                for req in requests {
                                    self.submit_request(&request_tx, req).await;
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
                            () = self.mqtt.shutdown.cancelled() => None,
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
                                self.flush_and_disconnect(
                                    &client,
                                    &mut eventloop,
                                    &mut mqtt_tx_receiver,
                                    pending.take(),
                                    &config_topic,
                                    outstanding_publishes,
                                )
                                .await;
                                return;
                        }
                    }
                }
            }
        });

        Ok(Some(handle))
    }

    /// QoS and retain flag for a message, by its kind: retained state is
    /// acknowledged, live traffic is not. A retained message keeps its QoS while
    /// `retain_suppressed` drops the flag.
    fn publish_params(&self, msg: &MqttMessage) -> (rumqttc::QoS, bool) {
        let qos = if msg.retain {
            rumqttc::QoS::AtLeastOnce
        } else {
            rumqttc::QoS::AtMostOnce
        };
        (
            qos,
            msg.retain && !self.mqtt.retain_suppressed.load(Ordering::Relaxed),
        )
    }

    /// Takes a retained snapshot from the seed phase into the cache, as backfill.
    /// Returns whether it held any DPS.
    fn seed_cache(&self, id: &str, dp: Option<&str>, payload: &str) -> bool {
        let dps =
            crate::payload::parse_seed_dps(payload, dp, self.mqtt.payload_template.as_deref());
        match (dps, &self.cache) {
            (Some(dps), Some(cache)) => {
                cache.fill_missing(id, &dps);
                true
            }
            _ => false,
        }
    }

    /// The end of the connection: publish everything already accepted, then clear
    /// the sentinel, wait for the broker to acknowledge it all, and disconnect.
    async fn flush_and_disconnect(
        &self,
        client: &rumqttc::AsyncClient,
        eventloop: &mut rumqttc::EventLoop,
        receiver: &mut mpsc::Receiver<Option<MqttMessage>>,
        pending: Option<MqttMessage>,
        config_topic: &str,
        mut outstanding_publishes: usize,
    ) {
        debug!("MQTT shutdown signal received, clearing and disconnecting...");

        // Queue the clear while polling, then wait for *all* queued
        // QoS1 publishes, including the clear, to be acknowledged.
        // A prior publish's ACK must never stand in for this clear.
        // Stop producers and preserve FIFO for all accepted messages.
        // The config clear must be the final retained publish.
        receiver.close();
        let mut outbound = std::collections::VecDeque::new();
        if let Some(msg) = pending {
            outbound.push_back(msg);
        }
        while let Ok(msg) = receiver.try_recv() {
            if let Some(msg) = msg {
                outbound.push_back(msg);
            }
        }
        let flush = async {
            let mut clear_queued = false;
            loop {
                while let Some(msg) = outbound.front() {
                    let (qos, retain) = self.publish_params(msg);
                    if client
                        .try_publish(&msg.topic, qos, retain, msg.payload.clone())
                        .is_err()
                    {
                        break;
                    }
                    if msg.retain {
                        outstanding_publishes += 1;
                    }
                    outbound.pop_front();
                }
                if outbound.is_empty()
                    && !clear_queued
                    && client
                        .try_publish(config_topic, rumqttc::QoS::AtLeastOnce, true, "")
                        .is_ok()
                {
                    outstanding_publishes += 1;
                    clear_queued = true;
                }
                if clear_queued && outstanding_publishes == 0 {
                    return Ok::<(), rumqttc::ConnectionError>(());
                }
                if let rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)) =
                    eventloop.poll().await?
                {
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
            self.mqtt
                .message_topic
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
            crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt.root_topic);
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
                topic: crate::config::BRIDGE_CONFIG_TOPIC.replace("{root}", &self.mqtt.root_topic),
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
            let mut lock = self.mqtt.scavenger_tx.lock().await;
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
            self.mqtt.event_topic.clone(),
            self.mqtt.message_topic.clone().unwrap_or_default(),
        ];

        let root_topic = self.mqtt.root_topic.clone();

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
        let scav_tx = Arc::clone(&self.mqtt.scavenger_tx);

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

        let root_topic = self.mqtt.root_topic.clone();
        let mut subs = HashSet::new();
        for tpl in [
            Some(self.mqtt.event_topic.as_str()),
            self.mqtt.message_topic.as_deref(),
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
        let s = IdentifierSet {
            id: true,
            ..Default::default()
        };
        assert!(s.satisfied_by(None, None));
        assert!(s.satisfied_by(Some(""), None));
    }

    #[test]
    fn identifier_set_satisfied_by_name_requires_nonempty() {
        let s = IdentifierSet {
            name: true,
            ..Default::default()
        };
        assert!(!s.satisfied_by(None, None));
        assert!(!s.satisfied_by(Some(""), None));
        assert!(s.satisfied_by(Some("kitchen"), None));
    }

    #[test]
    fn identifier_set_satisfied_by_cid_requires_nonempty() {
        let s = IdentifierSet {
            cid: true,
            ..Default::default()
        };
        assert!(!s.satisfied_by(None, None));
        assert!(s.satisfied_by(None, Some("sub-1")));
    }
    #[test]
    fn foreign_session_ignores_our_own_and_anything_without_one() {
        assert_eq!(
            foreign_session(r#"{"session_id":"other"}"#, Some("ours")),
            Some("other".to_string())
        );
        for payload in [
            r#"{"session_id":"ours"}"#,
            r#"{"session_id":1}"#,
            "{}",
            "",
            "not json",
        ] {
            assert_eq!(foreign_session(payload, Some("ours")), None, "{payload}");
        }
    }

    #[test]
    fn parse_commands_keeps_valid_requests_and_merges_topic_vars() {
        let template = "root/command/{action}/{id}";
        let re = compile_topic_regex(template);
        let parse = |topic, payload| parse_commands(topic, payload, template, re.as_ref());

        let from_topic = parse("root/command/status/x", "{}");
        assert!(matches!(from_topic[..], [BridgeRequest::Status { .. }]));

        let array = parse(
            "other",
            r#"[{"action":"status"},{"action":"no-such"},42,{"action":"status"}]"#,
        );
        assert_eq!(array.len(), 2);

        for payload in ["not json", "", "42", r#"{"action":"no-such"}"#] {
            assert!(parse("other", payload).is_empty(), "{payload}");
        }
    }

    #[test]
    fn seed_window_closes_on_quiet_but_never_past_its_cap() {
        let mut window = SeedWindow::opening_now();
        let cap = window.deadline;
        assert_eq!(window.wakeup(), cap);

        let now = Instant::now();
        window.last_retained = Some(now);
        assert_eq!(window.wakeup(), now + SeedWindow::SEED_QUIET);

        window.last_retained = Some(cap);
        assert_eq!(window.wakeup(), cap);
    }

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
