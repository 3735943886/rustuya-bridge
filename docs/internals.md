# Rustuya Bridge — Internals & Operator Notes

> **For advanced users.** The [README](../README.md) covers what the bridge
> does. This document explains *how* — the subtle behavior that surfaces only
> when you customize topics, deal with retained messages, swap MQTT brokers,
> or wonder what actually happens when you call `add` twice. Read selectively;
> you don't need any of this to use the bridge.
>
> Source references use function names rather than line numbers, since lines
> drift across refactors. Open the linked file and search by name.

---

## 1. Device lifecycle: what `add` actually does

### 1.1 Direct vs sub-device branches

The `add` action takes two completely different paths inside [add_device](../src/bridge.rs):

| You sent...                                  | Branch taken                                                | What lands where                                                  |
| -------------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------------------------- |
| `id` + `key` (+ optional `ip`/`ver`)         | **Direct device** — `DeviceBuilder::new(...).build()`       | `state.instances[id]` gets a live `Device` connection             |
| `id` + `cid` + `parent_id`                   | **Sub-device** — no instance, just a routing entry          | `state.cid_map[(parent_id, cid)] = id`; `instances[id]` *removed* |
| `id` + `cid` + `parent_id` + `key` (all four)| **Sub-device wins** — sub branch is checked first           | same as sub-device row; `key` is stored in config but unused      |
| neither key nor (cid+parent_id)              | `BridgeError::InvalidRequest` — must have one or the other  | nothing                                                           |

The split matters because **only direct devices appear in the unified listener
stream** (see §2). Sub-devices are pure routing metadata — events arrive
addressed to the *parent's* device ID, and the bridge rewrites them using
`cid_map`. If you accidentally provide `key` *and* `cid+parent_id`, the
device registers as sub-only — the key won't open a connection.

### 1.2 Calling `add` twice for the same `id`

This is a *replace*, not an error. The interesting part is the cleanup order in
[add_device](../src/bridge.rs):

1. **Snapshot the old config** (name, cid, parent_id, key, ip, version) before
   mutating.
2. **Strip the old `name_map` entry** — `name_map[name]` is a `Vec<String>`, so
   the device's id is `retain`'d out and the bucket dropped if empty. *Other
   devices sharing that name are untouched.*
3. **Strip the old `cid_map` entry** — `(old_parent, old_cid)` is removed.
4. Insert the new device (direct or sub branch). For a direct device, the
   pre-existing connection is **kept** if `key`/`ip`/`version` are all
   unchanged — see the `unchanged` check in `add_device`.
5. Overwrite `configs[id]`, push id into the new `name_map[name]` bucket.
6. `request_save()` (debounced — §6) and `request_refresh()` only when the
   listener topology actually changed (`listener_changed` flag — see §2).

Three non-obvious consequences:

- **Direct → sub conversion drops the live connection.** If you re-`add` an
  existing direct device with `cid`+`parent_id` instead of `key`, the device's
  entry in `instances` is *removed* and the listener refreshes.
- **Sub → direct conversion creates a new connection** and refreshes the
  listener. Inverse of the above.
- **Re-adding with identical fields is a true no-op for the listener.** The
  TCP connection is preserved and no refresh is triggered — only metadata
  bookkeeping runs. Use this freely for scripted "reapply config" flows.
- **Name move leaves no trace at the old name.** Re-`add` with a different
  `name` and the device disappears from the old name bucket *before* the
  new one is created. Lookups by the old name will not find it.

### 1.3 Name collision semantics

`name_map` deliberately holds a `Vec<String>` per name. Two devices can share a
name, and any `set`/`get`/`remove` by name will fan out to *all* matching
devices ([find_device_ids](../src/bridge.rs)). There is no disambiguation
prompt — but when a name-based lookup resolves to more than one device, the
response carries `matched: N` and `targets: [...]` in `extra`, and the
successful-set/get response suppression is bypassed so the caller actually
sees the fan-out:

```json
{
  "status": "ok",
  "action": "set",
  "id": "ebabc...,fbcde...",
  "matched": 2,
  "targets": ["ebabc...", "fbcde..."]
}
```

**`matched` counts name-matched devices only, not cascaded sub-devices.**
If you `remove` a single gateway by name and the bridge cascades to its
3 sub-devices, the `id` field shows 4 ids but `matched` is omitted (one
name match, no fan-out). This is on purpose: cascading is the bridge's
choice, not a result of the name lookup, so it doesn't represent
ambiguity in the caller's selector.

> **Operational caveat — partial failure in name fan-out.** The handler
> walks targets sequentially via `execute_per_target` and returns on the
> first device-level error. So if `set` matches 3 devices by name and
> device #2 times out, device #3 is never tried, the response is an
> `error` with no `matched` annotation, and the caller can't easily tell
> that part of the fleet was set and part wasn't. If you control fleets
> by name and need atomicity, query `status` after a name-set to verify
> all targets actually applied.

If a payload provides both `id` and `name`, `id` wins and `name` is silently
ignored — see the early-return in `find_device_ids`. A debug log is
emitted in that case so operators tracking down a confused selector can
spot it.

### 1.4 Device status reporting

[determine_device_status](../src/bridge.rs) is the source of the
`status` field returned by `status` queries:

| Config shape                                              | Reported status                              |
| --------------------------------------------------------- | -------------------------------------------- |
| Sub-device (cid set), parent_id set, parent registered    | `"subdevice"`                                |
| Sub-device (cid set), parent_id set, parent not registered| `"no parent"`                                |
| Has `cid` but no `parent_id` (only via hand-edited state) | `"invalid subdevice"`                        |
| Direct, in `instances`, no error history                  | `"online"`                                   |
| Direct, in `instances`, errored before                    | the raw error code as string (e.g. `"905"`)  |
| Direct, *not* in `instances` (rare; broken state)         | `"offline"`                                  |

`"invalid subdevice"` can only appear if you hand-edit the state file or
load one from a buggy producer — `add_device` rejects `cid` without
`parent_id`. If you see it, fix the config.

Error codes shown for direct devices are inherited from
[tinytuya](https://github.com/jasonacox/tinytuya) (rustuya keeps the
same numbering for compatibility) and live in the `0` and `900..=914`
range. The ones you'll most likely see in production:

| Code | Constant         | Meaning                                |
| ---- | ---------------- | -------------------------------------- |
| 901  | `ERR_CONNECT`    | Network error — couldn't reach device  |
| 902  | `ERR_TIMEOUT`    | Device didn't respond within timeout   |
| 905  | `ERR_OFFLINE`    | Device unreachable                     |
| 906  | `ERR_STATE`      | Unknown state (e.g. listener lagged)   |
| 914  | `ERR_KEY_OR_VER` | Wrong `key` or wrong protocol `version`|

`last_error_code` is **stored in memory only** — `#[serde(skip)]` in
`DeviceConfig` ([config.rs](../src/config.rs), field `last_error_code`) means
it is not persisted to the state file. After a restart, every direct device looks
`"online"` until it hits its next error.

---

## 2. The unified listener: one stream, many devices

### 2.1 Architecture

Tuya's local protocol gives each device its own long-lived TCP connection.
Rustuya wraps each connection in an actor with its own background task
(spawned eagerly inside `DeviceBuilder::build()` on rustuya's dedicated
runtime), and exposes per-device events through a broadcast channel inside
`DeviceInner`. The actor stays alive — performing reconnects, heartbeats,
IP rediscovery — until *every* `Device` clone is dropped, at which point
`DeviceInner::Drop` fires its `CancellationToken` and the task exits.

The bridge takes that per-device stream and multiplexes everything through
a single `rustuya::device::unified_listener(Vec<Device>)`, which internally
calls `device.listener()` on each device and merges via `select_all`:

```rust
let mut stream = rustuya::device::unified_listener(instances.clone());
loop {
    tokio::select! {
        () = cancel.cancelled() => return,
        res = timeout(LISTENER_TIMEOUT_SECS, stream.next()) => { ... }
        _ = refresh_rx.recv() => break,  // ← topology changed
    }
}
```

### 2.2 The refresh signal — why drop-and-rebuild?

When devices are added or removed, [request_refresh](../src/bridge.rs)
pokes a 1-capacity channel. The listener loop sees `refresh_rx.recv()`
resolve, breaks the inner loop, drops the entire stream, re-reads
`state.instances`, and starts a fresh `unified_listener`.

This is coarse because `unified_listener` is an immutable consumer — it
takes the `Vec<Device>` by value and uses `select_all` internally with no
public add/remove method. The only way to change the set is to drop the
stream and build a new one.

**How does dropping the stream actually close TCP connections?** It's an
Arc-refcount cascade:

1. `Device` is `#[derive(Clone)]` and holds `inner: Arc<DeviceInner>`.
   Every Device clone bumps the Arc.
2. `device.listener()` captures *another* Device clone into the returned
   `async_stream` future, so the stream itself holds one strong ref per
   device.
3. The bridge therefore has **two** strong refs per device while the
   listener is alive: one in `state.instances`, one inside the listener
   stream.
4. On refresh: stream dropped → per-device async_stream futures dropped →
   their captured Device clones dropped → -1 ref each.
5. **But the TCP task survives** unless `state.instances` also lost its
   reference. The bridge mutates `state.instances` in the calling action
   (`add_device` insert/overwrite, `remove_device` remove) *before*
   triggering refresh — so when refresh fires and the stream drops, the
   final strong ref is gone, `DeviceInner::Drop` fires its
   `cancel_token`, and the underlying TCP task exits cleanly.

The rustuya library has an explicit regression test for this contract
(`unified_listener_cycle_does_not_leak_inner_arcs`) that constructs and
drops 100 `unified_listener`s in a row and asserts `Arc::strong_count`
doesn't drift.

Implication of the design — **a flood of distinct `add`/`remove` calls
causes a flood of TCP reconnections**. Mitigations:

- The 1-capacity `refresh_tx` channel coalesces bursts somewhat — if a
  refresh is already queued, `try_send` is a no-op.
- `add_device` skips the refresh entirely when re-adding a direct device
  with identical `key`/`ip`/`version` (§1.2). Idempotent reapply is free.
- `remove_device` skips the refresh when only sub-devices were removed —
  subs aren't in `instances` so they don't appear in the listener.
- Sub-device adds that don't change shape don't refresh either.

So the worst case is scripted bulk *distinct* direct adds; everything
else is cheap.

### 2.3 The 300-second timeout

[LISTENER_TIMEOUT_SECS](../src/bridge.rs) wraps `stream.next()` in a
5-minute timeout. On expiry the bridge logs an `info!` and goes back to
the `select!` — it does *not* rebuild the listener. So if all your
devices fall silent for >5 minutes, you get a friendly log line, nothing
else.

Per-device reconnect, heartbeats, and IP rediscovery all live inside
rustuya's `DeviceInner` actor — the bridge's listener loop is purely a
consumer of the multiplexed broadcast. So the 300s timeout is a
liveness *signal*, not a recovery mechanism: rustuya is already trying
to reconnect underneath you regardless.

### 2.4 Empty-instance idle

If `state.instances` is empty (you only have sub-devices, or you cleared
everything), the listener doesn't try to construct a stream at all (the
empty-Vec early-return in `spawn_device_listener`) — it parks on `refresh_rx`
and waits for someone to add a direct device. This is also the state right
after `clear`.

---

## 3. Template engine: from `{root}/...` to bytes on the wire

### 3.1 The two-pass model

Templates are processed by [render_template](../src/bridge.rs), a tiny
callback-driven walker. For each `{key}` it found, it asks the substitute fn
"do you want this?" — `true` consumes the placeholder, `false` leaves the
literal `{key}` in the output. There is no shared variable table — each call
site passes its own `TopicVars` struct, and unknown keys simply pass through.

Three derived operations use this:

- [replace_vars](../src/bridge.rs) — actual substitution for publish.
- [tpl_to_wildcard](../src/bridge.rs) — replaces every known key with
  `+` for MQTT SUBSCRIBE.
- [compile_topic_regex](../src/bridge.rs) — turns the template into
  `^pattern$` with named captures, used to *extract* values from received
  topics.

The set of "known keys" for wildcard/regex purposes is hardcoded:

```rust
const TOPIC_WILDCARD_KEYS: &[&str] = &["id", "name", "dp", "action", "cid", "type", "level"];
```

Anything not in this list (e.g. `{timestamp}`) is left literal in the
subscribe URL and the regex. That's why `{timestamp}` makes sense in a
*publish* template but breaks if you try to put it in a command topic.

### 3.2 Worked example — the HA-style config

Take this config (a common Home Assistant / single-DP setup):

```json
{
  "mqtt_root_topic": "rustuya",
  "mqtt_command_topic": "{root}/command/{action}/{id}/{dp}",
  "mqtt_event_topic":   "{root}/event/{id}/{dp}",
  "mqtt_message_topic": "{root}/{level}/{id}",
  "mqtt_payload_template": "{\"type\": \"{type}\", \"value\": {value}}",
  "mqtt_retain": true
}
```

Imagine a registered device `id=ebabc...`, `name=kitchen_light`, no sub-device.

**At startup**, the bridge subscribes:

| What                            | Subscription                       | Source                                        |
| ------------------------------- | ---------------------------------- | --------------------------------------------- |
| Commands                        | `rustuya/command/+/+/+`            | `tpl_to_wildcard` of `mqtt_command_topic`     |
| Duplicate-instance config topic | `rustuya/bridge/config`            | hardcoded `BRIDGE_CONFIG_TOPIC`               |

It also compiles the command topic into a regex roughly equivalent to
`^rustuya/command/(?P<action>[^/]+)/(?P<id>[^/]+)/(?P<dp>[^/]+)$` for variable
extraction.

**Inbound command** — you publish `true` to `rustuya/command/set/ebabc.../1`:

1. `match_topic` extracts `{action: "set", id: "ebabc...", dp: "1"}`.
2. Payload is the literal string `true`, not JSON-object — so
   [parse_mqtt_payload](../src/bridge.rs) wraps it:
   because `{dp}` was in the topic, the scalar becomes `{"dps": {"1": true}}`.
3. Topic vars are merged in (final loop of `parse_mqtt_payload`) without
   overwriting existing keys
   → final payload `{"dps": {"1": true}, "action": "set", "id": "ebabc...", "dp": "1"}`.
4. Deserialized to `BridgeRequest::Set { id: Some(Single("ebabc...")), dps, ... }`.
5. Routed to the device. Response suppressed for successful `set`/`get` (see
   the `spawn_mqtt_task` Publish handler).

**Outbound event** — device reports DPs `{"1": true, "2": 50}`:

1. `mqtt_event_topic` contains `{dp}` → [single-DP mode](../src/bridge.rs).
   The bridge iterates the DPS object and emits one message per key.
2. For DP `1`:
   - Topic: `rustuya/event/ebabc.../1`
   - Payload: `{"type": "active", "value": true}`
3. For DP `2`: `rustuya/event/ebabc.../2`, payload `{"type": "active", "value": 50}`.
4. Each message: QoS 1, `retain: true` (since `{id}` resolves and retain is
   structurally safe — see §4).

**Outbound error** — device reports `{"errorCode": xxx}`:

1. `handle_device_event` takes the error branch
  .
2. `last_error_code` persisted on the in-memory config.
3. Topic: `rustuya/error/ebabc...` (from `mqtt_message_topic` with
   `level=error`).
4. Payload: the raw payload object, with `id` (and `name`/`cid` if present)
   injected.

### 3.3 Active vs passive events

[handle_device_event](../src/bridge.rs) decides `is_passive` by
whether the payload had any `dps` field (root or nested under `data`). Active
events are deliberate device-driven state pushes (the device telling you
"this just changed"); passive events are everything else that came over
the wire with a non-empty JSON payload but no `dps` field — typically
`DP_QUERY` responses (state read-back after you called `get`), periodic
device-initiated status reports that use a non-standard shape, or
sub-device messages with custom layouts. The bridge synthesizes a DPS
dict from the raw payload so downstream consumers see a uniform format.

The default event topic includes `{type}` (`active` or `passive`) so you can
subscribe selectively. If you use the HA-style topic without `{type}`, both
kinds collapse onto the same topic and you lose the distinction (the
`"type"` field is still in the payload template if you kept it).

**When the distinction actually matters.** The active/passive split maps
onto two different kinds of DP semantics:

- **State DPs** — values describing the *current* state of the device
  (on/off, current temperature, brightness, etc). Active and passive
  carry the same meaning here: "the device is currently in this state".
  A passive replay of `temperature=23.5` after a query is fine —
  it's still true. Downstream consumers can treat both interchangeably.
- **Event DPs** — values describing a *moment-in-time event*
  (`single_click`, `double_click`, `motion_detected`, etc).
  The DP value is essentially "the last thing that happened", and the
  active event is the only one that means "it happened *now*". Passive
  events for these DPs are dangerous — they re-deliver the last event
  value on reconnect / query, so an automation that
  triggers on `single_click` will fire spuriously unless you filter to
  `type == "active"` only.

If your fleet has any event-style DPs (scene controllers, wall-switch
buttons, PIR sensors), keep `{type}` in the event topic — or filter on
the `"type"` field in payloads — and route only active events to
automations. State-only fleets can collapse the topics without harm.

### 3.4 Single-DP vs multi-DP mode — the trap

The branch [generate_device_templates](../src/bridge.rs) decides
single vs multi-DP solely from the **event topic template**:

```rust
if tpl.contains("{dp}") || tpl.contains("{value}") { /* one msg per DP */ }
else                                                { /* one msg, all DPs */ }
```

**Asymmetric fallback**: if you put `{dp}` in the *payload template* but not
in the event topic, multi-DP mode is taken and `vars.dp` is `None`. The
template engine resolves the missing variable to an **empty string** rather
than leaving the literal `{dp}` placeholder — so a template like
`{"dp": "{dp}", "value": {value}}` becomes `{"dp": "", "value": {"1": true}}`.
The same applies to `{type}`, `{level}`, and `{cid}` when their values
aren't in scope. Visible only if you actually subscribe to your events; the
cleanest fix is still to put `{dp}` in the event topic too so single-DP mode
takes over.

In multi-DP mode the `{value}` placeholder doubles as "the entire DPS object"
— useful if you want one MQTT message per device update instead of per DP.

### 3.5 Payload variable reference

The full set of placeholders resolved by [replace_vars](../src/bridge.rs).
Every known placeholder is **always consumed** — if the value isn't in
scope for the current call site, it renders as an empty string. This is
deliberate: it prevents literal `{foo}` substrings from leaking into
topics and payloads when a template references a variable that the
current code path doesn't populate.

| Key           | Resolves to                                         | When not in scope     |
| ------------- | --------------------------------------------------- | --------------------- |
| `{root}`      | configured `mqtt_root_topic`                        | (always present)      |
| `{id}`        | device id, or `"bridge"` for bridge-level events    | (always present)      |
| `{name}`      | device name from config                             | empty string          |
| `{cid}`       | sub-device cid                                      | empty string          |
| `{level}`     | `"response"` / `"error"` / `"scanner"` (msg path)   | empty string          |
| `{type}`      | `"active"` / `"passive"` (event-publish only)       | empty string          |
| `{dp}`        | DP key (single-DP mode only)                        | empty string          |
| `{value}`     | per-DP value (single-DP) OR full DPS JSON (multi-DP)| empty string          |
| `{dps}`       | full DPS JSON string                                | empty string          |
| `{timestamp}` | unix seconds                                        | (always present)      |

Empty-string substitutions can produce slightly surprising shapes:

- Topics like `rustuya/event//1` (when `{name}` is unset). Valid MQTT,
  just visually empty. The retain gate (§4) blocks *retained* publishes
  in this shape — non-retained ones go through.
- Payloads like `{"dp":"","value":50}` in multi-DP mode when the payload
  template references `{dp}`. Valid JSON, just useless. Fix by including
  `{dp}` in the event topic too (→ single-DP mode takes over).

---

## 4. Retain semantics — the scavenger and the gate

Retained MQTT messages are the bridge's nicest feature for HA integrations:
restart your dashboard and the latest state is right there. They are also its
nastiest debugging surface, because deleting a device must also delete every
retained message that mentions it — otherwise stale state lingers on the
broker forever.

The bridge handles this with two cooperating mechanisms.

### 4.1 The retain gate (`IdentifierSet`)

[IdentifierSet::satisfied_by](../src/bridge.rs) decides per-event
whether `retain=true` is safe. The rule:

- Scan the event topic and payload templates once at startup, record which
  of `{id}`/`{name}`/`{cid}` they reference → `event_identifiers`.
- For each outbound event: retain only if at least one referenced
  identifier *actually has a value for this device*.
- `{id}` is always satisfiable (every device has an id). `{name}` and
  `{cid}` are only satisfiable when the device's config has a non-empty
  value for them.

So if your event topic is `rustuya/event/{name}/{dp}` and you publish for a
device with no name, the bridge **strips retain**. The per-device shortfall
is logged at `debug` only (to avoid flooding when a fleet has many devices
without names); enable debug logging if you're tracking down missing
retains. Without this guard the scavenger would later have no way to find
the retained message and clear it — it'd be orphaned.

If the templates reference no identifier at all (e.g. `rustuya/all/events`),
`IdentifierSet::is_empty()` is true and retain becomes structurally
impossible to scavenge — the bridge warns **once at startup** (in
`BridgeContext::new`) rather than on every event.

### 4.2 The scavenger

When you `remove` or `clear` devices,
[spawn_retain_scavenger](../src/bridge.rs) spins up a transient
MQTT client that:

1. Subscribes to wildcard versions of the event and message topics
   (`tpl_to_wildcard`, e.g. `rustuya/event/+/+`).
2. For each retained message it receives, runs two passes:
   - **Topic regex match** — extracts `{id}`/`{name}`/`{cid}` from the topic
     using `compile_topic_regex` and compares against the removed devices'
     identifiers.
   - **Payload string fallback** — if topic match misses (or topic is
     literal), searches the payload for `"<id>"`, `"<name>"`, or `"<cid>"`
     as quoted string literals.
3. On match, publishes a zero-length retained message to clear it.
4. Auto-exits after `scavenger_timeout_secs` (default 1s) of idle, unless
   new targets are queued (each new batch extends the deadline). Configurable
   via `--scavenger-timeout-secs` / `SCAVENGER_TIMEOUT_SECS` — bump it on
   slow brokers where retained messages don't arrive within a second.

Subtleties:

- The scavenger uses **its own MQTT client** with a unique client_id
  (`{base}_scavenger_{unix_millis}`). It does not share the main client.
- If a scavenger is already running and the channel is open, new targets
  are forwarded to it instead of spawning a second one. The select! at
  the heart of the scavenger uses `biased;` ordering so a simultaneous
  new-target arrival and deadline expiry cannot race — the new targets
  always win and the deadline gets extended.
- The payload-fallback match uses **substring search for `"<id>"`** — quoted.
  `publish_device_message` always wraps non-object payloads into an object
  before injecting `"id": "..."`, so the fallback always finds messages
  regardless of caller-provided payload shape.

### 4.3 The "deleting the state file" hazard

This is the operational footgun the README warns about. The chain:

1. You enable `mqtt_retain = true`.
2. You register 20 devices. Each publishes state → 20 retained messages on
   the broker.
3. You decide to "reset" the bridge: `systemctl stop` + `rm rustuya.json`
   + `systemctl start`.
4. Bridge starts fresh, knows about zero devices, doesn't publish anything.
5. **The broker still has all 20 retained messages.** Any new subscriber
   sees ghost device state from devices the bridge can no longer manage.

The scavenger only runs in response to `remove`/`clear` actions while the
bridge is alive. To reset cleanly:

```bash
mosquitto_pub -t 'rustuya/command' -m '{"action":"clear"}'  # scavenges retained
# wait a second
systemctl stop rustuya-bridge
rm /var/lib/rustuya/rustuya.json
systemctl start rustuya-bridge
```

With `mqtt_retain = false`, deleting the state file is harmless — no retained
messages were ever published.

### 4.4 The bridge config topic

`{root}/bridge/config` is published
retained at startup with the full running config + `session_id`. It serves
three purposes simultaneously:

- **Presence** — dashboards can watch it to know the bridge is up.
- **Duplicate-instance detection** — see §7.
- **Last Will and Testament** — registered as an empty retained payload on
  abnormal disconnect, so the
  topic clears itself if the bridge crashes.

On graceful shutdown the bridge publishes the empty retained payload itself
and waits for the PubAck before disconnecting
 — the LWT is a backup, not the
normal path.

### 4.5 When the broker doesn't honor retain or LWT

Several bridge features assume the broker treats `retain=true` and Last
Will messages the way the MQTT spec describes. Some hosted MQTT services,
strict broker ACL policies, or older brokers may strip one or both. The
silent failure modes:

| Broker behavior        | What breaks                                                      |
| ---------------------- | ---------------------------------------------------------------- |
| Retain stripped        | Scavenger has nothing to clear (no retained messages to begin with — but also no stale ones to worry about). Status dashboards subscribing to `{root}/bridge/config` see nothing until the next bridge restart publishes live. |
| LWT stripped (only)    | If the bridge crashes, the retained `bridge/config` from the prior run stays on the broker forever. The next bridge restart sees a different `session_id` → §7 startup check fails → **bridge refuses to start**. Manual recovery: `mosquitto_pub -r -t '<root>/bridge/config' -m ''`. |
| Both stripped          | Duplicate-instance detection effectively no-ops (nothing to see). Scavenger no-ops. The bridge will start under any conditions. |

The retain check in §4.1 only verifies that the *templates* allow
scavenging — it can't detect a broker that drops retain. If you have
`mqtt_retain = true` set but no retained messages ever accumulate on the
broker after running for a while, your broker is probably stripping
retain.

---

## 5. Sub-device routing

Tuya gateways (Zigbee/BLE) carry sub-devices that have no IP and no local
key of their own — they're addressed by a `cid` field inside the gateway's
payload. The bridge bridges this by maintaining a `cid_map`.

### 5.1 Inbound: event with CID

When the unified listener emits an event for a registered direct device,
[resolve_event_target](../src/bridge.rs) looks for `cid` in two
places:

1. Root of the payload object: `{"cid": "...", "dps": {...}}`.
2. Nested under `data`: `{"data": {"cid": "...", ...}}` — this is the
   `tuya2mqtt.py`-compatible shape.

If found, it looks up `cid_map[(parent_id, cid)]`. If a matching sub-device
is registered, `target_id` becomes the sub-device id; otherwise it falls
back to the parent. The sub-device's `name` (from its own config) is used
for downstream template substitution.

The DPS itself may be at the root *or* nested under `data` — the bridge
checks both and even synthesizes one from the raw payload if neither is
present, so passive events still
get a uniform shape.

If `cid` was present but the sub-device isn't registered, the bridge surfaces
the cid in the DPS object for the parent's event
 — gives you a foothold for
debugging without dropping data.

### 5.2 Outbound: command targeting a sub-device

You target a sub-device the same way as a direct device — by `id` (or
`name`). The handler calls
[get_connected_device](../src/bridge.rs), which:

1. First looks for the id in `instances` directly.
2. If absent (the sub-device case), reads the config, finds `parent_id`,
   and returns the parent's `Device` instance.

Then [resolve_cid](../src/bridge.rs) returns the sub-device's
`cid` (registered at `add` time), and the request goes out on the parent's
wire with the cid as the routing key.

**CID override**: requests can include their own `cid` field
 — this takes priority over the
device's registered cid. Useful for one-off control of an unregistered
sub-device on a known gateway.

### 5.3 Cascading remove

`remove` on a gateway also removes all its sub-devices
. The collection step happens
under the read lock, then sub-ids are extended into the targets list before
deletion. Scavenger targets are built from the deleted configs, so retained
sub-device state gets cleaned up too.

---

## 6. State persistence and the debounced saver

### 6.1 The debounce loop

[spawn_state_saver](../src/bridge.rs) is intentionally simple:

```rust
loop {
    select! {
        () = cancel.cancelled() => break,
        res = save_rx.recv() => {
            // got a request — wait debounce, then save
            select! {
                () = cancel.cancelled() => { save_now(); break; }
                () = sleep(save_debounce_secs) => { drain_pending(); save_now(); }
            }
        }
    }
}
```

The pattern: wait for any save request → start the debounce timer → drain
all pending requests when it fires → save once. Burst-friendly: 50 adds in
2 seconds = 1 disk write. The `save_tx` channel is capacity 1 and uses
`try_send`, so request-coalescing happens at the channel level too —
multiple `request_save()` calls during the debounce window are no-ops on
a full channel.

### 6.2 Shutdown semantics

Cancellation during the debounce window still triggers a save before exit
— your last `save_debounce_secs` worth of work isn't lost. Cancellation
while idle (no request pending) skips the save entirely. `close()` also
calls `save_state()` directly as a final belt-and-braces.

### 6.3 Atomicity and durability

[save_state](../src/bridge.rs) writes to `<path>.tmp` then `rename`s —
POSIX atomic on the same filesystem. The parent directory is
`create_dir_all`'d on every save (not just startup), so deleting the
parent directory between saves doesn't crash the saver.

For durability across unclean shutdown:

1. The temp file's contents are `fsync`'d (`File::sync_all`) before the
   rename, so a power loss between write and rename leaves either an
   incomplete `.tmp` (deleted on next save) or a complete one.
2. The parent **directory** is `fsync`'d after the rename, so the rename
   itself is durable. Without this, the directory entry update can sit in
   the page cache and a crash can revert to the pre-rename state on next
   boot. On Windows this fsync is silently skipped (opening a directory
   handle isn't supported there).

**Operational cost on slow flash.** `fsync` on a Raspberry Pi SD card or
similar low-end flash can block for tens to hundreds of milliseconds. The
debounce (default 30s) means this hits at most once per debounce window,
not per device change — but if you set `save_debounce_secs` very low on
slow storage, the bridge will spend visible time in IO. If you see
unexpected latency on commands during state-save bursts, this is the
likely cause; raise the debounce or move the state file to faster
storage.

### 6.4 The state file format quirk

[load_state](../src/config.rs) accepts *two* shapes via
`#[serde(untagged)]`:

```jsonc
// Map form (what the bridge writes)
{ "deviceId1": { "id": "deviceId1", "name": "...", ... }, ... }

// List form (also accepted, for hand-editing convenience)
[ { "id": "deviceId1", ... }, { "id": "deviceId2", ... } ]
```

Maps win on parse ambiguity (untagged tries variants in order). For map
form, if `id` is missing inside an entry, it's filled in from the key —
so `{"abc": {"name": "kitchen"}}` is valid and gets `id: "abc"`.

`DeviceConfig` field aliases are generous — `localKey`, `local_key`,
`localkey`, `devId`, `device_id`, `nodeId`, `gatewayId`, etc. all map to
their canonical Rust field. Convenient when importing from other Tuya
tooling.

**State file edge cases**: the load-time classifier in `BridgeContext::new`
uses the same precedence as `add_device` (sub-branch first, then direct):

- An entry with `cid` + `parent_id` + `key` all set lands as a sub-device.
  The `key` stays in `configs` but never opens a connection.
- An entry with `cid` but no `parent_id` logs `error!("Device {id} is
  invalid: missing key or parent info")`. The entry **still appears in
  `configs`** (and in `status` output as `"invalid subdevice"`), but it
  has no `cid_map` routing entry and no live connection.
- An entry with neither `key` nor `(cid + parent_id)` triggers the same
  error and lands in the same "in `configs`, no instance, no routing"
  limbo (reported as `"offline"` in `status`).

If a device appears in `status` with these markers after a restart,
check the bridge's startup logs.

### 6.5 Corruption recovery — there isn't one

`load_state` falls back to an **empty `HashMap`** if the JSON parse fails,
logging an `error!` and continuing. The next `save_state` then overwrites
the (corrupt) file with the empty config — losing every device
registration. The fsync logic in §6.3 protects against this for clean
shutdown sequences, but doesn't help against disk-level corruption
(bad sectors, FS errors, manual mis-edits).

There is **no backup mechanism** in the bridge. If you can't afford to
re-register your fleet on a corrupt-file event, copy the state file
into version control or a backup target on a schedule (`cron` +
`cp /var/lib/rustuya/rustuya.json` somewhere safe). The file is small
(JSON, one entry per device) so this is cheap.

A `mosquitto_pub -t 'rustuya/command' -m '{"action":"status"}'` against
a running bridge gives you a JSON snapshot of every registered device
that you can pipe into a backup — handy as a periodic check that the
state-on-disk matches state-in-memory.

---

## 7. Duplicate-instance detection

The mechanism is built on the bridge config topic (§4.4).

**At startup** ([check_existing_instance](../src/bridge.rs)):

1. Generate a `session_id = "sid_<unix_millis>"` for this instance.
2. Subscribe to `{root}/bridge/config` with a temporary client.
3. Wait 500ms for any retained config to arrive.
4. If a retained payload with a `session_id` field is received, bail —
   another bridge is already running on this `(broker, root_topic)`.

**At runtime**:

The main MQTT loop also subscribes to the config topic. If it ever sees a
config payload with a `session_id` different from its own, it logs an error
and self-cancels. This handles the case where a *second* bridge starts
after the first — both run their 500ms check, but the second one's
publish wins because the first one already shut down on receipt.

**LWT cleanup**: if a bridge crashes (kill -9, OOM, power loss), the broker
publishes the empty retained payload set as Last Will, clearing the config
topic. The next bridge to start sees nothing and proceeds.

**Limitations**:

- Only protects against duplicates on the *same* `(broker, root_topic)`.
  Two bridges with different `mqtt_root_topic` won't collide.
- The 500ms window is a heuristic. On a very slow broker connection,
  startup may falsely succeed before the retained message arrives. The
  runtime check catches this within seconds.
- **The whole mechanism assumes the broker honors LWT.** If your broker
  strips Last Will messages and your bridge crashes (kill -9, OOM,
  power loss), the prior session's retained config stays on the broker
  forever, and the next bridge startup will see it and refuse to start
  (different `session_id`). Recovery: clear the topic by hand —
  `mosquitto_pub -r -t '<root>/bridge/config' -m ''` — before
  restarting. See §4.5 for the broader broker-compatibility picture.

---

## 8. MQTT connection management

### 8.1 Retry with exponential backoff + jitter

[spawn_mqtt_task](../src/bridge.rs) wraps `eventloop.poll()` in a
backoff loop. On any error:

- `retry_delay` doubles, capped at `MAX_RETRY_DELAY_SECS = 1280` (~21
  minutes) → 10 → 20 → 40 → 80 → 160 → 320 → 640 → 1280.
- A per-attempt jitter of `unix_millis() % 1000` ms is added
  to desync multiple bridges
  that were all restarted by the same outage.
- On successful `ConnAck`, `retry_delay` resets to the initial value.

The 1280-second cap is high enough that a long broker outage won't burn
CPU reconnecting, but low enough that recovery happens within ~20 minutes
once the broker is back.

**Operational gotcha**: there's no manual "reconnect now" trigger. If
the broker was down long enough to push the delay to 1280s and then
comes back, the bridge can sit idle for up to ~21 minutes before its
next attempt — even though the broker is healthy. If you need faster
recovery during an incident, restart the bridge (`systemctl restart
rustuya-bridge`) — startup resets the delay to 10s.

### 8.2 Outbound channel: 100-deep buffer

The `MQTT_CHANNEL_CAPACITY = 100` constant bounds the event-publish queue.
`try_send_mqtt` uses a 500ms `send` timeout — if the channel is full for
half a second, the message is dropped at `error` log level. This protects
against a wedged MQTT broker causing unbounded memory growth.

Drops are counted in `BridgeContext::mqtt_drop_count` (an `AtomicU64`) and
the cumulative count is exposed in `status` responses under
`mqtt_drop_count`. Watch this field — a steadily growing count means your
broker or downstream consumer can't keep up, and retain=true users are
losing the last-known state of devices that drop messages.

Implications:

- During broker reconnect, publishes pile up. 100 messages buffer ≈ enough
  for a few seconds of normal activity. Heavy publishers (chatty devices,
  multi-DP mode with many DPs) may see drops during outages.
- No retry, no persistence, no replay — once dropped, the value is gone.
  For retain-sensitive setups the broker may serve stale retained data
  until the device's next change.

### 8.3 Clean shutdown sequence

Graceful shutdown:

1. Send `None` over the MQTT channel → loop enters the shutdown branch.
2. Publish empty retained payload to clear `bridge/config`.
3. Poll the eventloop until PubAck arrives (5s timeout).
4. Send `disconnect()`.
5. Drain the eventloop until it returns an error (its "socket closed"
   signal).

The whole sequence is wrapped in a 7s outer timeout in
[BridgeServer::close](../src/server.rs), so a misbehaving broker can't hang
shutdown indefinitely.

---

## 9. Payload parsing details

[parse_mqtt_payload](../src/bridge.rs) is the bridge's compatibility
layer. It tries to make sense of whatever shape arrives.

### 9.1 Topic variables → payload merge

After the topic regex extracts variables, they're merged into the payload
object **only when the key is absent**:

```rust
for (k, v) in vars {
    if !obj.contains_key(k) {
        obj.insert(k.clone(), Value::String(v.clone()));
    }
}
```

Payload-provided values always win. This is the mechanism behind several
useful tricks (§10).

### 9.2 Scalar payload + `{dp}` in topic

If the payload is a plain scalar (not an object) and the topic contained
`{dp}`, the scalar is wrapped as a DPS update:

- Publish `true` to `rustuya/command/set/abc/1` →
  `{"dps": {"1": true}, "action": "set", "id": "abc", "dp": "1"}`.
- Publish `50` to `rustuya/command/set/abc/2` →
  `{"dps": {"2": 50}, ...}`.

If the topic had no `{dp}`, the scalar is wrapped under a generic `payload`
field instead — `{"payload": <scalar>, ...topic_vars}`. The downstream
deserialize behavior depends on what else the topic provided:

- No `{action}` in topic, or `{action}` resolves to something other than
  `set`/`get`/`status`/`scan`: no `BridgeRequest` variant matches → silently
  dropped.
- `{action}` = `set` in topic: the set heuristic fires on the wrapped object
  (no `dps`/`data` present), creating an empty `dps` after the deny-list
  strips `payload`. The request then deserializes as `Set { dps: {} }`,
  routes to a device, and is a no-op write. Surprising but harmless.
- `{action}` = `get`: deserializes to `Get`, which has no `dps` field; the
  wrapped `payload` is silently discarded by serde and the get fires.

Bottom line: always include `{dp}` in your command topic when you intend
to publish raw scalar values; relying on the without-`{dp}` paths is
fragile and depends on which action the topic happens to specify.

### 9.3 The `set` heuristic

[apply_set_heuristic](../src/bridge.rs): if `action == "set"` and the
payload has neither `dps` nor `data`, treat all remaining fields (minus a
deny-list that covers every `BridgeRequest` field — `action`/`id`/`name`/
`key`/`ip`/`version`/`cid`/`parent_id`/`cmd`/`data`/`dps` plus the synthetic
`dp`/`payload`) as the DPS object. This means both of these work:

```jsonc
// Explicit
{"action": "set", "id": "abc", "dps": {"1": true, "2": 50}}

// Implicit — fields-as-dps
{"action": "set", "id": "abc", "1": true, "2": 50}
```

Convenient, but mildly dangerous — if you include a field that isn't in
the deny-list, it becomes a DP write. The deny-list lives in
`apply_set_heuristic` and must be kept in sync with the `BridgeRequest`
enum; adding a new top-level field to a `BridgeRequest` variant without
updating the deny-list will silently turn that field into a DP write.

### 9.4 Array payloads

If the payload root is a JSON array, *each element* gets the topic-var
merge and `set` heuristic applied independently. Useful for bulk operations:

```bash
mosquitto_pub -t 'rustuya/command' -m '[
  {"action":"set","id":"a","dps":{"1":true}},
  {"action":"set","id":"b","dps":{"1":false}}
]'
```

Each element produces a separate `BridgeRequest` and runs in its own
spawned task. They are *not* ordered relative to each other.

---

## 10. Operator tips

Small things that aren't bugs but aren't obvious either.

### 10.1 Controlling by `name` when the command topic has `{id}`

You configured `mqtt_command_topic = "{root}/command/{action}/{id}/{dp}"` and
your script wants to control by name. The topic *requires* a segment in the
`{id}` slot. What to put there?

Naive answer: `rustuya/command/set/_/1` with payload `{"name": "kitchen"}`.

What actually happens: the topic-var merge fills `id = "_"` because the
payload had no `id` key. `id` then wins over `name` in `find_device_ids`,
the lookup fails, and you get `NoMatchingDevices`.

The fix: **explicitly set `id` to JSON `null` in the payload**, which
counts as "key present" for the merge guard:

```bash
mosquitto_pub -t 'rustuya/command/set/_/1' -m '{"id": null, "name": "kitchen", "1": true}'
```

`Option<SingleOrList>` deserializes `null` as `None`, so the request
becomes `Set { id: None, name: Some(Single("kitchen")), ... }` and the
name path is taken.

**Caveat: empty string does *not* work.** `"id": ""` parses as
`Some(Single(""))`, and `find_device_ids` filters out empty-string ids
because `state.configs.contains_key("")` is false. The result is an empty
target list and no fallthrough to name lookup — only `null` works. The
bridge logs a `warn` when an `id` selector contains an empty string,
pointing back at this tip, so a confused setup is loud rather than silent.

### 10.2 Suppressing successful set/get responses

The MQTT main loop already does this for you — `set` and `get` only publish
a response on *error*, OR when a name-based lookup fanned out to multiple
devices (`matched > 1`). Don't subscribe to `{root}/response/#` expecting
acks for every set; you'll only see errors and fan-out notifications.

### 10.3 Tuning save debounce

`save_debounce_secs = 30` (default) is conservative — good for SSDs and
SD cards alike. If you're scripting hundreds of adds in a tight loop and
want a snapshot for backup, send a `status` query (which doesn't trigger
a save) and parse the response instead of waiting on the file.

Setting `save_debounce_secs = 0` makes saves effectively-immediate (the
debounce still has a `sleep(0)`, which yields once). Useful in tests; not
recommended for production on slow flash.

### 10.4 Forcing a refresh without changing topology

Both `add` and `remove` are now selective about triggering refresh:

- `add` skips refresh when re-adding a direct device with identical
  `key`/`ip`/`version` (§1.2).
- `remove` skips refresh when only sub-devices were removed — they
  aren't in the listener stream, so killing the parent's TCP connection
  would be gratuitous churn.

So you cannot accidentally force a reconnect by replaying your
registration script *or* by removing a sub-device.

If you genuinely need to rebuild the listener (e.g. you suspect stale
TCP state on a direct device), removing then re-adding that *direct*
device works — direct-device removal does refresh. Alternative hammer:
send `clear` and re-register everything.

### 10.5 Watching the bridge come up

`{root}/bridge/config` is retained, so subscribing *after* the bridge has
started still gives you the running config immediately:

```bash
mosquitto_sub -t 'rustuya/bridge/config' -v
```

Empty payload = bridge is down (LWT fired or graceful shutdown happened).
Non-empty = bridge is up and these are its effective settings — every
field that ended up in `Cli` after the CLI/env/config-file/default merge.
Handy for verifying that your config file actually got picked up.

### 10.6 Standalone mode for testing

Run without `--mqtt-broker` and the bridge still tracks devices, processes
adds/removes via... well, nothing, because there's no command channel. But
the state file is written and read normally, and you can poke at the rust
APIs from a test harness or sister binary. Used internally for debugging
the device layer without an MQTT broker in the loop.

### 10.7 Don't put `{timestamp}` in command topics

Command topics get compiled to a regex via `compile_topic_regex`. Only the
keys in `TOPIC_WILDCARD_KEYS` are replaced with named captures; `{timestamp}`
isn't in that list, so it's escaped literally — the bridge will subscribe
to `rustuya/command/{timestamp}` and the placeholder won't match anything
sensible. `{timestamp}` is for publish-side templates only.

### 10.8 Bulk operations are array publishes, not loops

If you have 20 devices to register at boot, send one array payload, not 20
publishes. The bridge processes each element as an independent
`BridgeRequest` in its own task — same effect, fewer MQTT round trips,
less log noise.

The bigger win is **listener-refresh coalescing** (§2.2). 20 separate
adds = 20 distinct refresh signals; with a 1-capacity refresh channel
that still produces several listener rebuilds (and N TCP reconnects per
rebuild). An array of 20 adds = 20 spawned tasks all racing to call
`try_send` on the same 1-capacity channel; most fail silently, the
listener typically rebuilds once or twice instead of ~20 times. The
difference is dramatic for fleets of dozens of devices.

### 10.9 Watch `mqtt_drop_count` in status

The MQTT outbound channel drops messages after a 500ms full-queue timeout
(§8.2). The cumulative drop count is in every `status` response under
`mqtt_drop_count`:

```bash
mosquitto_pub -t 'rustuya/command' -m '{"action":"status"}'
# response includes: "mqtt_drop_count": 0
```

If this number is non-zero and growing, your broker or downstream consumers
can't keep up. Reduce event volume (consolidate to one event per device
update rather than per DP) or fix the consumer.

The counter is **cumulative since process start** — there's no rolling
window or reset API. For monitoring, scrape it on a schedule and chart
the delta rather than the absolute value. A bridge restart resets it
to 0.

### 10.10 Tune `scavenger_timeout_secs` on slow brokers

The default 1-second scavenger idle timeout assumes a broker that delivers
retained messages near-instantly. On a slow broker (cross-region, congested,
or large retained backlog), retained messages for some removed devices may
arrive after the scavenger has already exited, leaving them orphaned.

Bump it via config:

```jsonc
{ "scavenger_timeout_secs": 5 }
```

or CLI: `--scavenger-timeout-secs 5`. Cost is just a 5-second delay after
`remove`/`clear` before the temporary scavenger client disconnects.

---

## Appendix: where to look in the source

| If you're investigating...               | Start at                                       |
| ---------------------------------------- | ---------------------------------------------- |
| Why an add changed something else        | [add_device](../src/bridge.rs)      |
| Why a retain didn't fire                 | [IdentifierSet](../src/bridge.rs)       |
| Why a retained message isn't clearing    | [spawn_retain_scavenger](../src/bridge.rs) |
| Why a topic substitution is wrong        | [render_template](../src/bridge.rs), [replace_vars](../src/bridge.rs) |
| Why a command isn't matching             | [parse_mqtt_payload](../src/bridge.rs), [compile_topic_regex](../src/bridge.rs) |
| Why a sub-device command goes nowhere    | [get_connected_device](../src/bridge.rs), [resolve_cid](../src/bridge.rs) |
| Why the listener seems wedged            | [spawn_device_listener](../src/bridge.rs) |
| Why MQTT keeps reconnecting              | [spawn_mqtt_task](../src/bridge.rs)   |
| Why two bridges fought                   | [check_existing_instance](../src/bridge.rs) |
| Why the state file looks weird           | [load_state](../src/config.rs)        |
