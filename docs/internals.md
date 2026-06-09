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
prompt — instead, **each matched device answers with its own response**,
addressed to its own `{id}` (so it lands on that device's response topic).
A name-set matching two devices produces two responses:

```json
{ "status": "ok", "action": "set", "id": "ebabc..." }
{ "status": "ok", "action": "set", "id": "fbcde..." }
```

The single-target suppression (a lone successful `set`/`get` is dropped as
redundant — §10.2) is bypassed whenever a command fans out, so the caller
sees every device it touched. There is no comma-joined `id` and no `matched`
count: the per-id responses *are* the fan-out signal. Subscribe to
`{root}/response/+` (or `/#`) and correlate by the `id` field.

**Cascade reports per-id too.** If you `remove` a single gateway by name and
the bridge cascades to its 3 sub-devices, you get **4 separate `remove`
responses** — one per removed id (gateway + 3 subs) — each on its own topic,
so a consumer can drop each device individually. Cascading is the bridge's
choice, not a result of the name lookup, but the wire shape is the same as
any fan-out: one response per affected id.

> **Operational note — partial failure in name fan-out.** [run_per_target](../src/handlers.rs)
> attempts **every** target independently (no abort on first error) and each
> reports its own outcome: a `set` matching 3 devices where device #2 times
> out yields an `ok` for #1 and #3 and an `error` for #2, each addressed to
> its own id. So partial failure is visible per-device — no need to re-query
> `status` to find out which targets applied. (A target that isn't currently
> connected is skipped and reported as `ok`; its offline state surfaces on the
> device `error` topic — §3.2.)

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
| Direct, in `instances`, last reported `errorCode 0`       | `"0"` — success/connected, **same as online**|
| Direct, in `instances`, errored before (non-zero code)    | the raw error code as string (e.g. `"905"`)  |
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

### 2.4 Onboarding the fleet — the connect-storm cap

Each `add` inserts a `Device` and pokes `request_refresh()` (a capacity-1
channel, so a burst of adds coalesces into one rebuild). On refresh the
listener rebuilds `unified_listener(instances)` over **all** devices —
existing ones keep their live actor connection, only the new ones connect.
So registering N devices at once fires up to N *simultaneous* connection
establishments.

That's a "connect storm": each handshake (TCP + crypto round-trips, plus
session-key negotiation on v3.4/3.5) is expensive, and a thousand at once
saturate the runtime — meanwhile devices that connected early can miss
their idle/heartbeat window and get dropped, feeding a reconnect storm
that may never converge. The fix lives in rustuya: a global
establishment-concurrency semaphore (permit acquired *before* connect,
released the instant the handshake finishes — an idle connection is cheap
and must not hold a permit, or fleets larger than the cap would deadlock).

The bridge opts in via `--connect-concurrency` (`CONNECT_CONCURRENCY`,
default **128**; `0` = unbounded), wired once at startup with
`rustuya::set_connect_concurrency`. The cap bounds only the *establishment*
phase, so steady-state heartbeats for an already-connected fleet are
unaffected. This caps the storm — it does not speed it up; a large slow
fleet still onboards gradually, just without the thundering herd.

Fleet-scale is validated end to end at **1000 devices** (mock fleet): the whole
fleet onboards and a single mass `clear` scavenges every retained snapshot with
zero orphans, exercised in CI (`Python Test` →
`python/tests/test_scavenger_scale.py`).

### 2.5 Empty-instance idle

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
  "mqtt_event_topic":   "{root}/event/{type}/{id}/{dp}",
  "mqtt_message_topic": "{root}/{level}/{id}",
  "mqtt_payload_template": "{value}",
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
5. Routed to the device. This is a single-target `set` that succeeds, so the
   response is suppressed (see [responses_for_results](../src/handlers.rs) and
   §10.2).

**Outbound event** — device reports DPs `{"1": true, "2": 50}`:

1. `mqtt_event_topic` contains `{dp}` → [single-DP mode](../src/bridge.rs).
   The bridge iterates the DPS object and emits one message per key.
2. For DP `1`:
   - Topic: `rustuya/event/active/ebabc.../1`
   - Payload: `true`
3. For DP `2`:
   - Topic: `rustuya/event/active/ebabc.../2`
   - Payload: `50`
4. Each message is QoS 1. Because these are **active** deltas, cache mode
   (`mqtt_retain: true`) publishes them **no-retain** on `{type}=active`; only
   the merged `{type}=state` snapshot is retained — see §4. (Retain is
   structurally safe there because `{id}` resolves.)

**Outbound error** — device reports `{"errorCode": xxx}`:

1. `handle_device_event` takes the error branch.
2. `last_error_code` persisted on the in-memory config.
3. Topic: `rustuya/error/ebabc...` (from `mqtt_message_topic` with
   `level=error`).
4. Payload: the raw payload object, with `id` (and `name`/`cid` if present)
   injected.

A successful connect therefore lands on the broker (retained) like this —
`errorCode 0` plus a human-readable `errorMsg`, with `id`/`name` injected:

```
rustuya/error/ebabc0000000000000aaaa  {"errorCode":0,"errorMsg":"Connection Successful","id":"ebabc0000000000000aaaa","name":"Smart Socket 20A"}
rustuya/error/ebfde0000000000000bbbb  {"errorCode":0,"errorMsg":"Connection Successful","id":"ebfde0000000000000bbbb","name":"office_light"}
```

> **`errorCode: 0` is success, not a fault.** Connect/disconnect is folded
> into this same `error` path *on purpose* — rather than maintaining a
> separate "device connected" topic, a successful connect/command emits
> `errorCode: 0` here and a real fault emits a non-zero code, so operators
> watch one topic for both liveness and errors. Consequently the `error`
> topic — and the `status` field (§1.4), which echoes `last_error_code`
> verbatim — can legitimately carry `0`, meaning *connected*. A `status` of
> `"0"` is equivalent to `"online"`; only a non-zero code is an actual fault.
>
> With `mqtt_retain` on the `error` topic is retained, so the latest `0` /
> non-zero value persists on the broker — one glance at the retained message
> tells you online (`0`) vs offline (anything else) without polling.

### 3.3 Active vs passive events

[handle_device_event](../src/bridge.rs) decides `is_passive` by
whether the payload had any `dps` field (root or nested under `data`).

- **Active** events are deliberate device-driven pushes
  (the device telling you "this just changed", e.g. button pressed,
  switch toggled). Wire shape: root or nested `dps` field present.
- **Passive** events are everything else that came over the wire with a
  non-empty JSON payload but no `dps` field — typically `DP_QUERY`
  responses (state read-back after `get`), periodic device-initiated
  status reports with non-standard shape, or sub-device messages with
  custom layouts. The bridge synthesizes a DPS dict from the raw
  payload so downstream consumers see a uniform format.

What the bridge does with these depends on `mqtt_retain` — see §4.

**Why the distinction exists at all.** Even before retain semantics, the
active/passive split maps onto two different kinds of DP semantics:

- **State DPs** — values describing the *current* state of the device
  (on/off, current temperature, brightness, etc). Both active and
  passive carry "the device is in this state" — interchangeable.
- **Event DPs** — values describing a *moment-in-time event*
  (`single_click`, `double_click`, etc). Only an
  active event means "this happened *now*"; a passive replay of
  `single_click` from a periodic status report or DP_QUERY response
  means nothing happened — the
  device just kept its last value cached. Automations that trigger on
  `single_click` must filter to `type == "active"` only, or use the
  cache-mode retain model (§4) which only retains snapshots, never event-style
  deltas.

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
| `{type}`      | `"active"`/`"passive"` (raw delta) or `"state"` (retained snapshot, cache mode); event-publish only | empty string          |
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

## 4. Retain semantics — pass-through and cache modes

The bridge has **two distinct retain models** selected by `mqtt_retain`:

- pass-through mode (`mqtt_retain=false`, default) — historical pass-through. Each
  device event publishes one MQTT message with `retain=false`. No
  in-memory cache, no seed phase. State recovery on consumer reload is
  the consumer's problem.
- cache mode (`mqtt_retain=true`) — separates **live deltas** (both active and
  passive events, no retain, on `{type}=active`/`{type}=passive`) from a
  **retained state snapshot** (the merged cache, published with retain on a
  distinct `{type}=state` topic). HA-style state recovery works automatically
  without spuriously re-firing event automations on reconnect, and a passive
  readback (e.g. the reply to `get`) is still observable as a live delta.

The rest of this section is about cache mode.

### 4.1 Why cache mode exists — the partial-overwrite hazard it avoids

Naive `retain=true` publishing has a data-loss trap in **multi-DP mode**
(event topic without `{dp}`): one retained MQTT message per device update,
straight from the *incoming* DPS dict. A device that sends a partial passive
update (battery report only, RSSI only, periodic timer tick) would shrink the
retained snapshot on the broker to that partial dict — wiping out the full
state a previous full status report established. An HA reload after a partial
then reads "switch state = unknown" until the next full device status report
(sometimes minutes).

Cache mode avoids this by keeping a **per-device merged DPS cache** in the bridge
and publishing snapshots from the cache, not from the incoming message.
A battery-only passive merges into the cache; the published snapshot
still contains the cached switch state, temperature, etc.

A secondary win: active events publish as **delta with no retain** on
the `{type}=active` topic, so HA event automations triggered by a
button press don't spuriously re-fire when HA reconnects and the broker
re-delivers a retained active.

### 4.2 The two publish routes

```
Direct publish route (trigger: handler call site)
  └─ Both active and passive → {type}=active / {type}=passive topic,
     no-retain, incoming raw delta. Fires unconditionally.

Cache publish route (trigger: cache.merge returns changed keys)
  └─ Both active and passive → {type}=state topic, retain,
     full merged DPS snapshot from the cache
```

Every event fires the direct route immediately (its raw delta, no retain)
*and* merges into the cache, which — when seeded and when something actually
changed — fires a snapshot publish on `{type}=state`. The distinction between
active and passive lives entirely in the **direct route's `{type}`** value;
the snapshot is always `state` regardless of origin.

The direct route is **ungated**: it fires even for a passive whose values all
match the cache. This is deliberate — it's the "something arrived" signal, so
a `get`/`status` readback that changes nothing is still visible on
`{type}=passive` rather than being silently absorbed into the (unchanged)
snapshot.

Identical-value updates are deduped only at the **cache layer** — `cache.merge()`
returns the keys that actually changed, and the *snapshot* publish is gated on
that being non-empty. A device sending the same periodic status report every
30s won't generate snapshot publishes after the first, but it *will* keep
emitting the no-retain `{type}=passive` delta each time.

### 4.3 `{type}` and live double-fire

Cache mode publishes a no-retain raw delta (`{type}=active`/`{type}=passive`)
*and*, when something changed, a retained snapshot (`{type}=state`) per event.
MQTT semantics keep the *reload* path safe unconditionally — a no-retain
publish doesn't update the broker's retained value, so a re-subscribing client
only ever receives the latest `state` snapshot, never a stale delta. The
spurious-re-fire-on-reload bug is gone regardless of template shape.

The only residual concern is **live double-fire**: when `{type}` is in the
topic, the three kinds (active delta, passive delta, state snapshot) each get
their own topic, so a subscriber receives exactly what it subscribed to and
there is no collision — pick `{type}=active` for event triggers, `{type}=state`
for current state, `{type}=passive` to watch readbacks. The distinction needs
`{type}` in one of:

- the **event topic** (`{root}/event/{type}/{id}`) — subscribe to just the
  `{type}` you want; each kind is a separate topic.
- the **payload template** (`{"type":"{type}","value":{value}}`) — all kinds
  share a topic, but consumers filter on `value_json.type`.

If `mqtt_retain=true` is set with **neither**, all three kinds collide on one
topic: a live subscriber gets every delta *and* every snapshot with no way to
tell them apart. The bridge logs a WARN and keeps cache mode enabled — the
user opted in explicitly, and a fleet with no event automations doesn't care
(state entities are idempotent). The WARN points at the templates to add
`{type}` to if the user does care.

### 4.4 The retain gate (`IdentifierSet`)

[IdentifierSet::satisfied_by](../src/bridge.rs) decides, **per snapshot
publish**, whether `retain=true` is safe. The gate exists so that every
retained message the bridge leaves on the broker can later be *located and
cleared* by the scavenger (§4.7) — a retained message whose topic carries no
resolvable identifier would orphan forever.

**What the gate reads.** At startup it scans **both** the event topic *and*
the payload template (their union) for `{id}`/`{name}`/`{cid}` →
`event_identifiers` ([from_templates](../src/bridge.rs)). So "has `{id}`"
means *either* template references it — not the topic alone.

```rust
// satisfied_by(name, cid):
self.id
    || (self.name && name.is_some_and(|s| !s.is_empty()))
    || (self.cid  && cid.is_some_and(|s| !s.is_empty()))
```

`{id}` is **always** satisfiable (every device has an id). `{name}`/`{cid}`
are satisfiable only when *this device's* config has a non-empty value.

**Two things this gate does NOT do:**

- It does **not** gate whether the `{type}=state` snapshot is *published*.
  In cache mode a changed snapshot is **always** published (post-seed); the
  gate only flips its `retain` flag. A "stripped" snapshot still goes out —
  as `retain=false`.
- It does **not** affect delta publishes. Active/passive deltas are
  *unconditionally* `retain=false`, so the gate only ever touches the
  `{type}=state` snapshot.

#### Behavior by identifier case

Hold `{type}` constant (present) to isolate the identifier axis — the
`{type}` warning is orthogonal (see below). "id referenced" = `{id}` is in
the event topic **or** the payload template.

| Templates reference… | Startup identifier warn | `{type}=state` published | `retain` on the snapshot | Runtime log |
| --- | --- | --- | --- | --- |
| **① `{id}`** (± name/cid) | no | yes | **always retained** — every device, regardless of name/cid | none |
| **② no `{id}`, but `{name}` and/or `{cid}`** | no | yes | **per-device**: retained iff this device has a non-empty value for a referenced identifier; otherwise retain stripped | stripped devices log one `debug` line per snapshot ([publish_snapshot](../src/bridge.rs)) |
| **③ no identifier at all** (`is_empty`) | **yes, once at startup** ([BridgeContext::new](../src/bridge.rs)) | yes | **never retained** — `satisfied_by` is `false` for all devices, so every snapshot is `retain=false` | every device logs one `debug` strip line per snapshot |

Case ① — `self.id == true` short-circuits `satisfied_by` to `true`
unconditionally, so a nameless device or a direct (no-cid) device still
retains fine. Case ② — e.g. `rustuya/event/{type}/{name}/{dp}`: a device with
a name retains; a nameless one has *that device's* snapshot published
no-retain (logged at `debug`). If both `{name}` and `{cid}` are referenced,
**any one** resolvable value is enough. Case ③ — e.g. `rustuya/event/{type}`:
the startup warning's "…will be dropped" means the **retain flag** is
dropped, **not** the message — the snapshot is still published, just
`retain=false`.

#### ⚠️ Retaining by `{name}`/`{cid}` without `{id}` — collision hazard

`{id}` is unique per device; `{name}` and `{cid}` are **not guaranteed
unique**. If you build a retained state topic from a non-unique identifier
(e.g. `rustuya/event/{type}/{name}` with no `{id}`), then **two devices that
share a name render the same `state` topic**. Because that topic is
*retained*, their snapshots **overwrite each other on the broker** —
last-writer-wins — and a re-subscribing consumer sees a single payload that
flip-flops between the two devices' DPS. The bridge's caches stay correct
(they're keyed by the unique device id — see §4.1), and the scavenger still
works (the name *is* resolvable), so **nothing logs a warning** for this:
the gate only checks "is the identifier resolvable?", not "is it unique?".

Guidance: if you want retained per-device state, **put `{id}` in the event
topic** (or the payload). Use `{name}`/`{cid}` for retain *only* when you can
guarantee they're unique across your fleet — otherwise keep them as
*additional* segments alongside `{id}`, never as the sole identifier. The
same caution applies to `{cid}` when sub-devices on different gateways can
share a cid.

#### Orthogonal: the `{type}` warning

Independent of the identifier gate, if `mqtt_retain=true` and `{type}` is
absent from **both** the event topic and payload template, the bridge emits a
**second** one-time startup warning ([BridgeContext::new](../src/bridge.rs)):
active/passive/state then collide on one topic and live subscribers can't
tell deltas from the retained snapshot. This warning changes nothing about
retain or publishing — it's purely advisory. The two startup warnings are
independent: `event/{type}` → identifier warn only; `event/{id}` → `{type}`
warn only; `event` alone → **both**.

### 4.5 The seed phase — recovering broker state on startup

On bridge startup in cache mode, the cache is empty. If the bridge starts
publishing snapshots immediately, the first publish for each device
contains only whatever DPs that device has reported so far in this
session — usually one (whatever active fired first). That partial
snapshot then overwrites the broker's prior full-state retained:
exactly the bug we fixed in §4.1, just at startup instead of mid-run.

The fix: **subscribe to your own state topic on connect, seed the
cache from broker-retained snapshots, then start publishing**.

The seed loop in [spawn_mqtt_task](../src/bridge.rs):

1. On first `ConnAck`, subscribes to the state wildcard (event topic
   with `{type}=state` and other placeholders as `+`).
2. Receives broker-retained payloads. For each, parses the payload as
   the cached snapshot for that device and calls `cache.fill_missing`
   — fills only empty slots, so anything the bridge already learned
   from a live device event during the seed window takes precedence.
3. Tracks the timestamp of the last seed message received.
4. Exits when either:
   - **Quiet period** — 200ms with no new seed message (broker
     finished bursting retained); or
   - **Hard cap** — 5s absolute, regardless. Protects against a
     broken or unresponsive broker.
5. Unsubscribes from the state wildcard and flips `seed_done`. Any
   devices whose cache changed during the seed window (queued in
   `seed_pending` by the publish gate) get one snapshot publish each.

During the seed window:
- **Deltas still publish** to `{type}=active`/`{type}=passive` (direct route
  is ungated). HA event automations and readback observability work normally.
- **Snapshot publishes are deferred** — gated on `seed_done`. The
  changed device is recorded in `seed_pending` for the flush at seed
  end.

The seed phase is one-shot per bridge lifetime. On MQTT reconnect, the
bridge does *not* re-seed — the in-memory cache is already the source
of truth at that point.

### 4.6 Seed limitations

- **Non-JSON payload templates disable seed.** The seed parser uses a
  sentinel-substitution + JSON-tree-walk reverse parser
  ([src/payload.rs](../src/payload.rs)), so any
  *JSON-shaped* `mqtt_payload_template` works:
  `{value}` (default), `{"v":{value}}`, `{"type":"{type}","value":{value}}`,
  `{"id":"{id}","data":{"dps":{dps}}}`, etc. Only text-style templates
  (`v={value};ts={timestamp}` — not valid JSON even after sentinel
  substitution) can't be reversed. Those skip the seed phase, pre-flip
  `seed_done=true`, and the first publish per device overwrites the
  broker's prior retained — a one-time partial state until the device
  reports its full state via an active push or DP_QUERY response.
- **Hard cap can truncate large fleets.** With 1000+ devices on a slow
  broker, 5s may not be enough to receive all retained messages.
  Uncached devices get the same first-publish overwrite as the
  unparseable template case. There is no config knob for the hard cap
  in v1 (YAGNI); add an issue if you hit this.

### 4.7 The scavenger

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

### 4.8 The "deleting the state file" hazard

This is the operational footgun the README warns about. The chain:

1. You enable `mqtt_retain = true`.
2. You register 20 devices. Each publishes state → 20 retained messages on
   the broker.
3. You decide to "reset" the bridge: `systemctl stop` + `rm rustuya.json`
   + `systemctl start`.
4. Bridge starts fresh, knows about zero devices, doesn't publish anything.
5. **The broker still has all 20 retained messages.** Any new subscriber
   sees ghost device state from devices the bridge can no longer manage.

The scavenger runs in response to `remove`/`clear` actions while the
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

### 4.9 The bridge config topic

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
normal path. The one failure both paths share — power loss with a co-located
broker, where the will never fires — is what the startup liveness probe
(§7.1) exists to recover from.

### 4.10 When the broker doesn't honor retain or LWT

Several bridge features assume the broker treats `retain=true` and Last
Will messages the way the MQTT spec describes. Some hosted MQTT services,
strict broker ACL policies, or older brokers may strip one or both. The
silent failure modes:

| Broker behavior        | What breaks                                                      |
| ---------------------- | ---------------------------------------------------------------- |
| Retain stripped        | Scavenger has nothing to clear (no retained messages to begin with — but also no stale ones to worry about). Status dashboards subscribing to `{root}/bridge/config` see nothing until the next bridge restart publishes live. |
| LWT stripped (only)    | If the bridge crashes, the retained `bridge/config` from the prior run stays on the broker. The next startup's liveness probe (§7) pings `status`, gets no answer from the dead session, and after ~24s treats the retained config as a stale ghost and **starts anyway** — no manual cleanup. The only visible cost is a one-time ~24s startup delay on the restart following an ungraceful exit. |
| Both stripped          | Duplicate-instance detection effectively no-ops (nothing to see). Scavenger no-ops. The bridge will start under any conditions. |

The retain check in §4.4 only verifies that the *templates* allow
scavenging — it can't detect a broker that drops retain. If you have
`mqtt_retain = true` set but no retained messages ever accumulate on the
broker after running for a while, your broker is probably stripping
retain.

### 4.11 `reconfigure` — applying template/retain changes without re-registering

The bridge reads its config only at startup, so changing the topic/payload
templates or `mqtt_retain` requires a restart. A naive restart orphans the
old-scheme retained snapshots on the broker (see §4.8 for the same hazard via
the state file). [reconfigure](../src/bridge.rs) (action `reconfigure`) is the
clean path:

0. **Skip-when-unchanged guard** — [scavenge_config_unchanged](../src/bridge.rs)
   re-reads the `--config` file and compares the *scavenge-relevant* fields
   (broker, root, event topic, message topic, `mqtt_retain` — **not** the
   payload template, which doesn't relocate retained topics) against the running
   in-memory config. The broker counts because switching brokers orphans the
   old broker's retained; the purge runs against the old (in-memory) broker
   before the restart, so a broker change must not be skipped. If they match,
   the retained topics won't move on restart,
   so the purge (steps 1–2) is skipped entirely and `reconfigure` is a plain
   clean restart — a casual `reconfigure` on an unchanged scheme won't blank
   valid state. The guard errs toward purging: no config file, an
   unreadable/unparseable file, a differing field, or a CLI/env override (which
   makes the file value diverge from in-memory) all fall through to the purge.
   This is the safe direction — a wrongly-skipped purge would orphan retained
   permanently, whereas a needless purge just re-syncs after restart.
1. **Latch retain off** — sets `retain_suppressed` (an `AtomicBool` on
   `BridgeContext`). The outbound publish loop in `spawn_mqtt_task` ANDs every
   message's `retain` with `!retain_suppressed`, so from this instant no new
   retained snapshots land — *even ones already queued* in the 100-deep channel,
   because the override is applied at the `client.publish` call, not at message
   creation. Live (non-retained) delivery continues, so unlike a hard publish
   stop there is **no event loss**.
2. **Purge** — [purge_all_retained](../src/bridge.rs) connects a transient MQTT
   client (its own client_id, like the scavenger), subscribes to the event +
   message topic wildcards under the *current* (old) templates, and clears every
   retained message it receives until `scavenger_timeout_secs` of idle. It skips
   non-retained messages, the `bridge/config` topic, and its own empty clears
   echoed back (empty payload). Unconditional — no per-device target matching,
   since a template migration clears everything anyway. Returns the count
   cleared (logged).
3. **Exit** — fires `self.cancel.cancel()`, which the server `run()` loop selects
   on and runs the normal graceful shutdown (`close()` → save state with
   **device configs intact** → clear `bridge/config` → disconnect). A process
   supervisor restarts the bridge into the edited config.

Key properties:

- **Uses in-memory (old) templates**, never re-reads the config file — so editing
  the config file *before* invoking is correct and necessary (the file change
  only takes effect on the supervisor's restart). Triggering before editing
  reloads the unchanged config.
- **Always restarts.** No broker → warns, skips the purge, still exits. Retain
  off → warns (nothing the bridge published to clear), still purges any stale
  retained and exits. This keeps "edit config → reconfigure" a single uniform
  habit rather than a conditional one.
- **Supervisor-dependent.** `cancel` makes the process exit; a supervisor
  (systemd `Restart=always`, Docker policy) must bring it back. `reconfigure`
  logs whether one was detected via `INVOCATION_ID` / `JOURNAL_STREAM`.
- **No new magic numbers** — the purge idle window reuses the existing
  `scavenger_timeout_secs` knob; bump it for large fleets on slow brokers.
- `retain_suppressed` is never cleared — the process exits shortly after it's
  set, and a fresh `BridgeContext` starts with it `false`.
- **Transient gap in retained state.** Between latching retain off and the
  restart re-seeding the cache (§4.5), the broker briefly holds no fresh
  snapshot. Live deltas keep flowing throughout, and the seed phase rebuilds
  the cache from whatever survived; to refresh immediately rather than wait for
  the next device push, send `get` to your devices after the restart.

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

The mechanism is built on the bridge config topic (§4.9): a live bridge holds
a retained `{root}/bridge/config` carrying its `session_id`. The hard part is
telling a *live* incumbent apart from a *stale ghost* — a retained config left
behind by an unclean exit whose LWT never cleared it (§7.1).

**At startup** ([check_existing_instance](../src/bridge.rs)) runs two phases:

1. Generate a `session_id = "sid_<unix_millis>"` for this instance, then
   subscribe to `{root}/bridge/config` with a temporary client.
2. **Phase 1 — observe the sentinel.** Wait 500ms for a retained payload with
   a `session_id` field. None arrives → the path is clear, start immediately
   (fast path, no probe).
3. **Phase 2 — liveness probe.** A sentinel exists, but presence alone doesn't
   prove the owner is alive. Ping the `status` action on the command topic and
   listen on the bridge-level response topic, up to `PROBE_ATTEMPTS` (12) times
   at `PROBE_INTERVAL` (2s) apart:
   - **Any response** → a live incumbent owns this `(broker, root_topic)`.
     Bail with "Duplicate instance detected".
   - **Sustained silence** for the full ~24s budget → treat the retained
     config as a stale ghost, log a WARN, and start anyway (the recovery
     path — §7.1).

The probe returns the instant the first response arrives and is biased
generously toward "alive" (many retries over a wide window), so a
momentarily-disconnected incumbent is never mistaken for a ghost — only
sustained silence proceeds. The ping rides the configured command topic
(`action = "status"`, every other variable a non-empty placeholder so it
matches the incumbent's subscription) and the reply lands on the bridge-level
response topic — `probe_command_topic` / `probe_response_topic` build both
through the same `replace_vars` translator as live traffic, so the probe
tracks whatever templates you configured. Responses are non-retained, so a
dead instance's old reply can't masquerade as liveness.

**At runtime** (the sid guard):

The main MQTT loop also subscribes to the config topic. If it ever sees a
config payload with a `session_id` different from its own, it logs an error
and self-cancels. This handles the case where a *second* bridge starts
after the first — both run their startup check, but the second one's
publish wins because the first one already shut down on receipt. It is also
the backstop if two live instances ever slip past the startup gate.

### 7.1 Stale ghosts, LWT, and why the probe exists

Two paths normally clear the retained `bridge/config` when a bridge goes away:

- **Graceful shutdown** — the bridge publishes the empty retained payload
  itself and waits for the PubAck (§8.3).
- **Crash** (kill -9, OOM, broker-side disconnect) — the broker publishes the
  empty retained payload as the bridge's Last Will, clearing the topic so the
  next startup sees nothing.

Both rely on *something* clearing the sentinel. The one case neither covers is
**power loss with a co-located broker**: the broker dies along with the bridge
before it can publish the will, then on reboot restores the stale retained
config from its own persistence. The will never fires — there was no live
broker to fire it — so the sentinel sits there permanently.

This is the case the liveness probe (Phase 2 above) covers: a ghost never
answers `status`, so after the ~24s budget the bridge declares the retained
config stale and starts on its own. A live incumbent always answers within
milliseconds, so the probe costs real wall-clock time *only* against a genuine
ghost — presence alone would instead block the restart indefinitely.

The probe's `PROBE_ATTEMPTS * PROBE_INTERVAL` budget is the upper bound on
this recovery latency, deliberately kept under the broker's LWT/keep-alive
fallback so the probe — not the will — is what normally resolves a ghost.

**Limitations**:

- Only protects against duplicates on the *same* `(broker, root_topic)`.
  Two bridges with different `mqtt_root_topic` won't collide.
- The 500ms Phase-1 window is a heuristic. On a very slow broker connection
  startup may observe no sentinel before it arrives; the runtime sid guard
  catches the resulting overlap within seconds.
- The probe trades ~24s of startup latency (ghost case only) for not needing
  manual recovery. A live incumbent is detected immediately, so the cost is
  paid only when there's genuinely nothing alive to collide with.

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

### 8.4 Packet size & the `status` paging gotcha

rumqttc caps **both** incoming and outgoing packets at a tiny **10 KiB by
default** — and crucially, the outgoing check fires *client-side, before the
packet ever reaches the broker*: an oversized publish surfaces as
`Cannot send packet of size '…'. It's greater than the broker's maximum
packet size of: '10240'` (misleading — that 10 KiB is rumqttc's own default,
not the broker's) and wedges the connection on a 10s retry loop, blocking
*all* other publishes. The bridge lifts this with
`set_max_packet_size(MAX_MQTT_PACKET_SIZE, …)` ([1 MiB](../src/bridge.rs)) so
the client is never the bottleneck; the broker still enforces its own limit.

This bit `status` hardest: it serialized **every** registered device into one
reply (~127 bytes each), so a 1000-device fleet produced a ~127 KB packet that
the client refused. `status` is now **paginated** —
[get_bridge_status](../src/bridge.rs) returns the `[offset, offset+limit)`
window of id-sorted devices (`limit` default
[`STATUS_DEFAULT_PAGE`](../src/bridge.rs) = 50, capped at
[`STATUS_MAX_PAGE`](../src/bridge.rs) = 500), always alongside the full
`device_count`, plus `offset`/`limit`/`returned`/`has_more`. The default reply
stays a few KB regardless of fleet size; page through a large fleet with
`{"action":"status","offset":50}`. Small fleets (≤ 50) are unchanged bar the
added metadata fields.

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

[responses_for_results](../src/handlers.rs) does this for you — a `set`/`get`
that hits exactly **one** target and succeeds publishes no response (it's
redundant; the device's own event/state already reflects it). You still get a
response on *error*, and one **per device** when a command fans out to more
than one target. Don't subscribe to `{root}/response/#` expecting an ack for
every single-target set; you'll only see errors and fan-out responses.

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

## 11. Python binding API — `pyrustuyabridge`

Besides the `PyBridgeServer` class, the [pyrustuyabridge](../python/src/lib.rs)
module exposes six free functions so any Python code can interpret the
bridge's wire format **identically to the bridge itself**. Every one is a thin wrapper over the canonical Rust implementation
in [src/template.rs](../src/template.rs) / [src/payload.rs](../src/payload.rs)
— there is no second implementation to drift out of sync. If you need to match
the bridge's topic/payload behavior from Python, call these rather than
re-deriving the logic.

They split into two groups: **forward/topic helpers** (build outbound, match
inbound topics) and **parsers** (interpret payloads).

| Function | Signature | Group | Role |
| --- | --- | --- | --- |
| `tpl_to_wildcard` | `(template, root_topic) -> str` | forward | Topic template → MQTT subscription wildcard (known keys → `+`). Mirrors what the bridge subscribes with. |
| `render_template` | `(template, vars: dict) -> str` | forward | Substitute `{key}` placeholders from `vars`. Unknown keys are left as the literal `{key}` (same as the bridge). |
| `match_topic` | `(topic, template) -> dict \| None` | parser (topic) | Extract `{id}`/`{dp}`/… from a received topic; `None` if it doesn't match. Variable-free templates do exact-string compare. |
| `parse_payload` | `(payload, vars: dict) -> value` | parser (command) | The bridge's permissive **inbound command** parser (`parse_mqtt_payload`): merges topic vars, wraps scalars under `dps` when `{dp}` is present, applies the `set` heuristic (§9). |
| `parse_payload_with_template` | `(payload, template) -> dict \| None` | parser (reverse) | **Reverse of `render_template` for payloads** — given a payload the bridge rendered and the template, recover each placeholder's captured value. `None` when not reversible (see below). This is the seed-phase parser (§4.5–4.6). |
| `validate_payload_template` | `(template) -> (ok: bool, message: str)` | parser (reverse) | Pre-flight check: can this payload template be reverse-parsed at all? `message` is human-readable and explains the failure when `ok` is false. |

### 11.1 The reverse parser is a *partial* inverse

`parse_payload_with_template` is **not** a total inverse of `render_template`,
and that's by design. It recovers placeholder values by walking the template
and payload as parallel JSON trees and capturing whatever the payload holds at
each placeholder slot. This is mathematically exact **only when** the template
is JSON-shaped (valid JSON after each placeholder is replaced by a value slot)
and every placeholder occupies a complete JSON value — then JSON's own
delimiters make each placeholder's boundary unambiguous and round-tripping is
lossless and type-exact.

Outside that domain it **fails closed** (`None`) rather than guessing:

- non-JSON / text-style templates (`v={value};ts={timestamp}`) — not valid
  JSON even after sentinel substitution;
- placeholders concatenated or embedded in a string literal (`"pre-{value}"`,
  `"{a}{b}"`) — boundary is genuinely ambiguous, so the substituted result
  isn't valid JSON and the parse declines;
- payload structure doesn't match the template (different keys, array length,
  literal mismatch);
- no recognized placeholders in the template.

There is no path that returns a *wrong* answer — the structural walk matches
object size/keys, array length, and literals strictly. Use
`validate_payload_template` at startup to surface a non-reversible
`mqtt_payload_template` before it silently disables the seed phase.

What forward rendering injects but the reverse can't (and needn't) recover —
`{timestamp}` (generated at render time, not input state), the per-DP identity
in single-DP mode (it lives in the **topic**, not the payload — recovered
separately from `match_topic`), and optional metadata vars that render to `""`
— is either re-derivable elsewhere or irrelevant to DPS state, so seed recovery
isn't affected. See §4.5–4.6 for how the seed phase uses this.

### 11.2 Example

```python
import pyrustuyabridge as rb

# Forward: build what the bridge would publish
topic = rb.render_template("{root}/event/{id}/{dp}",
                           {"root": "rustuya", "id": "ebabc", "dp": "1"})
# -> "rustuya/event/ebabc/1"

# Reverse: recover placeholders from a rendered payload
caps = rb.parse_payload_with_template(
    '{"type":"passive","value":true}',
    '{"type":"{type}","value":{value}}',
)
# -> {"type": "passive", "value": True}

# Pre-flight a user's template
ok, msg = rb.validate_payload_template("v={value};ts={timestamp}")
# -> (False, "payload template '...' isn't valid JSON after placeholder ...")
```

---

## Appendix: where to look in the source

| If you're investigating...               | Start at                                       |
| ---------------------------------------- | ---------------------------------------------- |
| Why an add changed something else        | [add_device](../src/bridge.rs)      |
| Why a retain didn't fire                 | [IdentifierSet](../src/bridge.rs)       |
| Why a retained message isn't clearing    | [spawn_retain_scavenger](../src/bridge.rs) |
| How to apply a template/retain change     | [reconfigure](../src/bridge.rs), [purge_all_retained](../src/bridge.rs) (§4.11) |
| Why a topic substitution is wrong        | [render_template](../src/bridge.rs), [replace_vars](../src/bridge.rs) |
| Why a command isn't matching             | [parse_mqtt_payload](../src/bridge.rs), [compile_topic_regex](../src/bridge.rs) |
| Why a sub-device command goes nowhere    | [get_connected_device](../src/bridge.rs), [resolve_cid](../src/bridge.rs) |
| Why the listener seems wedged            | [spawn_device_listener](../src/bridge.rs) |
| Why MQTT keeps reconnecting              | [spawn_mqtt_task](../src/bridge.rs)   |
| Why two bridges fought                   | [check_existing_instance](../src/bridge.rs) |
| Why the state file looks weird           | [load_state](../src/config.rs)        |
