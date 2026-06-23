# Rustuya Bridge

[![Docker Publish](https://github.com/3735943886/rustuya-bridge/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/3735943886/rustuya-bridge/actions)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/3735943886/rustuya?sort=semver)](https://hub.docker.com/r/3735943886/rustuya)
[![Docker Pulls](https://img.shields.io/docker/pulls/3735943886/rustuya)](https://hub.docker.com/r/3735943886/rustuya)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An MQTT-based bridge server for managing Tuya devices via [`rustuya`](https://github.com/3735943886/rustuya).

```text
                      Tuya local protocol                          MQTT
                   (encrypted TCP · rustuya)               (commands ↔ events)

  ┌──────────────────┐                  ┏━━━━━━━━━━━━━━━━━━┓                  ┌──────────────────┐
  │   Tuya devices   │                  ┃  Rustuya Bridge  ┃                  │   MQTT  broker   │
  │   ------------   │                  ┃  --------------  ┃                  │   ------------   │
  │  plugs · lights  │   ════════════   ┃  device actors   ┃   ════════════   │  Home Assistant  │
  │ gateways · subs  │                  ┃  + state cache   ┃                  │  scripts · apps  │
  └──────────────────┘                  ┗━━━━━━━━━━━━━━━━━━┛                  └──────────────────┘
```

The bridge talks to each Tuya device over its native encrypted local protocol
(via `rustuya`, one long-lived connection per device) and exposes them on MQTT:
publish a command to a topic and it reaches the device; device state and events
are published back out — so any MQTT client (Home Assistant, scripts,
dashboards) controls the whole fleet without touching the Tuya protocol.

## How to Run

> **Standalone mode**: If `--mqtt-broker` is omitted, the bridge runs in
> debug/standalone mode — devices are still tracked and persisted to the
> state file, but no MQTT publish/subscribe occurs. A warning is logged at
> startup.

### Pre-built Binary (recommended)
Download the archive for your platform from the
[Releases](https://github.com/3735943886/rustuya-bridge/releases) page,
extract, and run:
```bash
tar -xzf rustuya-bridge-<target>.tar.gz
./rustuya-bridge --mqtt-broker mqtt://localhost:1883
```

| Platform | Target |
|---|---|
| Linux x86_64 | `x86_64-unknown-linux-musl` |
| Linux ARM64 (RPi 4/5, ARM servers) | `aarch64-unknown-linux-musl` |
| Linux ARMv7 (RPi 3, Zero 2 W) | `armv7-unknown-linux-musleabihf` |
| macOS Intel | `x86_64-apple-darwin` |
| macOS Apple Silicon | `aarch64-apple-darwin` |
| Windows | `x86_64-pc-windows-msvc` |

For long-running deployments, run the binary under a process supervisor such
as **systemd** (Linux), **launchd** (macOS), or **supervisord** so that the
bridge restarts automatically on crash and starts at boot.

#### One-line install (Linux + systemd)
[`scripts/bridgectl.sh`](scripts/bridgectl.sh) installs the latest release,
creates a dedicated `rustuya` user, writes a default config, registers the
systemd unit, and starts the service. It also copies itself to
`/usr/local/bin/bridgectl` so subsequent management runs from anywhere.

```bash
curl -fsSL https://raw.githubusercontent.com/3735943886/rustuya-bridge/master/scripts/bridgectl.sh \
  | sudo bash -s -- install --yes
```

The service starts immediately with `mqtt://localhost:1883` as the default
broker. To point it at your own broker (or change topics, retain, log level,
etc.), edit the auto-generated config and restart:

```bash
sudo nano /var/lib/rustuya/config.json     # see Configuration File below for fields
sudo systemctl restart rustuya-bridge
journalctl -u rustuya-bridge -f            # tail logs
```

Subsequent management:
```bash
bridgectl                  # show status / latest version / service state
sudo bridgectl upgrade     # pull the latest release and restart
sudo bridgectl remove      # stop + uninstall (keeps data dir and user)
sudo bridgectl purge       # also wipe data dir, user, and the helper itself
bridgectl help             # full command + option reference
```

All `install` / `upgrade` / `remove` / `purge` accept `--yes` (`-y`) to skip
confirmation prompts — required when running non-interactively (e.g. via
`curl | sudo bash`).

#### Pre-release channel

`bridgectl` defaults to the stable channel (GitHub's `releases/latest`,
which excludes pre-releases). Pass `--prerelease` to target the newest
release including `-rc` / `-alpha` / `-beta` builds:

```bash
sudo bridgectl install   --prerelease   # install the latest RC
sudo bridgectl upgrade   --prerelease   # upgrade to a newer RC
bridgectl status         --prerelease   # show the newest RC vs installed
```

`status` and `upgrade` are semver-aware: if you have an RC installed and
run them without `--prerelease`, the channel's latest stable is reported
honestly, but the prompt is reframed as a **downgrade** so you don't yes
through a regression. To roll back from a problematic RC to the last
stable build:

```bash
sudo bridgectl upgrade   # detects RC > stable, prompts "Downgrade ... ?"
```

### Build from Source
Clone and run directly with cargo, or produce a release binary you can copy
elsewhere:
```bash
git clone https://github.com/3735943886/rustuya-bridge
cd rustuya-bridge

# One-shot run (development)
cargo run --release -- --mqtt-broker mqtt://localhost:1883

# Build only — binary at ./target/release/rustuya-bridge
cargo build --release
./target/release/rustuya-bridge --mqtt-broker mqtt://localhost:1883
```

### Docker
Run with **host network mode** to ensure Tuya device discovery works correctly:
```bash
docker run -d \
  --name rustuya \
  --network host \
  --restart unless-stopped \
  -e MQTT_BROKER=mqtt://localhost:1883 \
  -v $(pwd)/data:/data \
  3735943886/rustuya
```

`--restart unless-stopped` lets the container come back after a `reconfigure`
(which exits the process cleanly for the new config to take effect — see
[Changing templates or retain](#changing-templates-or-retain)).

## Configuration

The bridge can be configured via command-line arguments or environment variables:

| Argument | Environment Variable | Default | Description |
|----------|----------------------|---------|-------------|
| `--config`, `-C` | `CONFIG` | - | Path to a JSON configuration file |
| `--mqtt-broker`, `-m` | `MQTT_BROKER` | - | MQTT broker address. Scheme selects transport: `mqtt://`/`tcp://` (plaintext, default port 1883) or `mqtts://`/`ssl://` (TLS, default port 8883). Credentials inline (`mqtt://user:pass@host`) or via `--mqtt-user`/`--mqtt-password`. TLS validates against the system root CA store, so public-CA brokers work out of the box; self-signed / private-CA brokers are not supported (no custom CA option). |
| `--mqtt-root-topic` | `MQTT_ROOT_TOPIC` | `rustuya` | MQTT root topic prefix |
| `--mqtt-command-topic`| `MQTT_COMMAND_TOPIC` | `{root}/command` | MQTT topic for commands |
| `--mqtt-event-topic` | `MQTT_EVENT_TOPIC` | `{root}/event/{type}/{id}` | MQTT topic for events |
| `--mqtt-scanner-topic` | `MQTT_SCANNER_TOPIC` | `{root}/scanner` | MQTT topic for scanner results |
| `--mqtt-client-id` | `MQTT_CLIENT_ID` | `{root}-bridge` | MQTT client identifier (defaults to the root topic with a `-bridge` suffix, e.g. `rustuya-bridge`) |
| `--mqtt-message-topic` | `MQTT_MESSAGE_TOPIC` | `{root}/{level}/{id}` | MQTT topic for errors/responses (e.g., `tuya/logs/{level}`) |
| `--mqtt-payload-template` | `MQTT_PAYLOAD_TEMPLATE` | `{value}` | MQTT payload template (e.g., `{"val": {value}}`) |
| `--mqtt-retain` | `MQTT_RETAIN` | `false` | `true` enables the cache + snapshot retain model: live deltas publish no-retain to `{type}=active`/`{type}=passive`, and merged full-state snapshots publish retained to `{type}=state` — recommended when subscribers need to recover device state immediately on reconnect. `false` (default) passes events through with no retain. See [docs/internals.md §4](docs/internals.md). |
| `--state-file`, `-s` | `STATE_FILE` | `rustuya.json` | Path to the file where device snapshots are stored |
| `--save-debounce-secs`| `SAVE_DEBOUNCE_SECS` | `30` | Seconds to wait before saving state file (debounce) |
| `--scavenger-timeout-secs`| `SCAVENGER_TIMEOUT_SECS` | `1` | Seconds the retain scavenger waits for retained MQTT messages before exiting after `remove`/`clear`. Raise on slow brokers. |
| `--connect-concurrency`| `CONNECT_CONCURRENCY` | `128` | Max devices establishing a connection concurrently (handshake cap). Bounds the onboarding "connect storm" when a large fleet is added at once — once connected a device is cheap, so only the establishment phase is capped. `0` disables the cap (unbounded). |
| `--log-level`, `-l` | `LOG_LEVEL` | `info` | Log level: `error`, `warn`, `info`, `debug`, `trace` |

### Configuration File
A JSON file can be used to manage all settings. Command-line arguments take priority over settings in the config file.

**config.json example:**
```json
{
  "mqtt_broker": "mqtt://localhost:1883",
  "mqtt_root_topic": "myhome/tuya",
  "mqtt_event_topic": "tuya/{name}/{dp}/state",
  "mqtt_payload_template": "{value}",
  "save_debounce_secs": 10
}
```

Run with:
```bash
./rustuya-bridge --config config.json
```

### MQTT Usage
- **Commands**: Publish a JSON payload to the `mqtt-command-topic`.
- **Events**: Device events are published to the `mqtt-event-topic`, tagged with a `{type}`:
  - **Active**: real-time push initiated by the device, physical interaction, firmware-driven change, or response to a SET. Payload arrives with the `data.dps` wrapper. Published **no-retain**.
  - **Passive**: status reports without an initiating change (DP_QUERY response, or periodic report). Payload arrives with root `dps`, no `data` wrapper. Published **no-retain**.
  - **State** (only with `--mqtt-retain true`): the bridge's own merged full-state snapshot, published **retained** to `{type}=state` so late subscribers recover current state on connect. Not a device event type — it's synthesized by the cache. See [docs/internals.md §4](docs/internals.md).
- **Responses/Errors**: Command results and errors are published to `mqtt-message-topic`.
  - **Success**: Published with `{level}` set to `response`.
  - **Error**: Published with `{level}` set to `error`.
  - **Retain**: command responses/errors (the ack for a command you sent) are always **no-retain** — they're one-shot. **Device-reported errors** — the `errorCode` the bridge pushes from device events, where `errorCode: 0` means *connected* — instead follow `--mqtt-retain`: **retained when on**, so the last-known connect/error state stays on the broker and a late subscriber reads online (`0`) vs offline (non-zero) without polling; no-retain when off.
- **Scanner**: Results are published to `mqtt-scanner-topic`.

#### Why `--mqtt-retain` emits a separate `state` snapshot

With `--mqtt-retain true` (recommended when subscribers need to recover device state on reconnect), the bridge splits every device update into a **live no-retain delta** (on `{type}=active`/`{type}=passive`) and a **retained full-state snapshot** on a separate `{type}=state` topic. A single DP change therefore shows up as two messages — intentional, not a duplicate:

```text
rustuya/event/active/aabbccdd11223344eeff  →  {"1":true}                  (no retain — the delta that just arrived)
rustuya/event/state/aabbccdd11223344eeff   →  {"1":true,"2":50,"9":0}     (retain — full merged snapshot)
```

- **`active` / `passive`** — the raw delta exactly as it arrived from the device, published **no-retain**. `active` = a device-initiated change (button, toggle); `passive` = a readback or periodic report (e.g. the reply to a `get`/`status`). Use these as automation triggers or to observe that *something arrived*. Both fire on every update — so a `get` readback is never silent, even when it doesn't change anything.
- **`state`** — the **full merged snapshot** of the device, **retained**. A subscriber that connects late (after a reconnect or restart) reads it once and immediately knows the current state without waiting for the next device update. Deduped: republished only when a value actually changed.

If you only need *current state*, subscribe to `state`. If you need *event semantics*, watch `active` (and `passive` for readbacks). Many consumers want both — the deltas for the moment, `state` for state at rest.

In pass-through mode (`--mqtt-retain false`, the default), there is no `state` snapshot: each event publishes a single no-retain `active`/`passive` message reflecting which Tuya cmd produced it, and that's it.

### Examples (MQTT)

You can interact with the bridge using any MQTT client (e.g., `mosquitto_pub/sub`).

By default, the command topic is `rustuya/command` and events are published under `rustuya/event/#`.

#### Monitor Events
Before running the commands below, you might want to open a new terminal and subscribe to all topics:
```bash
mosquitto_sub -h localhost -t "rustuya/#" -v
```

#### Monitor Responses
Subscribe specifically to command responses:
```bash
mosquitto_sub -h localhost -t "rustuya/response/#" -v
```

#### Register a WiFi Device
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{
  "action": "add",
  "id": "device_id",
  "key": "local_key",
  "ip": "device_ip"
}'
```

#### Register a Sub-Device (Zigbee/Bluetooth Gateway)
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{
  "action": "add",
  "id": "sub_device_id",
  "parent_id": "gateway_device_id",
  "cid": "sub_device_cid"
}'
```

#### Remove a Device
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{
  "action": "remove",
  "id": "device_id"
}'
```

#### Control a Device (Set DPs)
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{
  "action": "set",
  "id": "device_id",
  "dps": {"1": true, "2": 50}
}'
```

#### Query Bridge Status
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{"action": "status"}'
```

The reply always reports the full `device_count` and `mqtt_drop_count`, but
the `devices` list is **paginated** so the response stays within broker packet
limits at fleet scale (the default page is 50). The reply carries
`offset`/`limit`/`returned`/`has_more`; page through a large fleet with:
```bash
# next 50 devices
mosquitto_pub -h localhost -t "rustuya/command" -m '{"action": "status", "offset": 50}'
# bigger page (capped at 500) on a broker that allows large packets
mosquitto_pub -h localhost -t "rustuya/command" -m '{"action": "status", "limit": 200}'
```

#### Clear All Devices
```bash
mosquitto_pub -h localhost -t "rustuya/command" -m '{"action": "clear"}'
```

### MQTT Customization (Templates)

MQTT topics and payloads can be fully customized using templates.

#### Publishing Templates
- **Topic Template (`--mqtt-event-topic`)**: Used for device status updates.
- **Message Topic (`--mqtt-message-topic`)**: Used for errors and responses.

**Variables:**
- `{root}`: MQTT root topic prefix
- `{id}`: Device ID (or `bridge` for bridge-level responses)
- `{name}`: Device Name (or `bridge` for bridge-level responses)
- `{cid}`: Sub-device CID (if applicable, otherwise empty)
- `{type}`: `active` or `passive` (raw device deltas) or `state` (retained merged snapshot, `--mqtt-retain true` only) — only for the event topic
- `{dp}`: Data Point ID (only in single DP mode)
- `{value}`: Data Point Value (only in single DP mode)
- `{dps}`: JSON string of all Data Points
- `{timestamp}`: Unix timestamp
- `{level}`: `response` or `error` (only for message topic)

#### Command Topic Matching
If `--mqtt-command-topic` contains variables like `{id}`, the bridge will automatically extract them from the incoming topic.

**Example:**
- Command Topic: `tuya/command/{id}/set`
- Received on: `tuya/command/light_1/set` with payload `true`
- Action: Sets the device `light_1` to `true` (if `{dp}` is also in the topic, it sets that specific DP).

#### Multi-DP vs Single-DP Mode
- If `--mqtt-payload-template` contains `{value}` or `{dp}`, the bridge will publish a separate message for **each changed DP**.
- Otherwise, it will publish a **single message** containing all changed DPs in `{dps}`.

## API Summary

| Action | Parameters | Description |
| :--- | :--- | :--- |
| `add` | `id`, `key`?, `ip`?, `version`?, `name`?, `cid`?, `parent_id`? | Register a device. `key` (alias: `local_key`) required for direct devices. |
| `remove` | `id` or `name` | Unregister device(s). Alias: `delete`. Supports lists `[...]`. |
| `clear` | - | Remove all devices. |
| `status` | - | Get bridge and device status. Alias: `query`. |
| `get` | `id` or `name`, `cid`? | Query device status. Supports lists `[...]`. |
| `set` | `id` or `name`, `dps`, `cid`? | Set device state. Supports lists `[...]`. |
| `request` | `id` or `name`, `cmd`, `data`?, `cid`? | Send raw command. Supports lists `[...]`. |
| `sub_discover` | `id` or `name` | Trigger sub-device discovery (Gateways). |
| `scan` | - | Scan for local devices (UDP 6666/6667). |
| `reconfigure` | - | Clear old-scheme retained messages and restart to apply config changes. See [Changing templates or retain](#changing-templates-or-retain). |

### Target Selection Rules
1. **ID Priority**: If both `id` and `name` are provided, `id` is used and `name` is ignored.
2. **Lists Support**: Both `id` and `name` can be a single string or a list of strings.
   - Example: `"id": ["dev1", "dev2"]`
   - Example: `"name": ["kitchen", "living_room"]`

> **Note**: Add `"cid": "SUB_DEVICE_ID"` to `get`, `set`, or `request` for sub-device control.

## Event Topics
The bridge publishes events to the following MQTT topics:
- `mqtt-event-topic`: Device status changes (Active/Passive).
- `mqtt-message-topic`: Errors and logs.
- `mqtt-scanner-topic`: Results from the `scan` action. Returns an empty object `{}` when a scan cycle is finished.
- `{root}/bridge/config`: Retained snapshot of the running configuration
  (including the bridge `version`), published at startup and cleared on
  graceful shutdown (also serves as the presence/heartbeat topic). MQTT
  credentials (`mqtt_user`/`mqtt_password`) are **not** included; note that
  credentials embedded inline in the broker URL still are — prefer the
  user/password flags or env vars.

## Operational Notes

Day-to-day essentials. Each is covered in depth in
[`docs/internals.md`](docs/internals.md):

- **One bridge per broker + root topic.** Starting a second bridge against
  the same `(broker, mqtt_root_topic)` is detected and refused. After an
  unclean shutdown (crash, power loss) a stale lock may linger on the broker;
  the next start probes for a live instance and, if none answers, recovers
  automatically after ~24s — no manual cleanup needed.
  ([internals §7](docs/internals.md#7-duplicate-instance-detection))

- **State file.** Device registrations persist to `--state-file` (default
  `rustuya.json`) with debounced writes. The path is relative to the working
  directory — use an absolute path (or the Docker default
  `/data/rustuya.json`) under systemd. **With `--mqtt-retain` enabled, don't
  delete the state file to "reset" the bridge** — it leaves stale retained
  messages on the broker for devices it no longer knows about. Send a `clear`
  (or per-device `remove`) first. With the default `mqtt_retain = false`,
  deleting the file is harmless.
  ([internals §4.8](docs/internals.md#48-the-deleting-the-state-file-hazard))

### Changing templates or retain

The bridge reads its configuration **only at startup**, so changing the
topic/payload templates or `mqtt_retain` needs a restart. A naive restart
orphans the retained state snapshots already on the broker under the *old*
scheme (turn retain off and they're never cleared; change topics and they sit
as ghosts). The `reconfigure` action handles this cleanly:

```bash
# 1. Edit your config FIRST (file / flags / env) — the running bridge ignores
#    it until restart.
# 2. Then trigger reconfigure:
mosquitto_pub -h localhost -t "rustuya/command" -m '{"action": "reconfigure"}'
```

It stops retaining new publishes (live events keep flowing), clears the
old-scheme retained messages, then exits; a process supervisor (systemd
`Restart=always`, Docker restart policy) restarts it into the edited config
with device registrations preserved. More generally, *edit config →
reconfigure* is the uniform "apply a config change" path. The full mechanics —
the skip-when-unchanged guard, broker-change handling, and retain-off behavior
— are in
[internals §4.11](docs/internals.md#411-reconfigure--applying-templateretain-changes-without-re-registering).

## Further Reading

[`docs/internals.md`](docs/internals.md) — deep dive on internals: device
lifecycle, unified listener, template engine, retain scavenger, sub-device
routing, MQTT reconnection, state persistence, and operator tips. For
advanced users only; not needed to use the bridge.
