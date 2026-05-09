# Rustuya Bridge

[![Docker Publish](https://github.com/3735943886/rustuya-bridge/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/3735943886/rustuya-bridge/actions)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/3735943886/rustuya?sort=semver)](https://hub.docker.com/r/3735943886/rustuya)
[![Docker Pulls](https://img.shields.io/docker/pulls/3735943886/rustuya)](https://hub.docker.com/r/3735943886/rustuya)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An MQTT-based bridge server for managing Tuya devices via [`rustuya`](https://github.com/3735943886/rustuya).

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
  -e MQTT_BROKER=mqtt://localhost:1883 \
  -v $(pwd)/data:/data \
  3735943886/rustuya
```

## Configuration

The bridge can be configured via command-line arguments or environment variables:

| Argument | Environment Variable | Default | Description |
|----------|----------------------|---------|-------------|
| `--config`, `-C` | `CONFIG` | - | Path to a JSON configuration file |
| `--mqtt-broker`, `-m` | `MQTT_BROKER` | - | MQTT Broker address (e.g., `mqtt://user:pass@localhost:1883`) |
| `--mqtt-root-topic` | `MQTT_ROOT_TOPIC` | `rustuya` | MQTT root topic prefix |
| `--mqtt-command-topic`| `MQTT_COMMAND_TOPIC` | `{root}/command` | MQTT topic for commands |
| `--mqtt-event-topic` | `MQTT_EVENT_TOPIC` | `{root}/event/{type}/{id}` | MQTT topic for events |
| `--mqtt-scanner-topic` | `MQTT_SCANNER_TOPIC` | `{root}/scanner` | MQTT topic for scanner results |
| `--mqtt-client-id` | `MQTT_CLIENT_ID` | `rustuya-bridge` | MQTT client identifier |
| `--mqtt-message-topic` | `MQTT_MESSAGE_TOPIC` | `{root}/{level}/{id}` | MQTT topic for errors/responses (e.g., `tuya/logs/{level}`) |
| `--mqtt-payload-template` | `MQTT_PAYLOAD_TEMPLATE` | `{value}` | MQTT payload template (e.g., `{"val": {value}}`) |
| `--mqtt-retain` | `MQTT_RETAIN` | `false` | Retain flag for device state messages |
| `--state-file`, `-s` | `STATE_FILE` | `rustuya.json` | Path to the file where device snapshots are stored |
| `--save-debounce-secs`| `SAVE_DEBOUNCE_SECS` | `30` | Seconds to wait before saving state file (debounce) |
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
- **Events**: Device events are published to the `mqtt-event-topic`.
  - **Active**: Published when the payload contains `dps` data (e.g., state changes).
  - **Passive**: Published when the payload contains no `dps` data (e.g., state reports).
- **Responses/Errors**: Command results and errors are published to `mqtt-message-topic`.
  - **Success**: Published with `{level}` set to `response`.
  - **Error**: Published with `{level}` set to `error`.
- **Scanner**: Results are published to `mqtt-scanner-topic`.

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
- `{type}`: `active` or `passive` (only for event topic)
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
- `{root}/bridge/config`: Retained snapshot of the running configuration,
  published at startup and cleared on graceful shutdown (also serves as the
  presence/heartbeat topic).

## Operational Notes

### Duplicate Instance Detection
Running two bridges against the same MQTT broker + root topic is prevented.

### State File
Device registrations are persisted to `--state-file` (default `rustuya.json`)
with debounced writes. The path is treated as relative to the working
directory — use an absolute path (or the Docker default `/data/rustuya.json`)
when running under systemd.

> **If `--mqtt-retain` is enabled, do not delete the state file to "reset"
> the bridge.** The retain-scavenger only runs in response to `remove` /
> `clear` actions while the bridge is alive, so deleting the file leaves
> stale retained messages on the broker for devices it no longer knows
> about. Send a `clear` command (or per-device `remove`) first. With the
> default `mqtt_retain = false`, deleting the file is harmless.
