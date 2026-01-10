# Rustuya Bridge

[![Docker Publish](https://github.com/3735943886/rustuya-bridge/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/3735943886/rustuya-bridge/actions)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/3735943886/rustuya?sort=semver)](https://hub.docker.com/r/3735943886/rustuya)
[![Docker Pulls](https://img.shields.io/docker/pulls/3735943886/rustuya)](https://hub.docker.com/r/3735943886/rustuya)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A MQTT-based bridge server for managing Tuya devices via `rustuya`.

## How to Run

### Direct Execution
```bash
cargo run --release -- --mqtt-broker tcp://localhost:1883
```

### Docker Execution
Run with **host network mode** to ensure Tuya device discovery works correctly:
```bash
docker run -d \
  --name rustuya \
  --network host \
  -e MQTT_BROKER=tcp://your-broker:1883 \
  -v $(pwd)/data:/data \
  3735943886/rustuya
```

## Configuration

The bridge can be configured via command-line arguments or environment variables:

| Argument | Environment Variable | Default | Description |
|----------|----------------------|---------|-------------|
| `--config`, `-C` | `CONFIG` | - | Path to a JSON configuration file |
| `--mqtt-broker`, `-m` | `MQTT_BROKER` | - | MQTT Broker address (e.g., `tcp://user:pass@localhost:1883`) |
| `--mqtt-root-topic` | `MQTT_ROOT_TOPIC` | `rustuya` | MQTT root topic prefix |
| `--mqtt-command-topic`| `MQTT_COMMAND_TOPIC` | `{root}/command` | MQTT topic for commands |
| `--mqtt-event-topic` | `MQTT_EVENT_TOPIC` | `{root}/event` | MQTT topic for events |
| `--mqtt-client-id` | `MQTT_CLIENT_ID` | `rustuya-bridge` | MQTT client identifier |
| `--mqtt-topic-template` | `MQTT_TOPIC_TEMPLATE` | | MQTT topic template for active status (e.g., `tuya/{id}/state`) |
| `--mqtt-passive-topic-template` | `MQTT_PASSIVE_TOPIC_TEMPLATE` | | MQTT topic template for command responses |
| `--mqtt-message-topic-template` | `MQTT_MESSAGE_TOPIC_TEMPLATE` | | MQTT topic template for errors/logs (e.g., `tuya/logs/{level}`) |
| `--mqtt-payload-template` | `MQTT_PAYLOAD_TEMPLATE` | | MQTT payload template (e.g., `{"val": {value}}`) |
| `--state-file`, `-s` | `STATE_FILE` | `rustuya.json` | Path to the file where device configurations are stored |
| `--save-debounce-secs`| `SAVE_DEBOUNCE_SECS` | `30` | Seconds to wait before saving state file (debounce) |

### Configuration File
You can use a JSON file to manage all settings. Command-line arguments take priority over settings in the config file.

**config.json example:**
```json
{
  "mqtt_broker": "tcp://localhost:1883",
  "mqtt_root_topic": "myhome/tuya",
  "mqtt_topic_template": "tuya/{name}/{dp}/state",
  "mqtt_payload_template": "{value}",
  "save_debounce_secs": 10
}
```

Run with:
```bash
cargo run -- --config config.json
```

### MQTT Usage
- **Commands**: Publish a JSON payload to the `mqtt-command-topic`.
- **Responses**: The bridge publishes responses to `mqtt-command-topic/response` by default, or to the topic specified in the `response_topic` field of the command JSON.
- **Events**: Device events are published to `mqtt-event-topic/device`, and scanner results to `mqtt-event-topic/scanner`.

### MQTT Customization (Templates)

You can fully customize the MQTT topics and payloads using templates, similar to `tuya2mqtt.py`.

#### Publishing Templates
- **Active Topic (`--mqtt-topic-template`)**: Used for device-initiated status updates (push).
- **Passive Topic (`--mqtt-passive-topic-template`)**: Used for responses to commands (poll/set).
- **Message Topic (`--mqtt-message-topic-template`)**: Used for errors and logs.

**Variables:**
- `{id}`: Device ID
- `{name}`: Device Name
- `{dp}`: Data Point ID (only in single DP mode)
- `{value}`: Data Point Value (only in single DP mode)
- `{dps}`: JSON string of all Data Points
- `{timestamp}`: Unix timestamp
- `{level}`: Message level (only for message topic)

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
| `add` | `id`, `key`, `ip`?, `version`?, `name`? | Register a device. `name` is optional. |
| `remove` | `id` or `name` | Unregister device(s) by ID or Name. Supports lists `[...]`. |
| `clear` | - | Remove all devices. |
| `status` | - | Get bridge and device status. |
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
The bridge publishes events to the following MQTT topics (relative to `mqtt-event-topic`):
- `device/active`: Device-initiated status changes (push).
- `device/passive`: Responses to commands (poll/set).
- `message/{level}`: Errors and logs.
- `scanner`: Results from the `scan` action. Returns an empty object `{}` when a scan cycle is finished.
