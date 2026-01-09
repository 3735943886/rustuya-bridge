# Rustuya Bridge

[![Docker Publish](https://github.com/3735943886/rustuya-bridge/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/3735943886/rustuya-bridge/actions)
[![Docker Image Version (latest semver)](https://img.shields.io/docker/v/3735943886/rustuya?sort=semver)](https://hub.docker.com/r/3735943886/rustuya)
[![Docker Pulls](https://img.shields.io/docker/pulls/3735943886/rustuya)](https://hub.docker.com/r/3735943886/rustuya)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A ZMQ-based bridge server for managing Tuya devices via `rustuya`.

## How to Run

### Direct Execution
```bash
cargo run --release -- --command-addr tcp://0.0.0.0:37358 --event-addr tcp://0.0.0.0:37359
```

### Docker Execution
Run with **host network mode** to ensure Tuya device discovery works correctly:
```bash
docker run -d \
  --name rustuya \
  --network host \
  -v $(pwd)/data:/data \
  3735943886/rustuya
```

## Configuration

The bridge can be configured via command-line arguments or environment variables:

| Argument | Environment Variable | Default | Description |
|----------|----------------------|---------|-------------|
| `--command-addr` | `ZMQ_COMMAND_ADDR` | `tcp://0.0.0.0:37358` | ZMQ **ROUTER** socket address for commands |
| `--event-addr` | `ZMQ_EVENT_ADDR` | `tcp://0.0.0.0:37359` | ZMQ **PUB** socket address for events |
| `--state-file` | `STATE_FILE` | `rustuya.json` | Path to the file where device configurations are stored |

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

## Usage Example (Rust)

The bridge uses the `zeromq` crate for async communication. You can use any ZMQ-compatible client, but here is an example using Rust and `tokio`.

**Cargo.toml**
```toml
[dependencies]
zeromq = "0.3"
tokio = { version = "1", features = ["full"] }
serde_json = "1.0"
```

**Client Implementation**
```rust
use zeromq::{ReqSocket, Socket, SocketRecv, SocketSend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut socket = ReqSocket::new();
    socket.connect("tcp://127.0.0.1:37358").await?;

    // 1. Add devices with names
    let add_cmd = serde_json::json!({
        "action": "add",
        "id": "DEV_ID_1",
        "name": "living_room",
        "key": "LOCAL_KEY",
    });
    socket.send(add_cmd.to_string().into()).await?;
    let res = socket.recv().await?;
    println!("Response: {:?}", res);

    // 2. Control multiple devices by name or list
    let set_cmd = serde_json::json!({
        "action": "set",
        "name": ["living_room", "kitchen"],
        "dps": {"1": true}
    });
    socket.send(set_cmd.to_string().into()).await?;
    
    Ok(())
}
```

### Async Communication
The bridge supports fully asynchronous communication using the `zeromq` crate. 

- **Bridge Side**: Built with `tokio` and `zeromq` for non-blocking I/O.
- **Client Side**: You can use the async `zeromq` crate (requires an async runtime like `tokio` or `async-std`) or any synchronous ZMQ library (like `zmq` crate) depending on your needs.


> **Note**: Add `"cid": "SUB_DEVICE_ID"` to `get`, `set`, or `request` for sub-device control.

## Event Topics
The bridge publishes events to the following topics:
- `device`: DP status changes and responses from devices.
- `scanner`: Results from the `scan` action. Returns an empty object `{}` when a scan cycle is finished.
