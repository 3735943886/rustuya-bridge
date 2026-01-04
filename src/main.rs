use clap::Parser;
use futures_util::StreamExt;
use log::{error, info};
use rustuya::Manager;
use rustuya::protocol::CommandType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use zeromq::{PubSocket, RepSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

// --- Constants ---
const STATE_FILE: &str = "rustuya.json";
const DEFAULT_COMMAND_ADDR: &str = "tcp://0.0.0.0:37358";
const DEFAULT_EVENT_ADDR: &str = "tcp://0.0.0.0:37359";
const SAVE_DEBOUNCE_SECS: u64 = 30;

// --- Config & Context ---
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// ZMQ bind address for Commands (REP)
    #[arg(short = 'c', long, env = "ZMQ_COMMAND_ADDR", default_value = DEFAULT_COMMAND_ADDR)]
    command_addr: String,

    /// ZMQ bind address for Events (PUB)
    #[arg(short = 'e', long, env = "ZMQ_EVENT_ADDR", default_value = DEFAULT_EVENT_ADDR)]
    event_addr: String,

    /// State file path to persist registered devices
    #[arg(short = 's', long, env = "STATE_FILE", default_value = STATE_FILE)]
    state_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceConfig {
    id: String,
    ip: String,
    key: String,
    version: String,
}

struct BridgeContext {
    manager: Manager,
    event_tx: mpsc::Sender<ZmqMessage>,
    state_file: String,
    devices: RwLock<HashMap<String, DeviceConfig>>,
    save_tx: mpsc::Sender<()>,
}

// --- Implementation ---
impl BridgeContext {
    /// Atomically saves the current device state to a JSON file.
    async fn save_state(&self) {
        let devices = self.devices.read().await;
        let json = match serde_json::to_string_pretty(&*devices) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize devices: {}", e);
                return;
            }
        };

        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&self.state_file).parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!("Failed to create directories for state file: {}", e);
            }
        }

        // Write to temporary file first, then rename (atomic write)
        let tmp_path = format!("{}.tmp", self.state_file);
        if let Err(e) = tokio::fs::write(&tmp_path, json).await {
            error!("Failed to write temporary state file: {}", e);
            return;
        }

        if let Err(e) = tokio::fs::rename(&tmp_path, &self.state_file).await {
            error!("Failed to commit state file {}: {}", self.state_file, e);
            let _ = tokio::fs::remove_file(&tmp_path).await;
        } else {
            info!("State successfully persisted to {}", self.state_file);
        }
    }

    /// Triggers a debounced state save.
    fn request_save(&self) {
        let _ = self.save_tx.try_send(());
    }

    /// Publishes an event to the ZMQ PUB socket.
    async fn publish_event(&self, topic: String, payload: String) {
        let mut msg = ZmqMessage::from(topic);
        msg.push_back(payload.into());
        if let Err(e) = self.event_tx.send(msg).await {
            error!("Failed to queue ZMQ event: {}", e);
        }
    }
}

/// Loads device state from the JSON file.
async fn load_state(path: &str) -> HashMap<String, DeviceConfig> {
    if !std::path::Path::new(path).exists() {
        return HashMap::new();
    }
    match tokio::fs::read_to_string(path).await {
        Ok(content) => match serde_json::from_str::<HashMap<String, DeviceConfig>>(&content) {
            Ok(devices) => {
                info!("Loaded {} devices from {}", devices.len(), path);
                devices
            }
            Err(e) => {
                error!("Failed to parse state file {}: {}", path, e);
                HashMap::new()
            }
        },
        Err(e) => {
            error!("Failed to read state file {}: {}", path, e);
            HashMap::new()
        }
    }
}

// --- Main Entry Point ---
#[tokio::main]
async fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let cli = Cli::parse();

    // 1. Initialize ZMQ Sockets
    let mut rep_socket = RepSocket::new();
    rep_socket
        .bind(&cli.command_addr)
        .await
        .expect("Failed to bind REP socket");

    let mut pub_socket = PubSocket::new();
    pub_socket
        .bind(&cli.event_addr)
        .await
        .expect("Failed to bind PUB socket");

    let (event_tx, mut event_rx) = mpsc::channel::<ZmqMessage>(100);
    let (save_tx, mut save_rx) = mpsc::channel(1);

    // 2. Load State and Initialize Manager
    let loaded_devices = load_state(&cli.state_file).await;
    let manager = Manager::new();
    for (_, config) in &loaded_devices {
        let _ = manager
            .add(&config.id, &config.ip, &config.key, config.version.as_str())
            .await;
    }

    let ctx = Arc::new(BridgeContext {
        manager,
        event_tx,
        state_file: cli.state_file,
        devices: RwLock::new(loaded_devices),
        save_tx,
    });

    // 3. Spawn Background Tasks

    // 3.1 Debounced State Persistence
    let ctx_save = ctx.clone();
    tokio::spawn(async move {
        while let Some(()) = save_rx.recv().await {
            loop {
                tokio::select! {
                    more = save_rx.recv() => {
                        if more.is_none() { return; }
                        continue;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(SAVE_DEBOUNCE_SECS)) => {
                        ctx_save.save_state().await;
                        break;
                    }
                }
            }
        }
    });

    // 3.2 ZMQ Event Broadcaster (PUB)
    tokio::spawn(async move {
        while let Some(msg) = event_rx.recv().await {
            if let Err(e) = pub_socket.send(msg).await {
                error!("ZMQ PUB error: {}", e);
            }
        }
    });

    // 3.3 Rustuya Manager Event Listener
    let ctx_listener = ctx.clone();
    tokio::spawn(async move {
        let rustuya_rx = ctx_listener.manager.listener();
        tokio::pin!(rustuya_rx);
        while let Some(event) = rustuya_rx.next().await {
            if let Some(payload) = event.message.payload_as_string() {
                ctx_listener
                    .publish_event(format!("device/{}", event.device_id), payload)
                    .await;
            }
        }
    });

    info!(
        "ZMQ Bridge running. Command(REP): {}, Event(PUB): {}",
        cli.command_addr, cli.event_addr
    );

    // 4. Main Command Loop (REP)
    loop {
        tokio::select! {
            req = rep_socket.recv() => {
                match req {
                    Ok(msg) => {
                        let res_payload = handle_request(ctx.clone(), msg).await;
                        if let Err(e) = rep_socket.send(ZmqMessage::from(res_payload)).await {
                            error!("ZMQ REP send error: {}", e);
                        }
                    }
                    Err(e) => error!("ZMQ REP recv error: {}", e),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received. Saving final state...");
                ctx.save_state().await;
                info!("Shutdown complete.");
                break;
            }
        }
    }
}

// --- Request Handler ---
async fn handle_request(ctx: Arc<BridgeContext>, msg: ZmqMessage) -> String {
    let payload = match msg.get(0) {
        Some(p) => String::from_utf8_lossy(p),
        None => return error_json("Empty message", None),
    };

    let req: Value = match serde_json::from_str(&payload) {
        Ok(v) => v,
        Err(e) => return error_json(&format!("Invalid JSON: {}", e), None),
    };

    let action = req["action"].as_str().unwrap_or("").to_string();

    // Track used keys and warnings
    let mut used_keys = std::collections::HashSet::new();
    let mut warnings = Vec::new();
    used_keys.insert("action".to_string());

    let res_json = match action.as_str() {
        "manager/add" | "manager/remove" => {
            let id = req["id"].as_str().unwrap_or("");
            let key = req["key"].as_str().unwrap_or("");
            let ip = req["ip"].as_str().unwrap_or("Auto");
            let version = req["version"].as_str().unwrap_or("Auto");

            used_keys.insert("id".to_string());
            if action == "manager/add" {
                used_keys.insert("key".to_string());
                used_keys.insert("ip".to_string());
                used_keys.insert("version".to_string());
            }

            if id.is_empty() {
                return error_json("Missing 'id'", None);
            }

            if action == "manager/remove" {
                ctx.manager.remove(id).await;
                let mut devices = ctx.devices.write().await;
                devices.remove(id);
                ctx.request_save();
                serde_json::json!({
                    "status": "ok",
                    "action": "removed",
                    "id": id
                })
            } else {
                // manager/add logic: check if exists, then add or modify
                let mut devices = ctx.devices.write().await;
                let exists = devices.contains_key(id);

                let res = if exists {
                    ctx.manager.modify(id, ip, key, version).await
                } else {
                    ctx.manager.add(id, ip, key, version).await
                };

                match res {
                    Ok(_) => {
                        let detail_action = if exists { "modified" } else { "added" };
                        devices.insert(
                            id.to_string(),
                            DeviceConfig {
                                id: id.to_string(),
                                ip: ip.to_string(),
                                key: key.to_string(),
                                version: version.to_string(),
                            },
                        );
                        ctx.request_save();
                        serde_json::json!({
                            "status": "ok",
                            "action": detail_action,
                            "id": id
                        })
                    }
                    Err(e) => return error_json(&format!("Manager error: {:?}", e), None),
                }
            }
        }
        "manager/status" => {
            // The bridge's device list is the source of truth because it contains the local keys.
            let devices_config = ctx.devices.read().await;
            let connection_states = ctx.manager.list().await;

            let mut synced = false;
            // 1. If in bridge config but missing in manager -> Add to manager
            for (id, config) in devices_config.iter() {
                if !connection_states.contains_key(id) {
                    let msg = format!(
                        "Device {} found in bridge config but missing in manager. Syncing (Add)...",
                        id
                    );
                    error!("{}", msg);
                    warnings.push(msg);
                    let _ = ctx
                        .manager
                        .add(&config.id, &config.ip, &config.key, config.version.as_str())
                        .await;
                    synced = true;
                }
            }

            // 2. If in manager but missing in bridge config -> Remove from manager
            let mut to_remove = Vec::new();
            for id in connection_states.keys() {
                if !devices_config.contains_key(id) {
                    let msg = format!(
                        "Device {} found in manager but missing in bridge config. Syncing (Remove)...",
                        id
                    );
                    error!("{}", msg);
                    warnings.push(msg);
                    to_remove.push(id.clone());
                    synced = true;
                }
            }
            for id in to_remove {
                ctx.manager.remove(&id).await;
            }

            // If we synced, re-fetch connection states to be accurate
            let connection_states = if synced {
                ctx.manager.list().await
            } else {
                connection_states
            };

            let mut devices_with_status = serde_json::Map::new();
            for (id, config) in devices_config.iter() {
                let mut device_val = serde_json::to_value(config).unwrap_or(Value::Null);
                if let Some(obj) = device_val.as_object_mut() {
                    let is_connected = connection_states.get(id).cloned().unwrap_or(false);
                    obj.insert("connected".to_string(), Value::Bool(is_connected));
                }
                devices_with_status.insert(id.clone(), device_val);
            }

            serde_json::json!({
                "status": "ok",
                "rustuya_version": env!("CARGO_PKG_VERSION"),
                "devices": devices_with_status,
                "synced": synced
            })
        }
        "device/status" | "device/set_dps" | "device/request" => {
            handle_device_command(ctx, &action, &req, &mut used_keys).await
        }
        _ => return error_json(&format!("Unknown action: {}", action), None),
    };

    // Detect unused keys
    if let Some(obj) = req.as_object() {
        for key in obj.keys() {
            if !used_keys.contains(key) {
                warnings.push(format!("Argument '{}' is not used", key));
            }
        }
    }

    let mut final_res = res_json;
    if !warnings.is_empty() {
        if let Some(obj) = final_res.as_object_mut() {
            if obj.get("status").and_then(|s| s.as_str()) == Some("ok") {
                obj.insert("status".to_string(), Value::String("warning".to_string()));
            }
            obj.insert("warnings".to_string(), Value::from(warnings));
        }
    }

    final_res.to_string()
}

/// Helper for device-specific commands to reduce handle_request size.
async fn handle_device_command(
    ctx: Arc<BridgeContext>,
    action: &str,
    req: &Value,
    used_keys: &mut std::collections::HashSet<String>,
) -> Value {
    let id = match req["id"].as_str() {
        Some(id) => {
            used_keys.insert("id".to_string());
            id
        }
        None => return serde_json::json!({"status": "error", "error": "Missing 'id'"}),
    };

    let dev = match ctx.manager.get(id).await {
        Some(dev) => dev,
        None => return serde_json::json!({"status": "error", "error": "Device not found"}),
    };

    let cid = req["cid"].as_str().map(|s| {
        used_keys.insert("cid".to_string());
        s.to_string()
    });

    match action {
        "device/status" => {
            dev.request(CommandType::DpQuery, None, cid).await;
            serde_json::json!({"status": "ok"})
        }
        "device/set_dps" => {
            if let Some(dps) = req["dps"].as_object() {
                used_keys.insert("dps".to_string());
                dev.request(CommandType::Control, Some(Value::Object(dps.clone())), cid)
                    .await;
                serde_json::json!({"status": "ok"})
            } else {
                serde_json::json!({"status": "error", "error": "'dps' object required"})
            }
        }
        "device/request" => {
            if let Some(cmd_val) = req["cmd"].as_u64() {
                used_keys.insert("cmd".to_string());
                if let Some(cmd) = CommandType::from_u32(cmd_val as u32) {
                    let req_data = req["data"].clone();
                    if !req_data.is_null() {
                        used_keys.insert("data".to_string());
                    }
                    let data_opt = if req_data.is_null() {
                        None
                    } else {
                        Some(req_data)
                    };
                    dev.request(cmd, data_opt, cid).await;
                    serde_json::json!({"status": "ok"})
                } else {
                    serde_json::json!({"status": "error", "error": format!("Invalid CommandType {}", cmd_val)})
                }
            } else {
                serde_json::json!({"status": "error", "error": "'cmd' (u32) required"})
            }
        }
        _ => unreachable!(),
    }
}

fn error_json(msg: &str, warnings: Option<Vec<String>>) -> String {
    let mut res = serde_json::json!({
        "status": "error",
        "error": msg
    });
    if let Some(w) = warnings {
        res["warnings"] = Value::from(w);
    }
    res.to_string()
}
