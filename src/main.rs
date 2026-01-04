use bytes::Bytes;
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
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

// --- Constants ---
const STATE_FILE: &str = "rustuya.json";
const DEFAULT_COMMAND_ADDR: &str = "tcp://0.0.0.0:37358";
const DEFAULT_EVENT_ADDR: &str = "tcp://0.0.0.0:37359";
const SAVE_DEBOUNCE_SECS: u64 = 30;
const CHANNEL_CAPACITY: usize = 100;
const AUTO_VALUE: &str = "Auto";

// --- Types & Responses ---
#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
    Warning,
    Error,
}

#[derive(Debug, Serialize)]
struct ApiResponse {
    status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

impl ApiResponse {
    fn base(status: Status) -> Self {
        Self {
            status,
            action: None,
            id: None,
            error: None,
            warnings: Vec::new(),
            extra: HashMap::new(),
        }
    }

    fn ok(action: impl Into<String>, id: impl Into<String>) -> Self {
        let mut res = Self::base(Status::Ok);
        res.action = Some(action.into());
        res.id = Some(id.into());
        res
    }

    fn error(msg: impl Into<String>) -> Self {
        let mut res = Self::base(Status::Error);
        res.error = Some(msg.into());
        res
    }

    fn warning(action: impl Into<String>, id: impl Into<String>, msg: String) -> Self {
        let mut res = Self::ok(action, id);
        res.status = Status::Warning;
        res.warnings = vec![msg];
        res
    }

    fn with_extra(mut self, key: &str, value: Value) -> Self {
        self.extra.insert(key.to_string(), value);
        self
    }

    fn with_warnings(mut self, warnings: Vec<String>) -> Self {
        self.warnings.extend(warnings);
        if !self.warnings.is_empty() && matches!(self.status, Status::Ok) {
            self.status = Status::Warning;
        }
        self
    }

    fn to_json_string(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| r#"{"status":"error","error":"Serialization failed"}"#.into())
    }
}

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
    let mut router_socket = RouterSocket::new();
    router_socket
        .bind(&cli.command_addr)
        .await
        .expect("Failed to bind ROUTER socket");

    let mut pub_socket = PubSocket::new();
    pub_socket
        .bind(&cli.event_addr)
        .await
        .expect("Failed to bind PUB socket");

    let (event_tx, mut event_rx) = mpsc::channel::<ZmqMessage>(CHANNEL_CAPACITY);
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
        "ZMQ Bridge running.\nCommand(REP): {}, Event(PUB): {}",
        cli.command_addr, cli.event_addr
    );

    // 4. Main Command Loop (ROUTER)
    // Using a channel to handle responses asynchronously (Identity, Payload, is_req)
    let (res_tx, mut res_rx) = mpsc::channel::<(Bytes, String, bool)>(CHANNEL_CAPACITY);

    loop {
        tokio::select! {
            req = router_socket.recv() => {
                match req {
                    Ok(msg) => {
                        let ctx = ctx.clone();
                        let tx = res_tx.clone();
                        tokio::spawn(async move {
                            if msg.len() < 2 {
                                error!("Invalid ROUTER message: expected at least 2 frames (Identity + Payload)");
                                return;
                            }
                            let identity = msg.get(0).unwrap().clone();
                            // Detect REQ style: [Identity, Empty, Payload]
                            let is_req = msg.len() >= 3 && msg.get(1).map(|f| f.is_empty()).unwrap_or(false);

                            let res_payload = handle_request(ctx, msg).await;
                            let _ = tx.send((identity, res_payload, is_req)).await;
                        });
                    }
                    Err(e) => error!("ZMQ ROUTER recv error: {}", e),
                }
            }
            Some((identity, res_payload, is_req)) = res_rx.recv() => {
                let mut res_msg = ZmqMessage::from(identity);
                if is_req {
                    res_msg.push_back(vec![].into()); // Empty delimiter for REQ
                }
                res_msg.push_back(res_payload.into());
                if let Err(e) = router_socket.send(res_msg).await {
                    error!("ZMQ ROUTER send error: {}", e);
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
    // In ROUTER mode:
    // - From DEALER: [Identity, Payload]
    // - From REQ:    [Identity, Empty, Payload]
    let payload_idx = if msg.len() >= 3 && msg.get(1).map(|f| f.is_empty()).unwrap_or(false) {
        2
    } else {
        1
    };

    let payload = match msg.get(payload_idx) {
        Some(p) => String::from_utf8_lossy(p),
        None => return ApiResponse::error("Empty message").to_json_string(),
    };

    let req: Value = match serde_json::from_str(&payload) {
        Ok(v) => v,
        Err(e) => return ApiResponse::error(format!("Invalid JSON: {}", e)).to_json_string(),
    };

    let action = req["action"].as_str().unwrap_or("").to_string();

    // Track used keys and warnings
    let mut used_keys = std::collections::HashSet::from(["action".to_string()]);
    let mut warnings = Vec::new();

    let response = match action.as_str() {
        "manager/add" | "manager/remove" => {
            let id = req["id"].as_str().unwrap_or("");
            let key = req["key"].as_str().unwrap_or("");
            let ip = req["ip"].as_str().unwrap_or(AUTO_VALUE);
            let version = req["version"].as_str().unwrap_or(AUTO_VALUE);

            used_keys.insert("id".to_string());
            if action == "manager/add" {
                used_keys.extend(["key", "ip", "version"].iter().map(|s| s.to_string()));
            }

            if id.is_empty() {
                return ApiResponse::error("Missing 'id'").to_json_string();
            }

            if action == "manager/remove" {
                ctx.manager.remove(id).await;
                let mut devices = ctx.devices.write().await;
                devices.remove(id);
                ctx.request_save();
                ApiResponse::ok("removed", id)
            } else {
                let mut devices = ctx.devices.write().await;
                let existing = devices.get(id);

                if let Some(config) = existing {
                    if config.ip == ip && config.key == key && config.version == version {
                        ApiResponse::warning(
                            "ignored",
                            id,
                            format!("Device {} already exists with same config. Ignored.", id),
                        )
                    } else {
                        match ctx.manager.modify(id, ip, key, version).await {
                            Ok(_) => {
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
                                ApiResponse::ok("modified", id)
                            }
                            Err(e) => ApiResponse::error(format!("Manager error: {:?}", e)),
                        }
                    }
                } else {
                    match ctx.manager.add(id, ip, key, version).await {
                        Ok(_) => {
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
                            ApiResponse::ok("added", id)
                        }
                        Err(e) => ApiResponse::error(format!("Manager error: {:?}", e)),
                    }
                }
            }
        }
        "manager/status" => {
            let devices_config = ctx.devices.read().await;
            let mut connection_states = ctx.manager.list().await;

            let mut synced = false;
            // 1. Bridge config -> Manager
            for (id, config) in devices_config.iter() {
                if !connection_states.contains_key(id) {
                    let msg = format!("Device {} missing in manager. Syncing (Add)...", id);
                    error!("{}", msg);
                    warnings.push(msg);
                    let _ = ctx
                        .manager
                        .add(&config.id, &config.ip, &config.key, config.version.as_str())
                        .await;
                    synced = true;
                }
            }

            // 2. Manager -> Bridge config
            let mut to_remove = Vec::new();
            for id in connection_states.keys() {
                if !devices_config.contains_key(id) {
                    let msg = format!(
                        "Device {} missing in bridge config. Syncing (Remove)...",
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

            if synced {
                connection_states = ctx.manager.list().await;
            }

            let mut devices_with_status = serde_json::Map::new();
            for (id, config) in devices_config.iter() {
                let mut device_val = serde_json::to_value(config).unwrap_or(Value::Null);
                if let Some(obj) = device_val.as_object_mut() {
                    let is_connected = connection_states.get(id).cloned().unwrap_or(false);
                    obj.insert("connected".to_string(), Value::Bool(is_connected));
                }
                devices_with_status.insert(id.clone(), device_val);
            }

            ApiResponse::base(Status::Ok)
                .with_extra("version", env!("CARGO_PKG_VERSION").into())
                .with_extra("devices", Value::Object(devices_with_status))
        }
        "device/status" | "device/set_dps" | "device/request" => {
            handle_device_command(ctx, &action, &req, &mut used_keys).await
        }
        _ => ApiResponse::error(format!("Unknown action: {}", action)),
    };

    // Detect unused keys
    if let Some(obj) = req.as_object() {
        for key in obj.keys() {
            if !used_keys.contains(key) {
                warnings.push(format!("Argument '{}' is not used", key));
            }
        }
    }

    response.with_warnings(warnings).to_json_string()
}

/// Helper for device-specific commands to reduce handle_request size.
async fn handle_device_command(
    ctx: Arc<BridgeContext>,
    action: &str,
    req: &Value,
    used_keys: &mut std::collections::HashSet<String>,
) -> ApiResponse {
    let id = match req["id"].as_str() {
        Some(id) => {
            used_keys.insert("id".to_string());
            id
        }
        None => return ApiResponse::error("Missing 'id'"),
    };

    let dev = match ctx.manager.get(id).await {
        Some(dev) => dev,
        None => return ApiResponse::error("Device not found"),
    };

    if !dev.is_connected() {
        return ApiResponse::error("Device is offline");
    }

    let cid = req["cid"].as_str().map(|s| {
        used_keys.insert("cid".to_string());
        s.to_string()
    });

    match action {
        "device/status" => {
            dev.request(CommandType::DpQuery, None, cid).await;
            ApiResponse::ok(action, id)
        }
        "device/set_dps" => {
            if let Some(dps) = req["dps"].as_object() {
                used_keys.insert("dps".to_string());
                dev.request(CommandType::Control, Some(Value::Object(dps.clone())), cid)
                    .await;
                ApiResponse::ok(action, id)
            } else {
                ApiResponse::error("'dps' object required")
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
                    ApiResponse::ok(action, id)
                } else {
                    ApiResponse::error(format!("Invalid CommandType {}", cmd_val))
                }
            } else {
                ApiResponse::error("'cmd' (u32) required")
            }
        }
        _ => unreachable!(),
    }
}
