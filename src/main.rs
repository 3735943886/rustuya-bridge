use bytes::Bytes;
use clap::Parser;
use futures_util::StreamExt;
use log::{error, info};
use rustuya::protocol::CommandType;
use rustuya::{Manager, Scanner};
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

// --- Types & Responses ---
#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum BridgeRequest {
    #[serde(rename = "manager/add")]
    Add {
        id: String,
        key: String,
        #[serde(default = "default_auto")]
        ip: String,
        #[serde(default = "default_auto")]
        version: String,
    },
    #[serde(rename = "manager/remove")]
    Remove { id: String },
    #[serde(rename = "manager/clear")]
    Clear,
    #[serde(rename = "manager/status")]
    Status,
    #[serde(rename = "device/status")]
    DeviceStatus { id: String, cid: Option<String> },
    #[serde(rename = "device/set_dps")]
    SetDps {
        id: String,
        dps: serde_json::Map<String, Value>,
        cid: Option<String>,
    },
    #[serde(rename = "device/request")]
    DeviceRequest {
        id: String,
        cmd: u32,
        data: Option<Value>,
        cid: Option<String>,
    },
    #[serde(rename = "scan")]
    Scan,
}

fn default_auto() -> String {
    "Auto".to_string()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum Status {
    Ok,
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

    fn with_extra(mut self, key: &str, value: Value) -> Self {
        self.extra.insert(key.to_string(), value);
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
        let json = {
            let devices = self.devices.read().await;
            match serde_json::to_string_pretty(&*devices) {
                Ok(j) => j,
                Err(e) => {
                    error!("Failed to serialize devices: {}", e);
                    return;
                }
            }
        };

        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&self.state_file).parent()
            && !parent.as_os_str().is_empty()
            && let Err(e) = tokio::fs::create_dir_all(parent).await
        {
            error!("Failed to create directories for state file: {}", e);
            return;
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

    // --- Atomic State Operations ---

    async fn add_device(&self, config: DeviceConfig) -> Result<ApiResponse, String> {
        let mut devices = self.devices.write().await;

        self.manager
            .add(&config.id, &config.ip, &config.key, config.version.as_str())
            .await
            .map_err(|e| format!("Manager error: {:?}", e))?;

        let is_new = !devices.contains_key(&config.id);
        devices.insert(config.id.clone(), config.clone());
        self.request_save();

        Ok(ApiResponse::ok(
            if is_new { "added" } else { "modified" },
            config.id,
        ))
    }

    async fn remove_device(&self, id: &str) -> Result<ApiResponse, String> {
        let mut devices = self.devices.write().await;
        self.manager.remove(id).await;
        devices.remove(id);
        self.request_save();
        Ok(ApiResponse::ok("removed", id))
    }

    async fn clear_devices(&self) -> Result<ApiResponse, String> {
        let mut devices = self.devices.write().await;
        self.manager.clear().await;
        devices.clear();
        self.request_save();
        Ok(ApiResponse::ok("cleared", "manager"))
    }

    /// Helper to get a connected device or return an error response.
    async fn get_connected_device(&self, id: &str) -> Result<rustuya::Device, ApiResponse> {
        let dev = self
            .manager
            .get(id)
            .await
            .ok_or_else(|| ApiResponse::error("Device not found"))?;

        if !dev.is_connected() {
            return Err(ApiResponse::error("Device is offline"));
        }

        Ok(dev)
    }

    /// Generates a status report of all registered devices.
    async fn get_manager_status(&self) -> ApiResponse {
        let devices_config = self.devices.read().await;
        let connection_states = self.manager.list().await;

        let mut devices_with_status = serde_json::Map::new();
        for (id, config) in devices_config.iter() {
            let mut device_val = serde_json::to_value(config).unwrap_or(Value::Null);
            if let Some(obj) = device_val.as_object_mut() {
                if let Some(info) = connection_states.iter().find(|d| d.id == *id) {
                    obj.insert("connected".to_string(), Value::Bool(info.is_connected));
                    obj.insert("ip".to_string(), Value::String(info.address.to_string()));
                    obj.insert(
                        "version".to_string(),
                        Value::String(info.version.to_string()),
                    );
                } else {
                    obj.insert("connected".to_string(), Value::Bool(false));
                }
            }
            devices_with_status.insert(id.clone(), device_val);
        }

        ApiResponse::base(Status::Ok)
            .with_extra("version", env!("CARGO_PKG_VERSION").into())
            .with_extra("devices", Value::Object(devices_with_status))
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

    // Create listener BEFORE adding devices to capture initial events (connection, etc.)
    let rustuya_rx = manager.listener();

    for config in loaded_devices.values() {
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

                            let payload_idx = if is_req { 2 } else { 1 };
                            let payload = match msg.get(payload_idx) {
                                Some(p) => String::from_utf8_lossy(p),
                                None => {
                                    error!("Empty payload received");
                                    return;
                                }
                            };

                            let res_payload = match serde_json::from_str::<BridgeRequest>(&payload) {
                                Ok(req) => handle_request(ctx, req).await.to_json_string(),
                                Err(e) => {
                                    error!("Invalid request: {} | Payload: {}", e, payload);
                                    ApiResponse::error(format!("Invalid request: {}", e))
                                        .to_json_string()
                                }
                            };
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
async fn handle_request(ctx: Arc<BridgeContext>, req: BridgeRequest) -> ApiResponse {
    match req {
        BridgeRequest::Add {
            id,
            key,
            ip,
            version,
        } => ctx
            .add_device(DeviceConfig {
                id,
                ip,
                key,
                version,
            })
            .await
            .unwrap_or_else(ApiResponse::error),
        BridgeRequest::Remove { id } => ctx
            .remove_device(&id)
            .await
            .unwrap_or_else(ApiResponse::error),
        BridgeRequest::Clear => ctx.clear_devices().await.unwrap_or_else(ApiResponse::error),
        BridgeRequest::Status => ctx.get_manager_status().await,
        BridgeRequest::DeviceStatus { id, cid } => {
            info!("Device status: id={}, cid={:?}", id, cid);
            match ctx.get_connected_device(&id).await {
                Ok(dev) => {
                    dev.status().await;
                    ApiResponse::ok("device/status", id)
                }
                Err(e) => e,
            }
        }
        BridgeRequest::SetDps { id, dps, cid } => {
            info!("Device set_dps: id={}, dps={:?}, cid={:?}", id, dps, cid);
            match ctx.get_connected_device(&id).await {
                Ok(dev) => {
                    dev.set_dps(Value::Object(dps)).await;
                    ApiResponse::ok("device/set_dps", id)
                }
                Err(e) => e,
            }
        }
        BridgeRequest::DeviceRequest { id, cmd, data, cid } => {
            let command = match CommandType::from_u32(cmd) {
                Some(c) => c,
                None => return ApiResponse::error(format!("Invalid CommandType {}", cmd)),
            };

            info!(
                "Device request: id={}, cmd={:?}, data={:?}, cid={:?}",
                id, command, data, cid
            );

            match ctx.get_connected_device(&id).await {
                Ok(dev) => {
                    dev.request(command, data, cid).await;
                    ApiResponse::ok(format!("device/{:?}", command).to_lowercase(), id)
                }
                Err(e) => e,
            }
        }
        BridgeRequest::Scan => {
            let ctx_scan = ctx.clone();
            tokio::spawn(async move {
                let scanner = Scanner::new().with_timeout(Duration::from_secs(18));
                let stream = scanner.scan_stream();
                tokio::pin!(stream);

                while let Some(dev) = stream.next().await {
                    let mut payload = serde_json::Map::new();
                    payload.insert("id".to_string(), Value::String(dev.id.clone()));
                    payload.insert("ip".to_string(), Value::String(dev.ip.clone()));
                    if let Some(v) = &dev.version {
                        payload.insert("version".to_string(), Value::String(v.to_string()));
                    }
                    if let Some(pk) = &dev.product_key {
                        payload.insert("product_key".to_string(), Value::String(pk.to_string()));
                    }

                    ctx_scan
                        .publish_event("scanner".to_string(), Value::Object(payload).to_string())
                        .await;
                }

                ctx_scan
                    .publish_event("scanner".to_string(), "{}".to_string())
                    .await;
            });

            ApiResponse::ok("scan", "bridge").with_extra(
                "message",
                "Scan started. Results will be published to 'scanner' topic.".into(),
            )
        }
    }
}
