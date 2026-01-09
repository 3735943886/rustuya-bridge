use clap::Parser;
use futures_util::StreamExt;
use log::{error, info};
use rustuya::protocol::CommandType;
use rustuya::{Device, DeviceBuilder};
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
    Add {
        id: String,
        key: String,
        #[serde(default = "default_auto")]
        ip: String,
        #[serde(default = "default_auto")]
        version: String,
    },
    Remove {
        id: String,
    },
    Clear,
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "get")]
    Get {
        id: String,
        cid: Option<String>,
    },
    #[serde(rename = "set")]
    Set {
        id: String,
        dps: serde_json::Map<String, Value>,
        cid: Option<String>,
    },
    #[serde(rename = "request")]
    Request {
        id: String,
        cmd: u32,
        data: Option<Value>,
        cid: Option<String>,
    },
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
        serde_json::to_string(self).unwrap_or_else(|e| {
            error!("Serialization failed: {}", e);
            r#"{"status":"error","error":"Serialization failed"}"#.to_string()
        })
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
    event_tx: mpsc::Sender<ZmqMessage>,
    state_file: String,
    configs: RwLock<HashMap<String, DeviceConfig>>,
    instances: RwLock<HashMap<String, Device>>,
    save_tx: mpsc::Sender<()>,
    refresh_tx: mpsc::Sender<()>,
}

// --- Implementation ---
impl BridgeContext {
    /// Atomically saves the current device state to a JSON file.
    async fn save_state(&self) {
        let json = {
            let configs = self.configs.read().await;
            match serde_json::to_string_pretty(&*configs) {
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

    /// Triggers a refresh of the unified listener.
    fn request_refresh(&self) {
        let _ = self.refresh_tx.try_send(());
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
        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;

        let dev = DeviceBuilder::new(&config.id, config.key.as_bytes().to_vec())
            .address(&config.ip)
            .version(config.version.as_str())
            .nowait(true)
            .run();

        let is_new = !configs.contains_key(&config.id);
        configs.insert(config.id.clone(), config.clone());
        instances.insert(config.id.clone(), dev);

        self.request_save();
        self.request_refresh();

        Ok(ApiResponse::ok(
            if is_new { "added" } else { "modified" },
            config.id,
        ))
    }

    async fn remove_device(&self, id: &str) -> Result<ApiResponse, String> {
        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;

        if let Some(dev) = instances.remove(id) {
            dev.stop().await;
        }
        configs.remove(id);

        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("removed", id))
    }

    async fn clear_devices(&self) -> Result<ApiResponse, String> {
        let mut configs = self.configs.write().await;
        let mut instances = self.instances.write().await;

        for dev in instances.values() {
            dev.stop().await;
        }
        instances.clear();
        configs.clear();

        self.request_save();
        self.request_refresh();
        Ok(ApiResponse::ok("cleared", "bridge"))
    }

    /// Helper to get a connected device or return an error response.
    async fn get_connected_device(&self, id: &str) -> Result<Device, ApiResponse> {
        let instances = self.instances.read().await;
        let dev = instances
            .get(id)
            .ok_or_else(|| ApiResponse::error("Device not found"))?;

        if !dev.is_connected() {
            return Err(ApiResponse::error("Device is offline"));
        }

        Ok(dev.clone())
    }

    /// Generates a status report of all registered devices.
    async fn get_bridge_status(&self) -> ApiResponse {
        let configs = self.configs.read().await;
        let instances = self.instances.read().await;

        let mut devices_with_status = serde_json::Map::new();
        for (id, config) in configs.iter() {
            let mut device_val = serde_json::to_value(config).unwrap_or(Value::Null);
            if let Some(obj) = device_val.as_object_mut() {
                if let Some(dev) = instances.get(id) {
                    obj.insert("connected".to_string(), Value::Bool(dev.is_connected()));
                    obj.insert("ip".to_string(), Value::String(dev.address()));
                    obj.insert(
                        "version".to_string(),
                        Value::String(dev.version().to_string()),
                    );
                } else {
                    obj.insert("connected".to_string(), Value::Bool(false));
                }
            }
            devices_with_status.insert(id.clone(), device_val);
        }

        ApiResponse::ok("status", "bridge")
            .with_extra("version", env!("CARGO_PKG_VERSION").into())
            .with_extra("devices", Value::Object(devices_with_status))
    }
}

/// Loads device state from the JSON file.
async fn load_state(path: &str) -> HashMap<String, DeviceConfig> {
    if !std::path::Path::new(path).exists() {
        return HashMap::new();
    }
    let res = async {
        let content = tokio::fs::read_to_string(path).await?;
        let devices = serde_json::from_str::<HashMap<String, DeviceConfig>>(&content)?;
        Ok::<_, Box<dyn std::error::Error>>(devices)
    }
    .await;

    match res {
        Ok(devices) => {
            info!("Loaded {} devices from {}", devices.len(), path);
            devices
        }
        Err(e) => {
            error!("Failed to load state file {}: {}", path, e);
            HashMap::new()
        }
    }
}

// --- Main Entry Point ---
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    let cli = Cli::parse();

    // 1. Initialize ZMQ Sockets
    let mut router_socket = RouterSocket::new();
    router_socket.bind(&cli.command_addr).await?;

    let mut pub_socket = PubSocket::new();
    pub_socket.bind(&cli.event_addr).await?;

    let (event_tx, mut event_rx) = mpsc::channel::<ZmqMessage>(CHANNEL_CAPACITY);
    let (save_tx, mut save_rx) = mpsc::channel(1);

    // 2. Load State and Initialize Devices
    let loaded_configs = load_state(&cli.state_file).await;
    let mut instances = HashMap::new();

    for config in loaded_configs.values() {
        let dev = DeviceBuilder::new(&config.id, config.key.as_bytes().to_vec())
            .address(&config.ip)
            .version(config.version.as_str())
            .nowait(true)
            .run();
        instances.insert(config.id.clone(), dev);
    }

    let (refresh_tx, mut refresh_rx) = mpsc::channel(1);

    let ctx = Arc::new(BridgeContext {
        event_tx,
        state_file: cli.state_file,
        configs: RwLock::new(loaded_configs),
        instances: RwLock::new(instances),
        save_tx,
        refresh_tx,
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

    // 3.3 Unified Device Event Listener
    let ctx_listener = ctx.clone();
    tokio::spawn(async move {
        loop {
            let devices: Vec<Device> = ctx_listener
                .instances
                .read()
                .await
                .values()
                .cloned()
                .collect();

            if devices.is_empty() {
                // Wait for devices to be added
                if refresh_rx.recv().await.is_none() {
                    break;
                }
                continue;
            }

            let mut rustuya_rx = rustuya::device::unified_listener(devices);

            loop {
                tokio::select! {
                    Some(event_res) = rustuya_rx.next() => {
                        match event_res {
                            Ok(event) => {
                                if let Some(payload_str) = event.message.payload_as_string() {
                                    // Inject device ID into payload for consistent topic subscription
                                    let mut payload: Value = serde_json::from_str(&payload_str)
                                        .unwrap_or(Value::String(payload_str));
                                    
                                    if let Some(obj) = payload.as_object_mut() {
                                        obj.insert("id".to_string(), Value::String(event.device_id.clone()));
                                    }

                                    ctx_listener
                                        .publish_event("device".to_string(), payload.to_string())
                                        .await;
                                }
                            }
                            Err(e) => error!("Rustuya event error: {}", e),
                        }
                    }
                    _ = refresh_rx.recv() => {
                        // Restart listener with updated device list
                        break;
                    }
                }
            }
        }
    });

    info!(
        "ZMQ Bridge running.\nCommand(REP): {}, Event(PUB): {}",
        cli.command_addr, cli.event_addr
    );

    // 4. Main Command Loop (ROUTER)
    loop {
        tokio::select! {
            req = router_socket.recv() => {
                match req {
                    Ok(msg) => {
                        // REQ style: [Identity, Empty, Payload] (3 frames)
                        // Dealer style: [Identity, Payload] (2 frames)
                        let (identity, is_req, payload_bytes) = match (msg.get(0), msg.get(1), msg.get(2)) {
                            (Some(id), Some(empty), Some(payload)) if empty.is_empty() => {
                                (id.clone(), true, payload)
                            }
                            (Some(id), Some(payload), _) if msg.len() >= 2 => {
                                (id.clone(), false, payload)
                            }
                            _ => {
                                error!("Invalid ROUTER message: expected at least 2 frames");
                                continue;
                            }
                        };

                        let payload = String::from_utf8_lossy(payload_bytes);
                        let res_payload = match serde_json::from_str::<BridgeRequest>(&payload) {
                            Ok(req) => handle_request(ctx.clone(), req).await.to_json_string(),
                            Err(e) => {
                                error!("Invalid request: {} | Payload: {}", e, payload);
                                ApiResponse::error(format!("Invalid request: {}", e))
                                    .to_json_string()
                            }
                        };

                        let mut res_msg = ZmqMessage::from(identity);
                        if is_req {
                            res_msg.push_back(vec![].into()); // Empty delimiter for REQ
                        }
                        res_msg.push_back(res_payload.into());
                        if let Err(e) = router_socket.send(res_msg).await {
                            error!("ZMQ ROUTER send error: {}", e);
                        }
                    }
                    Err(e) => error!("ZMQ ROUTER recv error: {}", e),
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
    Ok(())
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
        BridgeRequest::Status => ctx.get_bridge_status().await,
        BridgeRequest::Get { id, cid } => match ctx.get_connected_device(&id).await {
            Ok(dev) => {
                let _ = dev.request(CommandType::DpQuery, None, cid).await;
                ApiResponse::ok("get", id)
            }
            Err(e) => e,
        },
        BridgeRequest::Set { id, dps, cid } => match ctx.get_connected_device(&id).await {
            Ok(dev) => {
                let _ = dev
                    .request(CommandType::Control, Some(Value::Object(dps)), cid)
                    .await;
                ApiResponse::ok("set", id)
            }
            Err(e) => e,
        },
        BridgeRequest::Request { id, cmd, data, cid } => {
            let Some(command) = CommandType::from_u32(cmd) else {
                return ApiResponse::error(format!("Invalid CommandType {}", cmd));
            };

            match ctx.get_connected_device(&id).await {
                Ok(dev) => {
                    let _ = dev.request(command, data, cid).await;
                    ApiResponse::ok(format!("{:?}", command).to_lowercase(), id)
                }
                Err(e) => e,
            }
        }
        BridgeRequest::Scan => {
            let ctx_scan = ctx.clone();
            tokio::spawn(async move {
                let stream = rustuya::Scanner::scan_stream();
                tokio::pin!(stream);

                while let Some(dev) = stream.next().await {
                    let mut payload = serde_json::Map::new();
                    payload.insert("id".to_string(), Value::String(dev.id.clone()));
                    payload.insert("ip".to_string(), Value::String(dev.ip.clone()));
                    if let Some(v) = &dev.version {
                        payload
                            .insert("version".to_string(), Value::String(v.as_str().to_string()));
                    }
                    if let Some(pk) = &dev.product_key {
                        payload.insert("product_key".to_string(), Value::String(pk.clone()));
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
