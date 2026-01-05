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

struct WorkerTask {
    identity: Bytes,
    is_req: bool,
    request: BridgeRequest,
}

struct DeviceWorkerHandle {
    tx: mpsc::Sender<WorkerTask>,
    _handle: tokio::task::JoinHandle<()>,
}

impl Drop for DeviceWorkerHandle {
    fn drop(&mut self) {
        self._handle.abort();
    }
}

struct BridgeContext {
    manager: Manager,
    event_tx: mpsc::Sender<ZmqMessage>,
    state_file: String,
    devices: RwLock<HashMap<String, DeviceConfig>>,
    save_tx: mpsc::Sender<()>,
    // Worker channels
    manager_tx: mpsc::Sender<WorkerTask>,
    scanner_tx: mpsc::Sender<WorkerTask>,
    device_workers: RwLock<HashMap<String, DeviceWorkerHandle>>,
    res_tx: mpsc::Sender<(Bytes, String, bool)>,
    // For graceful shutdown
    _background_tasks: RwLock<Vec<tokio::task::JoinHandle<()>>>,
}

impl BridgeContext {
    async fn add_task(&self, handle: tokio::task::JoinHandle<()>) {
        self._background_tasks.write().await.push(handle);
    }

    async fn shutdown(&self) {
        info!("Shutting down bridge context...");

        // 1. Stop all device workers (RAII)
        {
            let mut workers = self.device_workers.write().await;
            workers.clear(); // Aborts all device workers
        }

        // 2. Stop all background tasks
        {
            let mut tasks = self._background_tasks.write().await;
            for task in tasks.drain(..) {
                task.abort();
            }
        }

        // 3. Final state save
        self.save_state().await;
        info!("Bridge context shutdown complete.");
    }
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

    async fn spawn_device_worker(self: &Arc<Self>, id: String) -> Result<(), String> {
        // 1. Check if worker already exists
        {
            let workers = self.device_workers.read().await;
            if workers.contains_key(&id) {
                return Ok(());
            }
        }

        // 2. Verify device exists in Manager
        if self.manager.get(&id).await.is_none() {
            return Err(format!("Device {} not found in Manager", id));
        }

        let (tx, mut rx) = mpsc::channel::<WorkerTask>(CHANNEL_CAPACITY);
        let ctx = self.clone();
        let device_id = id.clone();

        let handle = tokio::spawn(async move {
            info!("Worker started for device {}", device_id);
            while let Some(task) = rx.recv().await {
                let res = match task.request {
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
                            None => {
                                let _ = ctx
                                    .res_tx
                                    .send((
                                        task.identity,
                                        ApiResponse::error(format!("Invalid CommandType {}", cmd))
                                            .to_json_string(),
                                        task.is_req,
                                    ))
                                    .await;
                                continue;
                            }
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
                    _ => ApiResponse::error("Invalid request for device worker"),
                };

                let _ = ctx
                    .res_tx
                    .send((task.identity, res.to_json_string(), task.is_req))
                    .await;
            }
            info!("Worker stopped for device {}", device_id);
        });

        let mut workers = self.device_workers.write().await;
        workers.insert(
            id,
            DeviceWorkerHandle {
                tx,
                _handle: handle,
            },
        );
        Ok(())
    }

    async fn add_device(self: &Arc<Self>, config: DeviceConfig) -> Result<ApiResponse, String> {
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

        // Stop worker
        let mut workers = self.device_workers.write().await;
        workers.remove(id);

        self.request_save();
        Ok(ApiResponse::ok("removed", id))
    }

    async fn clear_devices(&self) -> Result<ApiResponse, String> {
        let mut devices = self.devices.write().await;
        self.manager.clear().await;
        devices.clear();

        // Stop all workers
        let mut workers = self.device_workers.write().await;
        workers.clear();

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

async fn manager_worker(ctx: Arc<BridgeContext>, mut rx: mpsc::Receiver<WorkerTask>) {
    info!("Manager worker started");
    while let Some(task) = rx.recv().await {
        let res = match task.request {
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
            _ => ApiResponse::error("Invalid request for manager worker"),
        };
        let _ = ctx
            .res_tx
            .send((task.identity, res.to_json_string(), task.is_req))
            .await;
    }
    info!("Manager worker stopped");
}

async fn scanner_worker(ctx: Arc<BridgeContext>, mut rx: mpsc::Receiver<WorkerTask>) {
    info!("Scanner worker started");
    while let Some(task) = rx.recv().await {
        match task.request {
            BridgeRequest::Scan => {
                let ctx_scan = ctx.clone();
                // Send immediate response that scan started
                let _ = ctx
                    .res_tx
                    .send((
                        task.identity,
                        ApiResponse::ok("scan", "bridge")
                            .with_extra(
                                "message",
                                "Scan started. Results will be published to 'scanner' topic."
                                    .into(),
                            )
                            .to_json_string(),
                        task.is_req,
                    ))
                    .await;

                // Then run the scan
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
            }
            _ => {
                let _ = ctx
                    .res_tx
                    .send((
                        task.identity,
                        ApiResponse::error("Invalid request for scanner worker").to_json_string(),
                        task.is_req,
                    ))
                    .await;
            }
        }
    }
    info!("Scanner worker stopped");
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
    let (res_tx, mut res_rx) = mpsc::channel::<(Bytes, String, bool)>(CHANNEL_CAPACITY);
    let (manager_tx, manager_rx) = mpsc::channel::<WorkerTask>(CHANNEL_CAPACITY);
    let (scanner_tx, scanner_rx) = mpsc::channel::<WorkerTask>(CHANNEL_CAPACITY);

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
        devices: RwLock::new(loaded_devices.clone()),
        save_tx,
        manager_tx,
        scanner_tx,
        device_workers: RwLock::new(HashMap::new()),
        res_tx: res_tx.clone(),
        _background_tasks: RwLock::new(Vec::new()),
    });

    // 3. Spawn Workers
    ctx.add_task(tokio::spawn(manager_worker(ctx.clone(), manager_rx)))
        .await;
    ctx.add_task(tokio::spawn(scanner_worker(ctx.clone(), scanner_rx)))
        .await;

    // 4. Spawn Background Tasks

    // 3.1 Debounced State Persistence
    let ctx_save = ctx.clone();
    ctx.add_task(tokio::spawn(async move {
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
    }))
    .await;

    // 3.2 ZMQ Event Broadcaster (PUB)
    ctx.add_task(tokio::spawn(async move {
        while let Some(msg) = event_rx.recv().await {
            if let Err(e) = pub_socket.send(msg).await {
                error!("ZMQ PUB error: {}", e);
            }
        }
    }))
    .await;

    // 3.3 Rustuya Manager Event Listener
    let ctx_listener = ctx.clone();
    ctx.add_task(tokio::spawn(async move {
        tokio::pin!(rustuya_rx);
        while let Some(event) = rustuya_rx.next().await {
            if let Some(payload) = event.message.payload_as_string() {
                ctx_listener
                    .publish_event(format!("device/{}", event.device_id), payload)
                    .await;
            }
        }
    }))
    .await;

    info!(
        "ZMQ Bridge running.\nCommand(REP): {}, Event(PUB): {}",
        cli.command_addr, cli.event_addr
    );

    // 5. Main Command Loop (ROUTER)
    loop {
        tokio::select! {
            req = router_socket.recv() => {
                match req {
                    Ok(msg) => {
                        let ctx = ctx.clone();
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

                            match serde_json::from_str::<BridgeRequest>(&payload) {
                                Ok(req) => {
                                    let task = WorkerTask {
                                        identity: identity.clone(),
                                        is_req,
                                        request: req,
                                    };

                                    match &task.request {
                                        BridgeRequest::Add { .. } |
                                        BridgeRequest::Remove { .. } |
                                        BridgeRequest::Clear |
                                        BridgeRequest::Status => {
                                            let _ = ctx.manager_tx.send(task).await;
                                        }
                                        BridgeRequest::Scan => {
                                            let _ = ctx.scanner_tx.send(task).await;
                                        }
                                        BridgeRequest::DeviceStatus { id, .. } |
                                        BridgeRequest::SetDps { id, .. } |
                                        BridgeRequest::DeviceRequest { id, .. } => {
                                            // 1. Check if worker exists
                                            let exists = {
                                                let workers = ctx.device_workers.read().await;
                                                workers.contains_key(id)
                                            };

                                            // 2. If not, check if device is registered and spawn worker
                                            if !exists {
                                                let is_registered = {
                                                    let devices = ctx.devices.read().await;
                                                    devices.contains_key(id)
                                                };

                                                if is_registered {
                                                    let spawn_res = ctx.spawn_device_worker(id.clone()).await;
                                                    if let Err(e) = spawn_res {
                                                        let _ = ctx.res_tx.send((
                                                            identity,
                                                            ApiResponse::error(e).to_json_string(),
                                                            is_req
                                                        )).await;
                                                        return;
                                                    }
                                                }
                                            }

                                            // 3. Dispatch to worker
                                            let workers = ctx.device_workers.read().await;
                                            if let Some(worker) = workers.get(id) {
                                                let _ = worker.tx.send(task).await;
                                            } else {
                                                let _ = ctx.res_tx.send((
                                                    identity,
                                                    ApiResponse::error("Device not found or not registered").to_json_string(),
                                                    is_req
                                                )).await;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Invalid request: {} | Payload: {}", e, payload);
                                    let _ = ctx.res_tx.send((
                                        identity,
                                        ApiResponse::error(format!("Invalid request: {}", e)).to_json_string(),
                                        is_req
                                    )).await;
                                }
                            };
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
                info!("Shutdown signal received. Cleaning up...");
                ctx.shutdown().await;
                break;
            }
        }
    }
}
