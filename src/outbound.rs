//! What the bridge tells its clients, in its own terms. A transport decides how each
//! event goes out: which topic, what payload, whether it is kept.

use serde_json::Value;

use crate::types::ApiResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Active,
    Passive,
}

#[derive(Debug)]
pub enum Outbound {
    /// DPs a device just reported, exactly as it reported them.
    DeviceDelta {
        id: String,
        name: Option<String>,
        cid: Option<String>,
        dps: Value,
        kind: DeltaKind,
    },
    /// A device's merged state. `changed` lists the DPs this update changed, for a
    /// transport that sends them one at a time; `None` is a full resync.
    DeviceState {
        id: String,
        name: Option<String>,
        cid: Option<String>,
        dps: Value,
        changed: Option<Vec<String>>,
    },
    /// A device's connection status (`errorCode`).
    DeviceStatus {
        id: String,
        name: Option<String>,
        cid: Option<String>,
        payload: Value,
    },
    /// The answer to a request. `name` is the name of the device it is about.
    Response {
        response: ApiResponse,
        name: Option<String>,
    },
    /// A device found by a scan; an empty object marks the scan's end.
    ScanResult(Value),
    /// The bridge's running configuration changed, or it just started.
    BridgeInfo,
}
