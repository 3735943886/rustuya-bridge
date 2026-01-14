use thiserror::Error;

#[derive(Error, Debug)]
pub enum BridgeError {
    #[error("Device not found: {0}")]
    DeviceNotFound(String),
    #[error("Invalid command: {0}")]
    InvalidCommand(u32),
    #[error("Matching devices not found")]
    NoMatchingDevices,
    #[error("Serialization failed: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
}
