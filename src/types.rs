use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SingleOrList {
    Single(String),
    List(Vec<String>),
}

impl SingleOrList {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            Self::Single(s) => vec![s],
            Self::List(l) => l,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BridgeRequest {
    Add {
        id: String,
        name: Option<String>,
        #[serde(default = "default_auto")]
        key: String,
        #[serde(default = "default_auto")]
        ip: String,
        #[serde(default = "default_auto")]
        version: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cid: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_id: Option<String>,
    },
    Remove {
        id: Option<SingleOrList>,
        name: Option<SingleOrList>,
    },
    Clear,
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "get")]
    Get {
        id: Option<SingleOrList>,
        name: Option<SingleOrList>,
        cid: Option<String>,
    },
    #[serde(rename = "set")]
    Set {
        id: Option<SingleOrList>,
        name: Option<SingleOrList>,
        dps: serde_json::Map<String, Value>,
        cid: Option<String>,
    },
    #[serde(rename = "request")]
    Request {
        id: Option<SingleOrList>,
        name: Option<SingleOrList>,
        cmd: u32,
        data: Option<Value>,
        cid: Option<String>,
    },
    #[serde(rename = "sub_discover")]
    SubDiscover {
        id: Option<SingleOrList>,
        name: Option<SingleOrList>,
    },
    Scan,
}

fn default_auto() -> String {
    "Auto".to_string()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Error,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl ApiResponse {
    pub fn ok(action: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            status: Status::Ok,
            action: Some(action.into()),
            id: Some(id.into()),
            error: None,
            extra: HashMap::new(),
        }
    }

    pub fn error(err: impl std::fmt::Display) -> Self {
        Self {
            status: Status::Error,
            action: None,
            id: None,
            error: Some(err.to_string()),
            extra: HashMap::new(),
        }
    }

    pub fn with_extra(mut self, key: &str, value: Value) -> Self {
        self.extra.insert(key.to_string(), value);
        self
    }
}
