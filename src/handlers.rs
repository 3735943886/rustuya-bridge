use rustuya::protocol::CommandType;
use serde_json::Value;
use std::sync::Arc;

use crate::bridge::BridgeContext;
use crate::config::DeviceConfig;
use crate::error::BridgeError;
use crate::types::{ApiResponse, BridgeRequest};
use futures_util::StreamExt;

/// Main entry point for processing bridge requests
pub async fn handle_request(ctx: Arc<BridgeContext>, req: BridgeRequest) -> ApiResponse {
    let result = match req {
        BridgeRequest::Add {
            id,
            name,
            key,
            ip,
            version,
            cid,
            parent_id,
        } => {
            ctx.add_device(DeviceConfig {
                id,
                name,
                ip,
                key,
                version,
                cid,
                parent_id,
            })
            .await
        }
        BridgeRequest::Remove { id, name } => {
            ctx.remove_device(id.map(|s| s.into_vec()), name.map(|s| s.into_vec()))
                .await
        }
        BridgeRequest::Clear => ctx.clear_devices().await,
        BridgeRequest::Status => Ok(ctx.get_bridge_status().await),
        BridgeRequest::Get { id, name, cid } => {
            let targets = ctx
                .find_device_ids(id.map(|s| s.into_vec()), name.map(|s| s.into_vec()))
                .await;
            if targets.is_empty() {
                Err(BridgeError::NoMatchingDevices)
            } else {
                for target_id in &targets {
                    // Determine actual CID: provided in request or from device config
                    let actual_cid = if cid.is_some() {
                        cid.clone()
                    } else {
                        let configs = ctx.configs.read().await;
                        configs.get(target_id).and_then(|c| c.cid.clone())
                    };

                    if let Ok(dev) = ctx.get_connected_device(target_id).await {
                        let _ = dev.request(CommandType::DpQuery, None, actual_cid).await;
                    }
                }
                Ok(ApiResponse::ok("get", targets.join(",")))
            }
        }
        BridgeRequest::Set { id, name, dps, cid } => {
            let targets = ctx
                .find_device_ids(id.map(|s| s.into_vec()), name.map(|s| s.into_vec()))
                .await;
            if targets.is_empty() {
                Err(BridgeError::NoMatchingDevices)
            } else {
                for target_id in &targets {
                    // Determine actual CID: provided in request or from device config
                    let actual_cid = if cid.is_some() {
                        cid.clone()
                    } else {
                        let configs = ctx.configs.read().await;
                        configs.get(target_id).and_then(|c| c.cid.clone())
                    };

                    if let Ok(dev) = ctx.get_connected_device(target_id).await {
                        let _ = dev
                            .request(
                                CommandType::Control,
                                Some(Value::Object(dps.clone())),
                                actual_cid,
                            )
                            .await;
                    }
                }
                Ok(ApiResponse::ok("set", targets.join(",")))
            }
        }
        BridgeRequest::Request {
            id,
            name,
            cmd,
            data,
            cid,
        } => {
            let command_res = CommandType::from_u32(cmd).ok_or(BridgeError::InvalidCommand(cmd));

            match command_res {
                Ok(command) => {
                    let targets = ctx
                        .find_device_ids(id.map(|s| s.into_vec()), name.map(|s| s.into_vec()))
                        .await;
                    if targets.is_empty() {
                        Err(BridgeError::NoMatchingDevices)
                    } else {
                        for target_id in &targets {
                            // Determine actual CID: provided in request or from device config
                            let actual_cid = if cid.is_some() {
                                cid.clone()
                            } else {
                                let configs = ctx.configs.read().await;
                                configs.get(target_id).and_then(|c| c.cid.clone())
                            };

                            if let Ok(dev) = ctx.get_connected_device(target_id).await {
                                let _ = dev.request(command, data.clone(), actual_cid).await;
                            }
                        }
                        Ok(ApiResponse::ok(
                            format!("{:?}", command).to_lowercase(),
                            targets.join(","),
                        ))
                    }
                }
                Err(e) => Err(e),
            }
        }
        BridgeRequest::SubDiscover { id, name } => {
            let targets = ctx
                .find_device_ids(id.map(|s| s.into_vec()), name.map(|s| s.into_vec()))
                .await;
            if targets.is_empty() {
                Err(BridgeError::NoMatchingDevices)
            } else {
                for target_id in &targets {
                    if let Ok(dev) = ctx.get_connected_device(target_id).await {
                        let _ = dev.sub_discover().await;
                    }
                }
                Ok(ApiResponse::ok("sub_discover", targets.join(",")))
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

                    ctx_scan.publish_scanner_event(Value::Object(payload)).await;
                }

                // Final empty payload marks scan end
                ctx_scan
                    .publish_scanner_event(Value::Object(serde_json::Map::new()))
                    .await;
            });

            Ok(ApiResponse::ok("scan", "bridge").with_extra(
                "message",
                "Scan started. Results will be published to 'scanner' topic.".into(),
            ))
        }
    };

    match result {
        Ok(res) => res,
        Err(e) => ApiResponse::error(e),
    }
}
