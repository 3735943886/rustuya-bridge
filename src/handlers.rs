use rustuya::Device;
use rustuya::protocol::CommandType;
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

use crate::bridge::BridgeContext;
use crate::bridge::REQUEST_TIMEOUT_SECS;
use crate::config::DeviceConfig;
use crate::error::BridgeError;
use crate::types::{ApiResponse, BridgeRequest, SingleOrList};
use futures_util::StreamExt;

/// Main entry point for processing bridge requests
pub async fn handle_request(ctx: Arc<BridgeContext>, req: BridgeRequest) -> ApiResponse {
    let action = req.action_name().to_string();
    let id = req.target_id().map(ToString::to_string);

    match handle_request_inner(ctx, req).await {
        Ok(res) => res,
        Err(e) => {
            let mut res = ApiResponse::error(e).with_action(action);
            if let Some(id) = id {
                res = res.with_id(id);
            }
            res
        }
    }
}

/// Resolves targets, then runs `op(device, resolved_cid)` for each connected target with a timeout.
/// Errors map to [`BridgeError::DeviceError`] (op failure) or [`BridgeError::Timeout`].
async fn execute_per_target<F, Fut, T, E>(
    ctx: &Arc<BridgeContext>,
    targets: &[String],
    op_label: &str,
    cid: Option<String>,
    op: F,
) -> Result<(), BridgeError>
where
    F: Fn(Device, Option<String>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    for target_id in targets {
        let actual_cid = ctx.resolve_cid(target_id, cid.clone()).await;
        let Ok(dev) = ctx.get_connected_device(target_id).await else {
            continue;
        };
        match timeout(
            Duration::from_secs(REQUEST_TIMEOUT_SECS),
            op(dev, actual_cid),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(BridgeError::DeviceError(e.to_string())),
            Err(_) => {
                return Err(BridgeError::Timeout(format!(
                    "{op_label} timeout for {target_id}"
                )));
            }
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "single dispatch switch over the BridgeRequest enum; splitting hurts locality"
)]
async fn handle_request_inner(
    ctx: Arc<BridgeContext>,
    req: BridgeRequest,
) -> Result<ApiResponse, BridgeError> {
    match req {
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
                last_error_code: None,
            })
            .await
        }
        BridgeRequest::Remove { id, name } => {
            ctx.remove_device(
                id.map(SingleOrList::into_vec),
                name.map(SingleOrList::into_vec),
            )
            .await
        }
        BridgeRequest::Clear => ctx.clear_devices().await,
        BridgeRequest::Status => Ok(ctx.get_bridge_status().await),
        BridgeRequest::Get { id, name, cid } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            execute_per_target(&ctx, &targets, "get", cid, |dev, actual_cid| async move {
                dev.request(CommandType::DpQuery, None, actual_cid).await
            })
            .await?;
            Ok(ApiResponse::ok("get", targets.join(",")))
        }
        BridgeRequest::Set { id, name, dps, cid } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            execute_per_target(&ctx, &targets, "set", cid, |dev, actual_cid| {
                let dps = dps.clone();
                async move {
                    dev.request(CommandType::Control, Some(Value::Object(dps)), actual_cid)
                        .await
                }
            })
            .await?;
            Ok(ApiResponse::ok("set", targets.join(",")))
        }
        BridgeRequest::Request {
            id,
            name,
            cmd,
            data,
            cid,
        } => {
            let command = CommandType::from_u32(cmd).ok_or(BridgeError::InvalidCommand(cmd))?;
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            execute_per_target(&ctx, &targets, "request", cid, |dev, actual_cid| {
                let data = data.clone();
                async move { dev.request(command, data, actual_cid).await }
            })
            .await?;
            Ok(ApiResponse::ok(
                format!("{command:?}").to_lowercase(),
                targets.join(","),
            ))
        }
        BridgeRequest::SubDiscover { id, name } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            execute_per_target(&ctx, &targets, "sub_discover", None, |dev, _| async move {
                dev.sub_discover().await
            })
            .await?;
            Ok(ApiResponse::ok("sub_discover", targets.join(",")))
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
                "Scan started. Results will be published to 'scanner' topic.",
            ))
        }
    }
}
