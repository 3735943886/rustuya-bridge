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

/// Builds one `ApiResponse` **per target id** from per-target results.
///
/// Multi-target commands (name fan-out, sub-device cascade) answer with one
/// response per affected id — each addressed to its own `{id}` so it lands on
/// that device's response topic — instead of a single comma-joined `id` that
/// would put commas in the topic and be unroutable to any single subscriber.
///
/// `suppress_single_ok` drops the response when there is exactly one target and
/// it succeeded: a single successful `set`/`get` is redundant (the device's own
/// event/state already reflects it). A fan-out always emits, so the caller sees
/// every device it touched; failures always emit.
fn responses_for_results(
    action: &str,
    results: Vec<(String, Result<(), BridgeError>)>,
    suppress_single_ok: bool,
) -> Vec<ApiResponse> {
    if suppress_single_ok && results.len() == 1 && results[0].1.is_ok() {
        return Vec::new();
    }
    results
        .into_iter()
        .map(|(id, result)| match result {
            Ok(()) => ApiResponse::ok(action, id),
            Err(e) => ApiResponse::error(e).with_action(action).with_id(id),
        })
        .collect()
}

/// Main entry point for processing bridge requests. Returns one response per
/// affected target (see [`responses_for_results`]); an empty `Vec` means the
/// result was deliberately suppressed (single successful `set`/`get`).
pub async fn handle_request(ctx: Arc<BridgeContext>, req: BridgeRequest) -> Vec<ApiResponse> {
    let action = req.action_name().to_string();
    let id = req.target_id().map(ToString::to_string);

    match handle_request_inner(ctx, req).await {
        Ok(responses) => responses,
        Err(e) => {
            let mut res = ApiResponse::error(e).with_action(action);
            if let Some(id) = id {
                res = res.with_id(id);
            }
            vec![res]
        }
    }
}

/// Resolves targets, then runs `op(device, resolved_cid)` for each connected
/// target with a timeout, collecting a per-target result. Unlike an
/// abort-on-first-error fold, every target is attempted so each can report its
/// own outcome. A target that isn't currently connected is skipped and recorded
/// as `Ok` (its offline state surfaces separately on the device error topic).
async fn run_per_target<F, Fut, T, E>(
    ctx: &Arc<BridgeContext>,
    targets: &[String],
    op_label: &str,
    cid: Option<String>,
    op: F,
) -> Vec<(String, Result<(), BridgeError>)>
where
    F: Fn(Device, Option<String>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut results = Vec::with_capacity(targets.len());
    for target_id in targets {
        let actual_cid = ctx.resolve_cid(target_id, cid.clone()).await;
        let Ok(dev) = ctx.get_connected_device(target_id).await else {
            results.push((target_id.clone(), Ok(())));
            continue;
        };
        let result = match timeout(
            Duration::from_secs(REQUEST_TIMEOUT_SECS),
            op(dev, actual_cid),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(BridgeError::DeviceError(e.to_string())),
            Err(_) => Err(BridgeError::Timeout(format!(
                "{op_label} timeout for {target_id}"
            ))),
        };
        results.push((target_id.clone(), result));
    }
    results
}

#[allow(
    clippy::too_many_lines,
    reason = "single dispatch switch over the BridgeRequest enum; splitting hurts locality"
)]
async fn handle_request_inner(
    ctx: Arc<BridgeContext>,
    req: BridgeRequest,
) -> Result<Vec<ApiResponse>, BridgeError> {
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
            let res = ctx
                .add_device(DeviceConfig {
                    id,
                    name,
                    ip,
                    key,
                    version,
                    cid,
                    parent_id,
                    last_error_code: None,
                })
                .await?;
            Ok(vec![res])
        }
        BridgeRequest::Remove { id, name } => {
            // Cascade + name fan-out all answer per-id: each removed device
            // (parent and every cascaded sub) gets its own `remove` ok on its
            // own response topic.
            let removed = ctx
                .remove_device(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            Ok(removed
                .into_iter()
                .map(|rid| ApiResponse::ok("remove", rid))
                .collect())
        }
        BridgeRequest::Clear => Ok(vec![ctx.clear_devices().await?]),
        BridgeRequest::Reconfigure => Ok(vec![ctx.reconfigure().await?]),
        BridgeRequest::Status { offset, limit } => {
            Ok(vec![ctx.get_bridge_status(offset, limit).await])
        }
        BridgeRequest::Get { id, name, cid } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            let results = run_per_target(&ctx, &targets, "get", cid, |dev, actual_cid| async move {
                dev.request(CommandType::DpQuery, None, actual_cid).await
            })
            .await;
            Ok(responses_for_results("get", results, true))
        }
        BridgeRequest::Set { id, name, dps, cid } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            let results = run_per_target(&ctx, &targets, "set", cid, |dev, actual_cid| {
                let dps = dps.clone();
                async move {
                    dev.request(CommandType::Control, Some(Value::Object(dps)), actual_cid)
                        .await
                }
            })
            .await;
            Ok(responses_for_results("set", results, true))
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
            let results = run_per_target(&ctx, &targets, "request", cid, |dev, actual_cid| {
                let data = data.clone();
                async move { dev.request(command, data, actual_cid).await }
            })
            .await;
            let label = format!("{command:?}").to_lowercase();
            Ok(responses_for_results(&label, results, false))
        }
        BridgeRequest::SubDiscover { id, name } => {
            let targets = ctx
                .get_targets(
                    id.map(SingleOrList::into_vec),
                    name.map(SingleOrList::into_vec),
                )
                .await?;
            let results =
                run_per_target(&ctx, &targets, "sub_discover", None, |dev, _| async move {
                    dev.sub_discover().await
                })
                .await;
            Ok(responses_for_results("sub_discover", results, false))
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

            Ok(vec![ApiResponse::ok("scan", "bridge").with_extra(
                "message",
                "Scan started. Results will be published to 'scanner' topic.",
            )])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_ok_is_suppressed_for_set_get() {
        let results = vec![("dev-only".to_string(), Ok(()))];
        let out = responses_for_results("set", results, true);
        assert!(out.is_empty(), "a lone successful set/get must be suppressed");
    }

    #[test]
    fn single_error_is_emitted_even_when_suppressing() {
        let results = vec![(
            "dev-only".to_string(),
            Err(BridgeError::Timeout("set timeout for dev-only".into())),
        )];
        let out = responses_for_results("set", results, true);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_deref(), Some("dev-only"));
        assert!(out[0].error.is_some());
    }

    #[test]
    fn fan_out_emits_one_response_per_id() {
        let results = vec![
            ("dev-a".to_string(), Ok(())),
            ("dev-b".to_string(), Ok(())),
        ];
        let out = responses_for_results("set", results, true);
        let ids: Vec<_> = out.iter().filter_map(|r| r.id.as_deref()).collect();
        assert_eq!(ids, vec!["dev-a", "dev-b"], "each target answers for itself");
        // No comma-joined id — each response addresses exactly one device.
        assert!(out.iter().all(|r| !r.id.as_deref().unwrap_or("").contains(',')));
    }

    #[test]
    fn non_suppressed_action_emits_single_ok() {
        // request/sub_discover never suppress — a single target still answers.
        let results = vec![("dev-only".to_string(), Ok(()))];
        let out = responses_for_results("sub_discover", results, false);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id.as_deref(), Some("dev-only"));
    }
}
