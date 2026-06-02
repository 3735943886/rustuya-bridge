//! Payload parsing for both directions: inbound *commands* the bridge
//! receives (heuristic-driven, [`parse_mqtt_payload`]) and outbound *event
//! retains* the bridge produced (template-driven reverse parse,
//! [`parse_payload_with_template`]).
//!
//! ### Reverse template parsing
//!
//! The bridge's [`crate::template::render_template`] substitutes `{var}`
//! placeholders forward — payload template + variables → MQTT payload
//! string. The reverse goes the other way: given the template and a
//! concrete payload the bridge produced from it, recover each
//! placeholder's captured value.
//!
//! Algorithm:
//!
//! 1. Replace every recognized `{var}` (and the equivalent `"{var}"` for
//!    already-quoted occurrences) with a unique sentinel string, wrapping
//!    in quotes so the result is always valid JSON at parse time.
//! 2. Parse both the sentinelled template and the incoming payload as JSON.
//! 3. Walk both trees in parallel — at each position where the template
//!    element is a sentinel, capture the payload element (whatever its
//!    type) under the corresponding var name.
//!
//! Failure modes (all return `None`):
//! - The sentinelled template doesn't parse as JSON (text-style templates
//!   like `value={value};ts={timestamp}` — sentinel substitution leaves
//!   something that still isn't valid JSON).
//! - The payload isn't valid JSON.
//! - The two structures don't match (different keys, wrong array length,
//!   mismatched literals).
//! - The template has no recognized placeholders to capture.
//!
//! ### Inbound command parsing
//!
//! [`parse_mqtt_payload`] is the bridge's permissive parser for commands
//! received on the command topic. It merges topic-extracted variables
//! into the payload object, wraps scalars under `dps` when a `{dp}` is
//! present, and applies the `set` heuristic via [`apply_set_heuristic`]
//! (loose top-level fields become DP id/value pairs unless they collide
//! with a reserved `BridgeRequest` field name).

use crate::template::TOPIC_VARS;
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Placeholders that may appear in payload templates but never in topic
/// templates — they have no value at topic-build time. The reverse parser
/// composes its placeholder regex from `template::TOPIC_VARS ∪
/// PAYLOAD_ONLY_VARS` so the topic side stays the single source of truth
/// for topic vocabulary.
pub const PAYLOAD_ONLY_VARS: &[&str] = &["value", "dps", "timestamp", "root"];

fn placeholder_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // Match either a quoted placeholder `"{var}"` or a bare `{var}`.
        // Group 1 = quoted form, group 2 = bare form (at most one fires per match).
        let alt: Vec<&str> = TOPIC_VARS
            .iter()
            .chain(PAYLOAD_ONLY_VARS.iter())
            .copied()
            .collect();
        let pattern = format!(
            r#""\{{({0})\}}"|\{{({0})\}}"#,
            alt.join("|")
        );
        Regex::new(&pattern).expect("known-vars regex compiles")
    })
}

/// Returns `(sentinelled_template, {sentinel_string -> var_name})`.
fn substitute_sentinels(template: &str) -> (String, HashMap<String, String>) {
    let mut sentinels: HashMap<String, String> = HashMap::new();
    let re = placeholder_regex();
    let mut counter: usize = 0;
    let sentinelled = re
        .replace_all(template, |caps: &regex::Captures<'_>| {
            let var = caps
                .get(1)
                .or_else(|| caps.get(2))
                .expect("one of the alternation groups must match")
                .as_str();
            let sent = format!("__RB_S_{counter}__");
            counter += 1;
            sentinels.insert(sent.clone(), var.to_string());
            format!("\"{sent}\"")
        })
        .into_owned();
    (sentinelled, sentinels)
}

/// Recursive structural match. Captures payload values at each sentinel
/// position; on any structural mismatch returns `false` and the captures
/// map should be discarded by the caller.
fn walk(
    template_val: &Value,
    payload_val: &Value,
    sentinels: &HashMap<String, String>,
    captures: &mut HashMap<String, Value>,
) -> bool {
    // Sentinel position — capture whatever the payload holds here.
    if let Value::String(s) = template_val
        && let Some(var) = sentinels.get(s)
    {
        captures.insert(var.clone(), payload_val.clone());
        return true;
    }
    match (template_val, payload_val) {
        (Value::Object(t_obj), Value::Object(p_obj)) => {
            if t_obj.len() != p_obj.len() {
                return false;
            }
            for (k, t_v) in t_obj {
                let Some(p_v) = p_obj.get(k) else {
                    return false;
                };
                if !walk(t_v, p_v, sentinels, captures) {
                    return false;
                }
            }
            true
        }
        (Value::Array(t_arr), Value::Array(p_arr)) => {
            if t_arr.len() != p_arr.len() {
                return false;
            }
            t_arr
                .iter()
                .zip(p_arr.iter())
                .all(|(t, p)| walk(t, p, sentinels, captures))
        }
        _ => template_val == payload_val,
    }
}

/// Extracts every `{var}`'s captured value from a concrete payload produced
/// using `template`. Returns `None` when the inputs don't match the
/// template structure or the template has no recognized placeholders. See
/// the module docs for the full failure-mode list.
#[must_use]
pub fn parse_payload_with_template(payload: &str, template: &str) -> Option<HashMap<String, Value>> {
    let (sentinelled, sentinels) = substitute_sentinels(template);
    if sentinels.is_empty() {
        return None;
    }
    let template_val: Value = serde_json::from_str(&sentinelled).ok()?;
    let payload_val: Value = serde_json::from_str(payload).ok()?;
    let mut captures: HashMap<String, Value> = HashMap::new();
    if !walk(&template_val, &payload_val, &sentinels, &mut captures) {
        return None;
    }
    Some(captures)
}

/// Answers "can this template be reverse-parsed at all?" — useful at
/// startup to flag user configs the seed phase can't recover from. Returns
/// `Ok(())` if the template is parseable, `Err(human_message)` otherwise.
///
/// # Errors
/// Returns the human-readable reason the template isn't reverse-parseable:
/// no recognized placeholders, or invalid JSON after sentinel substitution.
pub fn validate_payload_template(template: &str) -> Result<(), String> {
    let (sentinelled, sentinels) = substitute_sentinels(template);
    if sentinels.is_empty() {
        return Err(format!(
            "payload template '{template}' has no recognized placeholders \
             ({{value}}, {{dps}}, etc.) — bridge can't extract DPS from \
             its own retained snapshots"
        ));
    }
    serde_json::from_str::<Value>(&sentinelled).map_err(|e| {
        format!(
            "payload template '{template}' isn't valid JSON after placeholder \
             substitution ({e}); use a JSON shape (e.g. '{{value}}', \
             '{{\"v\":{{value}}}}', '{{\"id\":\"{{id}}\",\"data\":{{dps}}}}')"
        )
    })?;
    Ok(())
}

/// Convenience wrapper specialized for the seed-phase use case: extract the
/// DPS values from a retained event payload. Handles both topic modes:
///
/// - Single-DP (`dp` extracted from topic): the captured `{value}` is the
///   per-DP value; wrap into `{dp: value}`.
/// - Multi-DP (no `dp` in topic): the captured `{value}` or `{dps}` is the
///   full DPS dict.
///
/// Short-circuits the default `{value}` template (no template parsing
/// needed — payload *is* the value/dict directly), so the fast path stays
/// fast.
#[must_use]
pub fn parse_seed_dps(
    payload: &str,
    dp: Option<&str>,
    template: Option<&str>,
) -> Option<Map<String, Value>> {
    let is_default_tpl = matches!(template, None | Some("{value}"));
    if is_default_tpl {
        let v: Value = serde_json::from_str(payload).ok()?;
        return match dp {
            Some(dp_key) => {
                let mut m = Map::new();
                m.insert(dp_key.to_string(), v);
                Some(m)
            }
            None => v.as_object().cloned(),
        };
    }

    let captures = parse_payload_with_template(payload, template?)?;
    // Prefer `value` (the canonical DP payload var) but accept `dps` as
    // a fallback for templates that use that name instead.
    let dps_val = captures.get("value").or_else(|| captures.get("dps"))?;

    match dp {
        Some(dp_key) => {
            let mut m = Map::new();
            m.insert(dp_key.to_string(), dps_val.clone());
            Some(m)
        }
        None => dps_val.as_object().cloned(),
    }
}

// ── Inbound command parsing ────────────────────────────────────────────────

/// Parses an inbound MQTT command payload into a JSON value, merging in
/// topic-extracted variables (`id`, `name`, `dp`, etc.) so the downstream
/// `BridgeRequest` deserializer sees them as if they were in the payload.
///
/// Three layers of permissive handling:
///
/// 1. Non-JSON payload: wrap as `{"payload": "<raw>"}` so vars can still
///    merge. When a `{dp}` topic var is present, the bare scalar becomes
///    `{"dps": {"<dp>": <scalar>}}` instead — matches the common
///    "publish a value to a per-DP command topic" idiom.
/// 2. Topic vars don't overwrite payload keys (caller's explicit fields win).
/// 3. For `action == "set"`, [`apply_set_heuristic`] auto-promotes loose
///    top-level fields into `dps` unless a `dps`/`data` key is already
///    present.
///
/// Arrays of objects are handled per-element (vars merge into each item).
#[must_use]
pub fn parse_mqtt_payload(payload: &str, vars: &HashMap<String, String>) -> Value {
    let mut val =
        serde_json::from_str::<Value>(payload).unwrap_or_else(|_| Value::String(payload.to_string()));

    // Array: merge vars into each object element (and apply `set` heuristic).
    if let Some(arr) = val.as_array_mut() {
        for item in arr {
            if let Some(obj) = item.as_object_mut() {
                for (k, v) in vars {
                    if !obj.contains_key(k) {
                        obj.insert(k.clone(), Value::String(v.clone()));
                    }
                }
                if obj.get("action").and_then(Value::as_str) == Some("set") {
                    apply_set_heuristic(obj);
                }
            }
        }
        return val;
    }

    // Scalar: wrap so vars can be merged. With `{dp}` from topic, the
    // scalar becomes the value for that DP; otherwise stash under `payload`.
    if !val.is_object() {
        let mut obj = Map::new();
        if let Some(dp) = vars.get("dp") {
            let mut dps = Map::new();
            dps.insert(dp.clone(), val);
            obj.insert("dps".to_string(), Value::Object(dps));
        } else {
            obj.insert("payload".to_string(), val);
        }
        val = Value::Object(obj);
    }

    if let Some(obj) = val.as_object_mut() {
        for (k, v) in vars {
            if !obj.contains_key(k) {
                obj.insert(k.clone(), Value::String(v.clone()));
            }
        }
        if obj.get("action").and_then(Value::as_str) == Some("set") {
            apply_set_heuristic(obj);
        }
    }
    val
}

/// `set` action helper: when the caller didn't provide an explicit `dps`
/// (or `data`) object, treat every top-level field that is *not* a
/// reserved `BridgeRequest` key as a DP id/value pair and collect them
/// under `dps`. The deny-list must cover every field used by any
/// `BridgeRequest` variant (plus topic-merged keys like `dp` and the
/// scalar-wrap key `payload`) — adding a new field to `BridgeRequest`
/// without updating this list will silently turn it into a DP write.
///
/// The loose top-level fields are *not* removed after copying into `dps`;
/// `BridgeRequest`'s deserializer ignores fields it doesn't recognize, so
/// leaving them avoids touching the parent object's shape (preserving the
/// historical behavior the test suite locks in).
pub fn apply_set_heuristic(obj: &mut Map<String, Value>) {
    const RESERVED_FIELDS: &[&str] = &[
        // BridgeRequest variants' fields:
        "action",
        "id",
        "name",
        "key",
        "ip",
        "version",
        "cid",
        "parent_id",
        "cmd",
        "data",
        "dps",
        // Topic-merged / scalar-wrap synthetic keys:
        "dp",
        "payload",
    ];
    if !obj.contains_key("dps") && !obj.contains_key("data") {
        let mut dps = obj.clone();
        for f in RESERVED_FIELDS {
            dps.remove(*f);
        }
        obj.insert("dps".to_string(), Value::Object(dps));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_template_extracts_value() {
        let c = parse_payload_with_template("true", "{value}").unwrap();
        assert_eq!(c.get("value"), Some(&json!(true)));
    }

    #[test]
    fn wrapped_template_extracts_value_at_path() {
        let c = parse_payload_with_template(
            r#"{"type":"passive","value":true}"#,
            r#"{"type":"{type}","value":{value}}"#,
        )
        .unwrap();
        assert_eq!(c.get("type"), Some(&json!("passive")));
        assert_eq!(c.get("value"), Some(&json!(true)));
    }

    #[test]
    fn deeply_nested_template_extracts_dps() {
        let c = parse_payload_with_template(
            r#"{"id":"dev-1","data":{"dps":{"1":true,"2":50}}}"#,
            r#"{"id":"{id}","data":{"dps":{dps}}}"#,
        )
        .unwrap();
        assert_eq!(c.get("id"), Some(&json!("dev-1")));
        assert_eq!(c.get("dps"), Some(&json!({"1": true, "2": 50})));
    }

    #[test]
    fn multi_dp_dict_value_captured() {
        let c = parse_payload_with_template(
            r#"{"1":true,"2":50}"#,
            r#"{value}"#,
        )
        .unwrap();
        assert_eq!(c.get("value"), Some(&json!({"1": true, "2": 50})));
    }

    #[test]
    fn structural_mismatch_returns_none() {
        // Template expects "value" key, payload has "v"
        let c = parse_payload_with_template(
            r#"{"type":"passive","v":true}"#,
            r#"{"type":"{type}","value":{value}}"#,
        );
        assert!(c.is_none());
    }

    #[test]
    fn literal_mismatch_returns_none() {
        // Template has a literal "passive" string outside any placeholder
        let c = parse_payload_with_template(
            r#"{"label":"active","value":true}"#,
            r#"{"label":"passive","value":{value}}"#,
        );
        assert!(c.is_none());
    }

    #[test]
    fn no_placeholders_returns_none() {
        let c = parse_payload_with_template(r#"{}"#, r#"{}"#);
        assert!(c.is_none(), "template without {{var}} placeholders is unparseable");
    }

    #[test]
    fn non_json_template_returns_none() {
        // Text-style templates can't be sentinelled into valid JSON.
        let c = parse_payload_with_template("v=true;ts=1", "v={value};ts={timestamp}");
        assert!(c.is_none());
    }

    #[test]
    fn unrecognized_placeholder_left_literal_causes_mismatch() {
        // `{foo}` isn't in KNOWN_VARS — stays as literal text in template
        // → JSON parse fails on `{foo}` (not valid JSON literal).
        let c = parse_payload_with_template(r#"{"value":true}"#, r#"{"value":{foo}}"#);
        assert!(c.is_none());
    }

    #[test]
    fn quoted_and_bare_placeholders_both_work() {
        // {type} is bare in payload template; {value} is the JSON value.
        let c = parse_payload_with_template(
            r#"{"type":"passive","value":42}"#,
            r#"{"type":"{type}","value":{value}}"#,
        )
        .unwrap();
        assert_eq!(c.get("type"), Some(&json!("passive")));
        assert_eq!(c.get("value"), Some(&json!(42)));
    }

    #[test]
    fn validate_default_template_ok() {
        assert!(validate_payload_template("{value}").is_ok());
    }

    #[test]
    fn validate_wrapped_template_ok() {
        assert!(validate_payload_template(r#"{"type":"{type}","value":{value}}"#).is_ok());
    }

    #[test]
    fn validate_no_placeholders_errors() {
        let err = validate_payload_template("{}").unwrap_err();
        assert!(err.contains("no recognized placeholders"));
    }

    #[test]
    fn validate_non_json_template_errors() {
        let err = validate_payload_template("v={value};ts={timestamp}").unwrap_err();
        assert!(err.contains("isn't valid JSON"));
    }

    // ── parse_seed_dps integration ───────────────────────────────────────

    #[test]
    fn seed_dps_default_template_single_dp() {
        // Payload IS the value, dp from topic
        let dps = parse_seed_dps("true", Some("1"), None).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
    }

    #[test]
    fn seed_dps_default_template_multi_dp() {
        // Payload IS the dps dict
        let dps = parse_seed_dps(r#"{"1":true,"2":50}"#, None, Some("{value}")).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        assert_eq!(dps.get("2"), Some(&json!(50)));
    }

    #[test]
    fn seed_dps_custom_template_single_dp() {
        let dps = parse_seed_dps(
            r#"{"type":"passive","value":true}"#,
            Some("13"),
            Some(r#"{"type":"{type}","value":{value}}"#),
        )
        .unwrap();
        assert_eq!(dps.get("13"), Some(&json!(true)));
    }

    #[test]
    fn seed_dps_custom_template_multi_dp() {
        let dps = parse_seed_dps(
            r#"{"type":"passive","value":{"1":true,"2":50}}"#,
            None,
            Some(r#"{"type":"{type}","value":{value}}"#),
        )
        .unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        assert_eq!(dps.get("2"), Some(&json!(50)));
    }

    #[test]
    fn seed_dps_template_using_dps_var_works() {
        let dps = parse_seed_dps(
            r#"{"data":{"dps":{"1":true}}}"#,
            None,
            Some(r#"{"data":{"dps":{dps}}}"#),
        )
        .unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
    }

    #[test]
    fn seed_dps_unparseable_template_returns_none() {
        let dps = parse_seed_dps("anything", Some("1"), Some("v={value}"));
        assert!(dps.is_none());
    }

    // ── parse_mqtt_payload / apply_set_heuristic ─────────────────────────

    #[test]
    fn parse_payload_merges_topic_vars_into_object() {
        let vars: HashMap<String, String> =
            [("id".into(), "dev-1".into())].into_iter().collect();
        let val = parse_mqtt_payload(r#"{"action":"get"}"#, &vars);
        assert_eq!(val.get("id").and_then(|v| v.as_str()), Some("dev-1"));
        assert_eq!(val.get("action").and_then(|v| v.as_str()), Some("get"));
    }

    #[test]
    fn parse_payload_topic_vars_do_not_overwrite_payload_keys() {
        let vars: HashMap<String, String> =
            [("id".into(), "from-topic".into())].into_iter().collect();
        let val = parse_mqtt_payload(r#"{"id":"from-payload"}"#, &vars);
        assert_eq!(
            val.get("id").and_then(|v| v.as_str()),
            Some("from-payload")
        );
    }

    #[test]
    fn parse_payload_wraps_scalar_with_dp_var_into_dps() {
        let vars: HashMap<String, String> = [("dp".into(), "1".into())].into_iter().collect();
        let val = parse_mqtt_payload("true", &vars);
        assert_eq!(val.get("dps").and_then(|v| v.get("1")), Some(&json!(true)));
    }

    #[test]
    fn parse_payload_set_heuristic_promotes_loose_fields_into_dps() {
        let vars: HashMap<String, String> = HashMap::new();
        let val = parse_mqtt_payload(
            r#"{"action":"set","id":"dev-1","1":true,"2":50}"#,
            &vars,
        );
        let dps = val.get("dps").and_then(|v| v.as_object()).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        assert_eq!(dps.get("2"), Some(&json!(50)));
        // Selector/action fields must not be smuggled into dps.
        assert!(!dps.contains_key("id"));
        assert!(!dps.contains_key("action"));
    }

    #[test]
    fn parse_payload_set_heuristic_excludes_all_bridge_request_fields() {
        // When the heuristic runs (no explicit `dps` / `data`), every
        // BridgeRequest top-level field (key/ip/version/cid/parent_id/cmd
        // + the existing core ones) must be stripped from auto-built dps
        // so a typo'd/misplaced reserved field can't silently become a DP
        // write.
        let vars: HashMap<String, String> = HashMap::new();
        let val = parse_mqtt_payload(
            r#"{"action":"set","id":"dev","name":"n","key":"k","ip":"1.2.3.4",
                "version":"3.3","cid":"c","parent_id":"p","cmd":7,
                "dp":"99","payload":"x","1":true}"#,
            &vars,
        );
        let dps = val.get("dps").and_then(|v| v.as_object()).unwrap();
        assert_eq!(dps.get("1"), Some(&json!(true)));
        for reserved in [
            "action", "id", "name", "key", "ip", "version", "cid", "parent_id",
            "cmd", "dp", "payload", "dps",
        ] {
            assert!(
                !dps.contains_key(reserved),
                "reserved field `{reserved}` leaked into auto-built dps"
            );
        }
    }

    #[test]
    fn parse_payload_set_heuristic_skips_when_data_present() {
        // Documented behavior: `data` opts out of the auto-wrap, so the
        // payload is passed through unchanged (no `dps` synthesized).
        let vars: HashMap<String, String> = HashMap::new();
        let val = parse_mqtt_payload(
            r#"{"action":"set","id":"dev","data":{"x":1}}"#,
            &vars,
        );
        assert!(val.get("dps").is_none());
        assert_eq!(val.get("data"), Some(&json!({"x": 1})));
    }

    #[test]
    fn parse_payload_array_merges_vars_per_item() {
        let vars: HashMap<String, String> =
            [("id".into(), "dev-1".into())].into_iter().collect();
        let val = parse_mqtt_payload(
            r#"[{"action":"get"},{"action":"status","id":"override"}]"#,
            &vars,
        );
        let arr = val.as_array().unwrap();
        assert_eq!(arr[0].get("id").and_then(|v| v.as_str()), Some("dev-1"));
        assert_eq!(arr[1].get("id").and_then(|v| v.as_str()), Some("override"));
    }
}
