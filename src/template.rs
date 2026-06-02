//! MQTT topic-template engine.
//!
//! All of the bridge's *topic-side* template helpers live here: forward
//! substitution ([`render_template`]), the topic → MQTT wildcard
//! conversion ([`tpl_to_wildcard`]), and topic → regex / regex → variables
//! ([`compile_topic_regex`], [`match_topic`]).
//!
//! Reverse parsing of *payload* templates is a separate concern and lives
//! in [`crate::payload`].

use regex::Regex;
use std::collections::HashMap;

/// Placeholders that can appear in topic templates (and therefore become
/// MQTT `+` wildcards under [`tpl_to_wildcard`] and named capture groups
/// under [`compile_topic_regex`]). The set is shared with
/// [`crate::payload`] which extends it with payload-only vars (`value`,
/// `dps`, `timestamp`, `root`) for its reverse-parse regex.
pub const TOPIC_VARS: &[&str] = &[
    "id", "name", "dp", "action", "cid", "type", "level",
];

/// Renders a `{key}` template by calling `substitute(key, out)` for each
/// placeholder. The closure should either push a value onto `out` and
/// return `true`, or leave `out` untouched and return `false` (in which
/// case the literal `{key}` is emitted — matching the bridge's
/// permissive-substitution contract).
pub fn render_template<F>(template: &str, mut substitute: F) -> String
where
    F: FnMut(&str, &mut String) -> bool,
{
    let mut res = String::with_capacity(template.len() + 32);
    let mut last = 0;
    while let Some(start) = template[last..].find('{') {
        let actual_start = last + start;
        res.push_str(&template[last..actual_start]);
        let Some(end) = template[actual_start..].find('}') else {
            break;
        };
        let actual_end = actual_start + end;
        let key = &template[actual_start + 1..actual_end];
        if substitute(key, &mut res) {
            last = actual_end + 1;
        } else {
            res.push('{');
            last = actual_start + 1;
        }
    }
    res.push_str(&template[last..]);
    res
}

/// Converts an MQTT topic template to a wildcard subscription string. The
/// `{root}` placeholder is substituted with `root_topic`; every other
/// recognized topic variable becomes a `+`; unknown `{key}` is left as
/// literal text so user-defined fixed segments survive untouched.
#[must_use]
pub fn tpl_to_wildcard(template: &str, root_topic: &str) -> String {
    render_template(template, |key, out| match key {
        "root" => {
            out.push_str(root_topic);
            true
        }
        k if TOPIC_VARS.contains(&k) => {
            out.push('+');
            true
        }
        _ => false,
    })
}

/// Compiles a topic template into a regex with named capture groups for
/// each topic variable, anchored to the full string. Returns `None` for
/// templates with no `{` (literal topics that need only exact-string
/// comparison via [`match_topic`]).
#[must_use]
pub fn compile_topic_regex(template: &str) -> Option<Regex> {
    if !template.contains('{') {
        return None;
    }
    let mut pattern = regex::escape(template);
    for key in TOPIC_VARS {
        pattern = pattern.replace(&format!(r"\{{{key}\}}"), &format!(r"(?P<{key}>[^/]+)"));
    }
    Regex::new(&format!("^{pattern}$")).ok()
}

/// Matches an MQTT topic against a template and returns the extracted
/// variables. Pass the regex from [`compile_topic_regex`] when matching
/// repeatedly; pass `None` for literal templates (exact-string match,
/// empty captures on success).
#[must_use]
pub fn match_topic(
    topic: &str,
    template: &str,
    re: Option<&Regex>,
) -> Option<HashMap<String, String>> {
    if let Some(re) = re {
        let caps = re.captures(topic)?;
        let mut vars = HashMap::new();
        for name in TOPIC_VARS {
            if let Some(m) = caps.name(name) {
                vars.insert((*name).to_string(), m.as_str().to_string());
            }
        }
        Some(vars)
    } else {
        (topic == template).then(HashMap::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tpl_to_wildcard_substitutes_root_and_wildcards_variables() {
        assert_eq!(
            tpl_to_wildcard("{root}/event/{type}/{id}", "rustuya"),
            "rustuya/event/+/+"
        );
    }

    #[test]
    fn tpl_to_wildcard_preserves_unknown_keys_literally() {
        assert_eq!(
            tpl_to_wildcard("{root}/{value}/x", "rustuya"),
            "rustuya/{value}/x"
        );
    }

    #[test]
    fn compile_topic_regex_returns_none_for_literal_template() {
        assert!(compile_topic_regex("rustuya/command").is_none());
    }

    #[test]
    fn match_topic_extracts_variables() {
        let tpl = "rustuya/event/{type}/{id}";
        let re = compile_topic_regex(tpl).unwrap();
        let vars = match_topic("rustuya/event/active/dev-1", tpl, Some(&re)).unwrap();
        assert_eq!(vars.get("type").map(String::as_str), Some("active"));
        assert_eq!(vars.get("id").map(String::as_str), Some("dev-1"));
    }

    #[test]
    fn match_topic_rejects_mismatch() {
        let tpl = "rustuya/event/{id}";
        let re = compile_topic_regex(tpl).unwrap();
        assert!(match_topic("rustuya/other/dev-1", tpl, Some(&re)).is_none());
    }

    #[test]
    fn match_topic_literal_template_exact_match() {
        assert!(match_topic("rustuya/command", "rustuya/command", None).is_some());
        assert!(match_topic("rustuya/command/x", "rustuya/command", None).is_none());
    }

    #[test]
    fn render_template_substitutes_known_keys_and_keeps_unknown() {
        let mut vars = HashMap::new();
        vars.insert("a".to_string(), "1".to_string());
        let out = render_template("x={a},y={b}", |key, out| {
            vars.get(key).is_some_and(|v| {
                out.push_str(v);
                true
            })
        });
        assert_eq!(out, "x=1,y={b}");
    }
}
