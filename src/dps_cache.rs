//! Per-device merged DPS cache.
//!
//! Only instantiated in ☆ mode (`mqtt_retain=true`); ★ mode passes device
//! messages straight through without caching. The cache is the source of truth
//! for retained state snapshots published on the `type=passive` topic.
//!
//! See `docs/internals.md` §4 for the full retain model and how this cache
//! interacts with the broker-seeded startup phase.

use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct DpsCache {
    devices: Mutex<HashMap<String, BTreeMap<String, Value>>>,
}

impl DpsCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge incoming DPs into the device's cache, overwriting existing keys.
    /// Returns the keys whose value actually changed — values matching the
    /// prior cache entry are deduped out so callers can short-circuit publish
    /// when nothing meaningful changed. Used for device-driven updates
    /// (active/passive events) where incoming data is authoritative.
    pub fn merge(&self, id: &str, dps: &Map<String, Value>) -> Vec<String> {
        let mut devices = self.devices.lock().unwrap();
        let entry = devices.entry(id.to_string()).or_default();
        let mut changed = Vec::new();
        for (k, v) in dps {
            match entry.get(k) {
                Some(existing) if existing == v => {}
                _ => {
                    entry.insert(k.clone(), v.clone());
                    changed.push(k.clone());
                }
            }
        }
        changed
    }

    /// Fill in keys that are not yet present, leaving existing entries untouched.
    /// Used during the seed phase: broker-retained data is backfill, while
    /// anything the bridge has already learned from a live device event wins.
    pub fn fill_missing(&self, id: &str, dps: &Map<String, Value>) {
        let mut devices = self.devices.lock().unwrap();
        let entry = devices.entry(id.to_string()).or_default();
        for (k, v) in dps {
            entry.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    /// Current cache snapshot for the device, or `None` if no DPs are cached.
    pub fn snapshot(&self, id: &str) -> Option<BTreeMap<String, Value>> {
        let devices = self.devices.lock().unwrap();
        devices.get(id).filter(|m| !m.is_empty()).cloned()
    }

    /// Ids of all devices with at least one cached DP. Used at seed-end flush
    /// to walk every dirty device.
    pub fn device_ids(&self) -> Vec<String> {
        let devices = self.devices.lock().unwrap();
        devices
            .iter()
            .filter(|(_, m)| !m.is_empty())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Drops the cache for a device. Called on device removal.
    pub fn remove(&self, id: &str) {
        let mut devices = self.devices.lock().unwrap();
        devices.remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn merge_on_empty_returns_all_keys() {
        let cache = DpsCache::new();
        let mut changed = cache.merge("dev-1", &map(&[("1", json!(true)), ("2", json!(50))]));
        changed.sort();
        assert_eq!(changed, vec!["1", "2"]);
    }

    #[test]
    fn merge_same_value_dedupes() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true))]));
        let changed = cache.merge("dev-1", &map(&[("1", json!(true))]));
        assert!(changed.is_empty(), "same value should dedupe to no changes");
    }

    #[test]
    fn merge_different_value_reports_change() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true))]));
        let changed = cache.merge("dev-1", &map(&[("1", json!(false))]));
        assert_eq!(changed, vec!["1"]);
    }

    #[test]
    fn merge_mixed_returns_only_changed_keys() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true)), ("2", json!(50))]));
        let changed = cache.merge("dev-1", &map(&[("1", json!(true)), ("2", json!(60))]));
        assert_eq!(changed, vec!["2"]);
    }

    #[test]
    fn merge_partial_preserves_other_keys() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true)), ("2", json!(50))]));
        cache.merge("dev-1", &map(&[("battery", json!(80))]));
        let snap = cache.snapshot("dev-1").unwrap();
        assert_eq!(snap.get("1"), Some(&json!(true)));
        assert_eq!(snap.get("2"), Some(&json!(50)));
        assert_eq!(snap.get("battery"), Some(&json!(80)));
    }

    #[test]
    fn fill_missing_only_fills_empty_slots() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(false))]));
        cache.fill_missing("dev-1", &map(&[("1", json!(true)), ("2", json!(50))]));
        let snap = cache.snapshot("dev-1").unwrap();
        assert_eq!(snap.get("1"), Some(&json!(false)), "existing value preserved");
        assert_eq!(snap.get("2"), Some(&json!(50)), "missing key filled");
    }

    #[test]
    fn fill_missing_on_unknown_creates_entry() {
        let cache = DpsCache::new();
        cache.fill_missing("dev-1", &map(&[("1", json!(true))]));
        assert_eq!(
            cache.snapshot("dev-1").unwrap().get("1"),
            Some(&json!(true))
        );
    }

    #[test]
    fn snapshot_for_unknown_device_is_none() {
        let cache = DpsCache::new();
        assert!(cache.snapshot("nope").is_none());
    }

    #[test]
    fn snapshot_for_empty_device_is_none() {
        let cache = DpsCache::new();
        cache.fill_missing("dev-1", &Map::new());
        assert!(cache.snapshot("dev-1").is_none());
    }

    #[test]
    fn snapshot_keys_are_sorted() {
        let cache = DpsCache::new();
        cache.merge(
            "dev-1",
            &map(&[("z", json!(1)), ("a", json!(2)), ("m", json!(3))]),
        );
        let snap = cache.snapshot("dev-1").unwrap();
        let keys: Vec<_> = snap.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn device_ids_returns_non_empty_only() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true))]));
        cache.fill_missing("dev-2", &Map::new());
        let ids = cache.device_ids();
        assert_eq!(ids, vec!["dev-1"]);
    }

    #[test]
    fn remove_drops_cache() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true))]));
        cache.remove("dev-1");
        assert!(cache.snapshot("dev-1").is_none());
    }

    #[test]
    fn merge_isolates_devices() {
        let cache = DpsCache::new();
        cache.merge("dev-1", &map(&[("1", json!(true))]));
        cache.merge("dev-2", &map(&[("1", json!(false))]));
        assert_eq!(cache.snapshot("dev-1").unwrap().get("1"), Some(&json!(true)));
        assert_eq!(
            cache.snapshot("dev-2").unwrap().get("1"),
            Some(&json!(false))
        );
    }
}
