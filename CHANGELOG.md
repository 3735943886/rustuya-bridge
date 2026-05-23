# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0-rc.1] — Python 0.2.0-rc.1

### Changed
- **Bumped to `rustuya` 0.3.0-rc.1** (release candidate). Picks up the new
  `DeviceBuilder::build` API; the deprecated `DeviceBuilder::run` is replaced
  in both registration paths.
- Retain-safety gate is now driven by template structure (`IdentifierSet`)
  instead of per-publish substring matching. Eliminates false positives where
  an unrelated substring of a device id allowed retain to slip past the gate
  while the scavenger could not later identify the retained message
  (orphaned retained messages).
- `BridgeContext::new` warns once at startup if `mqtt_retain=true` but neither
  `mqtt_event_topic` nor `mqtt_payload_template` references an identifier
  (`{id}`, `{name}`, or `{cid}`).
- `publish_device_message` drops its redundant retain gate; the id is injected
  into the payload object earlier in the same function, so the scavenger's
  quoted-payload match always finds it.
- Extracted `handle_device_event` from `spawn_device_listener` to keep the
  `tokio::select!` driver shallow and restore consistent indentation.
- `python/src/lib.rs` consolidates the 17 kwarg → `Cli` field assignments
  behind a local `map_kwargs!` macro; new fields require a single line.

### Added
- Unit tests for `IdentifierSet`, `tpl_to_wildcard`, `compile_topic_regex`,
  `match_topic`, `render_template`, and `BridgeContext::parse_mqtt_payload`.
- `CHANGELOG.md` (this file) in Keep a Changelog format.

### Documentation
- Comment on the post-disconnect eventloop drain explaining its dependence on
  the 7s outer timeout in `BridgeServer::close`.

## [0.2.8] — Python 0.1.4

### Changed
- Bumped to `rustuya` 0.2.8.

## [0.2.7] — Python 0.1.3

### Changed
- Bumped to `rustuya` 0.2.7.

### Documentation
- Clarified post-install flow for `bridgectl`.

## [0.2.6] — Python 0.1.2

### Added
- `scripts/bridgectl.sh` installer for Linux + systemd: one-line install,
  upgrade, remove, and purge; self-installs to `/usr/local/bin/bridgectl`.
  Purge confirmation includes a retain-scavenger warning when
  `mqtt_retain=true` is detected in the config.
- Python bindings: `PyBridgeServer.config_path` kwarg, plus
  `tpl_to_wildcard` / `match_topic` / `render_template` / `parse_payload`
  helpers exposed for manager interop.
- PyPI publish workflow with `abi3` wheels (Linux x86_64 / aarch64, macOS,
  Windows).
- Multi-platform binary publish workflow.

### Changed
- Standalone mode: bridge now warns and continues without MQTT when
  `--mqtt-broker` is omitted (devices are still tracked and persisted).
- Bumped to `rustuya` 0.2.6 and adapted to its API changes.
- Idiomatic Rust cleanup across all modules.

## [0.2.5]

### Added
- Runtime session id + MQTT-based duplicate-instance detection
  (`{root}/bridge/config` retained topic) — refuses to start when another
  bridge is already running against the same broker/root.
- `verify_write_permission` check before initializing `BridgeContext`.
- `no_signals` option to disable internal signal handling (for library use).

### Changed
- Graceful shutdown timeout for background tasks; MQTT task waits for PubAcks
  before disconnecting to ensure retained-cleanup completes.
- Retain tracking replaced with a dedicated background scavenger task
  (`ScavengerTarget`) that performs precise topic + payload matching.
- LWT is now optional in MQTT options; scavenger ignores the bridge config
  topic.
- `BridgeServer` lifecycle decoupled into `setup` / `run` / `close` to support
  controlled cleanup and Python integration.
- Suppressed redundant API responses for successful `set` / `get` actions.

### Fixed
- Device instances are cleared before shutdown so background tasks terminate
  correctly.

## [0.2.4] and earlier

See `git log` for detailed history. Highlights:

- MQTT topic/payload templating with `{root}`, `{id}`, `{name}`, `{cid}`,
  `{type}`, `{dp}`, `{value}`, `{dps}`, `{timestamp}`, `{level}` variables.
- MQTT username/password authentication.
- Dynamic MQTT client id and topics based on root topic configuration.
- Device error code tracking and reporting in bridge state and API responses.
- Graceful shutdown with `CancellationToken`.
- Python bindings (`pyrustuyabridge`).
