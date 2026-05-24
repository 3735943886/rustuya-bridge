# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0-rc.3] — Python 0.2.0-rc.3 — 2026-05-25

### Changed
- **Bumped `rustuya` dep to 0.3.0-rc.3.** Scanner-only patch release with
  no API change. The bridge calls only
  [`rustuya::Scanner::scan_stream()`](src/handlers.rs#L178) (singleton
  entry point), so the rc.3 setter behaviour fixes (`set_ports` /
  `set_bind_address`) don't apply directly; what does apply, without any
  bridge code change, is the underlying scanner robustness:
  - `ensure_passive_listener` now serializes startup behind a mutex
    guard, so two MQTT-triggered `scan_stream()` calls landing close
    together can no longer race to spawn duplicate dispatcher tasks.
  - `PACKET_CHANNEL_CAPACITY` 100 → 1024, removing the headroom shortage
    that fleet-scale scans could hit when many devices respond in the
    same broadcast window.
  - Internal discovery-loop cleanup after `MAX_BROADCASTS` (no timing
    change).

## [0.3.0-rc.2] — Python 0.2.0-rc.2 — 2026-05-24

### Changed
- **Bumped `rustuya` dep to 0.3.0-rc.2.** rc.2 is API-compatible — the only
  removed surface (`DeviceBuilder::run`) was already migrated to
  `.build()` in `[0.3.0-rc.1]`. The bridge's
  [`rustuya::device::unified_listener`](src/bridge.rs#L407) path is
  preserved through rc.2's `device.rs → device/` module split, and
  [`rustuya::Scanner::scan_stream()`](src/handlers.rs#L178) still routes
  through the singleton after the `ScannerBuilder` removal.
- Behaviour gained from rc.2 without code change:
  - **`Device::listener` broadcast-lag visibility**: rc.2 emits a
    synthetic `{errorCode: 906, reason: "listener_lagged", skipped: n}`
    event when a listener falls behind. The bridge's existing
    error-path handler (`handle_device_event`) already routes any
    payload with `errorCode/errorMsg` to the MQTT `error` topic, so
    lag becomes observable without extra code — previously these
    events were silently swallowed by rustuya.
  - Local-IP caching removed in rustuya — `send_discovery_broadcast`
    now re-resolves the host IP per call, so v3.5 discovery on hosts
    whose IP can change mid-process (DHCP renewal, VPN, container
    restart) stops stamping stale addresses.

  Note: rc.2's `persist=false` burst-collapse fix does **not** apply to
  the bridge. The bridge sets `nowait(true)` (return immediately after
  queueing — different flag) but leaves `persist=true` (the default),
  so requests against an unreachable device hit the existing
  persistent-reconnect path, not the per-request connect path that
  rc.2 fixed.
- `rustuya-bridge` binary now suppresses `env_logger`'s UTC timestamp prefix
  when launched by systemd/journald (detected via the `$JOURNAL_STREAM`
  env var that systemd.exec sets on the inherited stderr). Journald
  already stamps each line with local time, so the duplicate UTC prefix
  is just noise. Docker, piped stderr, and interactive terminals don't
  set `$JOURNAL_STREAM`, so the timestamp is preserved there
  (`docker logs` still shows a usable time).
- `bridgectl status` is now semver-aware: when the installed binary is a
  pre-release that is newer than the channel's latest, the line reads
  "you are on pre-release X" instead of "⬆ upgrade available", avoiding
  the misleading downgrade hint.
- `bridgectl upgrade` warns and reframes the prompt as "Downgrade ... → ..."
  when the target version is older than the installed one. Stable-channel
  downgrades from a pre-release also point at `--prerelease` as the likely
  intent.

### Fixed
- `bridgectl` `api_latest_tag` no longer fails under `set -o pipefail`. The
  awk fallback parser used `exit` on the first match, which triggered
  SIGPIPE on the upstream `printf` when the response was large (≈50KB for
  the `/releases` endpoint behind `--prerelease`), causing `status` to
  print "could not query GitHub" despite a successful fetch.

## [0.3.0-rc.1] — Python 0.2.0-rc.1

### Added
- `bridgectl --prerelease` flag (applies to `status` / `install` / `upgrade`)
  — opt-in channel for installing or upgrading to the newest release including
  pre-releases. Without the flag, the stable channel (`releases/latest`,
  which excludes pre-releases) is used.

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

### CI
- `binary-publish.yml` auto-detects pre-release tags (`-rc` / `-alpha` /
  `-beta`) and creates the GitHub Release as `prerelease=true`, so RC builds
  do not become `releases/latest`.
- `docker-publish.yml` skips `latest` / `stable` raw tags for pre-release
  versions; only the exact semver tag is published. Prevents `docker pull`
  default from silently shipping an RC.

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
