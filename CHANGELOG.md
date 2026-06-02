# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0-rc.6] — Python 0.2.0-rc.6 — 2026-06-02

Retain-semantics overhaul. Fixes a long-standing data-loss bug for
`mqtt_retain=true` users and removes the spurious-event-replay footgun
on HA reconnect. Retain-off users (the default) are unaffected.

### Fixed
- **`mqtt_retain=true` no longer overwrites the retained snapshot with a
  partial passive update.** Previously, in multi-DP mode (event topic
  without `{dp}`), a battery-only or RSSI-only periodic device-initiated
  report would publish *that single field* as the device's retained
  snapshot — wiping the switch state, temperature, brightness etc. that
  the previous full status report had established. HA reload would then
  show "unknown" until the next full report (sometimes minutes). The
  bridge now keeps a per-device merged DPS cache and publishes snapshots
  from the cache, so partial updates merge instead of replacing.
- **Active events no longer re-fire HA event automations on reconnect.**
  Active deltas (button presses, motion fires) used to be published
  retained; on HA reload the broker re-delivered the retained active
  and HA automations re-fired the button-press. Active is now always
  published with `retain=false` to a dedicated `{type}=active` topic.

### Added
- **cache mode** (`mqtt_retain=true`): in-memory DPS cache + two publish
  routes — direct (active deltas, no retain, `{type}=active`) and cache
  (merged snapshots, retain, `{type}=passive`).
- **Seed phase** (cache mode only): on first MQTT connect the bridge subscribes
  to its own state wildcard to recover prior-session snapshots from the
  broker, drains them into the cache, then unsubscribes. Hard cap 5s,
  quiet 200ms. Snapshot publishes are deferred for any device that
  changes during the seed window; one batched flush at seed end.
- **`{type}` distinguishability check** at startup: cache mode WARN
  (no downgrade) if `{type}` is absent from both `mqtt_event_topic`
  and `mqtt_payload_template`. Reload re-fires are safe regardless;
  the warn flags potential live double-fire on event automations
  subscribed to the snapshot topic.
- **`src/dps_cache.rs`** — new module housing `DpsCache` with
  `merge`/`fill_missing`/`snapshot`/`remove`.

### Changed
- **`mqtt_retain` semantic clarified:** in cache mode the flag now means
  "publish full state snapshots on `{type}=passive` retained, and live
  deltas on `{type}=active` no-retain." Active deltas are no longer
  retained even when the flag is on. Previously `mqtt_retain=true`
  applied retain uniformly to every publish.
- **Default `mqtt_retain` stays `false`** (pass-through mode, current behavior).
  No silent breaking change for the majority of users.

### Migration
| Setup | Effect |
|---|---|
| `mqtt_retain=false` (default) | No change. |
| `mqtt_retain=true`, default topic | Auto-enrolls into the new model. Broker-resident retained from the old (buggy) layout is overwritten by the first full publish per device. Snapshot publishes are deferred ≤5s on startup to absorb broker retained. |
| `mqtt_retain=true`, custom event topic *with* `{type}` | Same as above. |
| `mqtt_retain=true`, neither topic nor payload template has `{type}` | Cache mode still enabled (reload path safe). Bridge logs WARN about potential live double-fire on event automations subscribed to the snapshot topic. Add `{type}` to either template if event automations matter. |
| `mqtt_retain=true`, custom `mqtt_payload_template` | Bridge logs WARN, runs in cache mode but **skips the seed phase**. First publish per device overwrites broker retained until the next full device status report (active push or DP_QUERY response). |

### Documentation
- `docs/internals.md` §3.3 (active vs passive) and §4 (retain
  semantics) rewritten around the pass-through/cache split. New §4.5 covers the seed
  phase mechanics; §4.6 documents known seed limitations.

## [0.3.0-rc.5] — Python 0.2.0-rc.5 — 2026-05-29

Embedded-shutdown release. The headline fix lets a host application
(e.g. rustuya-manager) stop the bridge programmatically — without an OS
signal — and have graceful MQTT cleanup actually run. Also folds in the
`state_file` resolution / `bridgectl` polish that had accumulated under
[Unreleased]. No rustuya dep bump; no on-the-wire MQTT/topic change.

### Fixed
- **Python binding: programmatic shutdown no longer deadlocks.** The
  binding held the internal `BridgeServer` mutex across `run()` for the
  server's entire lifetime, so `close()` could not acquire it to stop a
  running server. On the OS-signal path `run()`'s own handler returned
  and released the lock (racy, but worked); on the **non-signal path**
  (host app stops the bridge programmatically) nothing told `run()` to
  return — `close()` blocked forever on the lock, MQTT retained-message
  cleanup never ran, and the daemon thread was hard-killed at process
  exit (retained-state leak).

  Fix: the server's `CancellationToken` (which `run()` already selects
  on) is now created by the caller and shared *outside* the mutex.
  `PyBridgeServer.close()` trips it before awaiting the lock — so a
  running `run()` returns, performs graceful cleanup, and releases the
  lock, which `close()` then acquires for final (idempotent) cleanup.
  Verified: threaded `start()` → `stop()` joins in ~10 ms with state
  flushed, no signal involved.

### Added
- **`PyBridgeServer.stop()`** — synchronous, lock-free "request shutdown"
  that trips the cancel token and returns immediately (use `close()` if
  you need to await cleanup completion). Recommended embedded pattern:
  construct with `no_signals=True` (host owns SIGINT/SIGTERM) and drive
  shutdown via `stop()`/`close()`.
- **`BridgeServer::with_cancel(cli, token)`** and
  **`BridgeServer::cancellation_token()`** in the core crate, so an
  embedding application can own the shutdown signal. `BridgeServer::new`
  is unchanged in behavior (allocates a fresh token internally) but is no
  longer `const`.

### Documentation
- `docs/internals.md` §3.3: spelled out *when* active vs passive
  matters in practice. State DPs (on/off, temperature) can treat both
  types interchangeably; event DPs (single_click, motion_detected,
  scene buttons) MUST filter to `type == "active"` only, or
  automations re-fire spuriously on every periodic status report or
  reconnect.

### Changed
- `state_file` resolution is now anchored to the config file's directory
  whenever `--config` is given:
  - **Unset** → defaults to `<config-dir>/rustuya.json` (previously fell
    through to the literal `"rustuya.json"` against CWD — a UX trap when
    the bridge was launched from a directory other than the config's).
  - **Relative** (e.g. `"mystate.json"` or `"data/state.json"` in the
    config file) → reinterpreted as `<config-dir>/<state_file>`. Anyone
    writing just a filename almost certainly means "next to the config",
    not "whatever CWD I started from".
  - **Absolute** → left untouched.

  Implemented in [`Cli::resolve_default_state_file`](src/config.rs);
  six new unit tests cover the absolute / relative / unset / no-config
  combinations. Default `bridgectl install` is unaffected because it
  writes an absolute `state_file` into `config.json`.
- `bridgectl purge` now reads `state_file` from `config.json` and
  resolves it the same way as the bridge. When the resolved path falls
  outside `${DATA_DIR}`, the confirmation message lists it explicitly
  and the file is removed after the data dir is wiped — closes the gap
  where a hand-edited `state_file` pointing outside `/var/lib/rustuya/`
  was orphaned by purge.
- `bridgectl upgrade` now warns when it leaves the service inactive
  after an upgrade. Previous behavior preserved: a service that was
  stopped before the upgrade stays stopped (don't override operator
  intent). New behavior: if the service is also `enabled` (i.e. set to
  start at boot), print a yellow hint with the `systemctl start`
  command instead of silently finishing. Avoids the "I upgraded, why
  isn't it running?" confusion.

## [0.3.0-rc.4] — Python 0.2.0-rc.4 — 2026-05-26

Internals-review release. No rustuya dep bump; ships 10 bridge-side
behavior fixes uncovered while writing the new [internals.md](docs/internals.md)
deep-dive doc, plus operational caveats that the doc was previously
silent on (broker retain/LWT compatibility, fsync cost on slow flash,
state-file corruption recovery, name fan-out partial-failure
semantics, and the 1280s-retry-cap gotcha).

### Added
- New config option `scavenger_timeout_secs` / `--scavenger-timeout-secs` /
  `SCAVENGER_TIMEOUT_SECS` (default `1`). Controls how long the retained-MQTT
  scavenger waits for messages before exiting after a `remove`/`clear`. Raise
  this on slow brokers if retained device state isn't being cleared.
- `status` responses now include `mqtt_drop_count`, a cumulative counter of
  MQTT messages dropped because the outbound channel stayed full past the
  500ms timeout. Watch this to detect a wedged broker or downstream consumer
  that can't keep up.
- Name-based lookups (`set`/`get`/`remove`/`request`/`sub_discover` by `name`)
  that resolve to more than one device now publish a response containing
  `matched: N` and `targets: [...]` so the caller can detect fan-out — even
  for successful `set`/`get` which would normally be suppressed.
- New internals documentation at [`docs/internals.md`](docs/internals.md)
  covering device lifecycle, the unified listener, the template engine,
  retain semantics, sub-device routing, and operator tips.

### Changed
- **Saved state files are now `fsync`'d** before and after rename, so an
  unclean shutdown after `save_state` returns can no longer revert the
  state file to its pre-save contents on next boot (Unix). Windows
  silently skips the directory fsync (not supported by the OS).
- **`add_device` is now idempotent for the listener.** Re-adding a direct
  device with identical `key`/`ip`/`version` no longer rebuilds the
  internal `Device` or triggers a `request_refresh`; only metadata
  bookkeeping runs. Scripted "reapply config" flows no longer churn TCP
  connections.
- **MQTT outbound drops are now logged at `error`** (was `warn`) and the
  cumulative drop count is included in the message and exposed via
  `mqtt_drop_count` in `status`.
- **Per-event retain warnings are now `debug`-level** (was `warn`) to avoid
  log flooding when a fleet has devices without `name`/`cid` and the
  template references them. The structurally-impossible case (no
  identifier in any template) is still warned **once at startup**.
- **`apply_set_heuristic` deny-list** now covers every `BridgeRequest`
  field (`action`/`id`/`name`/`key`/`ip`/`version`/`cid`/`parent_id`/
  `cmd`/`data`/`dps`/`dp`/`payload`), preventing a typo'd reserved field
  from accidentally becoming a DP write.
- **`replace_vars` now resolves missing optional variables to empty
  string** (was: left the literal `{key}` placeholder). Affects `{dp}`,
  `{type}`, `{level}`, `{cid}`, `{name}`, `{value}` — symmetry across
  all optional vars and avoids `{"dp": "{dp}", ...}`-shaped payload
  breakage when payload-template variables aren't in scope.
- **`publish_device_message` now wraps non-object payloads** before
  injecting `id`/`name`/`cid`, so the scavenger's payload-fallback search
  can always locate retained messages by id regardless of caller-provided
  payload shape.

### Fixed
- **Scavenger race**: the `tokio::select!` driving the retain scavenger
  now uses `biased;` ordering so a simultaneous new-target arrival and
  deadline expiry cannot lose the deadline extension. Previously, with
  unlucky timing, a device added to an in-flight scavenger could have its
  retained messages left orphaned.
- **`remove_device` now refreshes the listener only when a direct device
  was evicted.** Sub-device-only removals no longer trigger gratuitous
  TCP reconnects of unrelated gateways. (Matches the selective-refresh
  treatment `add_device` already received.)
- **`remove` response's `matched` count now reflects name-matched
  devices only, not cascaded sub-devices.** Previously, removing a
  single gateway by name that cascaded to N sub-devices reported
  `matched: N+1`, conflating cascading with name fan-out. The
  `targets` extra now also lists only the name-matched ids.

### Diagnostics
- `find_device_ids` now logs a `warn` when an `id` selector contains an
  empty string (a common symptom of `{id}` placeholders being merged
  from a topic when name-based lookup was intended), and a `debug` when
  both `id` and `name` are provided in the same request (id wins, name
  is silently ignored).

### Documentation
- `docs/internals.md` — added operational caveats that aren't visible
  from code alone: partial-failure semantics in name fan-out, what
  silently breaks when a broker strips retain/LWT, fsync cost on slow
  flash, state-file corruption recovery (there isn't one — back up
  yourself), and the "stuck at 1280s retry" gotcha during long broker
  outages. Also: corrected the `last_error_code` example from the
  fictional `1106` to a real rustuya code (`905`, with a table of the
  `900..=914` codes the bridge actually surfaces).

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
