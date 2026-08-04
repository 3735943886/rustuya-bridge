//! The bridge's device layer — everything that touches the rustuya driver.
//!
//! Rewritten for rustuya 0.4 (the sans-I/O `rustuya-core` FSM + `rustuya-tokio`
//! driver). Three things moved into the bridge with that change, because 0.4
//! deliberately owns no process-wide state (its DESIGN Q1: no globals, no hidden
//! singletons):
//!
//! * **Discovery is an object, not a global scanner.** 0.3 resolved an
//!   addressless device through a hidden process-wide scanner behind the magic
//!   address `"Auto"`. 0.4 has no such thing: the bridge owns one [`Discovery`]
//!   and shares it with every device (see [`Fleet::device_for`]).
//! * **The connect-storm cap is an object too.** 0.3's
//!   `set_connect_concurrency(n)` global is 0.4's shared [`ConnectLimiter`].
//! * **`install_panic_logging` / `maximize_fd_limit` are the process's job.**
//!   They were library functions in 0.3; a library has no business setting a
//!   process's panic hook or rlimits, so they live here now.

use log::{info, warn};
use rustuya_tokio::{ConnectLimiter, Device, Discovery, Version};
use serde_json::{Value, json};

use crate::config::DeviceConfig;
use crate::error::BridgeError;

/// Device-status codes published on the message topic, matching tinytuya's
/// numbering — the same values 0.3 emitted, so MQTT consumers are unaffected by
/// the 0.4 migration.
///
/// 0.3 pushed these through the device event stream as synthetic `errorCode`
/// frames. 0.4 keeps connection state off the frame stream entirely (it's state,
/// not an event) and exposes it as watches instead, so the bridge synthesises the
/// same payloads from [`Device::watch_connected`] / [`Device::watch_error`]. See
/// [`connection_event`].
pub mod code {
    /// Connected. Deliberately on the error/message topic and deliberately not an
    /// error: it is the *liveness* signal, and `"0"` is what the `status` action
    /// reports as a device's state (it reads as "online", never as a fault).
    pub const SUCCESS: u32 = 0;
    /// The link is down — dropped, refused, or never established.
    pub const OFFLINE: u32 = 905;
    /// A payload failed to authenticate, which in practice means a wrong local
    /// key or protocol version.
    ///
    /// 0.3 could only produce this as the error return of a `request()` call, so
    /// it never reached a consumer that wasn't actively commanding the device.
    /// 0.4 surfaces auth failures on [`Device::watch_error`], so the bridge now
    /// reports the misconfiguration as soon as the device rejects us.
    pub const KEY_OR_VER: u32 = 914;

    /// The human-readable half of the pair, as 0.3 worded it.
    #[must_use]
    pub fn message(code: u32) -> &'static str {
        match code {
            SUCCESS => "Connection Successful",
            OFFLINE => "Network Error: Device Unreachable",
            KEY_OR_VER => "Check device key or version",
            _ => "Unknown Error",
        }
    }
}

/// Builds the `{"errorCode": n, "errorMsg": "..."}` payload the bridge publishes
/// for a connection-state change — byte-for-byte the shape 0.3's device event
/// stream carried, so downstream automations keep working.
#[must_use]
pub fn connection_event(code: u32) -> Value {
    json!({ "errorCode": code, "errorMsg": code::message(code) })
}

/// Dial target for a device registered without an IP that discovery has never
/// seen either (when it has, [`Fleet::located`] supplies the announced address
/// and version).
///
/// 0.4 has no addressless connect: an address is required up front, and the only
/// blocking alternative (`DeviceBuilder::discover`) would stall device
/// registration for the length of its timeout. So an unlocated device is pointed
/// at TEST-NET-1 ([RFC 5737], reserved for documentation and guaranteed never to
/// be a real host) and linked to the shared [`Discovery`].
///
/// That turns the placeholder into the *mechanism* rather than a hack: the dial
/// fails, which makes the driver ping discovery for an active probe, and the
/// device's answering announcement rewakes the actor with its real address. The
/// same path self-corrects a device whose IP later changes, so an unlocated
/// device and a relocated one are one code path, and neither blocks registration.
///
/// [RFC 5737]: https://www.rfc-editor.org/rfc/rfc5737
const UNLOCATED_ADDR: &str = "192.0.2.1";

/// Shared, per-process device machinery: the one [`Discovery`] every device
/// resolves and re-locates through, and the [`ConnectLimiter`] that bounds how
/// many of them may be dialling and handshaking at once.
///
/// Both are cheap clones of shared handles, so this whole struct is cheap to
/// clone and hold.
#[derive(Clone)]
pub struct Fleet {
    /// `None` when the discovery sockets could not be bound (a port already in
    /// use, or a container with no broadcast access). Devices with a configured
    /// IP work fine without it; unlocated ones cannot be resolved and say so.
    discovery: Option<Discovery>,
    /// `None` when the operator set `connect_concurrency = 0`, opting out of the
    /// cap (the historical unbounded behaviour).
    limiter: Option<ConnectLimiter>,
}

impl Fleet {
    /// Binds the shared discovery and installs the connect-storm cap.
    ///
    /// Never fails: discovery that cannot bind is logged and disabled rather than
    /// taking the bridge down with it, because a fleet of devices with configured
    /// IPs does not need it.
    #[must_use]
    pub fn new(connect_concurrency: usize) -> Self {
        let discovery = match Discovery::new() {
            Ok(d) => Some(d),
            Err(e) => {
                warn!(
                    "LAN discovery unavailable ({e}); devices registered with an explicit ip still \
                     connect, but devices without one cannot be located and the `scan` action will \
                     return nothing"
                );
                None
            }
        };
        let limiter = (connect_concurrency > 0).then(|| {
            info!("Connect-storm cap: {connect_concurrency} devices establishing at once");
            ConnectLimiter::new(connect_concurrency)
        });
        Self { discovery, limiter }
    }

    /// The shared discovery, if it could be bound. Used by the `scan` action.
    #[must_use]
    pub fn discovery(&self) -> Option<&Discovery> {
        self.discovery.as_ref()
    }

    /// What discovery last announced for `id` — address and protocol version —
    /// if it has ever seen it.
    ///
    /// Reads the cache rather than awaiting an announcement, so registration
    /// never blocks. A stale entry is self-correcting: the dial fails and the
    /// next announcement relocates the device.
    fn located(&self, id: &str) -> Option<(String, Option<Version>)> {
        let info = self
            .discovery
            .as_ref()?
            .known()
            .into_iter()
            .find(|d| d.id == id)?;
        info!(
            "Device {id}: discovery reports {} (version {})",
            info.ip,
            info.version.map_or("unknown", Version::as_str)
        );
        Some((info.ip.to_string(), info.version))
    }

    /// Spawns a connection for one **direct** device (a sub-device is pure
    /// routing metadata and never gets one).
    ///
    /// # Errors
    /// Returns [`BridgeError::InvalidRequest`] if the config has no key, the key
    /// is not the 16 bytes the protocol requires, or the version string is not a
    /// recognised protocol version.
    pub fn device_for(&self, cfg: &DeviceConfig) -> Result<Device, BridgeError> {
        let key = cfg.key.as_deref().ok_or_else(|| {
            BridgeError::InvalidRequest(format!("device {} has no local key", cfg.id))
        })?;
        if key.len() != 16 {
            return Err(BridgeError::InvalidRequest(format!(
                "device {}: local key must be 16 characters, got {}",
                cfg.id,
                key.len()
            )));
        }

        // An explicit but unrecognised version string is a config error worth
        // reporting, not something to silently downgrade to the default.
        let configured = match cfg.version.as_deref() {
            None => None,
            Some(s) => Some(Version::parse(s).ok_or_else(|| {
                BridgeError::InvalidRequest(format!(
                    "device {}: unknown protocol version '{s}'",
                    cfg.id
                ))
            })?),
        };

        // Address *and* version come from discovery when the config omits them —
        // they are one resolution, not two, and the library's own examples do
        // exactly this before building a device. Reading the cache (rather than
        // awaiting an announcement) keeps registration non-blocking, and it makes
        // the common case instant: the bridge has been listening since startup,
        // or the operator ran `scan` first, so a device's announcement is usually
        // already cached by the time it is registered.
        //
        // Resolving the version is not a nicety. `Version::Auto` runs the v3.3
        // dialect and the core deliberately never probes, so an unresolved v3.4
        // device *appears* to connect — v3.3 has no handshake, so TCP alone reads
        // as connected — and is then hung up on by the device the moment the
        // first frame arrives without a negotiated session. That is a 10-second
        // connect/drop cycle with no error anywhere. Discovery announcements
        // carry the version precisely so this is knowable.
        let discovered = (cfg.ip.is_none() || configured.is_none())
            .then(|| self.located(&cfg.id))
            .flatten();
        let address = cfg
            .ip
            .clone()
            .or_else(|| discovered.as_ref().map(|(ip, _)| ip.clone()));
        let version = configured
            .or_else(|| discovered.as_ref().and_then(|(_, v)| *v))
            .unwrap_or(Version::Auto);

        // Every remaining knob — backoff curve, heartbeat, idle-liveness,
        // handshake/connect/send timeouts, channel depths — is left at the
        // driver's default on purpose. Those defaults are derived from device
        // behaviour the library documents (the ~30 s idle-drop typical firmware
        // enforces, say); overriding them here would move that reasoning
        // somewhere it can't be maintained.
        let mut builder = Device::builder(&cfg.id, key.as_bytes().to_vec())
            .address(address.as_deref().unwrap_or(UNLOCATED_ADDR))
            .version(version);

        // Link discovery for *every* device, not just unlocated ones: it also
        // cancels a pending reconnect backoff the moment a device re-announces,
        // so a device that reboots comes back immediately instead of waiting out
        // a backoff that may have grown to a minute.
        if let Some(disco) = &self.discovery {
            builder = builder.rediscover(disco);
        } else if cfg.ip.is_none() {
            return Err(BridgeError::InvalidRequest(format!(
                "device {} has no ip and LAN discovery is unavailable, so it cannot be located",
                cfg.id
            )));
        }
        if let Some(limiter) = &self.limiter {
            builder = builder.connect_limiter(limiter);
        }

        builder
            .connect()
            .map_err(|e| BridgeError::DeviceError(format!("device {}: {e}", cfg.id)))
    }
}

/// Raises this process's file-descriptor limit to its hard maximum.
///
/// A device costs a socket, so the default soft limit (often 1024) caps the fleet
/// well below what the bridge can otherwise handle. Unix only; a no-op elsewhere.
///
/// # Errors
/// Returns an error if the limit cannot be read or raised.
pub fn maximize_fd_limit() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let (soft, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
        if soft < hard {
            rlimit::setrlimit(rlimit::Resource::NOFILE, hard, hard)?;
            info!("File descriptor limit raised from {soft} to {hard}");
        }
    }
    Ok(())
}

/// Installs a process-global panic hook that reports every panic's thread,
/// location, message, and backtrace before the default hook runs. Idempotent;
/// the previous hook is chained, not replaced.
///
/// Under the release profile (`panic = "abort"` + `strip`) a panic on a
/// background worker would otherwise vanish with no symbols. The panic
/// *location* survives stripping — it needs no symbols — so this keeps the
/// `file:line` even there.
///
/// It writes to raw stderr, **not** the `log` facade, and that is deliberate:
/// `log` may be backed by `pyo3-log` (the bridge ships a PyO3 extension), which
/// takes the Python GIL to forward a record. Re-entering Python from a panicking
/// non-Python thread — a tokio worker, especially during interpreter shutdown —
/// is fatal: CPython force-terminates the thread, whose unwind hits the
/// `panic = "abort"` nounwind boundary and aborts inside the hook itself, masking
/// the very panic being reported.
pub fn install_panic_logging() {
    use std::sync::OnceLock;
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map_or_else(|| "<unknown location>".to_string(), ToString::to_string);
            let msg = info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("<unnamed>");
            eprintln!(
                "rustuya-bridge: PANIC on thread '{thread_name}' at {location} — {msg}\nbacktrace:\n{}",
                std::backtrace::Backtrace::force_capture()
            );
            prev(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(id: &str, key: Option<&str>, ip: Option<&str>, version: Option<&str>) -> DeviceConfig {
        DeviceConfig {
            id: id.to_string(),
            name: None,
            ip: ip.map(ToString::to_string),
            key: key.map(ToString::to_string),
            version: version.map(ToString::to_string),
            cid: None,
            parent_id: None,
            last_error_code: None,
        }
    }

    /// The bridge's public status contract: `0` is success/liveness, and the two
    /// failure codes keep tinytuya's numbering so consumers written against 0.3
    /// keep working.
    #[test]
    fn connection_events_keep_the_0_3_codes() {
        assert_eq!(connection_event(code::SUCCESS)["errorCode"], 0);
        assert_eq!(
            connection_event(code::SUCCESS)["errorMsg"],
            "Connection Successful"
        );
        assert_eq!(connection_event(code::OFFLINE)["errorCode"], 905);
        assert_eq!(connection_event(code::KEY_OR_VER)["errorCode"], 914);
    }

    /// A bad key length is a config error caught at registration, not a device
    /// that silently never connects.
    #[tokio::test]
    async fn wrong_key_length_is_rejected() {
        let fleet = Fleet::new(0);
        let err = fleet
            .device_for(&cfg("dev-1", Some("tooshort"), Some("127.0.0.1"), None))
            .unwrap_err();
        assert!(
            err.to_string().contains("16 characters"),
            "expected a key-length error, got: {err}"
        );
    }

    /// Likewise an unrecognised version string: silently falling back to the
    /// default would leave the device failing to authenticate with no explanation.
    #[tokio::test]
    async fn unknown_version_is_rejected() {
        let fleet = Fleet::new(0);
        let err = fleet
            .device_for(&cfg(
                "dev-1",
                Some("0123456789abcdef"),
                Some("127.0.0.1"),
                Some("9.9"),
            ))
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown protocol version"),
            "expected a version error, got: {err}"
        );
    }

    /// An unset version is not an error — it is `Auto`, which discovery resolves.
    #[tokio::test]
    async fn missing_version_builds() {
        let fleet = Fleet::new(0);
        assert!(
            fleet
                .device_for(&cfg(
                    "dev-1",
                    Some("0123456789abcdef"),
                    Some("127.0.0.1"),
                    None
                ))
                .is_ok()
        );
    }
}
