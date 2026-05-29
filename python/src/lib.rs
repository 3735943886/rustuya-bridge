use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_async_runtimes::tokio::future_into_py;
use rustuyabridge::bridge::{
    BridgeContext, compile_topic_regex, match_topic, render_template, tpl_to_wildcard,
};
use rustuyabridge::config::Cli;
use rustuyabridge::server::BridgeServer;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Recursively converts a `serde_json::Value` into a Python object.
fn json_value_to_py<'py>(py: Python<'py>, val: &Value) -> PyResult<Bound<'py, PyAny>> {
    match val {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any())
            } else {
                Ok(py.None().into_bound(py))
            }
        }
        Value::String(s) => Ok(s.into_pyobject(py)?.into_any()),
        Value::Array(arr) => {
            let list = PyList::empty(py);
            for v in arr {
                list.append(json_value_to_py(py, v)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Converts an MQTT topic template to a subscription wildcard.
///
/// Example: `("rustuya/event/{type}/{id}", "rustuya")` -> `"rustuya/event/+/+"`.
#[pyfunction]
#[pyo3(name = "tpl_to_wildcard")]
fn tpl_to_wildcard_py(template: &str, root_topic: &str) -> String {
    tpl_to_wildcard(template, root_topic)
}

/// Matches a topic against a template, returning a dict of extracted variables.
///
/// Returns `None` if the topic does not match. For templates without variables
/// (no `{...}`), this performs exact-string comparison.
#[pyfunction]
#[pyo3(name = "match_topic")]
fn match_topic_py(topic: &str, template: &str) -> Option<HashMap<String, String>> {
    let re = compile_topic_regex(template);
    match_topic(topic, template, re.as_ref())
}

/// Substitutes `{key}` placeholders in `template` using `vars`. Unknown keys
/// are left as the literal `{key}` (mirroring the bridge's behavior).
#[pyfunction]
#[pyo3(name = "render_template")]
#[allow(clippy::needless_pass_by_value)]
fn render_template_py(template: &str, vars: HashMap<String, String>) -> String {
    render_template(template, |key, out| {
        vars.get(key).is_some_and(|v| {
            out.push_str(v);
            true
        })
    })
}

/// Parses an MQTT payload into a structured value, merging in topic variables.
///
/// This mirrors `BridgeContext::parse_mqtt_payload` so the manager interprets
/// custom payload templates identically to the bridge.
#[pyfunction]
#[pyo3(name = "parse_payload")]
#[allow(clippy::needless_pass_by_value)]
fn parse_payload_py<'py>(
    py: Python<'py>,
    payload: &str,
    vars: HashMap<String, String>,
) -> PyResult<Bound<'py, PyAny>> {
    let val = BridgeContext::parse_mqtt_payload(payload, &vars);
    json_value_to_py(py, &val)
}

#[pyclass]
pub struct PyBridgeServer {
    inner: Arc<Mutex<BridgeServer>>,
    /// Snapshot of the merged `Cli` at construction time. Used by
    /// `config_snapshot()` for inspection without locking `inner`.
    cli_snapshot: Cli,
    /// Out-of-band shutdown handle, shared with the wrapped `BridgeServer`.
    ///
    /// `start()`/`start_async()` hold the `inner` mutex for the server's
    /// ENTIRE lifetime (it's locked across `run()`), so `close()` cannot
    /// reach the server through that mutex to stop it. This token, held
    /// outside the mutex, lets `stop()`/`close()` trip the same
    /// `CancellationToken` that `run()` selects on — making shutdown work
    /// on the non-signal path (programmatic stop) and not just on SIGINT.
    cancel: CancellationToken,
}

#[pymethods]
impl PyBridgeServer {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        // Start with an all-`None` `Cli` so kwargs/file/defaults layer cleanly via
        // `merge` (which only fills `None` fields). Deserializing empty JSON gives
        // every `Option<T>` field as `None` automatically — resilient to new
        // optional fields added on the Rust side without touching this file.
        let mut cli: Cli = serde_json::from_str("{}")
            .expect("empty JSON must deserialize into Cli (all fields are Option<T>)");

        if let Some(dict) = kwargs {
            // Maps a Python kwarg name onto a `Cli` field. Adding a new field
            // means appending one line here — kwarg presence/extract/assignment
            // patterns are uniform.
            macro_rules! map_kwargs {
                ($($py_key:literal => $field:ident),* $(,)?) => {
                    $(
                        if let Ok(Some(val)) = dict.get_item($py_key) {
                            cli.$field = val.extract()?;
                        }
                    )*
                };
            }
            map_kwargs! {
                "config_path"           => config,
                "mqtt_broker"           => mqtt_broker,
                "mqtt_user"             => mqtt_user,
                "mqtt_password"         => mqtt_password,
                "mqtt_root_topic"       => mqtt_root_topic,
                "mqtt_command_topic"    => mqtt_command_topic,
                "mqtt_event_topic"      => mqtt_event_topic,
                "mqtt_client_id"        => mqtt_client_id,
                "mqtt_message_topic"    => mqtt_message_topic,
                "mqtt_payload_template" => mqtt_payload_template,
                "mqtt_scanner_topic"    => mqtt_scanner_topic,
                "mqtt_retain"           => mqtt_retain,
                "state_file"            => state_file,
                "save_debounce_secs"    => save_debounce_secs,
                "scavenger_timeout_secs" => scavenger_timeout_secs,
                "log_level"             => log_level,
                "no_signals"            => no_signals,
            }
        }

        // Apply config file (if requested) using a lightweight current-thread
        // runtime — this constructor is sync but `apply_config_file` is async.
        if cli.config.is_some() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(cli.apply_config_file())
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }

        // Fall back to bridge defaults for any field still `None`.
        cli.merge(Cli::default());

        let cli_snapshot = cli.clone();
        // Create the shutdown token here and wire the same clone into the
        // server, so we keep a handle reachable without the `inner` mutex.
        let cancel = CancellationToken::new();
        Ok(Self {
            inner: Arc::new(Mutex::new(BridgeServer::with_cancel(cli, cancel.clone()))),
            cli_snapshot,
            cancel,
        })
    }

    /// Returns the fully-merged configuration (kwargs > config file > defaults)
    /// as a Python dict. Captured at construction time; runtime-only fields
    /// like `session_id` will be `None` until the server is started.
    fn config_snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let val = serde_json::to_value(&self.cli_snapshot)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        json_value_to_py(py, &val)
    }

    /// Start the server and block the current thread until it exits — either
    /// via SIGINT/SIGTERM (unless `no_signals=True`) or a `stop()`/`close()`
    /// call from another thread. The Python GIL is released while running.
    /// `run()` performs graceful MQTT cleanup before returning.
    ///
    /// For embedded use (the host app owns signals), pass `no_signals=True`
    /// at construction and drive shutdown with `stop()`/`close()`.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        if self.cancel.is_cancelled() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "server already stopped; construct a new PyBridgeServer to start again",
            ));
        }
        let inner = self.inner.clone();

        py.detach(move || {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            rt.block_on(async {
                let mut server = inner.lock().await;
                server
                    .setup()
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                // run() selects on the shared cancel token and calls close()
                // internally on shutdown, waiting for the MQTT task to flush.
                server
                    .run()
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    /// Start the server asynchronously in the Python asyncio event loop.
    /// Resolves when the server shuts down (via signal or `stop()`/`close()`).
    fn start_async<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        if self.cancel.is_cancelled() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "server already stopped; construct a new PyBridgeServer to start again",
            ));
        }
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let mut server = inner.lock().await;
            server
                .setup()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            // run() calls close() internally on shutdown.
            server
                .run()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            drop(server);
            Ok(())
        })
    }

    /// Request a graceful shutdown without blocking. Trips the shared
    /// cancellation token; a concurrently-running `start()`/`start_async()`
    /// returns and performs MQTT cleanup (retained-message removal, broker
    /// disconnect, state flush).
    ///
    /// This is synchronous and lock-free — it does NOT wait for cleanup to
    /// finish. Use `close()` (or join the start thread) if you need to await
    /// completion. Safe to call before `start()` (no-op until something runs)
    /// and idempotent.
    fn stop(&self) {
        self.cancel.cancel();
    }

    /// Stop the bridge and wait for graceful MQTT cleanup to complete.
    ///
    /// Trips the shared cancellation token first (lock-free), so a running
    /// `run()` returns and releases the server lock; then acquires the lock
    /// and runs final cleanup (idempotent if `run()` already did it). This
    /// is what makes programmatic shutdown work even when no OS signal is
    /// delivered — the previous implementation could deadlock waiting for a
    /// lock that `run()` held for the whole server lifetime.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        // Trip the token BEFORE awaiting the lock: this unblocks run(), which
        // releases `inner` once it has finished its own graceful close.
        self.cancel.cancel();
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .close()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

#[pymodule]
fn pyrustuyabridge(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<PyBridgeServer>()?;
    m.add_function(wrap_pyfunction!(tpl_to_wildcard_py, m)?)?;
    m.add_function(wrap_pyfunction!(match_topic_py, m)?)?;
    m.add_function(wrap_pyfunction!(render_template_py, m)?)?;
    m.add_function(wrap_pyfunction!(parse_payload_py, m)?)?;
    Ok(())
}
