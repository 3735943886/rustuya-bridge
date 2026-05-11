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
}

#[pymethods]
impl PyBridgeServer {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut cli = Cli::default();

        if let Some(dict) = kwargs {
            if let Ok(Some(val)) = dict.get_item("config_path") {
                cli.config = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_broker") {
                cli.mqtt_broker = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_root_topic") {
                cli.mqtt_root_topic = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_user") {
                cli.mqtt_user = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_password") {
                cli.mqtt_password = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_command_topic") {
                cli.mqtt_command_topic = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_event_topic") {
                cli.mqtt_event_topic = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_client_id") {
                cli.mqtt_client_id = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_message_topic") {
                cli.mqtt_message_topic = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_payload_template") {
                cli.mqtt_payload_template = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_scanner_topic") {
                cli.mqtt_scanner_topic = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_retain") {
                cli.mqtt_retain = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("state_file") {
                cli.state_file = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("save_debounce_secs") {
                cli.save_debounce_secs = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("log_level") {
                cli.log_level = val.extract()?;
            }
            if let Ok(Some(val)) = dict.get_item("no_signals") {
                cli.no_signals = val.extract()?;
            }
        }

        Ok(Self {
            inner: Arc::new(Mutex::new(BridgeServer::new(cli))),
        })
    }

    /// Start the server and block the current thread until it exits (e.g. on Ctrl+C).
    /// The Python GIL is released while running. Cleans up MQTT before returning.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
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
                // run() now calls close() internally, which waits for MQTT task to finish
                server
                    .run()
                    .await
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
            })
        })
    }

    /// Start the server asynchronously in the Python asyncio event loop.
    fn start_async<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();

        future_into_py(py, async move {
            let mut server = inner.lock().await;
            server
                .setup()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            // run() now calls close() internally
            server
                .run()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            drop(server);
            Ok(())
        })
    }

    /// Stop the bridge and clean up MQTT retained messages.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
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
