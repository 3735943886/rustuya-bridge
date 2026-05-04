use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use rustuyabridge::config::Cli;
use rustuyabridge::server::BridgeServer;
use std::sync::Arc;
use tokio::sync::Mutex;

#[pyclass]
pub struct PyBridgeServer {
    inner: Arc<Mutex<BridgeServer>>,
}

#[pymethods]
impl PyBridgeServer {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut cli = Cli {
            config: None,
            mqtt_broker: None,
            mqtt_root_topic: None,
            mqtt_user: None,
            mqtt_password: None,
            mqtt_command_topic: None,
            mqtt_event_topic: None,
            mqtt_client_id: None,
            mqtt_message_topic: None,
            mqtt_payload_template: None,
            mqtt_scanner_topic: None,
            mqtt_retain: None,
            state_file: None,
            save_debounce_secs: None,
            log_level: None,
            no_signals: None,
        };

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

        cli.fill_defaults();
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
            Ok(())
        })
    }

    /// Stop the bridge and clean up MQTT retained messages.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut server = inner.lock().await;
            server
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
    Ok(())
}
