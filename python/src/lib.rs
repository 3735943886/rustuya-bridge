use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use rustuyabridge::config::Cli;
use rustuyabridge::server::BridgeServer;

#[pyclass]
pub struct PyBridgeServer {
    cli: Cli,
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
        };

        if let Some(dict) = kwargs {
            if let Ok(Some(val)) = dict.get_item("config_path") {
                cli.config = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_broker") {
                cli.mqtt_broker = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_root_topic") {
                cli.mqtt_root_topic = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_command_topic") {
                cli.mqtt_command_topic = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_event_topic") {
                cli.mqtt_event_topic = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_client_id") {
                cli.mqtt_client_id = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_message_topic") {
                cli.mqtt_message_topic = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_payload_template") {
                cli.mqtt_payload_template = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_scanner_topic") {
                cli.mqtt_scanner_topic = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("mqtt_retain") {
                cli.mqtt_retain = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("state_file") {
                cli.state_file = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("save_debounce_secs") {
                cli.save_debounce_secs = Some(val.extract()?);
            }
            if let Ok(Some(val)) = dict.get_item("log_level") {
                cli.log_level = Some(val.extract()?);
            }
        }

        cli.fill_defaults();
        Ok(Self { cli })
    }

    /// Start the server and block the current thread until it exits (e.g. on Ctrl+C).
    /// The Python GIL is released while running.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let cli = self.cli.clone();

        py.detach(move || {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let server = BridgeServer::new(cli);
                server.start().await
            })
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }

    /// Start the server asynchronously in the Python asyncio event loop.
    fn start_async<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let cli = self.cli.clone();

        future_into_py(py, async move {
            let server = BridgeServer::new(cli);
            server
                .start()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            Ok(())
        })
    }
}

#[pymodule]
fn rustuya_bridge_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<PyBridgeServer>()?;
    Ok(())
}
