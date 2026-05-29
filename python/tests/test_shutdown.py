"""Lifecycle / shutdown tests for the PyBridgeServer binding.

Requires the compiled extension to be importable (e.g. `maturin develop`
or an installed wheel). These run without an MQTT broker — the bridge's
standalone mode tracks devices and flushes state but does no MQTT I/O, so
the tests exercise the shutdown *control flow* without needing infra.

The key regression guarded here: a programmatic shutdown (no OS signal)
must stop a running server promptly. The previous binding held the
internal server mutex across run() for the whole lifetime, so close()
could not acquire it to stop the server and would deadlock on the
non-signal path.
"""

import os
import threading
import time
import tempfile

import pytest

pyrustuyabridge = pytest.importorskip("pyrustuyabridge")
PyBridgeServer = pyrustuyabridge.PyBridgeServer


def _standalone_server(tmp_path):
    """A no-broker, no-signals server writing state under tmp_path."""
    state_file = os.path.join(tmp_path, "state.json")
    srv = PyBridgeServer(
        mqtt_broker=None,      # standalone: no broker connection
        no_signals=True,       # embedded mode: host owns signals
        state_file=state_file,
        log_level="warn",
    )
    return srv, state_file


def test_threaded_start_then_stop_no_signal(tmp_path):
    """start() in a daemon thread, then stop() from the main thread.

    Proves the non-signal shutdown path: stop() trips the cancel token,
    run() returns and performs graceful close (state flush), and the
    thread joins well within the bound.
    """
    srv, state_file = _standalone_server(str(tmp_path))

    errors = {}

    def run():
        try:
            srv.start()  # blocks until shutdown
        except Exception as exc:  # noqa: BLE001
            errors["exc"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    time.sleep(1.0)  # allow setup() + run() to start
    assert t.is_alive(), "server thread exited before stop() was called"

    srv.stop()  # synchronous, lock-free shutdown request
    t.join(timeout=5)

    assert not t.is_alive(), "run() did not return within 5s of stop()"
    assert "exc" not in errors, f"start() raised: {errors.get('exc')}"
    assert os.path.exists(state_file), "graceful close should have flushed state file"


def test_stop_is_idempotent(tmp_path):
    """stop() before start and twice in a row must not raise."""
    srv, _ = _standalone_server(str(tmp_path))
    srv.stop()
    srv.stop()  # no-op, must not raise


def test_reuse_after_stop_is_rejected(tmp_path):
    """A stopped server refuses to (re)start with a clear error."""
    srv, _ = _standalone_server(str(tmp_path))
    srv.stop()
    with pytest.raises(RuntimeError, match="already stopped"):
        srv.start()
