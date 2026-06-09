"""Shared fixtures for the broker-backed integration tests.

These tests stand up a real `PyBridgeServer` (in-process, no signals) against
an MQTT broker and drive it with simulated Tuya devices from `tuyamock`.
Each mock binds a distinct **loopback IP** on the default Tuya port (6668),
so the bridge addresses them exactly like real devices — by IP, never by a
non-standard port (real devices can't change their port).

Linux-only: relies on the whole `127.0.0.0/8` block being loopback without
alias configuration (true on Linux; macOS would need `lo0` aliases).

Broker: set `MQTT_BROKER` (default `mqtt://localhost:1883`). Locally this
reuses whatever test broker is already running; CI points it at a mosquitto
service container.
"""
import json
import os
import queue
import threading
import time
import uuid
from urllib.parse import urlparse

import pytest

pyrustuyabridge = pytest.importorskip("pyrustuyabridge")
mqtt = pytest.importorskip("paho.mqtt.client")
tuyamock = pytest.importorskip("tuyamock")

BROKER_URL = os.environ.get("MQTT_BROKER", "mqtt://localhost:1883")
_u = urlparse(BROKER_URL)
BROKER_HOST = _u.hostname or "localhost"
BROKER_PORT = _u.port or 1883

# Seed phase waits a 5s hard cap on an empty root (no retained messages to
# quiet on) — see bridge.rs SEED_HARD_CAP. Wait past it once per bridge so
# the test body sees snapshots publish immediately instead of racing the
# deferred seed-end flush.
SEED_SETTLE_SECS = 6.0


def loopback_ips(n, start=2):
    """Yield `n` distinct 127.0.0.0/8 addresses, skipping .0/.255 octets."""
    a, b, c = 0, 0, start
    out = []
    while len(out) < n:
        if c not in (0, 255):
            out.append(f"127.{a}.{b}.{c}")
        c += 1
        if c > 255:
            c, b = 0, b + 1
        if b > 255:
            b, a = 0, a + 1
    return out


def _require_broker():
    probe = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"probe-{uuid.uuid4().hex[:6]}")
    try:
        probe.connect(BROKER_HOST, BROKER_PORT, 5)
        probe.disconnect()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no MQTT broker at {BROKER_HOST}:{BROKER_PORT} ({exc}); set MQTT_BROKER")


class Collector:
    """A paho client that records every message under a topic filter."""

    def __init__(self, client_id):
        self._c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self._c.on_message = self._on_message
        self.q = queue.Queue()
        self._c.connect(BROKER_HOST, BROKER_PORT, 60)
        self._c.loop_start()

    def _on_message(self, c, u, m):
        self.q.put((m.topic, m.payload.decode("utf-8", "replace"), bool(m.retain)))

    def subscribe(self, topic_filter):
        self._c.subscribe(topic_filter, qos=1)

    def publish(self, topic, payload, qos=1):
        self._c.publish(topic, payload, qos=qos)

    def drain(self):
        items = []
        while not self.q.empty():
            items.append(self.q.get())
        return items

    def wait_for(self, predicate, timeout=12.0):
        """Block until a message satisfying `predicate(topic, payload, retain)`
        arrives; return it, or None on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                item = self.q.get(timeout=0.25)
            except queue.Empty:
                continue
            if predicate(*item):
                return item
        return None

    def close(self):
        self._c.loop_stop()
        self._c.disconnect()


def fresh_retained(topic_filter, settle=2.0):
    """Connect a brand-new subscriber and return the retained messages the
    broker replays for `topic_filter`. This is the only reliable way to assert
    "is X actually retained?" — a live delivery to an existing subscription
    arrives with retain=False even when published retained.

    Order matters: start the network loop and wait for CONNACK *before*
    subscribing, otherwise the SUBSCRIBE can race the connection and the broker
    never replays the retained set (silently returning nothing). Best for
    presence/absence checks; for exact counts at scale prefer the bridge's own
    `status` (broker retained delivery isn't instantaneous under load)."""
    got = queue.Queue()
    connected = threading.Event()
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"fresh-{uuid.uuid4().hex[:6]}")
    c.on_connect = lambda *a: connected.set()
    c.on_message = lambda cl, u, m: got.put((m.topic, m.payload.decode("utf-8", "replace"), bool(m.retain)))
    c.connect(BROKER_HOST, BROKER_PORT, 60)
    c.loop_start()
    connected.wait(timeout=5)
    c.subscribe(topic_filter, qos=1)
    time.sleep(settle)
    c.loop_stop()
    c.disconnect()
    out = []
    while not got.empty():
        out.append(got.get())
    return out


@pytest.fixture
def bridge(tmp_path):
    """Start a cache-mode PyBridgeServer in a background thread on a unique
    root topic; settle past the seed phase; yield a control handle."""
    _require_broker()
    root = f"rustuyatest_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    state_file = str(tmp_path / "state.json")

    class Handle:
        def __init__(self, srv, root, thread):
            self.srv, self.root, self.thread = srv, root, thread

        def command(self):
            return f"{self.root}/command"

        def state_topic(self, dev_id):
            return f"{self.root}/event/state/{dev_id}"

    def make(**overrides):
        kwargs = dict(
            mqtt_broker=BROKER_URL,
            mqtt_root_topic=root,
            mqtt_event_topic="{root}/event/{type}/{id}",
            mqtt_command_topic="{root}/command",
            mqtt_payload_template="{value}",
            mqtt_retain=True,
            scavenger_timeout_secs=1,
            state_file=state_file,
            no_signals=True,
            log_level="warn",
        )
        kwargs.update(overrides)
        srv = pyrustuyabridge.PyBridgeServer(**kwargs)
        t = threading.Thread(target=srv.start, daemon=True)
        t.start()
        time.sleep(SEED_SETTLE_SECS)
        assert t.is_alive(), "bridge thread exited during startup"
        return Handle(srv, root, t)

    handles = []

    def factory(**overrides):
        h = make(**overrides)
        handles.append(h)
        return h

    yield factory

    for h in handles:
        h.srv.stop()
        h.thread.join(timeout=10)
