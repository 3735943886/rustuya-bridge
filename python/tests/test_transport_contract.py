"""The MQTT interface as a client sees it: what goes out, what comes in, and how
it behaves around the connection.

Like `test_retain_contract`, these assert only on what crosses the broker, so
they hold across changes to how the bridge is structured internally. The expected
values are the wire format existing subscribers depend on: a change here is a
change for them, not a test to update in passing.
"""
import json
import re
import socket
import threading
import time
from dataclasses import dataclass

import pytest
import tuyamock

from conftest import (
    BROKER_HOST,
    BROKER_PORT,
    BROKER_URL,
    Collector,
    loopback_ips,
    publish_retained,
    retained,
    sentinel_session,
)

pyrustuyabridge = pytest.importorskip("pyrustuyabridge")

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VER = "3.4"
# Past both of rumqttc's default 10 KiB packet limits.
OVER_DEFAULT_PACKET_LIMIT = 20 * 1024


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


def _add_cmd(dev_id, ip, **fields):
    return {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER, **fields}


def _start_mock(dev_id, ip, dps=None):
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps=dps or {"1": True})
    mock.start()
    return mock


def _settled(coll, quiet=1.0):
    """Everything `coll` receives until the bridge has been quiet for `quiet`."""
    got = []
    last = time.time()
    while time.time() - last < quiet:
        batch = coll.drain()
        if batch:
            got += batch
            last = time.time()
        time.sleep(0.05)
    return got


# ── outbound: exact topics and payloads per configuration ─────────────────────


@dataclass
class OutputCase:
    config: dict
    # Expected (topic, payload) from one device push, `{root}`-relative.
    events: list
    # Expected topic of the device's connect status and add response.
    status_topic: str
    response_topic: str


DEV = "ebwire00000000000001"
NAME = "lamp"
OUTPUT_CASES = {
    "default-multi-dp": OutputCase(
        config={},
        events=[
            (f"event/active/{DEV}", {"1": False, "2": 75}),
            (f"event/state/{DEV}", {"1": False, "2": 75}),
        ],
        status_topic=f"error/{DEV}",
        response_topic=f"response/{DEV}",
    ),
    "single-dp": OutputCase(
        config={"mqtt_event_topic": "{root}/event/{type}/{id}/{dp}"},
        events=[
            (f"event/active/{DEV}/1", False),
            (f"event/active/{DEV}/2", 75),
            (f"event/state/{DEV}/1", False),
            (f"event/state/{DEV}/2", 75),
        ],
        status_topic=f"error/{DEV}",
        response_topic=f"response/{DEV}",
    ),
    "name-in-topic-custom-payload": OutputCase(
        config={
            "mqtt_event_topic": "{root}/ev/{type}/{name}",
            "mqtt_payload_template": '{"id":"{id}","name":"{name}","type":"{type}","dps":{dps}}',
        },
        events=[
            (f"ev/active/{NAME}",
             {"id": DEV, "name": NAME, "type": "active", "dps": {"1": False, "2": 75}}),
            (f"ev/state/{NAME}",
             {"id": DEV, "name": NAME, "type": "state", "dps": {"1": False, "2": 75}}),
        ],
        status_topic=f"error/{DEV}",
        response_topic=f"response/{DEV}",
    ),
    "single-dp-custom-payload": OutputCase(
        config={
            "mqtt_event_topic": "{root}/event/{type}/{id}/{dp}",
            "mqtt_payload_template": '{"dp":"{dp}","type":"{type}","val":{value}}',
        },
        events=[
            (f"event/active/{DEV}/1", {"dp": "1", "type": "active", "val": False}),
            (f"event/active/{DEV}/2", {"dp": "2", "type": "active", "val": 75}),
            (f"event/state/{DEV}/1", {"dp": "1", "type": "state", "val": False}),
            (f"event/state/{DEV}/2", {"dp": "2", "type": "state", "val": 75}),
        ],
        status_topic=f"error/{DEV}",
        response_topic=f"response/{DEV}",
    ),
    "custom-message-topic": OutputCase(
        config={"mqtt_message_topic": "{root}/msg/{level}/{name}/{id}"},
        events=[
            (f"event/active/{DEV}", {"1": False, "2": 75}),
            (f"event/state/{DEV}", {"1": False, "2": 75}),
        ],
        status_topic=f"msg/error/{NAME}/{DEV}",
        # A device response carries no name, so `{name}` renders as an empty level.
        response_topic=f"msg/response//{DEV}",
    ),
}


@pytest.mark.parametrize("case", OUTPUT_CASES.values(), ids=OUTPUT_CASES.keys())
def test_device_output_on_the_wire(contract, case):
    ip = loopback_ips(1, start=180)[0]
    h = contract(**case.config)
    root = h.root
    coll = Collector(f"{root}-wire-col")
    coll.subscribe(f"{root}/#")
    mock = _start_mock(DEV, ip, {"1": True, "2": 50})
    try:
        # The sentinel's replay and anything else from startup are not under test.
        _settled(coll)
        coll.publish(h.command(), json.dumps(_add_cmd(DEV, ip, name=NAME)))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.5)
        startup = _settled(coll)

        status = next((p for t, p, _ in startup if t == f"{root}/{case.status_topic}"), None)
        assert status is not None, f"no connect status: {[t for t, _, _ in startup]}"
        assert json.loads(status) == {
            "errorCode": 0, "errorMsg": "Connection Successful", "id": DEV, "name": NAME,
        }

        response = next((p for t, p, _ in startup if t == f"{root}/{case.response_topic}"), None)
        assert response is not None, f"no add response: {[t for t, _, _ in startup]}"
        assert json.loads(response) == {"status": "ok", "action": "add", "id": DEV}

        assert mock.push({"1": False, "2": 75}) is True
        emitted = _settled(coll, quiet=2.0)
        got = sorted((t.removeprefix(f"{root}/"), json.loads(p)) for t, p, _ in emitted)
        assert got == sorted(case.events, key=lambda e: e[0]), f"got {got}"
    finally:
        mock.stop()
        coll.close()


def test_payload_template_timestamp_is_rendered(contract):
    dev_id = "ebwire00000000000002"
    ip = loopback_ips(1, start=181)[0]
    h = contract(mqtt_payload_template="v={value};ts={timestamp}")
    coll = Collector(f"{h.root}-ts-col")
    coll.subscribe(f"{h.root}/#")
    mock = _start_mock(dev_id, ip)
    try:
        coll.publish(h.command(), json.dumps(_add_cmd(dev_id, ip)))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)
        assert mock.push({"1": False}) is True
        topic = h.state_topic(dev_id)
        msg = coll.wait_for(lambda t, p, r: t == topic and p and not r, timeout=15)
        assert msg, "no snapshot published"
        assert re.fullmatch(r'v=\{"1":false\};ts=\d{10}', msg[1]), msg[1]
    finally:
        mock.stop()
        coll.close()


@pytest.mark.parametrize("config,topic", [
    ({}, "scanner"),
    ({"mqtt_scanner_topic": "{root}/found"}, "found"),
], ids=["default", "custom"])
def test_scan_reports_on_the_scanner_topic(contract, config, topic):
    """The empty object marking the end of a scan is emitted whatever is on the
    LAN, so this doesn't depend on discovery finding anything."""
    h = contract(scan_window_secs=1, **config)
    coll = Collector(f"{h.root}-scan-col")
    coll.subscribe(f"{h.root}/#")
    try:
        coll.publish(h.command(), json.dumps({"action": "scan"}))
        ack = coll.wait_for(lambda t, p, r: t == f"{h.root}/response/bridge", timeout=10)
        assert ack, "no response to scan"
        assert json.loads(ack[1])["action"] == "scan"
        assert coll.wait_for(lambda t, p, r: t == f"{h.root}/{topic}" and p == "{}", timeout=15), (
            "the end of the scan was not reported on the scanner topic"
        )
    finally:
        coll.close()


def test_status_keeps_its_drop_counter(contract):
    h = contract()
    coll = Collector(f"{h.root}-status-col")
    coll.subscribe(f"{h.root}/#")
    try:
        coll.publish(h.command(), json.dumps({"action": "status"}))
        reply = coll.wait_for(lambda t, p, r: t == f"{h.root}/response/bridge", timeout=10)
        assert reply, "no response to status"
        assert json.loads(reply[1])["mqtt_drop_count"] == 0
    finally:
        coll.close()


def test_messages_over_the_default_packet_limit_pass_both_ways(contract):
    """rumqttc refuses packets over 10 KiB in either direction by default, and
    outbound it does so before anything reaches the broker."""
    dev_id = "ebwire00000000000003"
    name = "n" * OVER_DEFAULT_PACKET_LIMIT
    ip = loopback_ips(1, start=182)[0]
    h = contract()
    coll = Collector(f"{h.root}-big-col")
    coll.subscribe(f"{h.root}/#")
    try:
        coll.publish(h.command(), json.dumps(_add_cmd(dev_id, ip, name=name)))
        assert coll.wait_for(
            lambda t, p, r: t == f"{h.root}/response/{dev_id}" and '"ok"' in p, timeout=10,
        ), "an oversized command was not processed"

        coll.publish(h.command(), json.dumps({"action": "status"}))
        reply = coll.wait_for(lambda t, p, r: t == f"{h.root}/response/bridge", timeout=10)
        assert reply, "the oversized status reply never reached the broker"
        assert name in reply[1]
    finally:
        coll.close()


# ── inbound: every way of addressing a command ────────────────────────────────


@pytest.mark.parametrize("template,topic,payload", [
    ("{root}/command", "command", {"action": "set", "id": "{id}", "dps": {"1": True}}),
    ("{root}/command", "command", {"action": "set", "id": "{id}", "1": True}),
    ("{root}/command/{action}", "command/set", {"id": "{id}", "dps": {"1": True}}),
    ("{root}/command/{id}", "command/{id}", {"action": "set", "1": True}),
    ("{root}/command/{action}/{id}", "command/set/{id}", {"1": True}),
    ("{root}/command/{action}/{id}/{dp}", "command/set/{id}/1", True),
], ids=["body", "body-loose-dps", "action-in-topic", "id-in-topic",
        "action-id-in-topic", "action-id-dp-in-topic"])
def test_set_reaches_the_device(contract, template, topic, payload):
    dev_id = "ebcmd000000000000001"
    ip = loopback_ips(1, start=183)[0]
    h = contract(mqtt_command_topic=template)
    root = h.root
    coll = Collector(f"{root}-cmd-col")
    coll.subscribe(f"{root}/#")
    mock = _start_mock(dev_id, ip, {"1": False})

    def topic_for(action):
        # `-` fills a variable `add` has no use for: every level must be non-empty.
        return f"{root}/" + (template.removeprefix("{root}/")
                             .replace("{action}", action)
                             .replace("{id}", dev_id)
                             .replace("{dp}", "-"))

    try:
        coll.publish(topic_for("add"), json.dumps(_add_cmd(dev_id, ip)))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)

        body = json.dumps(payload).replace("{id}", dev_id)
        coll.publish(f"{root}/{topic.replace('{id}', dev_id)}", body)
        assert _wait(lambda: mock.dps.get("1") is True, 15), (
            f"set never applied (device dps stayed {dict(mock.dps)})"
        )
    finally:
        mock.stop()
        coll.close()


def test_array_runs_every_valid_request_and_skips_the_rest(contract):
    h = contract()
    ips = loopback_ips(2, start=184)
    coll = Collector(f"{h.root}-array-col")
    coll.subscribe(f"{h.root}/#")
    try:
        coll.publish(h.command(), json.dumps([
            _add_cmd("ebarray0000000000001", ips[0]),
            {"action": "no-such-action"},
            42,
            _add_cmd("ebarray0000000000002", ips[1]),
        ]))
        for dev_id in ("ebarray0000000000001", "ebarray0000000000002"):
            assert coll.wait_for(
                lambda t, p, r: t == f"{h.root}/response/{dev_id}" and '"ok"' in p, timeout=10,
            ), f"{dev_id} from the array was not added"
    finally:
        coll.close()


@pytest.mark.parametrize("garbage", [
    "not json", "", "{", "[1,2]", "42", '{"action":"no-such-action"}', '{"no":"action"}',
    b"\xff\xfe",
], ids=["text", "empty", "truncated", "number-array", "number", "unknown-action",
        "no-action", "invalid-utf8"])
def test_bad_command_leaves_the_bridge_responsive(contract, garbage):
    h = contract()
    coll = Collector(f"{h.root}-bad-col")
    coll.subscribe(f"{h.root}/#")
    try:
        coll.publish(h.command(), garbage)
        coll.publish(h.command(), json.dumps({"action": "status"}))
        assert coll.wait_for(
            lambda t, p, r: t == f"{h.root}/response/bridge" and '"ok"' in p, timeout=10,
        ), "no status reply after a malformed command"
        assert h.thread.is_alive()
    finally:
        coll.close()


# ── connection: shutdown flush and reconnection ───────────────────────────────


def test_stop_right_after_a_device_report_keeps_the_snapshot(contract):
    """The end-to-end view of a stop racing a device report. This can't tell a
    flushed queue from a message that had already left, so the flush itself is
    covered by a fake-broker test in `bridge.rs`."""
    dev_id = "ebstop00000000000001"
    ip = loopback_ips(1, start=186)[0]
    h = contract()
    state = h.state_topic(dev_id)
    coll = Collector(f"{h.root}-stop-col")
    coll.subscribe(f"{h.root}/#")
    mock = _start_mock(dev_id, ip)
    try:
        coll.publish(h.command(), json.dumps(_add_cmd(dev_id, ip)))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)
        coll.drain()

        assert mock.push({"1": False}) is True
        assert coll.wait_for(lambda t, p, r: "/event/" in t and t != state, timeout=15), "no delta"
        h.srv.stop()
        h.thread.join(timeout=10)

        assert json.loads(retained(state).get(state, "{}")) == {"1": False}, (
            "the snapshot was lost in shutdown"
        )
        assert sentinel_session(h.root) is None, "stop left the sentinel retained"
    finally:
        mock.stop()
        coll.close()


class SeverableProxy:
    """A TCP proxy to the broker that can drop its connections and refuse new
    ones, which from the bridge's side is an unclean loss of the broker: no
    DISCONNECT is sent, so the broker publishes the bridge's will too."""

    def __init__(self):
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._lock = threading.Lock()
        self._conns = []
        self._up = True
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                if not self._up:
                    client.close()
                    continue
                upstream = socket.create_connection((BROKER_HOST, BROKER_PORT))
                self._conns += [client, upstream]
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    @staticmethod
    def _pump(src, dst):
        try:
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass
        for s in (src, dst):
            try:
                s.close()
            except OSError:
                pass

    def sever(self):
        with self._lock:
            self._up = False
            for s in self._conns:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            self._conns.clear()

    def restore(self):
        with self._lock:
            self._up = True

    def close(self):
        self.sever()
        self._listener.close()


@pytest.fixture
def proxy():
    p = SeverableProxy()
    yield p
    p.close()


@pytest.fixture
def blipped(contract, proxy):
    """A bridge with one connected device that has been through a broker outage
    and is confirmed back: its `status` reply came through. The collector talks
    to the broker directly and so stays connected throughout."""
    dev_id = "ebblip00000000000001"
    ip = loopback_ips(1, start=187)[0]
    h = contract(mqtt_broker=f"mqtt://127.0.0.1:{proxy.port}")
    coll = Collector(f"{h.root}-blip-col")
    coll.subscribe(f"{h.root}/#")
    mock = _start_mock(dev_id, ip, {"1": True, "2": 50})
    try:
        coll.publish(h.command(), json.dumps(_add_cmd(dev_id, ip)))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)
        assert mock.push({"1": False, "2": 75}) is True
        state = h.state_topic(dev_id)
        assert _wait(lambda: state in retained(state), 15), "no snapshot before the outage"
        config = f"{h.root}/bridge/config"
        sentinel = retained(config).get(config)
        assert sentinel, "no sentinel before the outage"

        proxy.sever()
        # A retained state nobody but a re-run seed phase would read.
        publish_retained({state: '{"1":false,"2":75,"99":"foreign"}'})
        time.sleep(2.0)
        proxy.restore()

        coll.drain()

        def answers():
            coll.publish(h.command(), json.dumps({"action": "status"}))
            return coll.wait_for(lambda t, p, r: t == f"{h.root}/response/bridge", timeout=2)

        assert _wait(answers, 90), "the bridge never came back after the outage"
        yield h, coll, mock, dev_id, sentinel
    finally:
        proxy.restore()
        mock.stop()
        coll.close()


def test_commands_work_after_a_broker_outage(blipped):
    h, coll, mock, dev_id, _ = blipped
    coll.publish(h.command(), json.dumps({"action": "set", "id": dev_id, "dps": {"1": True}}))
    assert _wait(lambda: mock.dps.get("1") is True, 15), (
        f"set never applied after reconnecting (device dps stayed {dict(mock.dps)})"
    )


def test_seed_does_not_run_again_after_a_broker_outage(blipped):
    """The seed phase belongs to startup. Run again on a reconnect, it would take
    whatever is retained for the bridge's own state."""
    h, _, mock, dev_id, _ = blipped
    state = h.state_topic(dev_id)
    assert mock.push({"1": True}) is True
    assert _wait(lambda: json.loads(retained(state).get(state, "{}")).get("1") is True, 15), (
        "no snapshot published after reconnecting"
    )
    assert json.loads(retained(state)[state]) == {"1": True, "2": 75}


def test_sentinel_is_restored_unchanged_after_a_broker_outage(blipped):
    """The outage fires the will, which clears the sentinel. It has to come back
    byte for byte: rustuya-manager reads a new `devices_updated_at` as a registry
    change, and nothing about the registry changed."""
    h, _, _, _, before = blipped
    config = f"{h.root}/bridge/config"
    # Shorter than the save debounce a device's `add` left pending, whose republish
    # would otherwise pass for the reconnect's.
    assert _wait(lambda: config in retained(config), 5), "sentinel gone after reconnecting"
    assert retained(config)[config] == before


def test_second_instance_is_still_refused_after_a_broker_outage(blipped, tmp_path):
    h, _, _, _, before = blipped
    config = f"{h.root}/bridge/config"
    assert _wait(lambda: config in retained(config), 5), "sentinel gone after reconnecting"

    second = pyrustuyabridge.PyBridgeServer(
        mqtt_broker=BROKER_URL,
        mqtt_root_topic=h.root,
        mqtt_client_id=f"{h.root}-second",
        state_file=str(tmp_path / "second.json"),
        no_signals=True,
        log_level="warn",
    )
    outcome = []

    def run():
        try:
            second.start()
            outcome.append(None)
        except RuntimeError as exc:
            outcome.append(str(exc))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout=30)
    if t.is_alive():
        second.stop()
        pytest.fail("a second instance started on the root after the outage")
    assert outcome and "Duplicate instance" in (outcome[0] or ""), f"got {outcome}"
    assert h.thread.is_alive(), "the incumbent went down"
    assert retained(config)[config] == before


def test_bridge_yields_to_an_instance_that_took_over_during_an_outage(contract, proxy):
    """Restoring the sentinel without checking for a new owner first would make
    each bridge see the other's and shut down, leaving the root with none."""
    h = contract(mqtt_broker=f"mqtt://127.0.0.1:{proxy.port}")
    assert sentinel_session(h.root), "no sentinel before the outage"
    proxy.sever()
    # Long enough for the will to land first, as it would before a new owner starts.
    time.sleep(1.0)
    publish_retained({f"{h.root}/bridge/config": '{"session_id":"new-owner"}'})
    proxy.restore()
    h.thread.join(timeout=60)
    assert not h.thread.is_alive(), "the bridge kept running alongside the new owner"
    assert sentinel_session(h.root) == "new-owner", "the bridge overwrote the new owner's sentinel"
