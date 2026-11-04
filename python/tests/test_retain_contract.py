"""The retain model as seen from the broker.

Everything here asserts on what a subscriber can observe (topics, retain, QoS,
which instance owns a root), never on the bridge's internal types, so these
tests stay valid however the bridge is structured internally.

The scavenger cases plant their retained messages by hand rather than driving a
device: what is under test is which topics a removal clears, and that needs
exact control over topics and payloads a real device can't be made to produce.
"""
import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import pytest
import tuyamock

from conftest import (
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
DEFAULT_EVENT_TOPIC = "{root}/event/{type}/{id}"

# Longer than a scavenger pass (1s idle window in the fixture), so a wrong clear
# issued by the pass that handled the removal has landed by the time we look.
SCAVENGER_SETTLE_SECS = 3.0


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


class QosCollector(Collector):
    """A Collector that also records each delivery's QoS. The subscription is
    QoS1, so a delivered QoS is the QoS the bridge published at."""

    def __init__(self, client_id):
        self.qos = {}
        super().__init__(client_id)

    def _on_message(self, c, u, m):
        self.qos.setdefault(m.topic, set()).add(m.qos)
        super()._on_message(c, u, m)


# ── scavenger: clears the removed device's topics and nobody else's ───────────


@dataclass
class ScavengeCase:
    """`target` is removed. `gone` must be cleared by that, `kept` must not be.
    Topics and payloads use `{root}`, filled in per test."""

    event_topic: str
    target: dict
    gone: dict
    kept: dict
    others: list = field(default_factory=list)


SCAVENGE_CASES = {
    "dp-value-equals-removed-name": ScavengeCase(
        DEFAULT_EVENT_TOPIC,
        target={"id": "target0001", "name": "white"},
        gone={
            "{root}/event/state/target0001": '{"1":true}',
            "{root}/error/target0001": '{"errorCode":0,"id":"target0001","name":"white"}',
        },
        kept={"{root}/event/state/other0001": '{"21":"white"}'},
    ),
    "dp-key-equals-removed-name": ScavengeCase(
        DEFAULT_EVENT_TOPIC,
        target={"id": "target0001", "name": "1"},
        gone={"{root}/event/state/target0001": '{"1":true}'},
        kept={"{root}/event/state/other0001": '{"1":true}'},
    ),
    "dp-key-equals-removed-cid": ScavengeCase(
        DEFAULT_EVENT_TOPIC,
        others=[{"id": "gw0001"}],
        target={"id": "sub0001", "cid": "1", "parent_id": "gw0001"},
        gone={"{root}/event/state/sub0001": '{"1":true}'},
        kept={"{root}/event/state/other0001": '{"1":true}'},
    ),
    "same-name-sibling-with-id-in-topic": ScavengeCase(
        "{root}/event/{type}/{id}/{name}",
        others=[{"id": "other0001", "name": "kitchen"}],
        target={"id": "target0001", "name": "kitchen"},
        gone={"{root}/event/state/target0001/kitchen": '{"1":true}'},
        kept={"{root}/event/state/other0001/kitchen": '{"1":true}'},
    ),
    "same-cid-on-another-gateway-with-id-in-topic": ScavengeCase(
        "{root}/event/{type}/{id}/{cid}",
        others=[
            {"id": "gw0001"},
            {"id": "gw0002"},
            {"id": "sub0002", "cid": "c1", "parent_id": "gw0002"},
        ],
        target={"id": "sub0001", "cid": "c1", "parent_id": "gw0001"},
        gone={"{root}/event/state/sub0001/c1": '{"1":true}'},
        kept={"{root}/event/state/sub0002/c1": '{"1":true}'},
    ),
    "name-only-topic": ScavengeCase(
        "{root}/event/{type}/{name}",
        target={"id": "target0001", "name": "kitchen"},
        gone={"{root}/event/state/kitchen": '{"1":true}'},
        kept={"{root}/event/state/hall": '{"21":"kitchen"}'},
    ),
    "cid-only-topic": ScavengeCase(
        "{root}/event/{type}/{cid}",
        others=[{"id": "gw0001"}],
        target={"id": "sub0001", "cid": "c1", "parent_id": "gw0001"},
        gone={"{root}/event/state/c1": '{"1":true}'},
        kept={"{root}/event/state/c2": '{"21":"c1"}'},
    ),
    "single-dp-topic": ScavengeCase(
        "{root}/event/{type}/{id}/{dp}",
        target={"id": "target0001"},
        gone={
            "{root}/event/state/target0001/1": "true",
            "{root}/event/state/target0001/2": "50",
        },
        kept={"{root}/event/state/other0001/1": "true"},
    ),
}


@pytest.mark.parametrize("case", SCAVENGE_CASES.values(), ids=SCAVENGE_CASES.keys())
def test_removal_clears_only_the_removed_device(contract, case):
    h = contract(mqtt_event_topic=case.event_topic)

    def fill(messages):
        return {t.replace("{root}", h.root): p for t, p in messages.items()}

    gone, kept = fill(case.gone), fill(case.kept)

    coll = Collector(f"{h.root}-scav-col")
    coll.subscribe(f"{h.root}/#")
    try:
        # Nothing listens on these addresses: only the registry entries matter.
        devices = [*case.others, case.target]
        for ip, device in zip(loopback_ips(len(devices), start=200), devices):
            coll.publish(h.command(), json.dumps(
                {"action": "add", "key": KEY, "version": VER, "ip": ip, **device}))
            dev_id = device["id"]
            assert coll.wait_for(
                lambda t, p, r: t == f"{h.root}/response/{dev_id}" and '"ok"' in p,
                timeout=10,
            ), f"no ok response to adding {dev_id}"

        publish_retained({**gone, **kept})
        assert _wait(lambda: (gone.keys() | kept.keys()) <= retained(f"{h.root}/#").keys(), 10)

        coll.publish(h.command(), json.dumps({"action": "remove", "id": case.target["id"]}))

        assert _wait(lambda: not (gone.keys() & retained(f"{h.root}/#").keys()), 15), (
            f"the removed device's retained was not cleared: {retained(f'{h.root}/#')}"
        )
        time.sleep(SCAVENGER_SETTLE_SECS)
        remaining = retained(f"{h.root}/#")
        assert kept.items() <= remaining.items(), (
            f"retained belonging to another device was cleared: kept={kept} remaining={remaining}"
        )
    finally:
        coll.close()


# ── seed: a restart recovers state from the broker ────────────────────────────


def test_restart_recovers_snapshot_and_partial_push_keeps_the_rest(contract):
    """The hazard cache mode exists for: a device reporting one DP right after a
    restart must not replace the retained full state with that one DP. The push
    repeats from the moment the device reconnects so it lands wherever it lands
    relative to the seed window; the outcome must be the same on either side."""
    dev_id = "ebseed00000000000001"
    ip = loopback_ips(1, start=190)[0]
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True, "2": 50})
    mock.start()
    first = contract()
    state_topic = first.state_topic(dev_id)
    coll = Collector(f"{first.root}-seed-col")
    coll.subscribe(f"{first.root}/#")
    stop_pushing = threading.Event()

    def keep_pushing():
        while not stop_pushing.wait(0.2):
            if mock.connected:
                mock.push({"1": True})

    pusher = threading.Thread(target=keep_pushing, daemon=True)
    try:
        coll.publish(first.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)
        assert mock.push({"1": False, "2": 75}) is True
        assert _wait(
            lambda: json.loads(retained(state_topic).get(state_topic, "{}")) == {"1": False, "2": 75},
            15,
        ), "the first bridge never retained the full state"

        first.srv.stop()
        first.thread.join(timeout=10)
        assert _wait(lambda: not mock.connected, 15), "the device stayed connected after stop"

        pusher.start()
        contract()  # same root and state file: a restart, with an empty cache

        assert _wait(
            lambda: json.loads(retained(state_topic).get(state_topic, "{}")).get("1") is True,
            20,
        ), "the partial push never reached the retained state"
        assert json.loads(retained(state_topic)[state_topic]) == {"1": True, "2": 75}, (
            "the seeded DP was lost: the partial push replaced the retained state"
        )
    finally:
        stop_pushing.set()
        if pusher.is_alive():
            pusher.join(timeout=5)
        mock.stop()
        coll.close()


def test_seed_ignores_live_messages_on_the_state_topic(contract):
    """Only retained messages are prior state. On an empty root the seed window
    stays open for its full hard cap, which is what makes this publish land
    inside it."""
    dev_id = "ebseed00000000000002"
    ip = loopback_ips(1, start=191)[0]
    state_topic = f"{contract.root}/event/state/{dev_id}"
    coll = Collector(f"{contract.root}-live-col")
    coll.subscribe(f"{contract.root}/#")
    live = threading.Timer(2.5, coll.publish, (state_topic, '{"9":"live"}'))
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        live.start()
        h = contract()
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
        time.sleep(0.3)
        assert mock.push({"1": False}) is True
        assert _wait(lambda: state_topic in retained(state_topic), 15), "no snapshot retained"
        assert json.loads(retained(state_topic)[state_topic]) == {"1": False}, (
            "a live message on the state topic was taken for prior state"
        )
    finally:
        live.cancel()
        mock.stop()
        coll.close()


# ── QoS and retain per message kind ───────────────────────────────────────────


def _exercise_device(h, coll, dev_id, ip, mock):
    """Connect a device and make it report, so every message kind is emitted."""
    coll.publish(h.command(), json.dumps(
        {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))
    assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"
    time.sleep(0.3)
    assert mock.push({"1": False}) is True


def test_qos_and_retain_by_message_kind_in_cache_mode(contract):
    dev_id = "ebqos000000000000001"
    ip = loopback_ips(1, start=192)[0]
    h = contract()
    coll = QosCollector(f"{h.root}-qos-col")
    coll.subscribe(f"{h.root}/#")
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        _exercise_device(h, coll, dev_id, ip, mock)
        state = h.state_topic(dev_id)
        error = f"{h.root}/error/{dev_id}"
        response = f"{h.root}/response/{dev_id}"
        config = f"{h.root}/bridge/config"
        assert _wait(
            lambda: {state, error, response} <= coll.qos.keys()
            and any(t.startswith(f"{h.root}/event/") and t != state for t in coll.qos),
            15,
        ), f"a message kind was never published: {sorted(coll.qos)}"
        delta = next(t for t in coll.qos if t.startswith(f"{h.root}/event/") and t != state)

        held = retained(f"{h.root}/#", settle=1.0)
        # Durable state: acknowledged and retained.
        assert coll.qos[state] == {1} and state in held
        assert coll.qos[error] == {1} and error in held
        assert coll.qos[config] == {1} and config in held
        # Live traffic: a fleet fan-out emits these by the hundred, and QoS1 here
        # stalled the loop and starved the device actors.
        assert coll.qos[delta] == {0} and delta not in held
        assert coll.qos[response] == {0} and response not in held
    finally:
        mock.stop()
        coll.close()


def test_qos_and_retain_by_message_kind_in_pass_through_mode(contract):
    dev_id = "ebqos000000000000002"
    ip = loopback_ips(1, start=193)[0]
    h = contract(mqtt_retain=False)
    coll = QosCollector(f"{h.root}-qos-col")
    coll.subscribe(f"{h.root}/#")
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        _exercise_device(h, coll, dev_id, ip, mock)
        state = h.state_topic(dev_id)
        error = f"{h.root}/error/{dev_id}"
        config = f"{h.root}/bridge/config"
        assert _wait(
            lambda: error in coll.qos
            and any(t.startswith(f"{h.root}/event/") for t in coll.qos),
            15,
        ), f"a message kind was never published: {sorted(coll.qos)}"
        delta = next(t for t in coll.qos if t.startswith(f"{h.root}/event/"))

        assert state not in coll.qos, "pass-through mode published a snapshot"
        assert coll.qos[delta] == {0}
        assert coll.qos[error] == {0}
        # The single-instance sentinel is retained in both modes.
        assert retained(f"{h.root}/#", settle=1.0).keys() == {config}
    finally:
        mock.stop()
        coll.close()


def test_turning_retain_on_starts_retaining_snapshots(contract, tmp_path):
    """The reverse of the flip `test_reconfigure_applies` covers: the restarted
    cycle has to build a cache and run a seed phase the old one never had."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mqtt_retain": False}))
    h = contract(config_path=str(config_path), mqtt_retain=None)

    dev_id = "ebflip00000000000001"
    ip = loopback_ips(1, start=194)[0]
    state = h.state_topic(dev_id)
    coll = Collector(f"{h.root}-flip-col")
    coll.subscribe(f"{h.root}/#")
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        _exercise_device(h, coll, dev_id, ip, mock)
        assert _wait(lambda: sentinel_session(h.root), 10), "no sentinel published"
        time.sleep(1.0)
        assert state not in retained(state), "retained a snapshot with mqtt_retain off"

        connections = mock.server.connections
        sid = sentinel_session(h.root)
        coll.publish(h.command(), json.dumps(
            {"action": "set_config", "mqtt_retain": True, "apply": True}))
        assert _wait(lambda: sentinel_session(h.root) not in (None, sid), 30), (
            "the bridge did not restart for the retain flip"
        )

        assert mock.push({"1": True}) is True
        assert _wait(lambda: state in retained(state), 20), (
            "no snapshot retained after turning mqtt_retain on"
        )
        assert json.loads(retained(state)[state]) == {"1": True}
        assert mock.server.connections == connections, "the flip cost the device its connection"
    finally:
        mock.stop()
        coll.close()


# ── one live bridge per root ──────────────────────────────────────────────────


def test_second_instance_on_a_live_root_is_refused(contract, tmp_path):
    h = contract()
    sid = sentinel_session(h.root)
    assert sid, "no sentinel published"

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
        pytest.fail("the second instance started on a root with a live bridge")
    assert outcome and "Duplicate instance" in (outcome[0] or ""), f"got {outcome}"
    assert h.thread.is_alive(), "the refused instance took the incumbent down"
    assert sentinel_session(h.root) == sid, "the refused instance overwrote the sentinel"


def test_stale_sentinel_does_not_block_startup(contract):
    """A sentinel nobody answers for is a ghost of an unclean exit (power loss
    with a co-located broker): startup must proceed once the probe gives up."""
    publish_retained({f"{contract.root}/bridge/config": '{"session_id":"ghost"}'})
    h = contract()
    assert _wait(lambda: sentinel_session(h.root) not in (None, "ghost"), 40), (
        f"still blocked behind the stale sentinel: {sentinel_session(h.root)}"
    )
    assert h.thread.is_alive()


def test_foreign_session_on_the_sentinel_shuts_the_bridge_down(contract):
    """The backstop for two live instances slipping past the startup probe."""
    h = contract()
    assert sentinel_session(h.root), "no sentinel published"
    publish_retained({f"{h.root}/bridge/config": '{"session_id":"foreign"}'})
    h.thread.join(timeout=20)
    assert not h.thread.is_alive(), "the bridge kept running after being taken over"


def test_stop_clears_the_sentinel(contract):
    h = contract()
    assert sentinel_session(h.root), "no sentinel published"
    h.srv.stop()
    h.thread.join(timeout=10)
    assert sentinel_session(h.root) is None, "stop left the sentinel retained"


def test_killed_process_has_its_sentinel_cleared_by_the_will(contract, tmp_path):
    """Needs a real process: only the OS closing the socket without a DISCONNECT
    makes the broker publish the will."""
    root = contract.root
    script = (
        "import sys, pyrustuyabridge as p\n"
        "p.PyBridgeServer(mqtt_broker=sys.argv[1], mqtt_root_topic=sys.argv[2],\n"
        "                 mqtt_client_id=sys.argv[3], state_file=sys.argv[4],\n"
        "                 no_signals=True, log_level='warn').start()\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", script, BROKER_URL, root,
                             f"{root}-killed", str(tmp_path / "killed.json")])
    try:
        assert _wait(lambda: sentinel_session(root), 15), "the bridge never published a sentinel"
        proc.kill()
        proc.wait(timeout=10)
        assert _wait(lambda: sentinel_session(root) is None, 15), (
            "the broker kept the sentinel after the bridge was killed"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
