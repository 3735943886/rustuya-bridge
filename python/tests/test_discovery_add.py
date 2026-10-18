"""A device registered without an `ip` must be located by LAN discovery.

This is the `"Auto"` address of 0.3, rebuilt on 0.4's owned `Discovery`. It has
one failure mode that only shows up against a live announcer, and it is the
*normal* case rather than an edge one: the bridge has been listening since
startup, so by the time an operator registers a device its announcement is
usually already cached — and a cached announcement is a liveness tick, not a
relocation. A build that only reacts to first-sightings looks perfect against a
device that appears after the `add` and never connects to one that was already
there.

So the cache is deliberately warmed first, and proven warm through the bridge's
own `scan` output before the `add` is sent. No sleep decides anything here: the
scanner result is the readiness edge, and the connection is awaited on
`mock.connected`.
"""
import json
import time

import tuyamock

from conftest import Collector, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VER = "3.4"
# Long enough for tuyamock's 8s beacon cadence to land at least one announcement,
# short enough not to dominate the test. It is a deadline, not a delay: the scan
# result below is awaited, so a faster beacon ends the wait sooner.
SCAN_WINDOW_SECS = 10


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


def test_device_added_without_an_ip_is_located_by_discovery(bridge):
    h = bridge(scan_window_secs=SCAN_WINDOW_SECS)

    dev_id = "ebdiscovery00000001"
    ip = loopback_ips(1, start=90)[0]

    coll = Collector(f"{h.root}-disco-col")
    coll.subscribe(f"{h.root}/#")

    # `discovery_addr` is where the mock sends its announcement; pointing it at
    # its own loopback address makes the beacon reach the bridge's discovery
    # socket without depending on broadcast routing.
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True},
                               discovery=True, discovery_addr=ip)
    mock.start()
    try:
        # Warm the discovery cache, and prove it is warm: the device shows up in
        # the bridge's own scan output. Every announcement after this one is a
        # repeat of a cached entry — exactly the state the `add` below must work
        # from.
        coll.publish(h.command(), json.dumps({"action": "scan"}))
        seen = coll.wait_for(lambda t, p, r: t.endswith("/scanner") and dev_id in p,
                             timeout=SCAN_WINDOW_SECS + 15)
        assert seen is not None, "discovery never announced the device"

        # No `ip` — the whole point. The bridge must resolve it.
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY, "version": VER}))
        assert _wait(lambda: mock.connected, 30), (
            "the bridge never connected to a device registered without an ip, "
            "even though discovery had already announced it"
        )

        # Located is not the same as controllable: prove the resolved address
        # carries a working session.
        coll.publish(h.command(), json.dumps(
            {"action": "set", "id": dev_id, "dps": {"1": False}}))
        assert _wait(lambda: mock.dps.get("1") is False, 15), (
            "the discovery-located device did not apply a `set`"
        )
    finally:
        mock.stop()
        coll.close()


def test_addressless_add_recovers_when_device_appears_later(bridge):
    """An add must acknowledge before discovery, then connect without re-adding."""
    h = bridge(scan_window_secs=3600)
    dev_id = "eblatediscovery000001"
    ip = loopback_ips(1, start=91)[0]
    coll = Collector(f"{h.root}-late-col")
    coll.subscribe(f"{h.root}/#")
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True},
                               discovery=True, discovery_addr=ip)
    try:
        # Neither IP nor version is known, and the mock is still stopped.
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY}))
        observed = {"ack": False, "offline": False}

        def registered_and_offline(topic, payload, retained):
            if topic.endswith(f"/response/{dev_id}"):
                observed["ack"] = json.loads(payload).get("status") == "ok"
            if topic.endswith(f"/error/{dev_id}"):
                observed["offline"] = json.loads(payload).get("errorCode") == 905
            return all(observed.values())

        assert coll.wait_for(registered_and_offline, timeout=5) is not None, (
            f"addressless add must acknowledge and report its first failure: {observed}"
        )
        mock.start()
        assert _wait(lambda: mock.connected, 30), "late discovery did not wake the registered driver"
        coll.publish(h.command(), json.dumps(
            {"action": "set", "id": dev_id, "dps": {"1": False}}))
        assert _wait(lambda: mock.dps.get("1") is False, 15), "Auto did not establish a working 3.4 session"
    finally:
        mock.stop()
        coll.close()


def test_cold_start_restores_undiscovered_device_and_recovers(bridge, tmp_path):
    """A persisted addressless device must keep its retry actor after cold start."""
    dev_id = "ebcoldiscovery0000001"
    ip = loopback_ips(1, start=92)[0]
    state_file = tmp_path / "cold-state.json"
    state_file.write_text(json.dumps({dev_id: {"id": dev_id, "key": KEY}}))
    h = bridge(scan_window_secs=3600, state_file=str(state_file))
    coll = Collector(f"{h.root}-cold-col")
    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True},
                               discovery=True, discovery_addr=ip)
    try:
        mock.start()
        assert _wait(lambda: mock.connected, 30), "cold start failed to retain the addressless driver"
        coll.publish(h.command(), json.dumps(
            {"action": "set", "id": dev_id, "dps": {"1": False}}))
        assert _wait(lambda: mock.dps.get("1") is False, 15), "restored device could not be controlled"
    finally:
        mock.stop()
        coll.close()
