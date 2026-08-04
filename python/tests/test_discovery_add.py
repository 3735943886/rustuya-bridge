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
