"""A device registered with neither `ip` nor `version` must still work.

That is the whole point of registering by id alone: the operator knows the id
and the key, and discovery is expected to supply the rest. It is also the case
that goes wrong most quietly, because an unresolved version does not look like
an error — the bridge reports the device connected and then either publishes
payloads it could not decrypt (a v3.3 device, whose headered pushes stop being
recognised) or cycles connect/offline forever (a v3.4 device, which has no
handshake at v3.3 so TCP alone reads as connected, and hangs up on the first
frame that skipped its session).

Both versions are covered, and the assertion is on decoded state reaching MQTT
rather than on a connection existing, since a connection is exactly what both
failure modes still produce.
"""
import json
import time

import pytest
import tuyamock

from conftest import Collector, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
# Long enough for tuyamock's 8s beacon cadence to land an announcement; the scan
# result is awaited, so a faster beacon ends the wait sooner.
SCAN_WINDOW_SECS = 10


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


@pytest.mark.parametrize("version,octet", [("3.3", 95), ("3.4", 96)])
def test_add_without_ip_or_version_resolves_and_works(bridge, version, octet):
    h = bridge(scan_window_secs=SCAN_WINDOW_SECS)

    dev_id = f"ebnover{version.replace('.', '')}0000001"
    ip = loopback_ips(1, start=octet)[0]

    coll = Collector(f"{h.root}-nover-col")
    coll.subscribe(f"{h.root}/#")

    mock = tuyamock.MockDevice(local_key=KEY, version=version, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True},
                               discovery=True, discovery_addr=ip)
    mock.start()
    try:
        # Warm the discovery cache and prove it warm, so the `add` below is the
        # realistic case: a device already announced before it was registered.
        coll.publish(h.command(), json.dumps({"action": "scan"}))
        seen = coll.wait_for(lambda t, p, r: t.endswith("/scanner") and dev_id in p,
                             timeout=SCAN_WINDOW_SECS + 15)
        assert seen is not None, "discovery never announced the device"

        # Neither `ip` nor `version`: everything but identity comes from discovery.
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY}))
        assert _wait(lambda: mock.connected, 30), "the bridge never connected"

        # A push must arrive as *decoded* state. An unresolved version still
        # connects, so only the decoded payload distinguishes success here.
        state_topic = h.state_topic(dev_id)
        assert mock.push({"1": False}) is True
        live = coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=20)
        assert live is not None, (
            "no state reached MQTT — the frame arrived but did not decrypt to "
            "text (the version was never resolved)"
        )
        assert json.loads(live[1])["1"] is False

        # And the link must be stable, not a connect/hang-up cycle. tuyamock
        # counts accepted connections; a wrong dialect shows up here as a rising
        # count long before anything else reports a problem.
        conns = mock.server.connections
        time.sleep(15)  # past the driver's 10s keepalive, the frame that trips it
        assert mock.server.connections == conns, (
            f"the device reconnected during an idle window "
            f"({conns} -> {mock.server.connections}); the link is flapping"
        )
        assert mock.connected, "the device is no longer connected"
    finally:
        mock.stop()
        coll.close()
