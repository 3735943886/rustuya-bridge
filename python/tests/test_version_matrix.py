"""Every protocol version, with `ip` and `version` each present or absent.

`add` takes four fields and two of them are optional, so a device can be
registered four ways per protocol version. Only the fully-specified corner was
ever tested, and both field bugs found in the field lived in the other three:
an addressless device that never connected, and an unresolved `Version::Auto`
that either garbled a v3.3 device's pushes or flapped a v3.4 one every ten
seconds. Optional fields exist to be left out, so each one gets left out here.

Each case asserts three things, because the failure modes are not all visible
in the same place:

* **decoding** — a device push arrives as readable state. A wrong dialect still
  connects, so this is what catches it.
* **control** — a `set` reaches the device and changes it. Decoding could work
  while the outbound envelope is wrong.
* **stability** — no reconnect across an idle window longer than the driver's
  10s keepalive. This is where a device that dislikes what we send hangs up,
  and it is invisible in any single-shot check.

The two rows matching the reported configuration (no ip, no version) run in the
default suite; the rest are `matrix`-marked, since 20 cases × an idle window is
minutes, not seconds.
"""
import json
import time

import pytest
import tuyamock

from conftest import Collector, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VERSIONS = ["3.1", "3.2", "3.3", "3.4", "3.5"]
# tuyamock beacons every 8s; this bounds the wait for the first announcement
# rather than causing one — the scan result is awaited, so a beacon that lands
# sooner ends the wait sooner.
SCAN_WINDOW_SECS = 10
# Must exceed the driver's 10s heartbeat: the keepalive is the frame a device
# rejects when the dialect is wrong, so an idle window shorter than one proves
# nothing about stability.
IDLE_WATCH_SECS = 14


def _cases():
    """One case per (version, ip?, version?), with the reported configuration —
    neither field given — left in the default suite."""
    octet = 100
    for version in VERSIONS:
        for with_ip in (False, True):
            for with_version in (False, True):
                octet += 1
                default = not with_ip and not with_version and version in ("3.3", "3.4")
                yield pytest.param(
                    version,
                    with_ip,
                    with_version,
                    octet,
                    id=f"{version}-{'ip' if with_ip else 'noip'}"
                    f"-{'ver' if with_version else 'nover'}",
                    marks=() if default else pytest.mark.matrix,
                )


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


@pytest.mark.parametrize("version,with_ip,with_version,octet", list(_cases()))
def test_add_field_combination(bridge, version, with_ip, with_version, octet):
    h = bridge(scan_window_secs=SCAN_WINDOW_SECS)

    dev_id = f"ebmx{version.replace('.', '')}{octet:03d}0000000"
    ip = loopback_ips(1, start=octet)[0]

    coll = Collector(f"{h.root}-mx-col")
    coll.subscribe(f"{h.root}/#")

    mock = tuyamock.MockDevice(local_key=KEY, version=version, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True}, discovery=True,
                               discovery_addr=ip)
    mock.start()
    try:
        if not with_ip or not with_version:
            # Whatever `add` omits has to come from discovery, so wait until the
            # bridge has actually heard the device before registering it. This
            # is also the realistic ordering: a long-running bridge has been
            # listening since startup.
            coll.publish(h.command(), json.dumps({"action": "scan"}))
            seen = coll.wait_for(lambda t, p, r: t.endswith("/scanner") and dev_id in p,
                                 timeout=SCAN_WINDOW_SECS + 15)
            assert seen is not None, "discovery never announced the device"

        req = {"action": "add", "id": dev_id, "key": KEY}
        if with_ip:
            req["ip"] = ip
        if with_version:
            req["version"] = version
        coll.publish(h.command(), json.dumps(req))
        assert _wait(lambda: mock.connected, 40), f"never connected ({req.keys()})"
        # v3.4/v3.5 accept the TCP connection before their session handshake, so
        # settle past it rather than racing a push against a session that does
        # not exist yet.
        assert _wait(lambda: mock.push({"1": False}) is True, 20), "no session to push over"

        # (1) The device's own push must arrive decoded.
        state_topic = h.state_topic(dev_id)
        live = coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=20)
        assert live is not None, (
            "no state reached MQTT — the frame arrived but did not decrypt to text"
        )
        assert json.loads(live[1])["1"] is False

        # (2) Control must reach the device, not just monitoring.
        coll.publish(h.command(), json.dumps(
            {"action": "set", "id": dev_id, "dps": {"1": True}}))
        assert _wait(lambda: mock.dps.get("1") is True, 20), (
            f"`set` never applied (device dps stayed {dict(mock.dps)})"
        )

        # (3) And the link must survive its own keepalive.
        conns = mock.server.connections
        time.sleep(IDLE_WATCH_SECS)
        assert mock.server.connections == conns, (
            f"reconnected during an idle window ({conns} -> {mock.server.connections}); "
            f"the device is hanging up on something we send"
        )
        assert mock.connected, "the device is no longer connected"
    finally:
        mock.stop()
        coll.close()
