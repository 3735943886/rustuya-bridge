"""`reconfigure` restarts the bridge in place, without dropping the fleet.

This is the 0.4 lifecycle change, and it has three parts that only a
broker-backed run can check together:

1. **The bridge does not exit.** 0.3 tripped its cancellation token and let the
   process die, relying on a supervisor to bring it back. Here the hosting
   thread must still be alive afterwards — and because the bridge also ships as
   a PyO3 extension, "the process survives" is not a nicety: an `exec`-based
   restart would take the interpreter with it.
2. **It really restarted.** A restart publishes a fresh `session_id` to the
   retained `bridge/config` sentinel, so a changed id is proof the context was
   rebuilt rather than the command being quietly ignored.
3. **The devices never reconnected.** `tuyamock` counts accepted connections;
   applying a config change must not increment it. Without this the first two
   would be satisfied by a restart that costs a large fleet a full connect
   storm.
"""
import json
import time

import tuyamock

from conftest import Collector, fresh_retained, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VER = "3.4"


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False


def _session_id(root, timeout=10.0):
    """The `session_id` on the retained bridge-config sentinel, or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for _, payload, retain in fresh_retained(f"{root}/bridge/config", settle=0.6):
            if not retain or not payload:
                continue
            try:
                sid = json.loads(payload).get("session_id")
            except json.JSONDecodeError:
                continue
            if sid:
                return sid
    return None


def test_reconfigure_restarts_in_place_keeping_the_device_connected(bridge, tmp_path):
    # `reconfigure` re-reads the config file, so give it one to read. It stays
    # empty: this test is about the restart mechanism, not about which setting
    # changed, and the fixture's kwargs are the CLI/env layer that outranks the
    # file anyway.
    config_path = tmp_path / "config.json"
    config_path.write_text("{}")
    h = bridge(config_path=str(config_path))

    dev_id = "ebrestart00000000001"
    ip = loopback_ips(1)[0]

    coll = Collector(f"{h.root}-restart-col")
    coll.subscribe(f"{h.root}/#")

    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"

        connections_before = mock.server.connections
        sid_before = _session_id(h.root)
        assert sid_before, "bridge never published a session_id sentinel"

        coll.publish(h.command(), json.dumps({"action": "reconfigure"}))

        # (2) A new cycle republishes the sentinel with a fresh session id.
        assert _wait(lambda: (_session_id(h.root, timeout=1.0) or sid_before) != sid_before,
                     timeout=30), "session_id never changed — the bridge did not restart"

        # (1) ...and the host thread is still running. In 0.3 this is where the
        # bridge had already exited.
        assert h.thread.is_alive(), "the bridge exited instead of restarting in place"

        # (3) The device's connection was carried over, not rebuilt.
        assert mock.connected, "the device is no longer connected after the restart"
        assert mock.server.connections == connections_before, (
            f"the device reconnected across the restart "
            f"({connections_before} -> {mock.server.connections}); the connection should "
            f"have been carried over"
        )

        # The restarted bridge is fully functional: a device push still reaches
        # the broker under the (unchanged) topic scheme.
        assert mock.push({"1": False}) is True
        state_topic = h.state_topic(dev_id)
        live = coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=15)
        assert live is not None, "no state published after the restart"
        assert json.loads(live[1])["1"] is False
    finally:
        mock.stop()
        coll.close()
