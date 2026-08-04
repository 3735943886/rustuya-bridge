"""A `reconfigure` must actually *change something*, observably, on the wire.

`test_reconfigure_restart.py` proves the restart mechanism — the process
survives, the session id turns over, the fleet keeps its connections — but it
deliberately changes no setting, and the Rust tests assert on the reloaded
config *object*. Nothing checked the thing the feature is for: an operator
changes a setting and the running bridge starts behaving differently.

So this drives the real operator path end to end. `mqtt_event_topic` is the
hardest of the six settings to apply, which makes it the right one to pin:

* it relocates every device's event topic, so the old retained snapshots become
  orphans and have to be purged before the restart (`purge_all_retained`),
* it re-renders the seed wildcard the new cycle subscribes to,
* and it must do all of that while the device connections are handed over
  untouched — the change is to the bridge, not to the fleet.

A device is also registered *after* the restart, because the new cycle owns a
fresh listener and delta channel: a carried-over device proves the handover, a
newly-added one proves the new cycle is fully operational rather than merely
alive.
"""
import json
import time

import tuyamock

from conftest import Collector, fresh_retained, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VER = "3.4"
NEW_EVENT_TOPIC = "{root}/v2/{type}/{id}"


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


def test_set_config_apply_relocates_the_event_topic_without_dropping_the_fleet(bridge, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mqtt_event_topic": "{root}/event/{type}/{id}"}))
    # `mqtt_event_topic` is passed as `None` on purpose. The binding's kwargs are
    # its command line, and CLI/env outranks the config file — so a kwarg here
    # would pin the topic and make `set_config` a documented no-op. The binary
    # has no flag or env for these six settings at all: the file is their only
    # source, and that is the arrangement being tested.
    h = bridge(config_path=str(config_path), mqtt_event_topic=None)

    dev_a, dev_b = "ebapply00000000001a", "ebapply00000000001b"
    ip_a, ip_b = loopback_ips(2, start=120)

    coll = Collector(f"{h.root}-apply-col")
    coll.subscribe(f"{h.root}/#")

    mock_a = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip_a, port=6668,
                                 gw_id=dev_a, dps={"1": True})
    mock_b = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip_b, port=6668,
                                 gw_id=dev_b, dps={"1": True})
    mock_a.start()
    mock_b.start()
    try:
        old_topic = h.state_topic(dev_a)                  # {root}/event/state/{id}
        new_topic = f"{h.root}/v2/state/{dev_a}"          # the relocated scheme

        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_a, "key": KEY, "ip": ip_a, "version": VER}))
        assert _wait(lambda: mock_a.connected, 15), "bridge never connected to the mock"

        # Produce a retained snapshot under the *old* scheme, so the purge has
        # something to orphan.
        assert _wait(lambda: mock_a.push({"1": False}) is True, 15)
        assert coll.wait_for(lambda t, p, r: t == old_topic and p, timeout=15), \
            "no state under the original topic scheme"
        assert any(r and p for _, p, r in fresh_retained(old_topic)), \
            "the original snapshot was not retained"

        sid_before = _session_id(h.root)
        assert sid_before, "bridge never published a session_id sentinel"
        conns_before = (mock_a.server.connections, mock_b.server.connections)

        # The operator's actual move: patch the setting and apply it in one go.
        coll.publish(h.command(), json.dumps(
            {"action": "set_config", "mqtt_event_topic": NEW_EVENT_TOPIC, "apply": True}))

        assert _wait(lambda: (_session_id(h.root, timeout=1.0) or sid_before) != sid_before,
                     timeout=40), "session_id never changed — the bridge did not restart"
        assert h.thread.is_alive(), "the bridge exited instead of restarting in place"

        # The setting is on disk, which is what a restart re-reads.
        on_disk = json.loads(config_path.read_text())
        assert on_disk.get("mqtt_event_topic") == NEW_EVENT_TOPIC

        # (1) The fleet was handed over, not rebuilt.
        assert mock_a.connected, "the device dropped across the restart"
        assert mock_a.server.connections == conns_before[0], (
            f"the device reconnected ({conns_before[0]} -> {mock_a.server.connections})"
        )

        # (2) The change is live: a push now publishes under the new scheme...
        assert _wait(lambda: mock_a.push({"1": True}) is True, 15)
        live = coll.wait_for(lambda t, p, r: t == new_topic and p, timeout=20)
        assert live is not None, f"no state on the relocated topic {new_topic}"
        assert json.loads(live[1])["1"] is True

        # ...and nothing arrives on the old one any more.
        stale = coll.wait_for(lambda t, p, r: t == old_topic and p, timeout=3)
        assert stale is None, f"still publishing to the old topic {old_topic}"

        # (3) The orphaned retained snapshot was purged, not left to mislead a
        # subscriber that will never hear from that topic again.
        assert not [p for _, p, r in fresh_retained(old_topic) if r and p], (
            "the old-scheme retained snapshot survived the topic change"
        )

        # (4) The restarted cycle is fully operational, not just alive: a device
        # registered now goes through the new cycle's listener and publishes
        # under the new scheme.
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_b, "key": KEY, "ip": ip_b, "version": VER}))
        assert _wait(lambda: mock_b.connected, 20), "no connection for a post-restart device"
        assert _wait(lambda: mock_b.push({"1": False}) is True, 15)
        fresh = coll.wait_for(
            lambda t, p, r: t == f"{h.root}/v2/state/{dev_b}" and p, timeout=20)
        assert fresh is not None, "a device added after the restart never published"
        assert json.loads(fresh[1])["1"] is False
    finally:
        mock_a.stop()
        mock_b.stop()
        coll.close()


def test_set_config_apply_flips_retain_and_clears_the_retained_state(bridge, tmp_path):
    """Turning `mqtt_retain` off is the other change a restart is claimed to
    apply, and it is a *mode* flip rather than a value edit: it decides whether
    snapshots are retained at all and whether the next cycle runs a seed phase.

    Retained state is the one setting whose old value keeps affecting
    subscribers after the change — a snapshot left on the broker outlives the
    config that produced it — so the purge is as much a part of "applied" as the
    new publishes are.
    """
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mqtt_retain": True}))
    # As above: a kwarg would outrank the file and pin the setting.
    h = bridge(config_path=str(config_path), mqtt_retain=None)

    dev_id = "ebretain0000000001"
    ip = loopback_ips(1, start=130)[0]

    coll = Collector(f"{h.root}-retain-col")
    coll.subscribe(f"{h.root}/#")

    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True})
    mock.start()
    try:
        state_topic = h.state_topic(dev_id)
        coll.publish(h.command(), json.dumps(
            {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))
        assert _wait(lambda: mock.connected, 15), "bridge never connected to the mock"

        assert _wait(lambda: mock.push({"1": False}) is True, 15)
        assert coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=15)
        assert any(r and p for _, p, r in fresh_retained(state_topic)), \
            "state was not retained while mqtt_retain was on"

        sid_before = _session_id(h.root)
        assert sid_before, "bridge never published a session_id sentinel"
        conns_before = mock.server.connections

        coll.publish(h.command(), json.dumps(
            {"action": "set_config", "mqtt_retain": False, "apply": True}))
        assert _wait(lambda: (_session_id(h.root, timeout=1.0) or sid_before) != sid_before,
                     timeout=40), "session_id never changed — the bridge did not restart"
        assert h.thread.is_alive(), "the bridge exited instead of restarting in place"
        assert mock.connected and mock.server.connections == conns_before, (
            "a retain flip must not cost the fleet its connections"
        )

        # The old retained snapshot is gone: it was published under a mode that
        # no longer applies, and nothing will ever overwrite it.
        assert not [p for _, p, r in fresh_retained(state_topic) if r and p], (
            "the retained snapshot survived a flip to mqtt_retain=false"
        )

        # `mqtt_retain` is a *mode*, not a flag on the same output: the merged
        # DPS cache exists only while it is on (`cache = effective_retain.then(…)`),
        # so turning it off retires the snapshot topic altogether and leaves the
        # raw delta as the device's only publication. Both halves are asserted,
        # because "no snapshot" is only correct if the delta still arrives.
        active_topic = f"{h.root}/event/active/{dev_id}"
        assert _wait(lambda: mock.push({"1": True}) is True, 15)
        delta = coll.wait_for(lambda t, p, r: t == active_topic and p, timeout=20)
        assert delta is not None, "no delta published after the retain flip"
        assert json.loads(delta[1])["1"] is True
        assert not delta[2], "the delta was published retained despite the flip"

        snapshot = coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=3)
        assert snapshot is None, (
            "a snapshot was published with mqtt_retain off — the cache should not "
            "exist in this mode"
        )
        assert not [p for _, p, r in fresh_retained(state_topic) if r and p], (
            "state is retained on the broker again after the flip"
        )
    finally:
        mock.stop()
        coll.close()
