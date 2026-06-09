"""Smoke test: full device lifecycle over a real broker.

add -> bridge connects to a mock -> device pushes state -> retained `state`
snapshot lands on the broker -> remove -> scavenger clears the retained
snapshot (no orphan). This is the plumbing the larger scale test builds on.
"""
import json
import time

import pytest

import tuyamock

from conftest import Collector, fresh_retained, loopback_ips

KEY = "thisisarealkey00"  # 16 bytes, must match the `add` key
VER = "3.4"


def _add(coll, h, dev_id, ip, name=None):
    cmd = {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}
    if name is not None:
        cmd["name"] = name
    coll.publish(h.command(), json.dumps(cmd))


def test_add_push_retain_remove_scavenge(bridge):
    h = bridge()  # default: {root}/event/{type}/{id}, retain=true, multi-DP
    dev_id = "ebsmoke0000000000001"
    ip = loopback_ips(1)[0]

    coll = Collector(f"{h.root}-col")
    coll.subscribe(f"{h.root}/#")

    mock = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                               gw_id=dev_id, dps={"1": True, "2": 50})
    mock.start()
    try:
        _add(coll, h, dev_id, ip, name="smoke_sock")

        # bridge connects to the mock
        resp = coll.wait_for(
            lambda t, p, r: t == f"{h.root}/response/{dev_id}" and '"ok"' in p,
            timeout=10,
        )
        assert resp is not None, "no ok response to add"
        assert _wait(lambda: mock.connected, 10), "bridge never connected to mock"

        # device-initiated state change -> retained snapshot
        time.sleep(0.3)
        assert mock.push({"1": False, "2": 75}) is True

        state_topic = h.state_topic(dev_id)
        live = coll.wait_for(lambda t, p, r: t == state_topic and p, timeout=12)
        assert live is not None, "no state snapshot published"
        assert json.loads(live[1]) == {"1": False, "2": 75}

        # positively confirm it is RETAINED on the broker (fresh subscriber)
        retained = fresh_retained(state_topic)
        assert any(r and json.loads(p) == {"1": False, "2": 75}
                   for _, p, r in retained), f"state not retained: {retained}"

        # remove -> scavenger must clear the retained snapshot
        coll.publish(h.command(), json.dumps({"action": "remove", "id": dev_id}))
        assert _wait(
            lambda: not any(p for _, p, _ in fresh_retained(state_topic, settle=0.6)),
            timeout=10,
        ), "retained state orphaned after remove (scavenger did not clear it)"
    finally:
        mock.stop()
        coll.close()


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.25)
    return False
