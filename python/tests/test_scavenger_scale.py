"""Opt-in scale test: many devices, then a mass `clear`, asserting the
scavenger leaves zero retained orphans on the broker.

Deselected by default (marker `scale`). Run explicitly:

    pytest -m scale
    SCALE_N=1000 pytest -m scale tests/test_scavenger_scale.py

The retain/scavenger path is the subtle, untested-end-to-end part; device-side
protocol correctness is already covered by rustuya's own mock tests, so every
device here uses one transport version.

**Why multiprocessing:** tuyamock is single-threaded-per-device and Python, so
N mock threads in one process serialize their Tuya handshakes on the GIL — past
a few hundred, the bridge's simultaneous connect storm outruns the mocks and
connections fail with errorCode 901. Spreading the mocks over several processes
(default 100/proc) gives each its own GIL, matching how rustuya's own large-fleet
test scales. This is purely a *test harness* limit, not a bridge limit.
"""
import json
import multiprocessing as mp
import os
import time

import pytest

import tuyamock

from conftest import Collector, fresh_retained, loopback_ips

KEY = "thisisarealkey00"
VER = "3.4"
N = int(os.environ.get("SCALE_N", "1000"))
MOCKS_PER_PROC = int(os.environ.get("MOCKS_PER_PROC", "100"))

pytestmark = pytest.mark.scale


def _dev_id(i):
    return f"ebscale{i:014d}"


def _mock_worker(ids, ips, base_idx, stop_evt):
    """Child process: hold a slice of mock devices, and push one state change
    on each as soon as the bridge connects to it."""
    mocks = []
    for j, (did, ip) in enumerate(zip(ids, ips)):
        # Keep tuyamock's realistic ~30s idle drop: real Tuya devices close a
        # connection that goes ~30s without a packet, so the controller MUST
        # heartbeat every device inside that window. Disabling it would hide a
        # genuine fleet-scale failure (heartbeat cadence vs. idle drop).
        m = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                                gw_id=did, dps={"1": True, "2": base_idx + j})
        m.start()
        mocks.append(m)
    pushed = [False] * len(mocks)
    while not stop_evt.is_set():
        done = True
        for j, m in enumerate(mocks):
            if not pushed[j]:
                if m.connected:
                    m.push({"1": False, "2": 1000 + base_idx + j})
                    pushed[j] = True
                else:
                    done = False
        if done:
            break
        time.sleep(0.3)
    stop_evt.wait()
    for m in mocks:
        m.stop()


def test_mass_clear_leaves_no_orphans(bridge):
    ips = loopback_ips(N)
    ids = [_dev_id(i) for i in range(N)]

    # Spawn the mock fleet across processes BEFORE starting the bridge thread,
    # so the fork is clean (no bridge/tokio threads in the child).
    stop = mp.Event()
    procs = []
    for k in range(0, N, MOCKS_PER_PROC):
        sl = slice(k, min(k + MOCKS_PER_PROC, N))
        p = mp.Process(target=_mock_worker, args=(ids[sl], ips[sl], k, stop), daemon=True)
        p.start()
        procs.append(p)
    print(f"\n[scale] {N} mocks across {len(procs)} procs ({MOCKS_PER_PROC}/proc)")

    h = bridge()  # starts bridge, settles past seed
    coll = Collector(f"{h.root}-col")
    coll.subscribe(f"{h.root}/event/state/#")

    t0 = time.time()
    try:
        # Register the whole fleet. The bridge must onboard all of them — if a
        # 1000-wide add storm can't be sustained, that's the bridge's problem to
        # solve (e.g. throttle connection establishment), NOT something this test
        # relaxes around. So we demand a full 1000/1000.
        for dev_id, ip in zip(ids, ips):
            coll.publish(h.command(), json.dumps(
                {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))

        def retained_count():
            got = fresh_retained(f"{h.root}/event/state/#", settle=1.0)
            return len({t.rsplit("/", 1)[1] for t, p, r in got if r and p})

        last = 0

        def all_retained():
            nonlocal last
            last = retained_count()
            return last >= N

        ok = _wait(all_retained, timeout=max(120, N // 2))
        print(f"[scale] {last}/{N} retained in {time.time()-t0:.1f}s")
        assert ok, f"only {last}/{N} retained — bridge failed to onboard the fleet"

        # ── the event under test: one mass clear must leave ZERO orphans ──
        tc = time.time()
        coll.publish(h.command(), json.dumps({"action": "clear"}))

        def no_orphans():
            got = fresh_retained(f"{h.root}/event/state/#", settle=1.0)
            return not any(p for t, p, r in got if r)

        assert _wait(no_orphans, timeout=max(60, N // 4)), (
            "scavenger left retained orphans after mass clear")
        print(f"[scale] mass clear scavenged {N} retained in {time.time()-tc:.1f}s")
    finally:
        stop.set()
        for p in procs:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        coll.close()


def _wait(predicate, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.3)
    return False
