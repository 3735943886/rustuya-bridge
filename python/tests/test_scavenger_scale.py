"""Opt-in scale test: many devices, then a mass `clear`, asserting the bridge
onboards the whole fleet and the scavenger leaves zero retained orphans.

Deselected by default (marker `scale`). Run explicitly:

    pytest -m scale
    SCALE_N=1000 pytest -m scale tests/test_scavenger_scale.py

The retain/scavenger path is the subtle, untested-end-to-end part; device-side
protocol correctness is already covered by rustuya's own mock tests, so every
device here uses one transport version.

A full **1000-device** mock fleet onboards cleanly (all N connected) and a
single mass `clear` scavenges every retained snapshot with zero orphans; this
is exercised in CI.

**Why multiprocessing:** tuyamock is single-threaded-per-device and Python, so
holding hundreds of mock devices in one process serializes their Tuya
handshakes on the GIL. Spreading the mocks over several processes (default
100/proc) gives each its own GIL — a test-harness throughput choice, not a
bridge limit.
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
# 1000-device mock fleet-scale is validated end to end (onboard + mass clear).
# Override with SCALE_N for a quicker local run or a larger observation.
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
    # Measure via the bridge's own `status` (paginated), NOT by counting
    # retained messages on the broker: retained delivery isn't instantaneous
    # under load, so a fresh subscriber undercounts a large fleet (it once
    # reported 0 of ~1000 actually-retained). `status` reads the bridge's own
    # state and is authoritative regardless of broker traffic.
    coll = Collector(f"{h.root}-col")
    coll.subscribe(f"{h.root}/response/#")

    t0 = time.time()
    try:
        # Register the whole fleet. The bridge must onboard all of them — if a
        # large add storm can't be sustained, that's the bridge's problem to
        # solve (throttle connection establishment), NOT something this test
        # relaxes around. So we demand a full N/N connected.
        for dev_id, ip in zip(ids, ips):
            coll.publish(h.command(), json.dumps(
                {"action": "add", "id": dev_id, "key": KEY, "ip": ip, "version": VER}))

        connected = 0

        def all_connected():
            nonlocal connected
            connected, total = _status_connected(coll, h.command())
            return total is not None and connected >= N

        ok = _wait(all_connected, timeout=max(180, N), interval=2.0)
        print(f"[scale] {connected}/{N} connected in {time.time()-t0:.1f}s")
        assert ok, f"only {connected}/{N} devices onboarded (status errorCode 0)"

        # ── the event under test: one mass clear must leave ZERO orphans ──
        tc = time.time()
        coll.publish(h.command(), json.dumps({"action": "clear"}))

        # Removal is authoritative via status; scavenging (broker retained
        # cleared) is an absence check, so a generous fresh-subscriber settle.
        assert _wait(lambda: _status_connected(coll, h.command())[1] == 0,
                     timeout=max(60, N // 4), interval=2.0), "clear did not drop all devices"

        def no_orphans():
            # Check EVERY retained topic under the root, not just event/state. A
            # device leaves more than its state snapshot retained — onboarding
            # also publishes a retained error/{id} (errorCode 0 = "connected").
            # An early-exiting scavenger can clear the state snapshots while
            # stranding the error topics, so scoping this to event/state would
            # pass on a half-done scavenge (it did, at large N, before the
            # loop-until-dry fix). The bridge's own {root}/bridge/config is
            # retained for the bridge's whole lifetime (not a device orphan), so
            # it's the one expected retained topic — exclude it.
            got = fresh_retained(f"{h.root}/#", settle=5.0)
            return not any("/bridge/" not in t for t, p, r in got if r)

        assert _wait(no_orphans, timeout=max(60, N // 4)), (
            "scavenger left retained orphans after mass clear")
        print(f"[scale] mass clear scavenged {N} in {time.time()-tc:.1f}s")
    finally:
        stop.set()
        for p in procs:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        coll.close()


def _status_connected(coll, cmd):
    """Page through the bridge's `status` and return (connected, registered).
    A device counts as connected once it carries errorCode 0 (status "0"); a
    freshly-registered-but-not-yet-connected device shows "online", and a
    failed one shows its error code (e.g. "901"). Returns (None, None) if the
    bridge didn't answer."""
    connected, total, offset = 0, None, 0
    while True:
        coll.drain()
        coll.publish(cmd, json.dumps({"action": "status", "offset": offset, "limit": 500}))
        item = coll.wait_for(
            lambda t, p, r: "/response" in t and '"device_count"' in p, timeout=10)
        if item is None:
            return None, None
        page = json.loads(item[1])
        total = page["device_count"]
        for dev in page.get("devices", {}).values():
            if dev.get("status") == "0":
                connected += 1
        if not page.get("has_more"):
            return connected, total
        offset = page["offset"] + page["returned"]


def _wait(predicate, timeout, interval=0.3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False
