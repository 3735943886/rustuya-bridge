"""Opt-in scale test: heartbeat survival + name-fan-out under a mass storm.

Deselected by default (marker `scale`). Run explicitly:

    pytest -m scale tests/test_name_fanout_scale.py
    NAMEFAN_N=500 MOCKS_PER_PROC=100 pytest -m scale tests/test_name_fanout_scale.py

Two things this exercises that the scavenger scale test does not:

1. **Heartbeat keeps a large fleet alive.** tuyamock closes a connection that
   goes ~30s without a packet (a real Tuya device does the same). After the add
   storm onboards every device, the test sits **quiet for 30s** — no commands —
   so the *only* thing keeping each connection open is the bridge heartbeating
   every device inside the idle window. The witness is **mock-side**: a worker
   watches each `MockDevice.connected` across the quiet window and reports any
   that dropped. Bridge `status` alone would lie here — a broken heartbeat drops
   the device but the bridge reconnects within seconds, so status reads "0"
   (connected) again and masks the failure. Only the mock sees the drop.

2. **The bridge fans a name-addressed command out to the whole fleet.** Every
   device shares ONE name, so a single `set` by name must hit all N. The
   per-id response redesign means each target answers on its own
   `{root}/response/{id}` topic; the test asserts all N distinct responses
   arrive within the response window. (A single-target success is suppressed,
   but a fan-out is not — each target reports.)

**Why multiprocessing:** tuyamock is single-threaded-per-device, so hundreds of
mocks in one process serialize their handshakes on the GIL. One process per
slice (default 100/proc) gives each its own GIL — a harness throughput choice,
not a bridge limit.
"""
import json
import multiprocessing as mp
import os
import time

import pytest

import tuyamock

from conftest import Collector, loopback_ips

KEY = "thisisarealkey00"
VER = "3.4"
NAME = "fleetlight"  # every device shares this name → set-by-name fans out to all

# Default 64. The fan-out set phase drives every mock to process a control AND
# push its state change at the same instant. Past ~100 co-located Python mocks
# on a small CI box (~4 cores) that simultaneous wake-up saturates CPU and the
# mocks reset their own connections — a harness limit, not the bridge (verified:
# 100% clean at N<=75, reset count is concurrency-independent and grows with
# more processes; a real fleet is independent hardware with no shared CPU). 64
# stays under that ceiling; raise NAMEFAN_N on bigger hardware to push harder.
N = int(os.environ.get("NAMEFAN_N", "64"))
MOCKS_PER_PROC = int(os.environ.get("MOCKS_PER_PROC", "100"))

# tuyamock's idle-drop is ~30s; sit quiet past it so the heartbeat is the only
# thing holding the fleet open.
QUIET_SECS = float(os.environ.get("NAMEFAN_QUIET_SECS", "30"))
# Response window: after the single set-by-name, all N per-id responses must
# land within this budget.
RESP_TIMEOUT = float(os.environ.get("NAMEFAN_RESP_TIMEOUT", "20"))

pytestmark = pytest.mark.scale


def _dev_id(i):
    return f"ebfanout{i:012d}"


def _mock_worker(ids, ips, base_idx, quiet_start, report, stop, results):
    """Child process: hold a slice of mocks. Once `quiet_start` fires, watch for
    any connection that drops before `report` fires, and put the count that
    stayed continuously connected through the quiet window on `results`."""
    mocks = []
    for j, (did, ip) in enumerate(zip(ids, ips)):
        m = tuyamock.MockDevice(local_key=KEY, version=VER, host=ip, port=6668,
                                gw_id=did, dps={"1": True, "2": base_idx + j})
        m.start()
        mocks.append(m)

    # Wait for the main process to declare the quiet window open (all onboarded).
    while not quiet_start.is_set():
        if stop.is_set():
            for m in mocks:
                m.stop()
            return
        time.sleep(0.2)

    # Snapshot who is connected at the start of the quiet window, then drop any
    # that lose their connection before the window closes. A live heartbeat
    # keeps every one of them; a broken one shows up as a removed index here
    # even though the bridge would reconnect it moments later.
    stayed = {j for j, m in enumerate(mocks) if m.connected}
    while not report.is_set() and not stop.is_set():
        for j in list(stayed):
            if not mocks[j].connected:
                stayed.discard(j)
        time.sleep(0.2)
    results.put(len(stayed))

    stop.wait()
    for m in mocks:
        m.stop()


def test_heartbeat_survives_then_name_fanout(bridge):
    ips = loopback_ips(N)
    ids = [_dev_id(i) for i in range(N)]

    # Fork the mock fleet BEFORE the bridge thread exists, so the child is clean
    # (no bridge/tokio threads inherited across fork).
    quiet_start = mp.Event()
    report = mp.Event()
    stop = mp.Event()
    results = mp.Queue()
    procs = []
    for k in range(0, N, MOCKS_PER_PROC):
        sl = slice(k, min(k + MOCKS_PER_PROC, N))
        p = mp.Process(target=_mock_worker,
                       args=(ids[sl], ips[sl], k, quiet_start, report, stop, results),
                       daemon=True)
        p.start()
        procs.append(p)
    print(f"\n[fanout] {N} mocks across {len(procs)} procs ({MOCKS_PER_PROC}/proc), name={NAME!r}")

    h = bridge()  # starts bridge, settles past seed
    coll = Collector(f"{h.root}-col")
    coll.subscribe(f"{h.root}/response/#")

    try:
        # ── 1. add storm: register all N under the SAME name ──
        t0 = time.time()
        for dev_id, ip in zip(ids, ips):
            coll.publish(h.command(), json.dumps({
                "action": "add", "id": dev_id, "key": KEY, "ip": ip,
                "version": VER, "name": NAME}))

        # Onboard: demand a full N/N connected (errorCode 0) before the quiet
        # window. This is the precondition, not the thing under measurement.
        connected = 0

        def all_connected():
            nonlocal connected
            connected, total = _status_connected(coll, h.command())
            return total is not None and connected >= N

        assert _wait(all_connected, timeout=max(120, N), interval=2.0), \
            f"only {connected}/{N} devices onboarded"
        print(f"[fanout] onboarded {connected}/{N} in {time.time()-t0:.1f}s")

        # ── 2. heartbeat survival: sit quiet past tuyamock's idle-drop ──
        quiet_start.set()
        print(f"[fanout] quiet window {QUIET_SECS:.0f}s (heartbeat-only)...")
        time.sleep(QUIET_SECS)
        report.set()
        stayed = sum(results.get(timeout=10) for _ in procs)
        assert stayed == N, (
            f"{N - stayed}/{N} devices dropped during the {QUIET_SECS:.0f}s quiet "
            f"window — heartbeat is not keeping the fleet alive")
        print(f"[fanout] {stayed}/{N} stayed connected through the quiet window")

        # ── 3. name storm: one set by name must fan out to all N ──
        coll.drain()
        ts = time.time()
        coll.publish(h.command(), json.dumps({
            "action": "set", "name": NAME, "dps": {"1": False}}))

        seen_ok = set()

        def all_responded():
            for topic, payload, _ in coll.drain():
                if "/response/" not in topic:
                    continue
                try:
                    msg = json.loads(payload)
                except ValueError:
                    continue
                if msg.get("action") == "set" and msg.get("status") == "ok" and msg.get("id"):
                    seen_ok.add(msg["id"])
            return len(seen_ok) >= N

        assert _wait(all_responded, timeout=RESP_TIMEOUT, interval=0.2), (
            f"only {len(seen_ok)}/{N} per-id set responses arrived within "
            f"{RESP_TIMEOUT:.0f}s of the name-addressed set")
        print(f"[fanout] {len(seen_ok)}/{N} per-id responses in {time.time()-ts:.1f}s")
    finally:
        stop.set()
        report.set()
        quiet_start.set()
        for p in procs:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        coll.close()


def _status_connected(coll, cmd):
    """Page through the bridge's `status` and return (connected, registered).
    A device counts as connected once its status is "0" (errorCode 0). Returns
    (None, None) if the bridge didn't answer."""
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
