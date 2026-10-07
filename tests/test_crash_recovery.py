"""Crash-during-persistence recovery.

Reproduces the crash window inside ``WalStore.write_snapshot``: the new
snapshot has been renamed into place but the WAL has not been truncated
yet, so every record the snapshot covers is still in the WAL. Recovery
must be idempotent against such duplicated / overlapping records:
each waiting request is restored exactly once, no session is placed on
two nodes, and all counters stay self-consistent.
"""
import json

from broker.broker import Broker, BrokerConfig, Queued
from broker.models import Placement, SessionState
from broker.store import WalStore, _crc


def make(tmp_path, clock):
    cfg = BrokerConfig(lease_ttl=10, heartbeat_timeout=20, reconnect_grace=30)
    store = WalStore(tmp_path)
    return Broker(cfg, secret=b"test-secret", store=store, now=clock), store


def spy_on(store):
    """Capture every record the broker appends, in order."""
    captured = []
    orig = store.append

    def append(record):
        captured.append(record)
        orig(record)

    store.append = append
    return captured


def rewrite_wal(tmp_path, records):
    """Replace wal.log with exactly these records (valid crc lines)."""
    with open(tmp_path / "wal.log", "w", encoding="utf-8") as f:
        for rec in records:
            line = json.dumps({"crc": _crc(rec), "record": rec},
                              separators=(",", ":"))
            f.write(line + "\n")


def test_crash_between_snapshot_rename_and_wal_truncate(tmp_path, clock):
    """Snapshot already covers the enqueue, WAL still holds it too: the
    waiting request must be restored exactly once and placed exactly once."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")          # fills the only slot
    q = b.request_session("p2")           # queued
    assert isinstance(q, Queued)
    b.snapshot()                          # snapshot covers everything so far
    # emulate the crash window: the pre-snapshot records reappear in the
    # WAL *after* the snapshot (truncation never happened)
    store.append({"op": "add_node", "node_id": "n1", "capacity": 1,
                  "at": clock()})
    store.append({"op": "session_admitted", "session_id": p1.session_id,
                  "player_id": "p1", "node_id": "n1",
                  "lease_expires_at": p1.lease_expires_at, "seq": 1,
                  "created_at": clock()})
    store.append({"op": "request_enqueued", "request_id": q.request_id,
                  "player_id": "p2", "seq": 2, "at": clock()})
    store.close()

    b2, store2 = make(tmp_path, clock)
    # exactly one queue entry for the request, slot count unchanged
    assert [r.request_id for r in b2.queue_snapshot()] == [q.request_id]
    assert b2.get_node("n1").used == 1
    b2.check_invariants()

    # capacity frees: the request is placed exactly once, on exactly one node
    b2.release(p1.session_id)
    assert b2.queue_snapshot() == []
    assert b2.get_node("n1").used == 1  # p1 out, p2 in: still exactly 1
    b2.check_invariants()
    s_p2 = [s for s in b2._sessions.values() if s.player_id == "p2"]
    assert len(s_p2) == 1 and s_p2[0].state is SessionState.ACTIVE
    # freeing capacity again must not revive the already-placed request
    b2.release(s_p2[0].session_id)
    assert b2.get_node("n1").used == 0
    assert b2.queue_snapshot() == []
    b2.check_invariants()
    store2.close()


def test_same_record_replayed_many_times_is_noop(tmp_path, clock):
    """The identical change record appended repeatedly (crash rewriting the
    same region) must recover to the same state as a single replay."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")
    q = b.request_session("p2")           # queued: only slot is taken
    assert isinstance(q, Queued)
    # no snapshot: duplicate raw records straight in the WAL
    dup = [
        {"op": "session_admitted", "session_id": p1.session_id,
         "player_id": "p1", "node_id": "n1",
         "lease_expires_at": p1.lease_expires_at, "seq": 1,
         "created_at": clock()},
        {"op": "request_enqueued", "request_id": q.request_id,
         "player_id": "p2", "seq": 2, "at": clock()},
        {"op": "lease_renewed", "session_id": p1.session_id,
         "lease_expires_at": p1.lease_expires_at},
    ]
    for _ in range(3):
        for rec in dup:
            store.append(rec)
    store.close()

    b2, store2 = make(tmp_path, clock)
    assert b2.get_node("n1").used == 1  # not 4
    assert [r.request_id for r in b2.queue_snapshot()] == [q.request_id]
    s1 = b2.get_session(p1.session_id)
    assert s1.state is SessionState.ACTIVE
    assert s1.lease_expires_at == p1.lease_expires_at
    b2.check_invariants()
    store2.close()


def test_overlapping_records_across_snapshot_boundary(tmp_path, clock):
    """Faithful crash-window emulation: snapshot covers records 1..N, the
    untruncated WAL still holds records 1..M (M > N). Records 1..N overlap
    the snapshot and must be no-ops; N+1..M must apply exactly once. The
    recovered state must equal the live broker's state at the crash."""
    b, store = make(tmp_path, clock)
    captured = spy_on(store)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")          # n1
    p2 = b.request_session("p2")          # n2
    q3 = b.request_session("p3")          # queued
    q_still = b.request_session("p3b")    # queued, still waiting at crash
    assert isinstance(q3, Queued) and isinstance(q_still, Queued)
    b.snapshot()                          # snapshot covers records so far (N)
    # history continues after the snapshot (records N+1..M)
    b.release(p1.session_id)              # q3 dequeued onto the freed slot
    q4 = b.request_session("p4")          # queued
    assert isinstance(q4, Queued)
    # crash mid-snapshot: new snapshot in place, WAL never truncated and
    # still holds every record since the beginning -> rewrite it that way
    store.close()
    rewrite_wal(tmp_path, captured)

    b2, store2 = make(tmp_path, clock)
    # ended session stays ended, not revived by the overlapping admit
    assert b2.get_session(p1.session_id).state is SessionState.ENDED
    # the dequeued-then-placed request is not back in the queue; the
    # still-waiting requests appear exactly once each, in FIFO order
    assert [r.request_id for r in b2.queue_snapshot()] == [
        q_still.request_id, q4.request_id]
    # per-node usage matches the pre-crash broker exactly
    assert b2.get_node("n1").used == b.get_node("n1").used == 1
    assert b2.get_node("n2").used == b.get_node("n2").used == 1
    # the migrated-into-n1 session is the one admitted after the snapshot
    s_p3 = [s for s in b2._sessions.values() if s.player_id == "p3"]
    assert len(s_p3) == 1 and s_p3[0].state is SessionState.ACTIVE
    assert s_p3[0].node_id == "n1"
    b2.check_invariants()
    store2.close()


def test_recovery_then_full_lifecycle_continues(tmp_path, clock):
    """After a crash-window recovery the service keeps working: new
    requests, release, renew, reconnect (pre-restart token), sweep and
    drain all behave, and a second recovery reproduces the same state."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")          # n1
    token = p1.reconnect_token
    b.request_session("p2")               # n2
    q = b.request_session("p3")           # queued
    assert isinstance(q, Queued)
    b.snapshot()
    # crash window: duplicate the enqueue record after the snapshot
    store.append({"op": "request_enqueued", "request_id": q.request_id,
                  "player_id": "p3", "seq": 4, "at": clock()})
    store.close()

    b2, store2 = make(tmp_path, clock)
    # pre-restart reconnect token still valid: same session, no re-placement
    again = b2.reconnect(token)
    assert again.session_id == p1.session_id
    assert again.node_id == p1.node_id
    # renew works after recovery
    renewed = b2.renew_lease(p1.session_id)
    assert renewed.session_id == p1.session_id
    # queue holds the request exactly once; release admits exactly it
    assert [r.request_id for r in b2.queue_snapshot()] == [q.request_id]
    b2.release(p1.session_id)
    assert b2.queue_snapshot() == []
    placed = [s for s in b2._sessions.values()
              if s.player_id == "p3" and s.state is SessionState.ACTIVE]
    assert len(placed) == 1
    # new requests still placed deterministically after recovery
    b2.release(placed[0].session_id)      # free n1 again
    p4 = b2.request_session("p4")
    assert isinstance(p4, Placement)
    assert p4.node_id == "n1"             # only node with a free slot
    # drain + sweep still work
    b2.start_drain("n1")
    clock.advance(11)                     # past lease_ttl
    b2.sweep()
    b2.check_invariants()
    # a second restart reproduces the same state (stable recovery)
    b2.snapshot()
    store2.close()
    b3, store3 = make(tmp_path, clock)
    for node_id in ("n1", "n2"):
        assert b3.get_node(node_id).used == b2.get_node(node_id).used
    assert [r.request_id for r in b3.queue_snapshot()] == [
        r.request_id for r in b2.queue_snapshot()]
    b3.check_invariants()
    store3.close()


def test_queue_order_preserved_with_duplicate_enqueues(tmp_path, clock):
    """Duplicated enqueue records in the WAL must not reorder or duplicate
    the FIFO queue; nobody is starved, jumped, or placed twice."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")
    q1 = b.request_session("p2")
    q2 = b.request_session("p3")
    q3 = b.request_session("p4")
    # crash re-wrote a WAL region: earlier enqueue records appear again
    for qd in (q1, q2):
        req = next(r for r in b.queue_snapshot()
                   if r.request_id == qd.request_id)
        store.append({"op": "request_enqueued", "request_id": req.request_id,
                      "player_id": req.player_id, "seq": req.seq,
                      "at": clock()})
    store.close()

    b2, store2 = make(tmp_path, clock)
    assert [r.request_id for r in b2.queue_snapshot()] == [
        q1.request_id, q2.request_id, q3.request_id]
    b2.check_invariants()
    # strict FIFO admission after recovery: p2, then p3, then p4
    b2.release(p1.session_id)
    order = []
    while b2.queue_snapshot():
        active = [s for s in b2._sessions.values()
                  if s.state is SessionState.ACTIVE]
        latest = max(active, key=lambda s: s.seq)
        order.append(latest.player_id)
        b2.release(latest.session_id)
    assert order == ["p2", "p3"]
    # exactly one active session remains (p4), holding exactly one slot
    active = [s for s in b2._sessions.values()
              if s.state is SessionState.ACTIVE]
    assert len(active) == 1 and active[0].player_id == "p4"
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()
