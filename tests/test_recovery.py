"""Restart recovery: sessions, leases, queue order restored; no re-placement."""
import json

from broker.broker import Broker, BrokerConfig, Queued
from broker.models import Placement, ReleaseReason, SessionState
from broker.store import WalStore


def make(tmp_path, clock):
    cfg = BrokerConfig(lease_ttl=10, heartbeat_timeout=20, reconnect_grace=30)
    store = WalStore(tmp_path)
    return Broker(cfg, secret=b"test-secret", store=store, now=clock), store


def test_recovery_restores_sessions_leases_and_queue(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 2)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    b.request_session("p3")  # fills n2
    q = b.request_session("p4")
    assert isinstance(q, Queued)
    b.release(p2.session_id)  # p4 dequeued onto freed slot
    q2 = b.request_session("p5")
    assert isinstance(q2, Queued)
    b.snapshot()
    store.close()

    b2, store2 = make(tmp_path, clock)
    # active sessions restored with their leases and nodes
    s1 = b2.get_session(p1.session_id)
    assert s1.state is SessionState.ACTIVE
    assert s1.node_id == p1.node_id
    assert s1.lease_expires_at == p1.lease_expires_at
    # released session NOT revived into a slot
    s2 = b2.get_session(p2.session_id)
    assert s2.state is SessionState.ENDED
    assert s2.end_reason is ReleaseReason.CLIENT_RELEASE
    # queue and its order restored
    qs = b2.queue_snapshot()
    assert [r.request_id for r in qs] == [q2.request_id]
    # capacity accounting consistent: no re-placement, no double count
    assert b2.get_node(p1.node_id).used >= 1
    b2.check_invariants()
    # reconnect with the pre-restart token still works after recovery
    again = b2.reconnect(p1.reconnect_token)
    assert again.session_id == p1.session_id
    store2.close()


def test_recovery_without_snapshot_replays_wal(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    store.close()  # no snapshot: pure WAL replay

    b2, store2 = make(tmp_path, clock)
    assert b2.get_session(p.session_id).state is SessionState.ACTIVE
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()


def test_torn_wal_tail_is_not_treated_as_complete(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 2)
    p = b.request_session("p1")
    store.close()
    # simulate a crash mid-write: half a record at the tail
    with open(tmp_path / "wal.log", "a") as f:
        f.write('{"crc": "bad", "record": {"op": "session_admitted", '
                '"session_id": "ghost", "player_id": "g", "node_id": "n1", ')
        f.write('\n')  # truncated JSON line

    b2, store2 = make(tmp_path, clock)
    # the half-written admission must not appear as a real session
    assert b2.get_session("ghost") is None
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()


def test_no_duplicate_placement_after_recovery(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    store.close()

    b2, store2 = make(tmp_path, clock)
    # node is still full after recovery: a new request must queue, not oversell
    r = b2.request_session("p2")
    assert isinstance(r, Queued)
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()


def test_duplicate_wal_record_after_snapshot_is_not_reapplied(tmp_path, clock):
    """Crash between snapshot rename and WAL truncation: the WAL may contain
    records already covered by the snapshot. Replay must not duplicate them."""
    import hashlib
    import json as _json

    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    b.request_session("p1")  # fills n1
    q = b.request_session("p2")  # queued
    assert isinstance(q, Queued)
    b.snapshot()
    store.close()

    # Simulate a post-snapshot crash that left a duplicate enqueue record
    # in the WAL (normally the snapshot truncates the WAL, but a crash can
    # leave records that are already covered by the snapshot).
    rec = {"op": "request_enqueued", "request_id": q.request_id,
           "player_id": "p2", "seq": 2, "at": 1000.0}
    payload = _json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()
    crc = hashlib.sha256(payload).hexdigest()
    with open(tmp_path / "wal.log", "a") as f:
        f.write(_json.dumps({"crc": crc, "record": rec}) + "\n")

    b2, store2 = make(tmp_path, clock)
    qs = b2.queue_snapshot()
    assert len(qs) == 1
    assert qs[0].request_id == q.request_id
    b2.check_invariants()
    store2.close()


def test_migrated_session_restored_on_target_node(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p = b.request_session("p1")
    draining = p.node_id
    b.start_drain(draining)
    clock.advance(11)
    b.sweep()  # migrates p1 to the other node
    s = b.get_session(p.session_id)
    assert s.state is SessionState.ACTIVE
    target = s.node_id
    assert target != draining
    b.snapshot()
    store.close()

    b2, store2 = make(tmp_path, clock)
    s2 = b2.get_session(p.session_id)
    assert s2.state is SessionState.ACTIVE
    assert s2.node_id == target  # restored on the migration target, no re-placement
    assert b2.get_node(target).used == 1
    assert b2.get_node(draining).used == 0
    b2.check_invariants()
    store2.close()
