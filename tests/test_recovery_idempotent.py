"""Crash-during-snapshot recovery: replay must be idempotent.

These tests simulate a crash in the middle of ``write_snapshot``: the new
snapshot has already been renamed into place, but the WAL has not yet been
truncated. Recovery then sees the snapshot *plus* WAL records that the
snapshot already covers (overlapping / duplicated history). Replay of those
records must be idempotent: a queued request must appear exactly once and
must never be placed twice.
"""

from broker.broker import Broker, BrokerConfig, Queued
from broker.models import Placement, SessionState
from broker.store import WalStore


def make(tmp_path, clock):
    cfg = BrokerConfig(lease_ttl=10, heartbeat_timeout=20, reconnect_grace=30)
    store = WalStore(tmp_path)
    return Broker(cfg, secret=b"test-secret", store=store, now=clock), store


def read_wal_lines(tmp_path):
    with open(tmp_path / "wal.log", encoding="utf-8") as f:
        return [line for line in f.read().splitlines() if line.strip()]


def write_wal_lines(tmp_path, lines):
    with open(tmp_path / "wal.log", "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def crash_mid_snapshot(tmp_path, pre_snapshot_wal_lines):
    """Simulate: snapshot.json renamed into place, crash before WAL truncate.

    ``b.snapshot()`` has already run (snapshot written, WAL truncated); we
    restore the pre-truncation WAL content, which is exactly what a crash in
    that window would leave on disk.
    """
    write_wal_lines(tmp_path, pre_snapshot_wal_lines)


def active_sessions(b, player_id):
    return [s for s in b._sessions.values()
            if s.player_id == player_id and s.state is SessionState.ACTIVE]


def test_overlapping_snapshot_and_wal_does_not_duplicate_queue(tmp_path, clock):
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    assert isinstance(p1, Placement) and isinstance(p2, Placement)
    q3 = b.request_session("p3")  # capacity full -> queued
    assert isinstance(q3, Queued)
    wal_before = read_wal_lines(tmp_path)  # full history, pre-truncation
    b.snapshot()
    store.close()
    # crash window: snapshot in place, WAL truncation never happened
    crash_mid_snapshot(tmp_path, wal_before)

    b2, store2 = make(tmp_path, clock)
    # the queued request must appear exactly once
    qs = b2.queue_snapshot()
    assert [q.request_id for q in qs] == [q3.request_id]
    assert b2.get_node("n1").used == 1
    assert b2.get_node("n2").used == 1
    b2.check_invariants()

    # capacity frees up: the request is placed exactly once
    b2.release(p1.session_id)
    placed = active_sessions(b2, "p3")
    assert len(placed) == 1
    assert b2.queue_snapshot() == []
    # freeing more capacity must NOT place the same request again elsewhere
    b2.release(p2.session_id)
    assert len(active_sessions(b2, "p3")) == 1
    assert b2.queue_snapshot() == []
    assert b2.get_node(placed[0].node_id).used == 1
    b2.check_invariants()
    store2.close()


def test_fully_doubled_wal_replays_as_if_once(tmp_path, clock):
    """No snapshot; every WAL record appears twice (e.g. duplicated segment)."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 2)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    assert isinstance(p1, Placement) and isinstance(p2, Placement)
    b.release(p1.session_id)
    p3 = b.request_session("p3")  # takes the freed slot
    assert isinstance(p3, Placement)
    store.close()
    lines = read_wal_lines(tmp_path)
    write_wal_lines(tmp_path, lines + lines)  # whole history duplicated

    b2, store2 = make(tmp_path, clock)
    # ended session stays ended, active ones hold exactly one slot each
    assert b2.get_session(p1.session_id).state is SessionState.ENDED
    assert b2.get_session(p2.session_id).state is SessionState.ACTIVE
    assert b2.get_session(p3.session_id).state is SessionState.ACTIVE
    assert b2.get_node("n1").used == 2  # not 3, not 4
    b2.check_invariants()
    store2.close()


def test_same_record_repeated_consecutively(tmp_path, clock):
    """The exact crash signature: one enqueue record replayed 3x in a row."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")
    assert isinstance(p1, Placement)
    q2 = b.request_session("p2")
    assert isinstance(q2, Queued)
    store.close()
    lines = read_wal_lines(tmp_path)
    repeated = []
    for line in lines:
        repeated.append(line)
        if '"request_enqueued"' in line:
            repeated.extend([line, line])  # same change, 3 times total
    write_wal_lines(tmp_path, repeated)

    b2, store2 = make(tmp_path, clock)
    qs = b2.queue_snapshot()
    assert [q.request_id for q in qs] == [q2.request_id]  # exactly once
    b2.check_invariants()
    # when capacity frees, the request is placed exactly once
    b2.release(p1.session_id)
    placed = active_sessions(b2, "p2")
    assert len(placed) == 1
    assert b2.queue_snapshot() == []
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()


def test_queue_order_preserved_under_overlap(tmp_path, clock):
    """Several waiters keep strict FIFO order despite overlapping records."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")
    assert isinstance(p1, Placement)
    q1 = b.request_session("w1")
    q2 = b.request_session("w2")
    q3 = b.request_session("w3")
    assert all(isinstance(q, Queued) for q in (q1, q2, q3))
    wal_before = read_wal_lines(tmp_path)
    b.snapshot()
    store.close()
    crash_mid_snapshot(tmp_path, wal_before)

    b2, store2 = make(tmp_path, clock)
    qs = b2.queue_snapshot()
    assert [q.request_id for q in qs] == [q1.request_id, q2.request_id,
                                          q3.request_id]
    b2.check_invariants()
    # free one slot at a time: strict FIFO, nobody jumped, nobody duplicated
    for expected in ("w1", "w2", "w3"):
        b2.release(b2.get_session(p1.session_id).session_id
                   if expected == "w1" else active_sessions(b2, prev)[0].session_id)
        placed = active_sessions(b2, expected)
        assert len(placed) == 1
        prev = expected
    assert b2.queue_snapshot() == []
    assert b2.get_node("n1").used == 1
    b2.check_invariants()
    store2.close()


def test_recovery_then_full_lifecycle_continues(tmp_path, clock):
    """After an overlapping-crash recovery the broker keeps working:
    request / release / renew / reconnect / drain all behave, and the
    pre-crash reconnect token stays valid."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    assert isinstance(p1, Placement) and isinstance(p2, Placement)
    q3 = b.request_session("p3")
    assert isinstance(q3, Queued)
    token = p1.reconnect_token
    wal_before = read_wal_lines(tmp_path)
    b.snapshot()
    store.close()
    crash_mid_snapshot(tmp_path, wal_before)

    b2, store2 = make(tmp_path, clock)
    b2.check_invariants()
    # pre-crash reconnect token still valid, same session / same node
    again = b2.reconnect(token)
    assert again.session_id == p1.session_id
    assert again.node_id == p1.node_id
    # renew works
    renewed = b2.renew_lease(p2.session_id)
    assert renewed.session_id == p2.session_id
    # release frees exactly one slot and dequeues the single waiter
    b2.release(p1.session_id)
    placed3 = active_sessions(b2, "p3")
    assert len(placed3) == 1
    assert b2.queue_snapshot() == []
    # both slots full again -> a new request queues exactly once
    q4 = b2.request_session("p4")
    assert isinstance(q4, Queued)
    assert [q.request_id for q in b2.queue_snapshot()] == [q4.request_id]
    # drain the node holding p3: the waiter must never land on it
    draining_node = placed3[0].node_id
    b2.start_drain(draining_node)
    assert not b2.drain_complete(draining_node)  # p3 still holds a slot
    # freeing the ACTIVE node places the waiter there, exactly once
    b2.release(p2.session_id)
    placed4 = active_sessions(b2, "p4")
    assert len(placed4) == 1
    assert placed4[0].node_id != draining_node
    assert b2.queue_snapshot() == []
    b2.check_invariants()
    # the draining node finishes once its last session is released
    b2.release(placed3[0].session_id)
    assert b2.drain_complete(draining_node)
    b2.check_invariants()
    # recovery again from the state written by the post-recovery traffic:
    # counts must stay self-consistent across a second restart
    b2.snapshot()
    store2.close()
    b3, store3 = make(tmp_path, clock)
    assert (b3.get_node("n1").used + b3.get_node("n2").used
            == len([s for s in b3._sessions.values()
                    if s.state is SessionState.ACTIVE]))
    b3.check_invariants()
    store3.close()


def test_overlap_with_ended_session_is_not_revived(tmp_path, clock):
    """A session ended before the snapshot must stay ended even though its
    whole history (admit .. end) is replayed on top of the snapshot."""
    b, store = make(tmp_path, clock)
    b.add_node("n1", 1)
    p1 = b.request_session("p1")
    assert isinstance(p1, Placement)
    b.release(p1.session_id)
    p2 = b.request_session("p2")
    assert isinstance(p2, Placement)
    wal_before = read_wal_lines(tmp_path)
    b.snapshot()
    store.close()
    crash_mid_snapshot(tmp_path, wal_before)

    b2, store2 = make(tmp_path, clock)
    assert b2.get_session(p1.session_id).state is SessionState.ENDED
    assert b2.get_session(p2.session_id).state is SessionState.ACTIVE
    assert b2.get_node("n1").used == 1  # ended session holds no slot
    b2.check_invariants()
    store2.close()
