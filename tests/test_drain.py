"""Node maintenance / draining semantics."""
from broker.broker import Queued
from broker.models import NodeState, ReleaseReason, SessionState


def test_draining_node_takes_no_new_sessions(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    b.start_drain("n1")
    p = b.request_session("p1")
    assert p.node_id == "n2"
    b.check_invariants()


def test_sessions_migrated_on_lease_expiry_during_drain(make_broker, clock):
    b = make_broker(lease_ttl=10)
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p = b.request_session("p1")
    assert p.node_id in ("n1", "n2")
    draining = p.node_id
    b.start_drain(draining)
    clock.advance(11)  # lease expires while draining
    b.sweep()
    s = b.get_session(p.session_id)
    assert s.state is SessionState.ACTIVE  # migrated, not ended
    assert s.node_id != draining
    assert b.get_node(draining).used == 0
    assert b.drain_complete(draining)
    b.check_invariants()


def test_session_ends_with_drain_reason_when_no_migration_target(make_broker, clock):
    b = make_broker(lease_ttl=10)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    b.start_drain("n1")
    clock.advance(11)
    b.sweep()
    s = b.get_session(p.session_id)
    assert s.state is SessionState.ENDED
    assert s.end_reason is ReleaseReason.NODE_DRAIN
    assert b.drain_complete("n1")
    b.check_invariants()


def test_inflight_queued_requests_not_lost_during_drain(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    b.request_session("p0")
    b.request_session("p0b")  # both nodes full
    q = b.request_session("p1")
    assert isinstance(q, Queued)
    b.start_drain("n1")
    b.start_drain("n2")
    # the queued request survives draining; when a slot frees it is placed
    # (on whichever node still exists -- here a new node joins the cluster)
    b.add_node("n3", 1)
    assert len(b.queue_snapshot()) == 0
    placed = [s for s in b._sessions.values() if s.player_id == "p1"]
    assert len(placed) == 1 and placed[0].node_id == "n3"
    b.check_invariants()


def test_drain_completion_condition(make_broker, clock):
    b = make_broker(lease_ttl=10)
    b.add_node("n1", 1)
    assert not b.drain_complete("n1")  # active node is never "drained"
    p = b.request_session("p1")
    b.start_drain("n1")
    assert not b.drain_complete("n1")  # still holds a session
    clock.advance(11)
    b.sweep()  # session ends (no migration target)
    assert b.drain_complete("n1")  # draining + zero sessions => complete
    b.check_invariants()
