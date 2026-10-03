"""Lease expiry, heartbeat loss, distinguishable release reasons."""
from broker.models import NodeState, ReleaseReason, SessionState


def test_lease_expiry_releases_session_with_reason(make_broker, clock):
    b = make_broker(lease_ttl=10)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    clock.advance(11)
    b.sweep()
    s = b.get_session(p.session_id)
    assert s.state is SessionState.ENDED
    assert s.end_reason is ReleaseReason.LEASE_EXPIRED
    assert b.get_node("n1").used == 0  # capacity matches actual occupancy
    b.check_invariants()


def test_lease_renewal_keeps_session_alive(make_broker, clock):
    b = make_broker(lease_ttl=10)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    clock.advance(8)
    b.renew_lease(p.session_id)
    clock.advance(8)  # 16 > 10 since start, but lease was renewed at t=8
    b.sweep()
    assert b.get_session(p.session_id).state is SessionState.ACTIVE
    b.check_invariants()


def test_heartbeat_loss_releases_sessions_with_distinct_reason(make_broker, clock):
    b = make_broker(lease_ttl=1000, heartbeat_timeout=20)
    b.add_node("n1", 2)
    b.add_node("n2", 1)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    p3 = b.request_session("p3")
    assert p3.node_id == "n2"
    # n1 stops heartbeating; n2 stays alive
    clock.advance(15)
    b.heartbeat("n2")
    clock.advance(10)  # n1 silent for 25 > timeout
    b.sweep()
    assert b.get_node("n1").state is NodeState.OFFLINE
    assert b.get_node("n2").state is NodeState.ACTIVE
    for pid in (p1, p2):
        s = b.get_session(pid.session_id)
        assert s.state is SessionState.ENDED
        assert s.end_reason is ReleaseReason.NODE_HEARTBEAT_LOST
    assert b.get_session(p3.session_id).state is SessionState.ACTIVE
    assert b.get_node("n1").used == 0
    b.check_invariants()


def test_explicit_offline_reason_is_distinguishable(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    p = b.request_session("p1")
    b.mark_offline("n1")
    assert b.get_session(p.session_id).end_reason is ReleaseReason.NODE_OFFLINE
    b.check_invariants()


def test_client_release_reason(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    p = b.request_session("p1")
    b.release(p.session_id)
    assert b.get_session(p.session_id).end_reason is ReleaseReason.CLIENT_RELEASE
    b.check_invariants()


def test_double_release_is_idempotent(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    p = b.request_session("p1")
    b.release(p.session_id)
    b.release(p.session_id)  # must not corrupt counters
    assert b.get_node("n1").used == 0
    b.check_invariants()
