"""Reconnect with credentials: same session back, or distinguishable rejection."""
import pytest

from broker.broker import BrokerError
from broker.models import RejectReason, SessionState


def test_reconnect_within_grace_returns_same_session(make_broker, clock):
    b = make_broker(lease_ttl=10, reconnect_grace=30)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    clock.advance(5)
    again = b.reconnect(p.reconnect_token)
    assert again.session_id == p.session_id
    assert again.node_id == p.node_id  # never re-placed
    # lease got renewed by the reconnect
    assert again.lease_expires_at == clock() + 10
    assert b.get_node("n1").used == 1  # still exactly one slot
    b.check_invariants()


def test_forged_token_rejected_without_side_effects(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    p = b.request_session("p1")
    forged = p.reconnect_token[:-2] + "xx"
    with pytest.raises(BrokerError) as ei:
        b.reconnect(forged)
    assert ei.value.reason is RejectReason.TOKEN_INVALID
    # existing session untouched
    assert b.get_session(p.session_id).state is SessionState.ACTIVE
    assert b.get_node("n1").used == 1
    b.check_invariants()


def test_expired_token_rejected_with_distinct_reason(make_broker, clock):
    b = make_broker(lease_ttl=10, reconnect_grace=30)
    b.add_node("n1", 1)
    p = b.request_session("p1")
    clock.advance(41)  # past lease (10) + grace (30)
    with pytest.raises(BrokerError) as ei:
        b.reconnect(p.reconnect_token)
    assert ei.value.reason is RejectReason.TOKEN_EXPIRED
    b.check_invariants()


def test_reconnect_to_ended_session_distinguishable(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    p = b.request_session("p1")
    b.release(p.session_id)
    with pytest.raises(BrokerError) as ei:
        b.reconnect(p.reconnect_token)
    assert ei.value.reason is RejectReason.SESSION_ENDED


def test_garbage_token_rejected(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    with pytest.raises(BrokerError) as ei:
        b.reconnect("not-a-token")
    assert ei.value.reason is RejectReason.TOKEN_INVALID
