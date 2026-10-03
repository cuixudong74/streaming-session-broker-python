"""Capacity contention, deterministic placement, queueing semantics."""
import pytest

from broker.broker import BrokerError, Queued
from broker.models import Placement, RejectReason


def test_exact_capacity_no_oversell(make_broker):
    b = make_broker()
    b.add_node("n1", 2)
    p1 = b.request_session("p1")
    p2 = b.request_session("p2")
    assert isinstance(p1, Placement) and isinstance(p2, Placement)
    # capacity exactly exhausted: next request must NOT be placed
    q = b.request_session("p3")
    assert isinstance(q, Queued)
    assert b.get_node("n1").used == 2
    b.check_invariants()


def test_queue_disabled_rejects_with_no_capacity(make_broker):
    b = make_broker(allow_queue=False)
    b.add_node("n1", 1)
    b.request_session("p1")
    with pytest.raises(BrokerError) as ei:
        b.request_session("p2")
    assert ei.value.reason is RejectReason.NO_CAPACITY


def test_placement_is_deterministic(make_broker):
    def run():
        b = make_broker()
        b.add_node("a", 2)
        b.add_node("b", 2)
        b.add_node("c", 1)
        return [b.request_session(f"p{i}").node_id for i in range(5)]

    first, second = run(), run()
    assert first == second  # same inputs + same capacity => same placement
    # most-free-first with id tie-break: a, b, a, b, c
    assert first == ["a", "b", "a", "b", "c"]


def test_fifo_queue_order_and_no_starvation(make_broker, clock):
    b = make_broker()
    b.add_node("n1", 1)
    placed = b.request_session("first")
    q1 = b.request_session("waiter-1")
    q2 = b.request_session("waiter-2")
    q3 = b.request_session("waiter-3")
    assert isinstance(q1, Queued) and isinstance(q2, Queued) and isinstance(q3, Queued)
    assert (q1.position, q2.position, q3.position) == (1, 2, 3)

    order = []
    b.release(placed.session_id)  # frees exactly one slot -> head only
    order.append([s.player_id for s in b._sessions.values()
                  if s.player_id.startswith("waiter")])
    assert order[0] == ["waiter-1"]  # strict FIFO: no jumping ahead

    # release again and again: waiters admitted in arrival order
    sid = [s for s in b._sessions.values()
           if s.player_id == "waiter-1"][0].session_id
    b.release(sid)
    sid = [s for s in b._sessions.values()
           if s.player_id == "waiter-2"][0].session_id
    b.release(sid)
    players = {s.player_id for s in b._sessions.values()
               if s.state.value == "active"}
    assert "waiter-3" in players
    assert len(b.queue_snapshot()) == 0
    b.check_invariants()


def test_queued_request_never_double_placed(make_broker):
    b = make_broker()
    b.add_node("n1", 1)
    b.add_node("n2", 1)
    p = b.request_session("p0")
    p_extra = b.request_session("p-extra")  # fill the second node
    q = b.request_session("p1")
    assert isinstance(q, Queued)
    b.release(p.session_id)  # dequeues p1 onto exactly one node
    # p1 must occupy exactly one slot across the whole cluster
    owners = [n.node_id for n in (b.get_node("n1"), b.get_node("n2"))
              for sid in n.sessions
              if b.get_session(sid).player_id == "p1"]
    assert len(owners) == 1
    b.check_invariants()
