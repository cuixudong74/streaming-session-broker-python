"""Concurrent request/release/heartbeat/expiry/drain: invariants must hold."""
import threading

from broker.broker import Queued
from broker.models import Placement


def test_concurrent_mixed_operations(make_broker, clock):
    b = make_broker(lease_ttl=5, heartbeat_timeout=8)
    for i in range(4):
        b.add_node(f"n{i}", 3)

    errors = []
    placed: list[Placement] = []
    placed_lock = threading.Lock()

    def requester(player: int) -> None:
        try:
            for _ in range(30):
                r = b.request_session(f"p{player}")
                if isinstance(r, Placement):
                    with placed_lock:
                        placed.append(r)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def releaser() -> None:
        try:
            for _ in range(60):
                with placed_lock:
                    target = placed.pop() if placed else None
                if target is not None:
                    try:
                        b.release(target.session_id)
                    except Exception:
                        pass  # may already be expired by sweeper
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def heartbeater() -> None:
        try:
            for _ in range(40):
                for i in range(4):
                    b.heartbeat(f"n{i}")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def sweeper() -> None:
        try:
            for _ in range(40):
                clock.advance(0.1)
                b.sweep()
                b.check_invariants()  # invariants hold at *every* instant
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def drainer() -> None:
        try:
            b.start_drain("n3")
            for _ in range(10):
                b.drain_complete("n3")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = ([threading.Thread(target=requester, args=(i,)) for i in range(6)]
               + [threading.Thread(target=releaser) for _ in range(2)]
               + [threading.Thread(target=heartbeater),
                  threading.Thread(target=sweeper),
                  threading.Thread(target=drainer)])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    # after the dust settles: counters self-consistent, no oversell,
    # no ended session holding a slot, no double-counted slot
    b.check_invariants()
    total_used = sum(b.get_node(f"n{i}").used for i in range(4))
    active = sum(1 for s in b._sessions.values() if s.state.value == "active")
    assert total_used == active
    assert all(b.get_node(f"n{i}").used <= 3 for i in range(4))


def test_no_oversell_under_pure_contention(make_broker):
    b = make_broker()
    b.add_node("n1", 5)
    barrier = threading.Barrier(20)
    results = []
    lock = threading.Lock()

    def contend(i: int) -> None:
        barrier.wait()
        r = b.request_session(f"p{i}")
        with lock:
            results.append(r)

    threads = [threading.Thread(target=contend, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    placed = [r for r in results if isinstance(r, Placement)]
    queued = [r for r in results if isinstance(r, Queued)]
    assert len(placed) == 5          # exactly capacity, never more
    assert len(queued) == 15
    assert b.get_node("n1").used == 5
    b.check_invariants()
