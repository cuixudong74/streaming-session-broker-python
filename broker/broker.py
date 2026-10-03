"""Core scheduling broker: admission, queueing, leases, drain, recovery.

Design notes
------------
- Single writer: every public mutating method takes one reentrant lock, so
  concurrent request/release/heartbeat/expiry/drain operations are
  serialised. All counters (node used/free) are *derived* from canonical
  state (``Node.sessions`` / ``Session.node_id``), never incremented
  independently, so they cannot drift apart under concurrency.
- Determinism: placement picks the ACTIVE node with the most free slots,
  ties broken by node id. A monotonic ``seq`` orders requests and sessions.
  Same batch of inputs + same capacity state => same placement.
- Queue: strict FIFO by ``seq``. Only the head is tried when capacity
  frees up, so no request can be starved by later arrivals jumping ahead.
- Durability: every mutation is appended to the WAL *before* being
  applied. Recovery loads the snapshot then replays WAL records as facts
  (never re-running scheduling), so nothing is re-placed or lost.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from .credentials import TokenExpired, TokenInvalid, TokenIssuer
from .models import (
    Node,
    NodeState,
    Placement,
    QueuedRequest,
    ReleaseReason,
    RejectReason,
    Session,
    SessionState,
)
from .store import WalStore


class BrokerError(Exception):
    """Broker-level rejection; always carries a distinguishable reason."""

    def __init__(self, reason: RejectReason, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason


@dataclass(frozen=True)
class Queued:
    """Result when a request could not be placed immediately."""

    request_id: str
    position: int


@dataclass(frozen=True)
class BrokerConfig:
    lease_ttl: float = 30.0
    heartbeat_timeout: float = 45.0
    reconnect_grace: float = 60.0
    allow_queue: bool = True


class Broker:
    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        secret: bytes = b"broker-secret",
        store: WalStore | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = config or BrokerConfig()
        self._tokens = TokenIssuer(secret)
        self._store = store
        self._now = now
        self._lock = threading.RLock()
        self._nodes: dict[str, Node] = {}
        self._sessions: dict[str, Session] = {}
        self._queue: list[QueuedRequest] = []  # sorted by seq
        self._seq = 0
        if store is not None:
            self._recover()

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------
    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _persist(self, record: dict) -> None:
        if self._store is not None:
            self._store.append(record)

    def _pick_node(self) -> Optional[Node]:
        """Deterministic: most free slots, tie-break by node id."""
        candidates = [n for n in self._nodes.values()
                      if n.accepts_new() and n.free > 0]
        if not candidates:
            return None
        return min(candidates, key=lambda n: (-n.free, n.node_id))

    def _place(self, player_id: str, now: float) -> Placement:
        node = self._pick_node()
        assert node is not None, "_place called with no capacity"
        seq = self._next_seq()
        session_id = f"sess-{seq}-{uuid.uuid4().hex[:8]}"
        lease_expires = now + self._cfg.lease_ttl
        self._persist({"op": "session_admitted", "session_id": session_id,
                       "player_id": player_id, "node_id": node.node_id,
                       "lease_expires_at": lease_expires, "seq": seq,
                       "created_at": now})
        session = Session(session_id=session_id, player_id=player_id,
                          node_id=node.node_id, created_at=now,
                          lease_expires_at=lease_expires, seq=seq)
        self._sessions[session_id] = session
        node.sessions.add(session_id)
        return self._placement_for(session)

    def _placement_for(self, session: Session) -> Placement:
        token = self._tokens.issue(
            session.session_id,
            session.lease_expires_at + self._cfg.reconnect_grace)
        assert session.node_id is not None
        return Placement(session_id=session.session_id,
                         node_id=session.node_id,
                         lease_expires_at=session.lease_expires_at,
                         reconnect_token=token)

    def _end_session(self, session: Session, reason: ReleaseReason,
                     now: float) -> None:
        """Idempotent: ending an ended session is a no-op."""
        if session.state is SessionState.ENDED:
            return
        self._persist({"op": "session_ended", "session_id": session.session_id,
                       "reason": reason.value, "ended_at": now})
        self._apply_session_ended(session.session_id, reason, now)

    def _apply_session_ended(self, session_id: str, reason: ReleaseReason,
                             now: float) -> None:
        session = self._sessions.get(session_id)
        if session is None or session.state is SessionState.ENDED:
            return
        if session.node_id is not None:
            node = self._nodes.get(session.node_id)
            if node is not None:
                node.sessions.discard(session_id)
            session.node_id = None
        session.state = SessionState.ENDED
        session.ended_at = now
        session.end_reason = reason

    def _drain_queue(self, now: float) -> None:
        """Place queued requests in strict FIFO order while capacity lasts."""
        while self._queue:
            head = self._queue[0]
            node = self._pick_node()
            if node is None:
                break  # strict FIFO: nobody jumps the head
            self._persist({"op": "request_dequeued",
                           "request_id": head.request_id})
            self._queue.pop(0)
            self._place(head.player_id, now)

    # ------------------------------------------------------------------
    # node management
    # ------------------------------------------------------------------
    def add_node(self, node_id: str, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        with self._lock:
            if node_id in self._nodes:
                raise BrokerError(RejectReason.NO_CAPACITY,
                                  f"node {node_id} already exists")
            now = self._now()
            self._persist({"op": "add_node", "node_id": node_id,
                           "capacity": capacity, "at": now})
            self._nodes[node_id] = Node(node_id=node_id, capacity=capacity,
                                        last_heartbeat=now)
            self._drain_queue(now)

    def heartbeat(self, node_id: str) -> None:
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise BrokerError(RejectReason.SESSION_NOT_FOUND,
                                  f"unknown node {node_id}")
            now = self._now()
            self._persist({"op": "heartbeat", "node_id": node_id, "at": now})
            node.last_heartbeat = now

    def start_drain(self, node_id: str) -> None:
        """Put a node into maintenance: no new sessions; existing sessions
        are migrated on lease expiry or end normally. Queued (in-flight)
        requests are untouched — they were never bound to this node and
        remain in the queue for other nodes."""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                raise BrokerError(RejectReason.SESSION_NOT_FOUND,
                                  f"unknown node {node_id}")
            now = self._now()
            self._persist({"op": "node_draining", "node_id": node_id,
                           "at": now})
            node.state = NodeState.DRAINING

    def drain_complete(self, node_id: str) -> bool:
        """A node is fully drained iff it is not ACTIVE and holds no
        sessions. (In-flight queued requests are never assigned to a
        draining node, so they cannot block drain completion.)"""
        with self._lock:
            node = self._nodes.get(node_id)
            if node is None:
                return True
            return node.state is not NodeState.ACTIVE and not node.sessions

    def mark_offline(self, node_id: str) -> None:
        """Explicitly take a node offline; its sessions are released."""
        with self._lock:
            self._node_offline(node_id, ReleaseReason.NODE_OFFLINE)

    def _node_offline(self, node_id: str, reason: ReleaseReason) -> None:
        node = self._nodes.get(node_id)
        if node is None or node.state is NodeState.OFFLINE:
            return
        now = self._now()
        self._persist({"op": "node_offline", "node_id": node_id,
                       "reason": reason.value, "at": now})
        node.state = NodeState.OFFLINE
        for sid in list(node.sessions):
            session = self._sessions[sid]
            self._end_session(session, reason, now)

    # ------------------------------------------------------------------
    # session lifecycle
    # ------------------------------------------------------------------
    def request_session(self, player_id: str,
                         request_id: str | None = None) -> Placement | Queued:
        """Place immediately if capacity allows, else enqueue (strict FIFO)
        or reject with NO_CAPACITY when queueing is disabled."""
        with self._lock:
            now = self._now()
            node = self._pick_node()
            if node is not None:
                return self._place(player_id, now)
            if not self._cfg.allow_queue:
                raise BrokerError(RejectReason.NO_CAPACITY)
            seq = self._next_seq()
            rid = request_id or f"req-{seq}-{uuid.uuid4().hex[:8]}"
            self._persist({"op": "request_enqueued", "request_id": rid,
                           "player_id": player_id, "seq": seq, "at": now})
            self._queue.append(QueuedRequest(request_id=rid,
                                             player_id=player_id,
                                             enqueued_at=now, seq=seq))
            return Queued(request_id=rid, position=len(self._queue))

    def release(self, session_id: str) -> None:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise BrokerError(RejectReason.SESSION_NOT_FOUND, session_id)
            self._end_session(session, ReleaseReason.CLIENT_RELEASE,
                              self._now())
            self._drain_queue(self._now())

    def renew_lease(self, session_id: str) -> Placement:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise BrokerError(RejectReason.SESSION_NOT_FOUND, session_id)
            if session.state is SessionState.ENDED:
                raise BrokerError(RejectReason.SESSION_ENDED, session_id)
            now = self._now()
            session.lease_expires_at = now + self._cfg.lease_ttl
            self._persist({"op": "lease_renewed", "session_id": session_id,
                           "lease_expires_at": session.lease_expires_at})
            return self._placement_for(session)

    def reconnect(self, token: str) -> Placement:
        """Return the *same* session for a valid in-grace token; never
        re-places. Forged/expired tokens are rejected without side effects."""
        with self._lock:
            now = self._now()
            try:
                session_id, _ = self._tokens.verify(token, now)
            except TokenInvalid as exc:
                raise BrokerError(RejectReason.TOKEN_INVALID, str(exc))
            except TokenExpired as exc:
                raise BrokerError(RejectReason.TOKEN_EXPIRED, str(exc))
            session = self._sessions.get(session_id)
            if session is None:
                raise BrokerError(RejectReason.SESSION_NOT_FOUND, session_id)
            if session.state is SessionState.ENDED:
                raise BrokerError(RejectReason.SESSION_ENDED, session_id)
            # Same session, same node; renew the lease and re-issue token.
            session.lease_expires_at = now + self._cfg.lease_ttl
            self._persist({"op": "lease_renewed", "session_id": session_id,
                           "lease_expires_at": session.lease_expires_at})
            return self._placement_for(session)

    # ------------------------------------------------------------------
    # expiry / failure sweep
    # ------------------------------------------------------------------
    def sweep(self) -> None:
        """Expire leases, detect lost heartbeats, migrate drained sessions,
        then drain the queue. Safe to call concurrently with anything."""
        with self._lock:
            now = self._now()
            # 1. node heartbeat timeouts
            for node in list(self._nodes.values()):
                if (node.state is not NodeState.OFFLINE
                        and now - node.last_heartbeat
                        > self._cfg.heartbeat_timeout):
                    self._node_offline(node.node_id,
                                       ReleaseReason.NODE_HEARTBEAT_LOST)
            # 2. lease expiries (draining nodes migrate instead of ending)
            for session in list(self._sessions.values()):
                if (session.state is SessionState.ACTIVE
                        and now > session.lease_expires_at):
                    node = self._nodes.get(session.node_id or "")
                    if node is not None and node.state is NodeState.DRAINING:
                        self._migrate_or_end(session, now)
                    else:
                        self._end_session(session,
                                          ReleaseReason.LEASE_EXPIRED, now)
            # 3. admit waiters with freed capacity
            self._drain_queue(now)

    def _migrate_or_end(self, session: Session, now: float) -> None:
        """On a draining node, a session whose lease ended is migrated to
        another node if capacity exists, else ended with NODE_DRAIN."""
        old_node_id = session.node_id
        target = self._pick_node()
        if target is None:
            self._end_session(session, ReleaseReason.NODE_DRAIN, now)
            return
        self._persist({"op": "session_migrated",
                       "session_id": session.session_id,
                       "from_node": old_node_id, "to_node": target.node_id,
                       "lease_expires_at": now + self._cfg.lease_ttl})
        old_node = self._nodes.get(old_node_id or "")
        if old_node is not None:
            old_node.sessions.discard(session.session_id)
        target.sessions.add(session.session_id)
        session.node_id = target.node_id
        session.lease_expires_at = now + self._cfg.lease_ttl

    # ------------------------------------------------------------------
    # persistence: snapshot & recovery
    # ------------------------------------------------------------------
    def snapshot(self) -> None:
        """Write a compacted snapshot and truncate the WAL."""
        with self._lock:
            if self._store is None:
                return
            state = {
                "seq": self._seq,
                "nodes": [
                    {"node_id": n.node_id, "capacity": n.capacity,
                     "state": n.state.value,
                     "last_heartbeat": n.last_heartbeat,
                     "sessions": sorted(n.sessions)}
                    for n in self._nodes.values()
                ],
                "sessions": [
                    {"session_id": s.session_id, "player_id": s.player_id,
                     "node_id": s.node_id, "state": s.state.value,
                     "created_at": s.created_at,
                     "lease_expires_at": s.lease_expires_at,
                     "ended_at": s.ended_at,
                     "end_reason": (s.end_reason.value
                                    if s.end_reason else None),
                     "seq": s.seq}
                    for s in self._sessions.values()
                ],
                "queue": [
                    {"request_id": q.request_id, "player_id": q.player_id,
                     "enqueued_at": q.enqueued_at, "seq": q.seq}
                    for q in self._queue
                ],
            }
            self._store.write_snapshot(state)

    def _recover(self) -> None:
        assert self._store is not None
        snapshot, records = self._store.recover()
        if snapshot is not None:
            self._load_snapshot(snapshot)
        for rec in records:
            self._apply_record(rec)

    def _load_snapshot(self, state: dict) -> None:
        self._seq = state["seq"]
        for nd in state["nodes"]:
            self._nodes[nd["node_id"]] = Node(
                node_id=nd["node_id"], capacity=nd["capacity"],
                state=NodeState(nd["state"]),
                last_heartbeat=nd["last_heartbeat"],
                sessions=set(nd["sessions"]))
        for sd in state["sessions"]:
            self._sessions[sd["session_id"]] = Session(
                session_id=sd["session_id"], player_id=sd["player_id"],
                node_id=sd["node_id"], state=SessionState(sd["state"]),
                created_at=sd["created_at"],
                lease_expires_at=sd["lease_expires_at"],
                ended_at=sd["ended_at"],
                end_reason=(ReleaseReason(sd["end_reason"])
                            if sd["end_reason"] else None),
                seq=sd["seq"])
        for qd in state["queue"]:
            self._queue.append(QueuedRequest(
                request_id=qd["request_id"], player_id=qd["player_id"],
                enqueued_at=qd["enqueued_at"], seq=qd["seq"]))
        self._queue.sort(key=lambda q: q.seq)

    def _apply_record(self, rec: dict) -> None:
        """Replay a WAL record as a fact; never re-runs scheduling."""
        op = rec["op"]
        if op == "add_node":
            self._nodes[rec["node_id"]] = Node(
                node_id=rec["node_id"], capacity=rec["capacity"],
                last_heartbeat=rec["at"])
        elif op == "heartbeat":
            node = self._nodes.get(rec["node_id"])
            if node is not None:
                node.last_heartbeat = rec["at"]
        elif op == "node_draining":
            node = self._nodes.get(rec["node_id"])
            if node is not None:
                node.state = NodeState.DRAINING
        elif op == "node_offline":
            node = self._nodes.get(rec["node_id"])
            if node is not None:
                node.state = NodeState.OFFLINE
        elif op == "session_admitted":
            # Idempotent: a snapshot may already contain this session if a
            # crash landed between the snapshot rename and WAL truncation.
            if rec["session_id"] in self._sessions:
                return
            self._seq = max(self._seq, rec["seq"])
            session = Session(session_id=rec["session_id"],
                              player_id=rec["player_id"],
                              node_id=rec["node_id"],
                              created_at=rec["created_at"],
                              lease_expires_at=rec["lease_expires_at"],
                              seq=rec["seq"])
            self._sessions[session.session_id] = session
            node = self._nodes.get(rec["node_id"])
            if node is not None:
                node.sessions.add(session.session_id)
        elif op == "session_ended":
            self._apply_session_ended(rec["session_id"],
                                      ReleaseReason(rec["reason"]),
                                      rec["ended_at"])
        elif op == "session_migrated":
            session = self._sessions.get(rec["session_id"])
            if session is not None and session.state is SessionState.ACTIVE:
                old = self._nodes.get(rec["from_node"])
                if old is not None:
                    old.sessions.discard(session.session_id)
                new = self._nodes.get(rec["to_node"])
                if new is not None:
                    new.sessions.add(session.session_id)
                session.node_id = rec["to_node"]
                session.lease_expires_at = rec["lease_expires_at"]
        elif op == "lease_renewed":
            session = self._sessions.get(rec["session_id"])
            if session is not None:
                session.lease_expires_at = rec["lease_expires_at"]
        elif op == "request_enqueued":
            # Idempotent for the same snapshot/WAL crash edge.
            if any(q.request_id == rec["request_id"] for q in self._queue):
                return
            self._seq = max(self._seq, rec["seq"])
            self._queue.append(QueuedRequest(request_id=rec["request_id"],
                                             player_id=rec["player_id"],
                                             enqueued_at=rec["at"],
                                             seq=rec["seq"]))
            self._queue.sort(key=lambda q: q.seq)
        elif op == "request_dequeued":
            self._queue = [q for q in self._queue
                           if q.request_id != rec["request_id"]]
        else:  # unknown record: ignore, never crash recovery
            pass

    # ------------------------------------------------------------------
    # introspection & invariants
    # ------------------------------------------------------------------
    def get_session(self, session_id: str) -> Optional[Session]:
        with self._lock:
            return self._sessions.get(session_id)

    def get_node(self, node_id: str) -> Optional[Node]:
        with self._lock:
            return self._nodes.get(node_id)

    def queue_snapshot(self) -> list[QueuedRequest]:
        with self._lock:
            return list(self._queue)

    def check_invariants(self) -> None:
        """Raise AssertionError if any capacity/consistency invariant is
        violated. Safe to call at any time, including after concurrency."""
        with self._lock:
            for node in self._nodes.values():
                assert node.used <= node.capacity, (
                    f"node {node.node_id} oversold: "
                    f"{node.used}/{node.capacity}")
                for sid in node.sessions:
                    s = self._sessions[sid]
                    assert s.state is SessionState.ACTIVE, (
                        f"ended session {sid} still holds a slot")
                    assert s.node_id == node.node_id, (
                        f"session {sid} slot counted on wrong node")
            owners: dict[str, str] = {}
            for node in self._nodes.values():
                for sid in node.sessions:
                    assert sid not in owners, (
                        f"session {sid} double-counted on "
                        f"{owners[sid]} and {node.node_id}")
                    owners[sid] = node.node_id
            for s in self._sessions.values():
                if s.state is SessionState.ACTIVE:
                    assert s.node_id is not None
                    node = self._nodes[s.node_id]
                    assert s.session_id in node.sessions
                else:
                    assert s.node_id is None, (
                        f"ended session {s.session_id} still bound to node")
            seqs = [q.seq for q in self._queue]
            assert seqs == sorted(seqs), "queue order broken"
            assert len({q.request_id for q in self._queue}) == len(self._queue)
