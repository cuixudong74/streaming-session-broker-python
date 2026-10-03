"""Core data types for the streaming session broker.

This module only contains type definitions and pure helpers; it has no
scheduling logic so it can be imported by every other module safely.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Optional


class NodeState(str, enum.Enum):
    """Lifecycle state of a streaming node."""

    ACTIVE = "active"          # accepts new sessions
    DRAINING = "draining"      # maintenance: no new sessions, existing ones finish
    OFFLINE = "offline"        # heartbeat lost; sessions force-released


class SessionState(str, enum.Enum):
    """Lifecycle state of a session."""

    ACTIVE = "active"          # placed on a node, holding a lease
    ENDED = "ended"            # released; keeps record for audit/reconnect window


class ReleaseReason(str, enum.Enum):
    """Why a session slot was released. Must be distinguishable."""

    CLIENT_RELEASE = "client_release"        # client explicitly released
    LEASE_EXPIRED = "lease_expired"          # session lease not renewed in time
    NODE_HEARTBEAT_LOST = "node_heartbeat_lost"  # node missed heartbeats
    NODE_DRAIN = "node_drain"                # node draining ended the session
    NODE_OFFLINE = "node_offline"            # node explicitly marked offline


class RejectReason(str, enum.Enum):
    """Why a request (placement or reconnect) was rejected."""

    NO_CAPACITY = "no_capacity"              # not enough capacity and queue disabled
    TOKEN_INVALID = "token_invalid"          # forged / malformed reconnect token
    TOKEN_EXPIRED = "token_expired"          # reconnect grace window passed
    SESSION_NOT_FOUND = "session_not_found"  # unknown session id
    SESSION_ENDED = "session_ended"          # session already ended, cannot reconnect


@dataclass
class Node:
    """A streaming node with finite concurrent slots."""

    node_id: str
    capacity: int
    state: NodeState = NodeState.ACTIVE
    last_heartbeat: float = 0.0
    # session ids currently occupying slots on this node
    sessions: set[str] = field(default_factory=set)

    @property
    def used(self) -> int:
        return len(self.sessions)

    @property
    def free(self) -> int:
        return self.capacity - len(self.sessions)

    def accepts_new(self) -> bool:
        return self.state is NodeState.ACTIVE


@dataclass
class Session:
    """A player streaming session."""

    session_id: str
    player_id: str
    node_id: Optional[str] = None
    state: SessionState = SessionState.ACTIVE
    created_at: float = 0.0
    lease_expires_at: float = 0.0
    ended_at: Optional[float] = None
    end_reason: Optional[ReleaseReason] = None
    # monotonic sequence assigned at admission; makes placement reproducible
    seq: int = 0


@dataclass
class QueuedRequest:
    """A placement request waiting for capacity."""

    request_id: str
    player_id: str
    enqueued_at: float
    seq: int  # FIFO order; strictly increasing, never reused


@dataclass(frozen=True)
class Placement:
    """Result of a successful placement."""

    session_id: str
    node_id: str
    lease_expires_at: float
    reconnect_token: str
