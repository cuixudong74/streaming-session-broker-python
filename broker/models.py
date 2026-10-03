"""Core data types for the streaming session broker.

Pure type definitions only; no scheduling logic.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Optional


class NodeState(str, enum.Enum):
    ACTIVE = "active"          # accepts new sessions
    DRAINING = "draining"      # maintenance: no new sessions
    OFFLINE = "offline"        # heartbeat lost; sessions force-released


class SessionState(str, enum.Enum):
    ACTIVE = "active"
    ENDED = "ended"


class ReleaseReason(str, enum.Enum):
    CLIENT_RELEASE = "client_release"
    LEASE_EXPIRED = "lease_expired"
    NODE_HEARTBEAT_LOST = "node_heartbeat_lost"
    NODE_DRAIN = "node_drain"
    NODE_OFFLINE = "node_offline"


class RejectReason(str, enum.Enum):
    NO_CAPACITY = "no_capacity"
    TOKEN_INVALID = "token_invalid"
    TOKEN_EXPIRED = "token_expired"
    SESSION_NOT_FOUND = "session_not_found"
    SESSION_ENDED = "session_ended"


@dataclass
class Node:
    node_id: str
    capacity: int
    state: NodeState = NodeState.ACTIVE
    last_heartbeat: float = 0.0
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
    session_id: str
    player_id: str
    node_id: Optional[str] = None
    state: SessionState = SessionState.ACTIVE
    created_at: float = 0.0
    lease_expires_at: float = 0.0
    ended_at: Optional[float] = None
    end_reason: Optional[ReleaseReason] = None
    seq: int = 0


@dataclass
class QueuedRequest:
    request_id: str
    player_id: str
    enqueued_at: float
    seq: int


@dataclass(frozen=True)
class Placement:
    session_id: str
    node_id: str
    lease_expires_at: float
    reconnect_token: str
