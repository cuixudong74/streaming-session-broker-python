"""HTTP API for the streaming session broker."""
from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from broker.broker import Broker, BrokerConfig, BrokerError, Queued
from broker.models import Placement
from broker.store import WalStore



class NodeIn(BaseModel):
    node_id: str
    capacity: int


class RequestIn(BaseModel):
    player_id: str
    request_id: str | None = None


class TokenIn(BaseModel):
    token: str


def build_app() -> FastAPI:
    cfg = BrokerConfig(
        lease_ttl=float(os.getenv("BROKER_LEASE_TTL", "30")),
        heartbeat_timeout=float(os.getenv("BROKER_HEARTBEAT_TIMEOUT", "45")),
        reconnect_grace=float(os.getenv("BROKER_RECONNECT_GRACE", "60")),
        allow_queue=os.getenv("BROKER_ALLOW_QUEUE", "1") != "0",
    )
    store = None
    state_dir = os.getenv("BROKER_STATE_DIR")
    if state_dir:
        store = WalStore(Path(state_dir))
    broker = Broker(cfg, secret=os.getenv("BROKER_SECRET", "dev-secret").encode(),
                    store=store)

    app = FastAPI(title="streaming-session-broker")
    app.state.broker = broker

    def handle(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except BrokerError as exc:
            raise HTTPException(status_code=409,
                                detail={"reason": exc.reason.value,
                                        "message": str(exc)})

    @app.post("/nodes", status_code=201)
    def add_node(body: NodeIn):
        handle(broker.add_node, body.node_id, body.capacity)
        return {"node_id": body.node_id}

    @app.post("/nodes/{node_id}/heartbeat", status_code=204)
    def heartbeat(node_id: str):
        handle(broker.heartbeat, node_id)

    @app.post("/nodes/{node_id}/drain")
    def drain(node_id: str):
        handle(broker.start_drain, node_id)
        return {"node_id": node_id, "draining": True}

    @app.get("/nodes/{node_id}/drain")
    def drain_status(node_id: str):
        return {"node_id": node_id, "complete": broker.drain_complete(node_id)}

    @app.post("/nodes/{node_id}/offline", status_code=204)
    def offline(node_id: str):
        handle(broker.mark_offline, node_id)

    @app.post("/sessions", status_code=201)
    def request_session(body: RequestIn):
        result = handle(broker.request_session, body.player_id, body.request_id)
        if isinstance(result, Queued):
            return {"status": "queued", "request_id": result.request_id,
                    "position": result.position}
        assert isinstance(result, Placement)
        return {"status": "placed", "session_id": result.session_id,
                "node_id": result.node_id,
                "lease_expires_at": result.lease_expires_at,
                "reconnect_token": result.reconnect_token}

    @app.post("/sessions/{session_id}/release", status_code=204)
    def release(session_id: str):
        handle(broker.release, session_id)

    @app.post("/sessions/{session_id}/renew")
    def renew(session_id: str):
        p = handle(broker.renew_lease, session_id)
        return {"session_id": p.session_id, "node_id": p.node_id,
                "lease_expires_at": p.lease_expires_at,
                "reconnect_token": p.reconnect_token}

    @app.post("/reconnect")
    def reconnect(body: TokenIn):
        p = handle(broker.reconnect, body.token)
        return {"session_id": p.session_id, "node_id": p.node_id,
                "lease_expires_at": p.lease_expires_at,
                "reconnect_token": p.reconnect_token}

    @app.post("/admin/sweep", status_code=204)
    def sweep():
        broker.sweep()

    @app.post("/admin/snapshot", status_code=204)
    def snapshot():
        broker.snapshot()

    @app.get("/admin/invariants")
    def invariants():
        broker.check_invariants()
        return {"ok": True}

    return app


app = build_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
