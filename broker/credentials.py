"""Reconnect credential issuing & verification (HMAC-signed tokens)."""
from __future__ import annotations

import base64
import hashlib
import hmac


class TokenError(Exception):
    """Base class for token failures."""


class TokenInvalid(TokenError):
    """Malformed or forged token."""


class TokenExpired(TokenError):
    """Well-formed token whose expiry has passed."""


class TokenIssuer:
    """Issues and verifies reconnect tokens.

    Token format: ``<session_id>.<expiry_ms>.<sig>`` where ``sig`` is a
    URL-safe base64 HMAC-SHA256 over ``"<session_id>.<expiry_ms>"``.
    Verification never mutates any state, so forged/expired tokens are
    rejected without side effects on existing sessions.
    """

    def __init__(self, secret: bytes) -> None:
        if not secret:
            raise ValueError("secret must be non-empty")
        self._secret = secret

    def _sign(self, payload: str) -> str:
        digest = hmac.new(self._secret, payload.encode(), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(digest).decode().rstrip("=")

    def issue(self, session_id: str, expires_at: float) -> str:
        expiry_ms = int(expires_at * 1000)
        payload = f"{session_id}.{expiry_ms}"
        return f"{payload}.{self._sign(payload)}"

    def verify(self, token: str, now: float) -> tuple[str, float]:
        """Return (session_id, expires_at); raise TokenInvalid/TokenExpired."""
        if not token or token.count(".") != 2:
            raise TokenInvalid("malformed token")
        session_id, expiry_ms_s, sig = token.rsplit(".", 2)
        payload = f"{session_id}.{expiry_ms_s}"
        expected = self._sign(payload)
        if not hmac.compare_digest(sig, expected):
            raise TokenInvalid("bad signature")
        try:
            expiry_ms = int(expiry_ms_s)
        except ValueError as exc:
            raise TokenInvalid("bad expiry") from exc
        expires_at = expiry_ms / 1000.0
        if now > expires_at:
            raise TokenExpired("token expired")
        return session_id, expires_at
