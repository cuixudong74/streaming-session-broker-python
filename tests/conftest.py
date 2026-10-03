import pytest

from broker.broker import Broker, BrokerConfig


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def make_broker(clock):
    def _make(**cfg_kwargs) -> Broker:
        cfg = BrokerConfig(
            lease_ttl=cfg_kwargs.pop("lease_ttl", 10.0),
            heartbeat_timeout=cfg_kwargs.pop("heartbeat_timeout", 20.0),
            reconnect_grace=cfg_kwargs.pop("reconnect_grace", 30.0),
            allow_queue=cfg_kwargs.pop("allow_queue", True),
        )
        return Broker(cfg, secret=b"test-secret", now=clock)
    return _make
