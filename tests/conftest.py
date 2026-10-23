import pytest

from fleetq import cli


@pytest.fixture(autouse=True)
def no_keepalive(monkeypatch):
    """`fleetq acquire` would leave a detached keepalive process behind each test."""
    monkeypatch.setattr(cli, 'start_keepalive', lambda *a: None)
