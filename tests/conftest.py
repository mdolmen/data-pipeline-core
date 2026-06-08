"""Shared test fixtures for the SDK suite.

These are the three isolation primitives the SDK's mechanisms are tested
against: a fake Redis (IP-guard counters / latest-state cache), an httpx mock
(outbound HTTP — provided automatically as the ``httpx_mock`` fixture by the
pytest-httpx plugin), and a fresh Prometheus registry (so metric series don't
leak between tests). They are deliberately lightweight scaffolding; the
mechanisms that consume them arrive in later build phases.
"""

from __future__ import annotations

from collections.abc import Iterator

import fakeredis
import pytest
from prometheus_client import CollectorRegistry


@pytest.fixture
def fake_redis() -> Iterator[fakeredis.FakeStrictRedis]:
    """In-memory Redis double (sliding-window counters, latest-state cache)."""
    server = fakeredis.FakeStrictRedis()
    try:
        yield server
    finally:
        server.flushall()


@pytest.fixture
def metrics_registry() -> CollectorRegistry:
    """Isolated Prometheus registry so metric series don't leak across tests."""
    return CollectorRegistry()
