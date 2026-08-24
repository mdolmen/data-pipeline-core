"""HttpClient: retry/jitter, UA rotation, status metrics, breaker integration."""

from __future__ import annotations

import fakeredis
import httpx
import pytest
from prometheus_client import CollectorRegistry
from pytest_httpx import HTTPXMock

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.http import CircuitOpenError, HttpClient
from data_pipeline_core.ingestion.ip_guard import IpGuard
from data_pipeline_core.ingestion.proxy import ProxyRouter
from data_pipeline_core.obs.metrics import StandardMetrics
from data_pipeline_core.runtime.config import Settings


def _client(
    *,
    ip_guard: IpGuard | None = None,
    proxy: ProxyRouter | None = None,
    **overrides: object,
) -> tuple[HttpClient, CircuitBreaker, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = StandardMetrics(registry, source="s", stage="ingest")
    settings = Settings(**overrides)  # type: ignore[arg-type]
    breaker = CircuitBreaker("s", threshold=2, cooldown_seconds=900.0, metrics=metrics)
    client = HttpClient(
        "s",
        settings=settings,
        breaker=breaker,
        metrics=metrics,
        ip_guard=ip_guard,
        proxy=proxy,
        sleep=lambda _: None,  # no real backoff sleeps in tests
    )
    return client, breaker, registry


def _aggressive_guard(redis_client: fakeredis.FakeStrictRedis | None = None) -> IpGuard:
    redis_client = redis_client or fakeredis.FakeStrictRedis()
    redis_client.set("ratelimit:s:0", 500)  # already at the Aggressive threshold
    return IpGuard(
        "s",
        redis_client,
        warning_at=300,
        aggressive_at=500,
        window_seconds=3600,
        clock=lambda: 0.0,
    )


def test_retries_5xx_then_succeeds(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(status_code=200, text="ok")
    client, _, registry = _client(http_max_retries=2)

    response = client.get("https://api.test/odds")

    assert response.status_code == 200
    assert client.request_count == 2
    base = {"source": "s", "stage": "ingest"}
    assert registry.get_sample_value("http_status_total", {**base, "code": "503"}) == 1
    assert registry.get_sample_value("http_status_total", {**base, "code": "200"}) == 1


def test_rotates_user_agent_from_settings(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200)
    client, _, _ = _client()

    client.get("https://api.test/odds")

    sent = httpx_mock.get_requests()[0]
    assert sent.headers["User-Agent"] in Settings().http_user_agents


def test_429_records_failure_and_returns(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=429)
    client, _, registry = _client()

    response = client.get("https://api.test/odds")

    assert response.status_code == 429
    assert client.request_count == 1  # not retried
    assert (
        registry.get_sample_value(
            "http_status_total", {"source": "s", "stage": "ingest", "code": "429"}
        )
        == 1
    )


def test_open_breaker_blocks_request(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=429, is_reusable=True)
    client, breaker, registry = _client()

    client.get("https://api.test/odds")  # 1st 429
    client.get("https://api.test/odds")  # 2nd 429 → breaker opens (threshold=2)
    assert breaker.is_open

    with pytest.raises(CircuitOpenError):
        client.get("https://api.test/odds")
    assert (
        registry.get_sample_value(
            "circuit_breaker_state", {"source": "s", "stage": "ingest"}
        )
        == 1
    )


def test_retries_transport_error_then_raises(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_exception(httpx.ConnectError("boom"), is_reusable=True)
    client, _, _ = _client(http_max_retries=1)

    with pytest.raises(httpx.ConnectError):
        client.get("https://api.test/odds")


def test_aggressive_mode_routes_via_proxy(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200)
    proxy = ProxyRouter(
        proxy_url="http://proxy:8000", enabled=True, timeout_seconds=30.0
    )
    client, _, _ = _client(ip_guard=_aggressive_guard(), proxy=proxy)

    client.get("https://api.test/odds")

    assert client.request_count == 1
    assert client.proxied_count == 1  # density ≥500 → routed through the proxy
    proxy.close()


def test_counts_guard_and_proxy_per_attempt_not_per_call(
    httpx_mock: HTTPXMock,
) -> None:
    # Three attempts are three packets from the same IP: the guard's density
    # counter and proxied_count must each read 3. Counted per call instead, a
    # retrying worker spends its rate budget unseen and proxy_usage_ratio reads
    # 1/3 of the share actually proxied.
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(status_code=503)
    httpx_mock.add_response(status_code=200)
    redis_client = fakeredis.FakeStrictRedis()
    proxy = ProxyRouter(
        proxy_url="http://proxy:8000", enabled=True, timeout_seconds=30.0
    )
    client, _, _ = _client(
        ip_guard=_aggressive_guard(redis_client), proxy=proxy, http_max_retries=2
    )
    before = int(redis_client.get("ratelimit:s:0") or 0)

    response = client.get("https://api.test/odds")

    assert response.status_code == 200
    assert client.request_count == 3
    assert client.proxied_count == 3
    assert int(redis_client.get("ratelimit:s:0") or 0) == before + 3
    proxy.close()


def test_proxy_disabled_by_config_keeps_direct(httpx_mock: HTTPXMock) -> None:
    # Polytricks: even at Aggressive density, proxy_enabled=False stays direct.
    httpx_mock.add_response(status_code=200)
    proxy = ProxyRouter(
        proxy_url="http://proxy:8000", enabled=False, timeout_seconds=30.0
    )
    client, _, _ = _client(ip_guard=_aggressive_guard(), proxy=proxy)

    response = client.get("https://api.test/odds")

    assert response.status_code == 200
    assert client.proxied_count == 0  # never routed through the proxy


def test_response_exposes_is_success(httpx_mock: HTTPXMock) -> None:
    # `get` is declared to return the public `Response` protocol, so this is the
    # surface a consumer's `mypy --strict` sees — not the concrete backend
    # response the union happens to carry. Reading it here fails the type check
    # if the protocol stops declaring what the SDK's own run loop already uses.
    httpx_mock.add_response(status_code=200)
    client, _, _ = _client()

    assert client.get("https://api.test/odds").is_success


def test_read_until_returns_body_and_records_status(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(content=b"chunk-one-two-three")
    client, _, registry = _client()

    body = client.read_until(
        "GET", "https://api.test/stream", until=lambda b: len(b) >= 5
    )

    assert body.startswith(b"chunk")
    assert client.request_count == 1
    assert (
        registry.get_sample_value(
            "http_status_total", {"source": "s", "stage": "ingest", "code": "200"}
        )
        == 1
    )
