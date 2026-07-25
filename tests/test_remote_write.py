"""Prometheus remote-write: the hand-rolled Protobuf + Snappy encode round-trips,
and the POST carries the right headers/auth and never raises."""

from __future__ import annotations

import struct

import structlog
from prometheus_client import CollectorRegistry, Gauge
from pytest_httpx import HTTPXMock

from data_pipeline_core.obs.remote_write import (
    _encode_write_request,
    _snappy_compress_literal,
    remote_write_metrics,
)

log = structlog.get_logger()


# --- minimal decoders, just enough to verify what the emitter produces ---


def _read_varint(buf: bytes, i: int) -> tuple[int, int]:
    shift = result = 0
    while True:
        byte = buf[i]
        i += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, i
        shift += 7


def _fields(buf: bytes) -> list[tuple[int, int, object]]:
    out: list[tuple[int, int, object]] = []
    i = 0
    while i < len(buf):
        key, i = _read_varint(buf, i)
        field, wire = key >> 3, key & 7
        if wire == 2:
            length, i = _read_varint(buf, i)
            out.append((field, wire, buf[i : i + length]))
            i += length
        elif wire == 0:
            val, i = _read_varint(buf, i)
            out.append((field, wire, val))
        elif wire == 1:
            out.append((field, wire, buf[i : i + 8]))
            i += 8
        else:  # pragma: no cover - we never emit other wire types
            raise ValueError(wire)
    return out


def _snappy_decode_literal(buf: bytes) -> bytes:
    _, i = _read_varint(buf, 0)  # uncompressed length (ignored)
    if i >= len(buf):
        return b""
    tag = buf[i]
    i += 1
    assert tag & 3 == 0  # literal element
    n = tag >> 2
    if n < 60:
        length = n + 1
    else:
        extra = n - 59
        length = int.from_bytes(buf[i : i + extra], "little") + 1
        i += extra
    return buf[i : i + length]


def _decode_series(write_request: bytes) -> list[tuple[dict[str, str], float]]:
    series: list[tuple[dict[str, str], float]] = []
    for field, _, ts_bytes in _fields(write_request):
        assert field == 1  # WriteRequest.timeseries
        labels: dict[str, str] = {}
        value = None
        for sub_field, _, payload in _fields(ts_bytes):  # type: ignore[arg-type]
            if sub_field == 1:  # Label
                parts = {f: v for f, _, v in _fields(payload)}  # type: ignore[arg-type]
                labels[parts[1].decode()] = parts[2].decode()  # type: ignore[union-attr]
            elif sub_field == 2:  # Sample
                for s_field, _, s_val in _fields(payload):  # type: ignore[arg-type]
                    if s_field == 1:
                        value = struct.unpack("<d", s_val)[0]  # type: ignore[arg-type]
        assert value is not None
        series.append((labels, value))
    return series


# --- tests ---


def test_snappy_literal_roundtrips() -> None:
    for data in (b"", b"x", b"hello world" * 10, bytes(range(256)) * 5):
        assert _snappy_decode_literal(_snappy_compress_literal(data)) == data


def test_write_request_encodes_series_with_name_job_and_labels() -> None:
    registry = CollectorRegistry()
    Gauge("demo_metric", "d", ["source"], registry=registry).labels(source="s").set(3)

    decoded = _decode_series(_encode_write_request(registry, job="worker-a"))

    assert len(decoded) == 1
    labels, value = decoded[0]
    assert labels["__name__"] == "demo_metric"
    assert labels["job"] == "worker-a"
    assert labels["source"] == "s"
    assert value == 3.0


def test_remote_write_posts_with_headers_and_basic_auth(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=200)
    registry = CollectorRegistry()
    Gauge("demo_metric", "d", registry=registry).set(1)

    remote_write_metrics(
        registry,
        url="https://prom.example/api/prom/push",
        username="12345",
        password="tok",
        job="worker-a",
        logger=log,
    )

    request = httpx_mock.get_request()
    assert request is not None
    assert request.method == "POST"
    assert request.headers["Content-Encoding"] == "snappy"
    assert request.headers["Content-Type"] == "application/x-protobuf"
    assert request.headers["X-Prometheus-Remote-Write-Version"] == "0.1.0"
    # base64("12345:tok")
    assert request.headers["Authorization"] == "Basic MTIzNDU6dG9r"


def test_remote_write_never_raises_on_http_error(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(status_code=500)
    registry = CollectorRegistry()
    Gauge("demo_metric", "d", registry=registry).set(1)

    # Must not raise — observability never breaks the run.
    remote_write_metrics(
        registry,
        url="https://prom.example/api/prom/push",
        username=None,
        password=None,
        job="worker-a",
        logger=log,
    )


def test_remote_write_skipped_when_no_url(httpx_mock: HTTPXMock) -> None:
    remote_write_metrics(
        CollectorRegistry(),
        url=None,
        username=None,
        password=None,
        job="worker-a",
        logger=log,
    )
    assert httpx_mock.get_requests() == []
