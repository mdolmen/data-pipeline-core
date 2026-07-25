"""End-of-run **Prometheus remote-write** — the preferred metrics push.

Short-lived workers exit before a scrape can reach them. Rather than a PushGateway
plus a scraper (always-on middle-boxes), the worker writes its final series
straight to a TSDB (e.g. Grafana Cloud) at exit. Remote-write wants a
Snappy-compressed Protobuf ``WriteRequest``; both are tiny fixed shapes, so this
does them by hand with **no extra dependency**:

- the Protobuf ``WriteRequest`` is encoded directly (a handful of nested
  length-delimited messages — see ``_encode_write_request``);
- Snappy is emitted as a single **literal** block (spec-compliant; every Snappy
  decoder accepts it). Metric payloads are a few KB, so skipping back-references
  costs nothing and buys zero-dependency compression.

A push never fails the run — observability must not break ingestion.
"""

from __future__ import annotations

import base64
import struct
import time

import httpx
from prometheus_client import CollectorRegistry
from structlog.typing import FilteringBoundLogger

_CONTENT_TYPE = "application/x-protobuf"
_WRITE_VERSION = "0.1.0"
_SKIP_SUFFIXES = ("_created",)  # prometheus_client's per-counter creation gauge


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _tag(field: int, wire: int) -> bytes:
    return _varint((field << 3) | wire)


def _len_delim(field: int, payload: bytes) -> bytes:
    # wire type 2: tag, length varint, bytes.
    return _tag(field, 2) + _varint(len(payload)) + payload


def _string_field(field: int, text: str) -> bytes:
    return _len_delim(field, text.encode("utf-8"))


def _encode_label(name: str, value: str) -> bytes:
    # Label { string name = 1; string value = 2; }
    return _string_field(1, name) + _string_field(2, value)


def _encode_sample(value: float, timestamp_ms: int) -> bytes:
    # Sample { double value = 1; int64 timestamp = 2; }
    return (
        _tag(1, 1)
        + struct.pack("<d", value)  # wire type 1: 64-bit little-endian double
        + _tag(2, 0)
        + _varint(timestamp_ms)  # wire type 0: varint
    )


def _encode_series(labels: dict[str, str], value: float, timestamp_ms: int) -> bytes:
    # TimeSeries { repeated Label labels = 1; repeated Sample samples = 2; }
    body = b"".join(
        _len_delim(1, _encode_label(name, labels[name])) for name in sorted(labels)
    )
    body += _len_delim(2, _encode_sample(value, timestamp_ms))
    return body


def _encode_write_request(registry: CollectorRegistry, *, job: str) -> bytes:
    """Serialise every current sample as a Protobuf ``WriteRequest``."""
    timestamp_ms = int(time.time() * 1000)
    request = bytearray()
    for metric in registry.collect():
        for sample in metric.samples:
            if sample.name.endswith(_SKIP_SUFFIXES):
                continue
            labels = {"__name__": sample.name, "job": job, **sample.labels}
            series = _encode_series(labels, sample.value, timestamp_ms)
            # WriteRequest { repeated TimeSeries timeseries = 1; }
            request += _len_delim(1, series)
    return bytes(request)


def _snappy_compress_literal(data: bytes) -> bytes:
    """Snappy block-format encode ``data`` as one literal (no back-references).

    Valid Snappy any decoder accepts. Layout: uncompressed length (varint), then a
    literal element (tag + optional extended length bytes + the raw bytes).
    """
    out = bytearray(_varint(len(data)))
    if not data:
        return bytes(out)
    lit_len = len(data) - 1  # literals store length-1
    if lit_len < 60:
        out.append(lit_len << 2)
    else:
        extra = (lit_len.bit_length() + 7) // 8
        out.append((59 + extra) << 2)  # tag 60..63 → 1..4 following length bytes
        out += lit_len.to_bytes(extra, "little")
    out += data
    return bytes(out)


def remote_write_metrics(
    registry: CollectorRegistry,
    *,
    url: str | None,
    username: str | None,
    password: str | None,
    job: str,
    logger: FilteringBoundLogger,
    timeout_seconds: float = 10.0,
) -> None:
    """Remote-write ``registry`` to ``url``. No-op (logged) when no url is set."""
    if not url:
        logger.info("metrics remote-write skipped", reason="no remote-write url")
        return
    headers = {
        "Content-Type": _CONTENT_TYPE,
        "Content-Encoding": "snappy",
        "X-Prometheus-Remote-Write-Version": _WRITE_VERSION,
    }
    if username is not None and password is not None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    try:
        body = _snappy_compress_literal(_encode_write_request(registry, job=job))
        response = httpx.post(
            url, content=body, headers=headers, timeout=timeout_seconds
        )
        response.raise_for_status()
    except Exception:
        logger.exception("metrics remote-write failed", url=url)
    else:
        logger.info("metrics remote-written", url=url, job=job)
