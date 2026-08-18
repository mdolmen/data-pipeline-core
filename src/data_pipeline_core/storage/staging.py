"""Raw-landing staging boundary — the handoff between ingest and transform.

An ingest worker writes the *raw* payloads with ``raw_landing_sink``; a
transform worker reads them back with ``raw_landing_source``. Raw data is the
immutable, replayable source of truth: a parser bug never loses data, and the
transform can be re-run without re-fetching.

This implements the GCS-raw variant over fsspec, so the same code targets a
local ``file://`` dir (dev) or a ``gs://`` bucket (cloud) by URL. The Pub/Sub
variant is deferred until a real GCP target exists. Records land as
newline-delimited JSON, one file per run under ``{bucket}/{channel}/``.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Generic, TypeVar

from fsspec.core import url_to_fs

from data_pipeline_core.storage.protocols import Sink, Source, WriteResult

# What a replay yields is whatever the ingest worker landed — the project knows
# that shape, the SDK doesn't. Inferred from the wiring at the call site.
RecordT = TypeVar("RecordT", bound=Mapping[str, object])

if TYPE_CHECKING:
    from data_pipeline_core.runtime.context import RunContext


def _resolve_bucket(bucket_url: str | None) -> str:
    url = bucket_url or os.environ.get("RAW_BUCKET_URL")
    if not url:
        raise ValueError(
            "raw bucket url not set (pass bucket_url= or set RAW_BUCKET_URL)"
        )
    return url


class _RawLandingSink:
    def __init__(self, channel: str, bucket_url: str | None) -> None:
        self._channel = channel
        self._bucket_url = bucket_url

    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        fs, base = url_to_fs(_resolve_bucket(self._bucket_url))
        directory = f"{base.rstrip('/')}/{self._channel}"
        fs.makedirs(directory, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        path = f"{directory}/{stamp}-{uuid.uuid4().hex}.jsonl"

        row_count = 0
        byte_count = 0
        with fs.open(path, "w") as handle:
            for record in records:
                line = json.dumps(record) + "\n"
                handle.write(line)
                row_count += 1
                byte_count += len(line.encode("utf-8"))
        return WriteResult(row_count=row_count, byte_count=byte_count)


class _RawLandingSource(Generic[RecordT]):
    def __init__(self, channel: str, bucket_url: str | None) -> None:
        self.name = f"raw-{channel}"
        self._channel = channel
        self._bucket_url = bucket_url

    def fetch(self, ctx: RunContext) -> Iterable[RecordT]:
        fs, base = url_to_fs(_resolve_bucket(self._bucket_url))
        pattern = f"{base.rstrip('/')}/{self._channel}/*.jsonl"
        for path in sorted(fs.glob(pattern)):
            with fs.open(path, "r") as handle:
                for line in handle:
                    text = line.decode() if isinstance(line, bytes) else line
                    if text.strip():
                        record: RecordT = json.loads(text)
                        yield record


def raw_landing_sink(
    channel: str, *, bucket_url: str | None = None
) -> Sink[Mapping[str, object]]:
    """A ``Sink`` that lands raw records as JSONL under ``{bucket}/{channel}/``."""
    return _RawLandingSink(channel, bucket_url)


def raw_landing_source(
    channel: str, *, bucket_url: str | None = None
) -> Source[RecordT]:
    """A ``Source`` that replays raw records from ``{bucket}/{channel}/``."""
    return _RawLandingSource(channel, bucket_url)
