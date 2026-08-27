"""The SDK's own adapters must satisfy the contract the kit enforces.

Dogfooding, and a regression guard: consumers are told to check their `Source`
and `Sink` implementations against `data_pipeline_core.testing`, so the shipped
ones had better pass. This is also the compatibility spec to run against when
the contract grows a batch-oriented or long-running variant — whatever changes,
these four must keep holding or the change is breaking.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import fakeredis
import pytest

from data_pipeline_core import (
    Record,
    Source,
    dlt_sink,
    raw_landing_sink,
    raw_landing_source,
    redis_latest_sink,
)
from data_pipeline_core.testing import check_sink_contract, check_source_contract

_RECORDS: list[Mapping[str, object]] = [
    {"id": 1, "x": "a"},
    {"id": 2, "x": "b"},
    {"id": 3, "x": "c"},
]


def test_raw_landing_sink_conforms(tmp_path: Path) -> None:
    sink = raw_landing_sink("channel", bucket_url=tmp_path.as_uri())
    result = check_sink_contract(sink, _RECORDS)
    assert result.byte_count is not None and result.byte_count > 0


def test_raw_landing_source_conforms(tmp_path: Path) -> None:
    raw_landing_sink("channel", bucket_url=tmp_path.as_uri()).write(_RECORDS)
    source: Source[Record] = raw_landing_source("channel", bucket_url=tmp_path.as_uri())
    assert len(check_source_contract(source)) == len(_RECORDS)


def test_redis_latest_sink_conforms() -> None:
    sink = redis_latest_sink(["id"], fakeredis.FakeStrictRedis())
    check_sink_contract(sink, _RECORDS)


def test_dlt_sink_conforms(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", bucket.as_uri())
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    check_sink_contract(dlt_sink("conformance", table_name="records"), _RECORDS)
