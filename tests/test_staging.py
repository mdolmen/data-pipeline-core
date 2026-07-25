"""Raw-landing staging: written records replay back, immutably."""

from __future__ import annotations

import json
from pathlib import Path

from data_pipeline_core import raw_landing_sink, raw_landing_source


def test_roundtrip_through_raw_bucket(tmp_path: Path) -> None:
    bucket = tmp_path.as_uri()
    sink = raw_landing_sink("odds-raw", bucket_url=bucket)

    records = [{"id": 1, "x": "a"}, {"id": 2, "x": "b"}]
    result = sink.write(records)
    assert result.row_count == 2
    # bytes reported = the JSONL payload actually written (one line per record).
    assert result.byte_count == sum(
        len((json.dumps(r) + "\n").encode()) for r in records
    )

    source = raw_landing_source("odds-raw", bucket_url=bucket)
    records = list(source.fetch(None))  # type: ignore[arg-type]
    assert sorted(records, key=lambda r: r["id"]) == [
        {"id": 1, "x": "a"},
        {"id": 2, "x": "b"},
    ]


def test_replayable_without_refetch(tmp_path: Path) -> None:
    bucket = tmp_path.as_uri()
    raw_landing_sink("odds-raw", bucket_url=bucket).write([{"id": 1}])

    source = raw_landing_source("odds-raw", bucket_url=bucket)
    first = list(source.fetch(None))  # type: ignore[arg-type]
    second = list(source.fetch(None))  # type: ignore[arg-type]
    assert first == second == [{"id": 1}]


def test_landing_source_has_a_name(tmp_path: Path) -> None:
    source = raw_landing_source("odds-raw", bucket_url=tmp_path.as_uri())
    assert source.name == "raw-odds-raw"
