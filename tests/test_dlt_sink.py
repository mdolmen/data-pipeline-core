"""dlt_sink lands records as parquet at the configured destination."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from data_pipeline_core import dlt_sink


@pytest.fixture
def filesystem_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point dlt's filesystem destination and working dir at a temp tree."""
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", bucket.as_uri())
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    return bucket


def test_write_lands_parquet(filesystem_bucket: Path) -> None:
    sink = dlt_sink(dataset="odds", destination="filesystem", table_name="obs")

    result = sink.write([{"x": 1}, {"x": 2}, {"x": 3}])

    assert result.row_count == 3
    obs_files = [f for f in filesystem_bucket.rglob("*.parquet") if "obs" in f.parts]
    assert obs_files, "no parquet landed for the obs table"
    rows = sum(pq.read_table(f).num_rows for f in obs_files)
    assert rows == 3
