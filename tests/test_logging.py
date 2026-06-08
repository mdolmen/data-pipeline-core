"""Structured logging emits one JSON object per line with bound context."""

from __future__ import annotations

import json

import pytest

from data_pipeline_core.runtime.logging import configure_logging, get_logger


def test_json_output_carries_bound_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(level="INFO", fmt="json")
    log = get_logger().bind(run_id="abc123", source="demo")

    log.info("worker finished", row_count=3)

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "worker finished"
    assert payload["level"] == "info"
    assert payload["run_id"] == "abc123"
    assert payload["source"] == "demo"
    assert payload["row_count"] == 3
    assert "timestamp" in payload


def test_level_filtering_drops_below_threshold(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="WARNING", fmt="json")
    log = get_logger()

    log.info("ignored")
    log.warning("kept")

    out = capsys.readouterr().out
    assert "ignored" not in out
    assert "kept" in out
