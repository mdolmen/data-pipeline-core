"""WorkerApp run-loop behaviour: streaming, exit codes, error handling."""

from __future__ import annotations

from collections.abc import Iterable

import pytest
from prometheus_client import CollectorRegistry

import data_pipeline_core.runtime.app as app_mod
from data_pipeline_core import Record, RunContext, WorkerApp, WriteResult


class _ListSink:
    def __init__(self) -> None:
        self.written: list[Record] = []

    def write(self, records: Iterable[Record]) -> WriteResult:
        self.written.extend(records)
        return WriteResult(row_count=len(self.written))


@pytest.fixture
def captured_registry(monkeypatch: pytest.MonkeyPatch) -> dict[str, CollectorRegistry]:
    """Capture the registry WorkerApp would push instead of pushing it."""
    box: dict[str, CollectorRegistry] = {}

    def fake_push(registry: CollectorRegistry, **_: object) -> None:
        box["registry"] = registry

    monkeypatch.setattr(app_mod, "push_metrics", fake_push)
    return box


def test_run_streams_fetch_into_write() -> None:
    seen_run_id: list[str] = []

    class _Source:
        name = "demo"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            seen_run_id.append(ctx.run_id)
            yield {"a": 1}
            yield {"a": 2}

    sink = _ListSink()
    exit_code = WorkerApp(_Source(), sink).run()

    assert exit_code == 0
    assert sink.written == [{"a": 1}, {"a": 2}]
    assert seen_run_id and seen_run_id[0]


def test_run_applies_transform_in_process() -> None:
    class _Source:
        name = "demo"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            yield {"n": 1}
            yield {"n": 2}

    class _Double:
        def transform(self, record: Record, ctx: RunContext) -> Iterable[Record]:
            yield {"n": record["n"] * 2}

    sink = _ListSink()
    exit_code = WorkerApp(_Source(), sink, transform=_Double()).run()

    assert exit_code == 0
    assert sink.written == [{"n": 2}, {"n": 4}]


def test_run_returns_1_when_source_raises() -> None:
    class _BoomSource:
        name = "boom"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            raise RuntimeError("fetch exploded")

    sink = _ListSink()
    assert WorkerApp(_BoomSource(), sink).run() == 1
    assert sink.written == []


def test_run_sets_worker_up_on_success(
    captured_registry: dict[str, CollectorRegistry],
) -> None:
    class _Source:
        name = "demo"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            yield {"a": 1}

    assert WorkerApp(_Source(), _ListSink()).run() == 0
    registry = captured_registry["registry"]
    assert (
        registry.get_sample_value("worker_up", {"source": "demo", "stage": "ingest"})
        == 1
    )


def test_run_sets_worker_up_zero_on_failure(
    captured_registry: dict[str, CollectorRegistry],
) -> None:
    class _BoomSource:
        name = "boom"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            raise RuntimeError("nope")

    assert WorkerApp(_BoomSource(), _ListSink()).run() == 1
    registry = captured_registry["registry"]
    assert (
        registry.get_sample_value("worker_up", {"source": "boom", "stage": "ingest"})
        == 0
    )
