"""WorkerApp run-loop behaviour: streaming, exit codes, error handling."""

from __future__ import annotations

from collections.abc import Iterable

from data_pipeline_core import Record, RunContext, WorkerApp, WriteResult


class _ListSink:
    def __init__(self) -> None:
        self.written: list[Record] = []

    def write(self, records: Iterable[Record]) -> WriteResult:
        self.written.extend(records)
        return WriteResult(row_count=len(self.written))


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


def test_run_returns_1_when_source_raises() -> None:
    class _BoomSource:
        name = "boom"

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            raise RuntimeError("fetch exploded")

    sink = _ListSink()
    assert WorkerApp(_BoomSource(), sink).run() == 1
    assert sink.written == []
