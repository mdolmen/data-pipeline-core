"""The conformance kit: each check must reject the violation it exists for."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping

import httpx
import pytest

from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.storage.protocols import Record, WriteResult
from data_pipeline_core.testing import (
    ContractViolation,
    check_sink_contract,
    check_source_contract,
    check_transform_contract,
    make_test_context,
)

_RECORDS: list[Mapping[str, object]] = [{"id": 1}, {"id": 2}, {"id": 3}]


class _GoodSink:
    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        count = 0
        for _ in records:
            count += 1
        return WriteResult(row_count=count)


class _MaterialisingSink:
    """The mistake that works at ten records and holds a run at ten million."""

    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        rows = list(records)
        list(records)  # second pass — silently empty in the real run loop
        return WriteResult(row_count=len(rows))


class _MiscountingSink:
    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        for _ in records:
            pass
        return WriteResult(row_count=99)


class _LazySource:
    name = "good"

    def fetch(self, ctx: RunContext) -> Iterator[Record]:
        yield from ({"id": i} for i in range(3))


class _EagerSource:
    name = "eager"

    def fetch(self, ctx: RunContext) -> Iterable[Record]:
        return [{"id": i} for i in range(3)]


class _MutatingTransform:
    def transform(self, record: Record, ctx: RunContext) -> Iterable[Record]:
        record["seen"] = True  # the caller still owns this mapping
        yield record


class _CleanTransform:
    def transform(self, record: Record, ctx: RunContext) -> Iterable[Record]:
        yield {**record, "seen": True}


def test_accepts_a_conforming_sink() -> None:
    result = check_sink_contract(_GoodSink(), _RECORDS)
    assert result.row_count == 3


def test_rejects_a_sink_that_materialises_and_re_iterates() -> None:
    with pytest.raises(ContractViolation, match="more than once"):
        check_sink_contract(_MaterialisingSink(), _RECORDS)


def test_rejects_a_sink_that_misreports_its_row_count() -> None:
    with pytest.raises(ContractViolation, match="row_count"):
        check_sink_contract(_MiscountingSink(), _RECORDS)


def test_sink_input_has_no_len() -> None:
    # len() is the tempting shortcut the run loop can't support.
    captured: list[Iterable[Mapping[str, object]]] = []

    class _Capturing:
        def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
            captured.append(records)
            return WriteResult(row_count=sum(1 for _ in records))

    check_sink_contract(_Capturing(), _RECORDS)
    with pytest.raises(TypeError):
        len(captured[0])  # type: ignore[arg-type]


def test_accepts_a_streaming_source() -> None:
    assert len(check_source_contract(_LazySource())) == 3


def test_rejects_a_source_that_materialises() -> None:
    with pytest.raises(ContractViolation, match="before returning"):
        check_source_contract(_EagerSource())


def test_eager_source_allowed_when_bounded_and_small() -> None:
    assert len(check_source_contract(_EagerSource(), require_lazy=False)) == 3


def test_rejects_a_source_without_a_usable_name() -> None:
    class _Nameless:
        name = ""

        def fetch(self, ctx: RunContext) -> Iterable[Record]:
            yield {"id": 1}

    with pytest.raises(ContractViolation, match="non-empty str"):
        check_source_contract(_Nameless())


def test_rejects_a_transform_that_mutates_its_input() -> None:
    with pytest.raises(ContractViolation, match="mutated"):
        check_transform_contract(_MutatingTransform(), {"id": 1})


def test_accepts_a_transform_that_returns_new_mappings() -> None:
    record: Record = {"id": 1}
    assert check_transform_contract(_CleanTransform(), record) == [
        {"id": 1, "seen": True}
    ]
    assert record == {"id": 1}  # untouched


def test_test_context_refuses_accidental_network_calls() -> None:
    ctx = make_test_context()
    with pytest.raises(ContractViolation, match="unexpected outbound request"):
        ctx.http.get("https://api.test/live")


def test_test_context_accepts_a_mocked_client() -> None:
    class _Probe:
        name = "probe"

        def fetch(self, ctx: RunContext) -> Iterator[Record]:
            response = ctx.http.get("https://api.test/odds")
            yield {"status": response.status_code}

    from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
    from data_pipeline_core.ingestion.http import HttpClient
    from data_pipeline_core.runtime.config import Settings

    http = HttpClient(
        "probe",
        settings=Settings(),
        breaker=CircuitBreaker("probe", threshold=5, cooldown_seconds=1.0),
        sleep=lambda _: None,
        transport=httpx.MockTransport(lambda _: httpx.Response(200)),
    )
    records = check_source_contract(_Probe(), make_test_context(http=http))
    assert records == [{"status": 200}]
