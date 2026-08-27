"""Conformance checks for the ``Source`` / ``Transform`` / ``Sink`` contract.

The protocols are ``runtime_checkable``, which verifies only that the attributes
*exist*. Nothing checks that an implementation **behaves** — and the behavioural
half is the half a consumer breaks silently: a sink that calls ``list()`` on its
input works fine at ten records and holds a run in memory at ten million; a
source that returns a list instead of yielding loses the streaming property the
run loop depends on; a transform that mutates its argument corrupts a record the
caller still owns. None of that is visible to the type checker.

These helpers assert that behaviour, so a second consumer inherits the first
one's hard-won invariants instead of rediscovering them.

Framework-agnostic on purpose: every check raises ``ContractViolation`` (an
``AssertionError``), so it drops into pytest, unittest or a plain script without
the SDK taking a test-time dependency::

    from data_pipeline_core.testing import check_sink_contract

    def test_my_sink_honours_the_contract() -> None:
        check_sink_contract(MySink(), [{"id": 1}, {"id": 2}])
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any

import httpx

from data_pipeline_core.ingestion.circuit_breaker import CircuitBreaker
from data_pipeline_core.ingestion.http import HttpClient
from data_pipeline_core.runtime.config import Settings
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.storage.protocols import Sink, Source, Transform, WriteResult

__all__ = [
    "ContractViolation",
    "check_sink_contract",
    "check_source_contract",
    "check_transform_contract",
    "make_test_context",
]

# Containers that prove the whole result was built before returning. Anything
# else (generator, custom lazy iterator) is assumed to stream.
_MATERIALISED = (list, tuple, set, frozenset, dict)


class ContractViolation(AssertionError):
    """An implementation satisfies the protocol's shape but not its behaviour."""


class _OneShot:
    """The iterable a sink is actually handed: no ``len()``, single pass.

    ``Sink.write`` promises to drain a lazy iterable exactly once. Passing a
    plain list would hide both mistakes — ``len()`` would succeed and a second
    pass would silently yield the same records again.
    """

    def __init__(self, records: Sequence[Mapping[str, object]]) -> None:
        self._records = records
        self.passes = 0
        self.pulled = 0

    def __iter__(self) -> Iterator[Mapping[str, object]]:
        self.passes += 1
        if self.passes > 1:
            raise ContractViolation(
                "sink iterated its input more than once; the iterable the run "
                "loop passes is one-shot, so a second pass sees nothing"
            )
        for record in self._records:
            self.pulled += 1
            yield record


def _reject_materialised(value: object, *, what: str, hint: str) -> None:
    if isinstance(value, _MATERIALISED):
        raise ContractViolation(
            f"{what} returned {type(value).__name__}, which means every record "
            f"was built before returning. {hint}"
        )


def make_test_context(
    *,
    source_name: str = "test",
    settings: Settings | None = None,
    http: HttpClient | None = None,
    should_stop: Callable[[], bool] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> RunContext:
    """A ``RunContext`` for tests, so consumers stop passing ``None``.

    The default ``http`` client fails loudly on any outbound call, so a test
    that reaches the network by accident says so instead of hanging or hitting a
    live endpoint. Pass your own client (e.g. backed by ``httpx.MockTransport``)
    to exercise a source's request handling.
    """
    resolved = settings or Settings()
    if http is None:

        def _refuse(request: httpx.Request) -> httpx.Response:
            raise ContractViolation(
                f"unexpected outbound request to {request.url} — pass "
                "make_test_context(http=...) with a mocked client to allow it"
            )

        http = HttpClient(
            source_name,
            settings=resolved,
            breaker=CircuitBreaker(
                source_name,
                threshold=resolved.circuit_breaker_threshold,
                cooldown_seconds=resolved.circuit_breaker_cooldown_seconds,
            ),
            sleep=lambda _: None,
            transport=httpx.MockTransport(_refuse),
        )
    return RunContext.create(
        source_name=source_name,
        http=http,
        logger=None,
        clock=clock,
        should_stop=should_stop,
    )


def check_sink_contract(
    sink: Sink[Any], records: Sequence[Mapping[str, object]]
) -> WriteResult:
    """Assert ``sink`` drains its input once and reports what it received.

    ``records`` should hold at least one item; it is replayed through a one-shot
    iterable that has no ``__len__``, which is what the run loop actually hands
    over. Returns the ``WriteResult`` so a caller can make further assertions.
    """
    if not records:
        raise ValueError("pass at least one record, or the checks prove nothing")

    probe = _OneShot(records)
    result = sink.write(probe)

    if not isinstance(result, WriteResult):
        raise ContractViolation(
            f"Sink.write returned {type(result).__name__}, expected WriteResult"
        )
    if probe.passes == 0:
        raise ContractViolation(
            "sink never iterated its input, so nothing was persisted"
        )
    if probe.pulled != len(records):
        raise ContractViolation(
            f"sink consumed {probe.pulled} of {len(records)} records; a sink "
            "must drain the iterable, since the run loop discards it afterwards"
        )
    if result.row_count != len(records):
        raise ContractViolation(
            f"WriteResult.row_count is {result.row_count} but {len(records)} "
            "records were supplied; records_written_total would misreport"
        )
    if result.byte_count is not None and result.byte_count < 0:
        raise ContractViolation(
            f"WriteResult.byte_count is negative ({result.byte_count}); use "
            "None when the volume is not cheaply known"
        )
    return result


def check_source_contract(
    source: Source[Any],
    ctx: RunContext | None = None,
    *,
    require_lazy: bool = True,
) -> list[Mapping[str, object]]:
    """Assert ``source`` is named, streams, and yields mappings.

    ``require_lazy`` enforces that ``fetch`` returns before building every
    record — the property that keeps memory flat regardless of volume. Turn it
    off only for a source whose result is genuinely bounded and small.
    """
    name = getattr(source, "name", None)
    if not isinstance(name, str) or not name:
        raise ContractViolation(
            "Source.name must be a non-empty str; it becomes the `source` label "
            "on every metric series and log line"
        )

    produced = source.fetch(ctx if ctx is not None else make_test_context())
    if require_lazy:
        _reject_materialised(
            produced,
            what="Source.fetch",
            hint="Yield instead, so the run loop streams to the sink.",
        )

    records = list(produced)
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ContractViolation(
                f"Source.fetch yielded {type(record).__name__} at position "
                f"{index}, expected a Mapping"
            )
    return records


def check_transform_contract(
    transform: Transform[Any, Any],
    record: Mapping[str, object],
    ctx: RunContext | None = None,
) -> list[Mapping[str, object]]:
    """Assert ``transform`` yields mappings and leaves its input untouched.

    Zero, one or many outputs are all valid — dropping, normalizing and
    exploding are the point. What is not valid is mutating ``record``: the
    caller still owns it, and in the decoupled topology it is the raw payload
    another worker may replay.
    """
    try:
        before = copy.deepcopy(record)
    except Exception:  # pragma: no cover - exotic values; fall back to shallow
        before = dict(record)

    resolved_ctx = ctx if ctx is not None else make_test_context()
    records = list(transform.transform(record, resolved_ctx))

    if dict(record) != dict(before):
        raise ContractViolation(
            "Transform.transform mutated the record it was given; return new "
            "mappings instead, the caller still owns that one"
        )
    for index, out in enumerate(records):
        if not isinstance(out, Mapping):
            raise ContractViolation(
                f"Transform.transform yielded {type(out).__name__} at position "
                f"{index}, expected a Mapping"
            )
    return records
