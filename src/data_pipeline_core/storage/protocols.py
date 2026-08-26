"""The core contract: the typed slots a consuming project fills.

The SDK owns the run loop (``WorkerApp``); a project implements ``Source`` (its
ingestion business logic) and provides a ``Sink`` (usually the SDK's
``dlt_sink``). ``Record`` and ``WriteResult`` are the data that flows between
them. Changing any of these is a SemVer-major event — keep it small and stable.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from data_pipeline_core.runtime.context import RunContext

Record = dict[str, Any]

# The record type each slot moves. Bound to ``Mapping``, not ``dict``: a
# ``TypedDict`` (the way a project pins its own schema) is assignable to a
# read-only mapping but not to a mutable ``dict``, which could be mutated out of
# shape. Variance follows position — a source only ever *returns* records, a sink
# only ever *accepts* them — and mypy rejects the protocol outright if it doesn't.
# ``_out`` is covariant (produced), ``_in`` contravariant (consumed).
RecordT_out = TypeVar("RecordT_out", bound=Mapping[str, object], covariant=True)
RecordT_in = TypeVar("RecordT_in", bound=Mapping[str, object], contravariant=True)


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of a ``Sink.write`` call.

    ``byte_count`` is the volume written when the sink can report it cheaply
    (e.g. raw landing sums its JSONL payload); sinks that can't leave it ``None``
    and the ``bytes_written_total`` metric stays flat for that run.
    """

    row_count: int
    byte_count: int | None = None


@runtime_checkable
class Source(Protocol[RecordT_out]):
    """Implemented per project: THE ingestion business logic.

    ``name`` identifies the source and becomes the ``source`` label on every
    metric series and log line, so keep it stable — dashboards key off it.
    """

    name: str

    def fetch(self, ctx: RunContext) -> Iterable[RecordT_out]:
        """Produce this run's records. Called once per run.

        Prefer a generator: the run loop hands the iterable straight to the sink
        without materialising it, so yielding keeps memory flat regardless of
        volume. Make outbound calls through ``ctx.http`` to inherit retry,
        backoff, UA rotation, the circuit breaker and the IP guard — a bare
        ``httpx`` call gets none of them and is invisible to the metrics. In a
        long loop, check ``ctx.should_stop()`` between items and return early
        when it is true, so a SIGTERM ends the run cleanly instead of killing it
        mid-write.

        Raising fails the run (exit code 1, ``worker_up=0``); the exception is
        logged with a traceback by the run loop.
        """
        ...


@runtime_checkable
class Transform(Protocol[RecordT_in, RecordT_out]):
    """Optional, per project: parsing / normalization of raw records.

    One record in, zero or more out (normalize, explode, drop). Wired into a
    worker either in-process (between fetch and write) or as its own transform
    worker reading from the raw-landing staging boundary.
    """

    def transform(self, record: RecordT_in, ctx: RunContext) -> Iterable[RecordT_out]:
        """Map one input record to zero or more output records.

        Yield nothing to drop a record, once to normalize it, many times to
        explode it. Applied lazily between ``fetch`` and ``write``, so the
        streaming property of the chain is preserved.
        """
        ...


@runtime_checkable
class Sink(Protocol[RecordT_in]):
    """Receives the records a run produced and persists them."""

    def write(self, records: Iterable[RecordT_in]) -> WriteResult:
        """Persist the run's records and report what was written.

        ``records`` is a lazy iterable that can only be consumed once — iterate
        it directly rather than taking ``len()``, and count as you go. The
        returned ``WriteResult`` drives ``records_written_total`` and
        ``bytes_written_total``; leave ``byte_count`` as ``None`` when volume
        isn't cheaply known.
        """
        ...
