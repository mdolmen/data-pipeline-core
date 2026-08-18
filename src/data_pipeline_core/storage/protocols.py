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
    """Implemented per project: THE ingestion business logic."""

    name: str

    def fetch(self, ctx: RunContext) -> Iterable[RecordT_out]: ...


@runtime_checkable
class Transform(Protocol[RecordT_in, RecordT_out]):
    """Optional, per project: parsing / normalization of raw records.

    One record in, zero or more out (normalize, explode, drop). Wired into a
    worker either in-process (between fetch and write) or as its own transform
    worker reading from the raw-landing staging boundary.
    """

    def transform(
        self, record: RecordT_in, ctx: RunContext
    ) -> Iterable[RecordT_out]: ...


@runtime_checkable
class Sink(Protocol[RecordT_in]):
    """Receives the records a run produced and persists them."""

    def write(self, records: Iterable[RecordT_in]) -> WriteResult: ...
