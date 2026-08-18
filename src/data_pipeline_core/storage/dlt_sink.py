"""``dlt_sink`` — a ``Sink`` backed by dlt's load layer.

Leans on dlt for schema inference and the parquet load; the SDK adds only the
``Sink`` adapter. The concrete destination (local ``file://`` dir vs ``gs://``
bucket) is dlt configuration, not code — a consumer swaps it without forking.

Passing ``primary_key`` makes writes idempotent: dlt runs a ``merge`` keyed on
it, so replaying the same records upserts instead of appending. On the
filesystem destination that requires a table format, so Delta Lake is selected
automatically (ACID merge on plain objects, no database service).
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import dlt

from data_pipeline_core.storage.protocols import Sink, WriteResult


@dataclass
class _DltSink:
    dataset: str
    destination: str = "filesystem"
    table_name: str = "records"
    primary_key: str | tuple[str, ...] | None = None
    table_format: str | None = None

    def write(self, records: Iterable[Mapping[str, object]]) -> WriteResult:
        row_count = 0

        def _counting() -> Iterator[Mapping[str, object]]:
            nonlocal row_count
            for record in records:
                row_count += 1
                yield record

        run_kwargs: dict[str, Any] = {"table_name": self.table_name}
        table_format = self.table_format
        if self.primary_key is not None:
            run_kwargs["primary_key"] = self.primary_key
            run_kwargs["write_disposition"] = "merge"
            # filesystem merge needs a table format; Delta gives ACID upsert.
            if table_format is None and self.destination == "filesystem":
                table_format = "delta"
        if table_format is not None:
            run_kwargs["table_format"] = table_format
        else:
            run_kwargs["loader_file_format"] = "parquet"

        pipeline = dlt.pipeline(
            pipeline_name=f"dpc_{self.dataset}",
            destination=self.destination,
            dataset_name=self.dataset,
        )
        pipeline.run(_counting(), **run_kwargs)
        return WriteResult(row_count=row_count)


def dlt_sink(
    dataset: str,
    destination: str = "filesystem",
    *,
    table_name: str = "records",
    primary_key: str | tuple[str, ...] | None = None,
    table_format: str | None = None,
) -> Sink[Mapping[str, object]]:
    """A ``Sink`` that loads records via dlt; ``primary_key`` → idempotent merge."""
    return _DltSink(
        dataset=dataset,
        destination=destination,
        table_name=table_name,
        primary_key=primary_key,
        table_format=table_format,
    )
