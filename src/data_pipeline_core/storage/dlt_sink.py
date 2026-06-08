"""``dlt_sink`` — a ``Sink`` backed by dlt's load layer.

Leans on dlt for schema inference and the parquet load; the SDK adds only the
``Sink`` adapter. The concrete destination (local ``file://`` dir vs ``gs://``
bucket) is dlt configuration, not code — a consumer swaps it without forking.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import dlt

from data_pipeline_core.storage.protocols import Record, Sink, WriteResult


@dataclass
class _DltSink:
    dataset: str
    destination: str = "filesystem"
    table_name: str = "records"

    def write(self, records: Iterable[Record]) -> WriteResult:
        row_count = 0

        def _counting() -> Iterator[Record]:
            nonlocal row_count
            for record in records:
                row_count += 1
                yield record

        pipeline = dlt.pipeline(
            pipeline_name=f"dpc_{self.dataset}",
            destination=self.destination,
            dataset_name=self.dataset,
        )
        pipeline.run(
            _counting(),
            table_name=self.table_name,
            loader_file_format="parquet",
        )
        return WriteResult(row_count=row_count)


def dlt_sink(
    dataset: str,
    destination: str = "filesystem",
    *,
    table_name: str = "records",
) -> Sink:
    """A ``Sink`` that loads records as parquet into ``dataset`` via dlt."""
    return _DltSink(dataset=dataset, destination=destination, table_name=table_name)
