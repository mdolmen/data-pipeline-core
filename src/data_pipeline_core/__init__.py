"""data-pipeline-core: reusable plumbing for GCP data-ingestion pipelines.

The public contract is exported here: implement a ``Source``, hand it to a
``WorkerApp`` together with a ``Sink`` (e.g. ``dlt_sink``), and call ``run()``.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from data_pipeline_core.runtime.app import WorkerApp
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.storage.dlt_sink import dlt_sink
from data_pipeline_core.storage.protocols import Record, Sink, Source, WriteResult

try:
    __version__ = version("data-pipeline-core")
except PackageNotFoundError:  # pragma: no cover - source tree without install
    __version__ = "0.0.0"

__all__ = [
    "Record",
    "RunContext",
    "Sink",
    "Source",
    "WorkerApp",
    "WriteResult",
    "__version__",
    "dlt_sink",
]
