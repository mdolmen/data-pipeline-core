"""data-pipeline-core: reusable plumbing for GCP data-ingestion pipelines.

The public contract is exported here: implement a ``Source``, hand it to a
``WorkerApp`` together with a ``Sink`` (e.g. ``dlt_sink``), and call ``run()``.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from data_pipeline_core.ingestion.http import CircuitOpenError
from data_pipeline_core.runtime.app import WorkerApp
from data_pipeline_core.runtime.config import Settings
from data_pipeline_core.runtime.context import RunContext
from data_pipeline_core.storage.dlt_sink import dlt_sink
from data_pipeline_core.storage.ids import deterministic_id
from data_pipeline_core.storage.protocols import (
    Record,
    Sink,
    Source,
    Transform,
    WriteResult,
)
from data_pipeline_core.storage.redis_cache import make_redis, redis_latest_sink
from data_pipeline_core.storage.staging import raw_landing_sink, raw_landing_source

try:
    __version__ = version("data-pipeline-core")
except PackageNotFoundError:  # pragma: no cover - source tree without install
    __version__ = "0.0.0"

__all__ = [
    "CircuitOpenError",
    "Record",
    "RunContext",
    "Settings",
    "Sink",
    "Source",
    "Transform",
    "WorkerApp",
    "WriteResult",
    "__version__",
    "deterministic_id",
    "dlt_sink",
    "make_redis",
    "raw_landing_sink",
    "raw_landing_source",
    "redis_latest_sink",
]
