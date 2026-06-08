"""data-pipeline-core: reusable plumbing for GCP data-ingestion pipelines.

The public contract (``WorkerApp``, ``Source``, ``Sink``, ``RunContext``) is
introduced in later build phases. This Phase-0 scaffold only establishes the
package so a consumer can wire the editable dependency and confirm the import.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("data-pipeline-core")
except PackageNotFoundError:  # pragma: no cover - source tree without install
    __version__ = "0.0.0"

__all__ = ["__version__"]
