# Changelog

All notable changes to `data-pipeline-core` are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/). During
co-development the SDK stays in `0.x`; breaking changes are free until `v0.1.0`
is cut.

## [Unreleased] — `0.1.0-dev`

### Added

- **Phase 1 — walking skeleton.** The core contract
  (`Source` / `Sink` / `Record` / `WriteResult`), a minimal `RunContext`
  (run_id, logger, clock), `WorkerApp(source, sink).run()` streaming
  `fetch()` → `write()` with a clean exit code, and `dlt_sink(dataset,
  destination)` loading records as parquet (filesystem / GCS). Public API is
  exported from `data_pipeline_core`. Validated by a real betting worker
  (Betclic Ligue 1) landing parquet end-to-end.
- **Phase 0 — repo & tooling.** uv + hatchling packaging (`src/` layout),
  `ruff` (lint + format), `mypy --strict`, `pytest`, `pre-commit`, and a CI
  workflow (lint → type → test). Test scaffolding (`conftest.py`): `fake_redis`
  (fakeredis), `metrics_registry` (isolated Prometheus registry), and the
  `httpx_mock` fixture from pytest-httpx. The package is a hollow but importable
  scaffold; the public contract arrives in Phase 1.

## Decision log

What went **into the SDK**, what stayed **in the consuming project**, and what is
**deferred** (generalize on the 2nd real usage). This briefs the future
Polytricks instance — see `TODO.md`.

| Item | Decision | Rationale |
|---|---|---|
| Runtime dependencies | Deferred — added per phase | Tracer-bullet, bottom-up; avoid speculative weight. The package ships with only what a real consumer demands (e.g. `dlt` in Phase 1, `prometheus-client` in Phase 3). |
| `prometheus-client` | Dev-only for now | Only used by the Phase-0 test registry fixture; promotes to a runtime dep when `obs/metrics.py` lands (Phase 3). |
| `dlt` (load layer) | SDK, runtime dep | Generic schema-inference + parquet load behind `dlt_sink`; the canonical/business schema stays in the project. |
| Destination (`file://` vs `gs://`) | SDK, config-driven | One `dlt_sink`; local vs GCS is dlt config (`DESTINATION__FILESYSTEM__BUCKET_URL`), not a code fork. |
| `WorkerApp(source, sink)` signature | SDK, minimal | `transform=` / `settings=` are deferred to their phases (6 / 2) as non-breaking optional params, not added speculatively. |
| Betting odds schema / `bookmaker="betclic"` | Stays in betting | Business data model and constants live in the consumer, never the SDK. |
