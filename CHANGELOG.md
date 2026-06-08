# Changelog

All notable changes to `data-pipeline-core` are recorded here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/). During
co-development the SDK stays in `0.x`; breaking changes are free until `v0.1.0`
is cut.

## [Unreleased] — `0.1.0-dev`

### Added

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
| Runtime dependencies | Deferred — added per phase | Tracer-bullet, bottom-up; avoid speculative weight. The package ships with empty `dependencies` until a real consumer demands each (e.g. `dlt` in Phase 1, `prometheus-client` in Phase 3). |
| `prometheus-client` | Dev-only for now | Only used by the Phase-0 test registry fixture; promotes to a runtime dep when `obs/metrics.py` lands (Phase 3). |
