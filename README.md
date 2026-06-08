# data-pipeline-core

Reusable Python SDK that provides the **plumbing** for GCP data-ingestion
pipelines — worker runtime, resilience, observability, and storage adapters — so
a new pipeline only writes its business logic (the worker `fetch()`, an optional
`transform()`) and its interface.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the design and [`TODO.md`](TODO.md)
for the sequenced, phased build plan. This repo is co-developed bottom-up with
its first consumer, the `proba-markets-analysis` (sports-betting) pipeline.

## Status

Pre-release (`0.1.0-dev`). The package is being built phase by phase; the public
contract (`WorkerApp`, `Source`, `Sink`, `RunContext`) lands in Phase 1. During
co-development the SDK stays in `0.x` and consumers depend on it via an editable
local path.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```bash
uv sync                    # create the env, install dev tooling
uv run ruff check .        # lint
uv run ruff format --check .
uv run mypy                # strict type-check
uv run pytest              # tests
```

## Using it as a consumer (co-development)

Depend on the SDK via an editable local path while both repos evolve together:

```toml
# <your-project>/pyproject.toml
dependencies = ["data-pipeline-core"]

[tool.uv.sources]
data-pipeline-core = { path = "../data-pipeline-core", editable = true }
```
