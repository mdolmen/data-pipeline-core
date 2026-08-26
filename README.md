# data-pipeline-core

Reusable Python SDK that provides the **plumbing** for GCP data-ingestion
pipelines — worker runtime, resilience, observability, and storage adapters — so
a new pipeline only writes its business logic (the worker `fetch()`, an optional
`transform()`) and its interface.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the design, [`TODO.md`](TODO.md)
for the sequenced, phased build plan, and [`DEVELOPMENT.md`](DEVELOPMENT.md) for
the strategy and decision log. This repo is co-developed bottom-up with
its first consumer, the `proba-markets-analysis` (sports-betting) pipeline.

## Status

`v0.1.0` — the first cut. The public contract (`WorkerApp`, `Source`, `Sink`,
`RunContext`) is in place and changing it is a SemVer event. Still `0.x` while
co-development continues, so the contract can move on a minor bump.

Distributed by **git tag**, not a package registry: a tag pins an immutable
version without a publish pipeline or private-index credentials in CI and in
every image build.

## Usage

Write a `Source`; the SDK owns everything else. `WorkerApp.run()` returns a
process exit code, so a worker's `main` is one line.

```python
from collections.abc import Iterable

from data_pipeline_core import Record, RunContext, WorkerApp, dlt_sink


class OddsSource:
    name = "acme"  # becomes the `source` label on every metric and log line

    def fetch(self, ctx: RunContext) -> Iterable[Record]:
        for page in range(10):
            if ctx.should_stop():          # SIGTERM: finish cleanly, don't die mid-write
                return
            response = ctx.http.get(f"https://api.acme.test/odds?page={page}")
            if response.is_success:
                yield from response.json()["items"]


if __name__ == "__main__":
    raise SystemExit(WorkerApp(OddsSource(), dlt_sink("acme_odds")).run())
```

Calls go through `ctx.http` to inherit retry with jitter, User-Agent rotation,
the circuit breaker, the IP guard and per-response metrics. `fetch` yields, so
records stream to the sink instead of accumulating in memory.

Configuration is environment-driven:

```bash
DESTINATION__FILESYSTEM__BUCKET_URL=gs://acme-curated   # local file:// in dev
REDIS_URL=rediss://…          # unset → IP guard off, breaker is per-run only
PROXY_URL=http://…            # unset → direct; PROXY_ENABLED=false to force direct
IMPERSONATE=chrome            # unset → plain httpx; set → browser-TLS via curl_cffi
METRICS_OTLP_URL=https://…    # unset → metrics push is skipped (logged)
```

To add project settings, subclass `Settings` and pass an instance to
`WorkerApp(..., settings=...)`.

### Two-worker topology

For pipelines that separate ingestion from parsing, an **ingest** worker lands
raw payloads and a **transform** worker replays them — so a parser bug never
loses data, and re-parsing needs no re-fetch:

```python
# ingest worker: land raw, nothing more
WorkerApp(OddsSource(), raw_landing_sink("odds-raw")).run()

# transform worker: replay raw → normalize → curated
WorkerApp(
    raw_landing_source("odds-raw"),
    dlt_sink("acme_odds", primary_key="id"),   # primary_key → idempotent merge
    transform=Normalize(),
).run()
```

Pass `primary_key` and replaying becomes an upsert rather than a duplicate;
`deterministic_id(...)` builds a stable key from a record's identifying fields.
A single worker can also do all of it in one pass by wiring
`source + transform + sink` together.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11+.

```bash
uv sync                    # create the env, install dev tooling
uv run ruff check .        # lint
uv run ruff format --check .
uv run mypy                # strict type-check
uv run pytest              # tests
```

## Using it as a consumer

Pin a released version by tag:

```toml
# <your-project>/pyproject.toml
dependencies = ["data-pipeline-core"]

[tool.uv.sources]
data-pipeline-core = { git = "ssh://git@github.com/mdolmen/data-pipeline-core", tag = "v0.1.0" }
```

While co-developing the SDK against a consumer, swap that for an editable local
path so edits land without a tag-and-bump cycle — the two repos then have to sit
side by side:

```toml
[tool.uv.sources]
data-pipeline-core = { path = "../data-pipeline-core", editable = true }
```
