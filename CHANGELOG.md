# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/).

## [0.3.2](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.3.2) - 2026-09-21

### Fixed

- `AssetExecutionContext.run_id` deprecation warning, introduced in dagster 1.13.23 ([#68](https://github.com/HirofumiTsuda/dagster-otel/issues/68), [#69](https://github.com/HirofumiTsuda/dagster-otel/pull/69)). Switched to `context.run.run_id`, which carries no deprecation on either `OpExecutionContext` or `AssetExecutionContext` and has been available since this library's declared `dagster >= 1.5` floor.

## [0.3.1](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.3.1) - 2026-09-20

### Fixed

- Root spans no longer carry a fake, dangling `parentSpanId` ([#63](https://github.com/HirofumiTsuda/dagster-otel/issues/63), [#64](https://github.com/HirofumiTsuda/dagster-otel/pull/64)). Trace-id determinism for a Dagster run is now achieved via a custom `IdGenerator` instead of seeding a placeholder parent span -- fixes backends like Tempo failing to resolve `rootServiceName`, which also caused Grafana's native traces panel to show no data for affected traces.

## [0.3.0](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.3.0) - 2026-09-18

### Added

- Honor `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` (`grpc` or `http/protobuf`) when configuring the OTLP exporter ([#48](https://github.com/HirofumiTsuda/dagster-otel/issues/48), [#59](https://github.com/HirofumiTsuda/dagster-otel/pull/59)).

### Chore

- Add PyPI keywords and Topic classifiers for discoverability ([#58](https://github.com/HirofumiTsuda/dagster-otel/pull/58)).

## [0.2.1](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.2.1) - 2026-09-16

### Changed

- Deduped redundant run-storage fetches ([#35](https://github.com/HirofumiTsuda/dagster-otel/issues/35), [#40](https://github.com/HirofumiTsuda/dagster-otel/pull/40)) -- `_ancestor_runs()` now fetches each `DagsterRun` in the retry-from-failure ancestor chain exactly once and reuses it across every lookup that needs it, instead of independently re-walking and re-fetching the same chain. A single `_traced_span()` call with `U` real upstream dependencies on a run with `N` ancestor hops previously issued on the order of `(U + 1) * 2N` `get_run_by_id` calls where `N` suffices. No public API or behavior change -- purely fewer round-trips to run storage.

## [0.2.0](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.2.0) - 2026-09-16

### Added

- **`traced_dbt()`** (`dagster_otel.dbt`) -- per-dbt-node spans for `@dbt_assets`, keyed by the real Dagster `asset_key`/`check_name` (not dbt's internal `unique_id`), nested `step -> asset -> check`. Drop-in for `@traced()`, zero changes to your `@dbt_assets` function body. Verified: 16 spans against the real jaffle_shop example (1 step + 3 assets + 12 checks), accurate durations matching what dbt itself measured.
- **Span attributes** -- every span now carries `dagster.run_id`, `dagster.job_name`, `dagster.step_key`, and `dagster.retry_number`, matching the same attribute set Airflow's native tracing and Leoflow's ADR 0010 independently converge on.
- **RetryPolicy attempts are linked** -- a retried step's span now carries a `Link` back to the attempt it retried, alongside its real dependency-graph parent (unchanged).
- **Externally-provided trace context** -- set `EXTERNAL_TRACE_CONTEXT_TAG_KEY` as a run tag at launch time to nest a whole Dagster run's trace under an external caller's (a CI/CD pipeline, a scheduler, another OTel-instrumented system), instead of always starting a fresh, unrelated trace.
- **Bare `@traced`/`@traced_dbt`** -- both now work without parens, matching `@op`/`@asset`. (Bare use previously failed silently rather than raising.)

### Fixed

- **No surprise network calls** -- `configure()` no longer installs a real OTLP exporter unless `OTEL_EXPORTER_OTLP_ENDPOINT`/`..._TRACES_ENDPOINT` (or `OTEL_SDK_DISABLED`) is actually set. Trying `@traced()` with zero setup now costs nothing, instead of a bounded-but-real network attempt on every step.

### Chore

- `docker-compose.yaml` for a local Jaeger (`docker compose up -d`).
- `CONTRIBUTING.md`, `SECURITY.md`, issue/PR templates, README restructuring (Installation/Configuration/Compatibility/Contributing sections).
- Dependabot enabled (`uv` + `github-actions` ecosystems).

## [0.1.0](https://github.com/HirofumiTsuda/dagster-otel/releases/tag/v0.1.0) - 2026-09-15

First release.

OpenTelemetry tracing for Dagster ops and assets, via a `@traced()` decorator that stacks under Dagster's own `@op`/`@asset` -- no monkeypatching, no giving up ownership of your op/asset definitions.

### Highlights

- **Cross-process trace propagation** via Dagster's own run storage -- verified against `multiprocess`, `k8s_job_executor` (real `kind` cluster, separate pods), and retry-from-failure runs. Correct parent-span attribution for multi-root and fan-in graphs (real dependency graph, not subgraph-path guessing).
- **Log correlation** -- `trace_id`/`span_id` available to any `@logger` via a public `logging.Filter` on `context.log` (and `get_dagster_logger()`).
- **Self-configuring** -- `@traced()` alone is enough; no `@resource`/`required_resource_keys` wiring, honors standard `OTEL_*` env vars.
- Verified against a real `@dbt_assets` pipeline (`examples/`), not just hand-written toy fixtures.

See [docs/design.md](docs/design.md) for the full design rationale, prior-art comparison, and every verification writeup.
