# examples

Not sample code to read -- a real, runnable Dagster pipeline used to verify
`dagster-otel` against something more real than the hand-written toy ops/assets in
`tests/`. See [Issue #2](https://github.com/HirofumiTsuda/dagster-otel/issues/2) for
why this exists.

`jaffle_shop/` is a small hand-written dbt project (seed → staging model → mart
model), copied from
[`dagster-prometheus-exporter`](https://github.com/HirofumiTsuda/dagster-prometheus-exporter)'s
dev fixture of the same name (same author, same license, not vendored from anywhere
else -- see `jaffle_shop/README.md` for its own attribution notes).
`dagster_workspace/definitions.py` wraps its `@dbt_assets` function with `@traced()`.

## Setup

```sh
uv sync --group dbt
```

Generate the dbt manifest once (`dagster dev` does this automatically via
`DbtProject.prepare_if_dev()`, but a one-shot `dagster asset materialize` run does
not -- see the comment in `dagster_workspace/definitions.py`). `profiles.yml` reads
`DAGSTER_HOME` (must be set and exist):

```sh
mkdir -p /tmp/dagster-otel-jaffle-shop
cd examples/jaffle_shop && DAGSTER_HOME=/tmp/dagster-otel-jaffle-shop uv run --project .. dbt parse --profiles-dir .
```

## Running

A local Jaeger with OTLP ingest is enough to see the result:

```sh
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```

```sh
DAGSTER_HOME=/tmp/dagster-otel-jaffle-shop \
OTEL_SERVICE_NAME=jaffle_shop_example \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
uv run dagster asset materialize -f examples/dagster_workspace/definitions.py --select '*'
```

Open http://localhost:16686, select the `jaffle_shop_example` service, and you
should see a single `jaffle_shop_dbt_assets` span covering the whole `dbt build` run
(one Dagster step materializes every dbt model regardless of how many there are --
this is expected, not a bug).

### Log correlation

`capturing_logger` in `definitions.py` needs to be explicitly selected via run
config, same as any custom `@logger` (confirmed: `Definitions(loggers=...)` alone
isn't enough to activate one for `dagster asset materialize`, matching how
`logger_defs` works for `@job`s -- relevant to
[dagster-io/dagster#12495](https://github.com/dagster-io/dagster/discussions/12495)):

```yaml
# run_config.yaml
loggers:
  capturing_logger: {}
```

```sh
DAGSTER_OTEL_EXAMPLE_LOG_CAPTURE=/tmp/jaffle_shop_captured.log \
DAGSTER_HOME=/tmp/dagster-otel-jaffle-shop \
OTEL_SERVICE_NAME=jaffle_shop_example \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
uv run dagster asset materialize -f examples/dagster_workspace/definitions.py --select '*' \
  --config run_config.yaml
```

`/tmp/jaffle_shop_captured.log` should show `trace_id`/`span_id` in `extra` on every
line, including dbt's own progress messages ("Running dbt command...") -- those
route through `get_dagster_logger()`, not `context.log` directly, which is why
`traced()` filters both (see `_tracing.py`).
