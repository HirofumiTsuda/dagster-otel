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
`dagster_workspace/definitions.py` wraps its `@dbt_assets` function with
`traced_dbt()` (`dagster_otel.dbt`, see [Issue #8](https://github.com/HirofumiTsuda/dagster-otel/issues/8))
-- a child span per dbt node, not just one span for the whole step.

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

A local Jaeger with OTLP ingest is enough to see the result -- `docker compose up -d`
from the repo root starts one (see the top-level `docker-compose.yaml`).

```sh
DAGSTER_HOME=/tmp/dagster-otel-jaffle-shop \
OTEL_SERVICE_NAME=jaffle_shop_example \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
uv run dagster asset materialize -f examples/dagster_workspace/definitions.py --select '*'
```

Open http://localhost:16686, select the `jaffle_shop_example` service, and you
should see a `jaffle_shop_dbt_assets` span with one child span per dbt node
(`raw_customers`, `stg_customers`, `customers`, each with their own dbt test spans
nested underneath) -- confirmed real, accurate durations per node, not zero-width
markers (see Issue #8 / `docs/design.md`).

### Against Grafana Tempo instead (through a real OTel Collector)

`docker compose up -d` also starts `tempo` and `otel-collector` (Issue #42) -- point
at the Collector, not Tempo directly, since that's the real deployment shape this
verifies:

```sh
DAGSTER_HOME=/tmp/dagster-otel-jaffle-shop \
OTEL_SERVICE_NAME=jaffle_shop_example \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4327 \
uv run dagster asset materialize -f examples/dagster_workspace/definitions.py --select '*'
```

Tempo has no UI of its own to click through -- query its API instead:

```sh
curl -s --get http://localhost:3200/api/search --data-urlencode 'q={}' | python3 -m json.tool
```

Find the trace with `serviceStats.jaffle_shop_example.spanCount: 16`, then
`curl http://localhost:3200/api/traces/<traceID>` for the full span tree (same
`step -> asset -> check` shape as the Jaeger case above, see `docs/design.md`).

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
