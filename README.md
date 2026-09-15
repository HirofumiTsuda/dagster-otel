# dagster-otel

[![PyPI](https://img.shields.io/pypi/v/dagster-otel)](https://pypi.org/project/dagster-otel/)
[![Python versions](https://img.shields.io/pypi/pyversions/dagster-otel)](https://pypi.org/project/dagster-otel/)
[![Release](https://img.shields.io/github/v/release/HirofumiTsuda/dagster-otel)](https://github.com/HirofumiTsuda/dagster-otel/releases/latest)
[![CI](https://github.com/HirofumiTsuda/dagster-otel/actions/workflows/ci.yml/badge.svg)](https://github.com/HirofumiTsuda/dagster-otel/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/HirofumiTsuda/dagster-otel)](LICENSE)

OpenTelemetry tracing for Dagster ops and assets -- with trace/span IDs correlated into
your own log lines -- without giving up ownership of your op/asset definitions to a
third-party decorator, and without monkeypatching Dagster internals.

**Status: early release, self-tested locally against real Dagster runs (`multiprocess`,
`k8s_job_executor`, retry-from-failure) + a real trace backend.**

```python
from dagster import asset, job, op

from dagster_otel import traced

@op(...)                # Dagster's own @op still owns op-ness; @traced() is a thin
@traced()                # layer underneath. No @resource/required_resource_keys, no
def upstream_op(context) -> int:  # manual "root" step -- the first @traced() step to
    ...                            # run in a given run just becomes the root.

@op(...)
@traced()
def downstream_op(context, x: int) -> int:
    ...

@asset(...)
@traced()  # same decorator, works for assets too
def downstream_asset(context) -> None:
    ...

@job(...)
def my_job():
    downstream_op(upstream_op())
```

Set `OTEL_SERVICE_NAME` and `OTEL_EXPORTER_OTLP_ENDPOINT` (standard OTel env vars) to
name your service and point it at a collector/backend -- `@traced()` configures the
SDK for you the first time it runs in a process, so there's nothing else to wire up.
Call `configure()` yourself only if you want configuration to happen eagerly (e.g.
from a `@resource`) rather than lazily on first use.

Works across Dagster's `multiprocess` and `k8s_job_executor` executors: each step
usually runs in its own process, sometimes on its own node, so trace context is
propagated via Dagster's own run storage rather than in-process memory. See
[docs/design.md](docs/design.md) for how, and what's verified vs. still assumed.

## Why this exists

See [docs/design.md](docs/design.md) for the full rationale, including a comparison
against prior art ([formenergy-observability](https://github.com/Form-Energy/formenergy-observability),
a monkeypatch-based prototype) and the design decisions (no monkeypatching, decorators
stack under Dagster's own `@op`/`@asset` rather than replacing it, log correlation via
a public `logging.Filter` on `context.log`).

## License

MIT.
