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

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Compatibility](#compatibility)
- [Why this exists](#why-this-exists)
- [Contributing](#contributing)
- [License](#license)

## Installation

```sh
pip install dagster-otel
```

## Usage

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

@op(...)
@traced  # bare works too, like @op/@asset themselves -- same as @traced()
def another_op(context) -> None:
    ...

@job(...)
def my_job():
    downstream_op(upstream_op())
```

Works across Dagster's `multiprocess` and `k8s_job_executor` executors: each step
usually runs in its own process, sometimes on its own node, so trace context is
propagated via Dagster's own run storage rather than in-process memory. See
[docs/design.md](docs/design.md) for how, and what's verified vs. still assumed.

## Configuration

Standard OTel environment variables -- nothing bespoke:

| Variable | Purpose |
| --- | --- |
| `OTEL_SERVICE_NAME` | Names your service in the trace backend. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._TRACES_ENDPOINT`) | Where to send spans (e.g. `http://localhost:4317`). |
| `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` / `..._TIMEOUT` | Per-export timeout. Set this yourself if the default (2s) doesn't fit -- see [docs/design.md](docs/design.md) for why a default exists at all (an unreachable collector otherwise blocked every step for ~7s). |

`@traced()` reads these itself (idempotently) the first time it runs in a process --
there's nothing else to wire up, no `@resource`/`required_resource_keys` needed. Call
`configure()` yourself only if you want configuration to happen eagerly (e.g. at
`Definitions` load time) rather than lazily on first use.

## Compatibility

Built and verified against **Dagster 1.13.22** and **`opentelemetry-sdk` 1.44.0**
(`pyproject.toml`/`uv.lock`) -- this is the combination every behavior described here
has actually been checked against, including the `multiprocess`/`k8s_job_executor`/
retry-from-failure verification in [docs/design.md](docs/design.md). `pyproject.toml`
declares a much wider floor (`dagster >= 1.5`) since nothing here relies on
version-specific Dagster internals beyond what's documented as an accepted-risk
private-API dependency there -- but that wide range isn't individually spot-checked
the way it is for [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter#compatibility).
If you hit an incompatibility on another version, please
[open an issue](https://github.com/HirofumiTsuda/dagster-otel/issues/new/choose).

Requires Python 3.10+ (matches Dagster's own floor).

## Why this exists

See [docs/design.md](docs/design.md) for the full rationale, including a comparison
against prior art ([formenergy-observability](https://github.com/Form-Energy/formenergy-observability),
a monkeypatch-based prototype) and the design decisions (no monkeypatching, decorators
stack under Dagster's own `@op`/`@asset` rather than replacing it, log correlation via
a public `logging.Filter` on `context.log`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to set up your toolchain, run checks
locally, and submit a pull request. Bug reports and feature requests go through
[GitHub issues](https://github.com/HirofumiTsuda/dagster-otel/issues/new/choose); a
security vulnerability goes to [SECURITY.md](SECURITY.md) instead.

## License

MIT -- see [LICENSE](LICENSE).
