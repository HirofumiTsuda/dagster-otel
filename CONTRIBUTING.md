# Contributing

## Reporting bugs / requesting features

Open a [GitHub issue](https://github.com/HirofumiTsuda/dagster-otel/issues/new/choose) using the appropriate template. For a security vulnerability, see [SECURITY.md](SECURITY.md) instead — don't file a public issue.

## Submitting a pull request

1. Fork the repo and create a branch from `main`.
2. Make your change.
3. Run the checks below locally and make sure they pass.
4. Open a pull request against `main` and fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md) (`What` / `Why` / `QA` / `Ref`). Link any related issue in the `Ref` section (e.g. `Closes #26`).

CI (`.github/workflows/ci.yml`) runs the same checks on every push and pull request.

## Setting up your toolchain

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. With it installed:

```sh
uv sync --group test --group lint
```

mypy/pyright are pinned exactly in `pyproject.toml`'s `[dependency-groups].lint` (not just "latest") so a local run matches CI — see that group's comment in `pyproject.toml`. ruff is instead pinned by version string directly in CI (`uvx ruff@<version>`), matching `dagster-prometheus-exporter`'s own convention for the same tool.

`examples/` needs the separate `dbt` group (`uv sync --group dbt`) — kept out of the base install since `dagster-dbt` releases track an exact `dagster` version 1:1, and most contributions won't touch it.

## Running checks locally

```sh
uv run pytest
uvx ruff@0.16.7 check .
uv run mypy -p dagster_otel --explicit-package-bases
uv run mypy --explicit-package-bases tests/
uv run pyright src tests
```

## Verifying against real Dagster

`tests/` uses a fake `DagsterInstance`/context and never touches a real Dagster instance or trace backend — useful for proving this library's own logic is internally consistent, not that Dagster still behaves the way it assumes. See `docs/design.md`'s repeated theme: several real bugs here (multi-root trace collisions, `AssetMaterialization` catalog pollution, generator-vs-plain-return handling against a real `@dbt_assets` function) were only ever found by actually running against a live Dagster instance + Jaeger, never from hand-written toy fixtures alone.

`docker compose up -d` starts a local Jaeger (OTLP on `localhost:4317`, UI at `http://localhost:16686`) — point `OTEL_EXPORTER_OTLP_ENDPOINT` at it and run a job with `dagster job execute` to see real spans. This is the only service in the compose file; the Dagster side is just `uv run dagster ...` on the host, not containerized.

If a change touches propagation (`_propagation.py`) or executor-specific behavior, verifying it against something real is expected, not optional:

- `examples/` — a real `@dbt_assets` pipeline (see its own README)
- `dev/kubernetes/` — a `kind`-based setup for verifying against `k8s_job_executor` specifically (see its own README)

Neither runs in CI (a full Dagster run, or a `kind` cluster, is too heavy for every PR) — reproduce them locally.
