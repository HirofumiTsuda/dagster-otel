# Runs examples/dagster_workspace (the jaffle_shop @dbt_assets pipeline, already
# wrapped in traced_dbt()) as a persistent `dagster dev` webserver, for Issue #56's
# combined demo -- dagster-prometheus-exporter needs a live GraphQL endpoint to scrape,
# which the one-shot `dagster asset materialize` command examples/README.md's basic
# walkthrough uses doesn't provide. Mirrors dagster-prometheus-exporter's own
# docker/dagster-dev.Dockerfile conventions (same author, same dev-stack shape).
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV DAGSTER_HOME=/app/dev/jaffle_shop_dagster_home

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --group dbt --group demo

COPY src/ src/
COPY examples/ examples/

RUN mkdir -p /app/dev/jaffle_shop_dagster_home

EXPOSE 3000

# `dagster dev` (unlike a one-shot `dagster asset materialize`) prepares the dbt
# manifest itself via DbtProject.prepare_if_dev() at module import time -- no separate
# `dbt parse` step needed here, see examples/README.md.
CMD ["uv", "run", "--group", "dbt", "--group", "demo", "dagster", "dev", "-f", "examples/dagster_workspace/definitions.py", "-h", "0.0.0.0", "-p", "3000"]
