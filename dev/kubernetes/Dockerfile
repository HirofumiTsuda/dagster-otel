# Build from the repo root: docker build -f dev/kubernetes/Dockerfile -t dagster-otel-k8s-e2e:latest .
# (matches dagster-prometheus-exporter's docker/*.Dockerfile convention)
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
# sqlalchemy<2.1: SQLAlchemy 2.1 made psycopg (v3) the default driver for bare
# postgresql:// URLs, which is what dagster-postgres 0.29.22 builds -- but it only
# depends on psycopg2-binary, so the instance fails with "No module named 'psycopg'".
RUN pip install --no-cache-dir . dagster-postgres==0.29.22 dagster-k8s==0.29.22 "sqlalchemy<2.1"

COPY dev/kubernetes/workspace /workspace

# Baked into the image (not mounted via configmap) so that EVERY pod using this
# image -- the runner pod, and every step pod k8s_job_executor launches from the
# same job_image -- automatically has a working DagsterInstance config, without
# needing per-pod volume/configmap wiring for something this simple.
COPY dev/kubernetes/dagster_home /dagster-home
ENV DAGSTER_HOME=/dagster-home
