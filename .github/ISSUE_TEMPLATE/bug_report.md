---
name: Bug report
about: Report a problem with dagster-otel
title: ""
labels: bug
---

## Description

What happened, and what did you expect instead?

## Steps to reproduce

## Environment

- `dagster-otel` version: (`python -c "import dagster_otel; print(dagster_otel.__version__)"`)
- Dagster version:
- Executor (`in_process` / `multiprocess` / `k8s_job_executor` / other):
- Trace backend (Jaeger / Tempo / etc.), if relevant:

## Trace / log output

Relevant span data from your trace backend, or `context.log`/`get_dagster_logger()` output with `trace_id`/`span_id` attributes, if the issue is about propagation or log correlation specifically.
