# Security Policy

## Reporting a vulnerability

Please report it privately via [GitHub Security Advisories](https://github.com/HirofumiTsuda/dagster-otel/security/advisories/new) rather than a public issue. There's no dedicated security email — this is a solo-maintained project, and the private advisory flow already routes reports only to the maintainer.

Include what you'd include in a normal bug report — steps to reproduce, affected version, and what the actual impact is — plus anything specific to why it's a security issue rather than a correctness one.

## Response

Best-effort, not an SLA. This is maintained by one person outside of paid work, not a company with a security team. If you don't hear back within a reasonable time, a follow-up comment on the advisory is welcome.

## Supported versions

Only the latest released version (PyPI `dagster-otel` / `v*` tag) is supported. There's no LTS or backport policy — fixes land on `main` and go out in the next release.

## Scope

This library reads/writes Dagster run tags (`context.instance.add_run_tags`/`run.tags`) to propagate trace context between steps, and sends trace data via OTLP to whatever endpoint `OTEL_EXPORTER_OTLP_ENDPOINT` names — it doesn't hold credentials of its own beyond that endpoint, and doesn't otherwise write to Dagster's run storage. Reports involving what ends up in trace data (e.g. sensitive values leaking into span attributes or log correlation output) are in scope alongside the library code itself.
