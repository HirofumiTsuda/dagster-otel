---
name: verify-tracing
description: Verify a dagster-otel change end to end against a real Dagster run and a real trace backend — confirm spans actually nest correctly across processes, and that log lines actually carry trace_id/span_id. Use whenever changing the propagation mechanism (_propagation.py), the traced() decorator, or the logging filter. Unit tests that mock Dagster's context object cannot catch a change to internal Dagster behavior (op_handle shape, event log query behavior, DagsterLogManager's class hierarchy) that this library depends on.
---

# Verify tracing end to end

This library leans on internal Dagster behavior that isn't a public contract: the
shape of `context.op_handle`, `context.instance.all_logs(...)`, and the fact that
`DagsterLogManager` subclasses `logging.Logger`. A unit test with a mocked context can
prove the Python code runs — it cannot prove Dagster still behaves the way this library
assumes. The whole point of this project (see docs/design.md) is that this behavior
has held for 3+ years unmodified, but that's an empirical finding, not a guarantee —
recheck it against a real instance whenever the propagation or logging code changes.

## 1. Bring up a real trace backend

A local Jaeger all-in-one with OTLP ingest is enough — no vendor account needed.

```sh
docker run -d --name jaeger -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one:latest
```

`OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` and `OTEL_EXPORTER_OTLP_HEADERS_JSON='{}'`
(or whatever env vars `configure()` currently reads) point at it.

## 2. Exercise both @op and @asset, across a real process boundary

Multiprocess (Dagster's default executor) is the case that matters — same-process
tracing works trivially and proves nothing about the propagation mechanism. Write a
small job/asset graph with a root step that calls `publish_trace_context()` and at
least one downstream step decorated with `@traced()`, then:

```sh
dagster job execute -f <file> -j <job_name>
# and separately
dagster asset materialize -f <file> --select '<root>,<child>'
```

Confirm in the run log that steps actually launched in separate subprocesses
(`STEP_WORKER_STARTED - Executing step "..." in subprocess`, with different PIDs) — if
Dagster ever changes its default executor or this repro accidentally runs in-process,
the test proves nothing about cross-process propagation.

## 3. Check the actual trace shape in Jaeger, don't just check for "no exception"

A missing/broken trace context lookup can fail silently in some configurations. Query
Jaeger's API directly and check parent/child span references, not just that the run
succeeded:

```sh
curl -s "http://localhost:16686/api/traces?service=<service_name>&limit=5&lookback=5m" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
for trace in d['data']:
    for s in trace['spans']:
        parent = [r['spanID'] for r in s.get('references', [])]
        print(f\"span={s['spanID']} op={s['operationName']} parent={parent}\")
"
```

The downstream step's span must show the root step's span ID as its parent. If it
shows no parent (a fresh root trace) or the run threw `Could not find trace context`,
propagation broke — check `_find_trace_context`'s `op_handle.path` walk and the
`AssetMaterialization` metadata key first, since those are exactly what a Dagster
internals change would most likely touch.

## 4. Check log correlation separately — it uses a different code path

The trace-context propagation and the logging filter are independent mechanisms
(`_propagation.py` vs `_logging.py`); a change to one does not imply the other still
works. Attach a throwaway `@logger` that dumps non-default `LogRecord` attributes to a
file, and confirm `trace_id`/`span_id` show up on log lines emitted *inside* a span and
are absent *outside* one (scoping matters — a filter that stamps unconditionally is a
bug, not a feature):

```python
class DumpHandler(logging.Handler):
    def emit(self, record):
        default_keys = set(logging.makeLogRecord({}).__dict__.keys())
        extra = {k: v for k, v in record.__dict__.items() if k not in default_keys}
        with open("/tmp/captured.log", "a") as f:
            f.write(f"msg={record.getMessage()!r} extra={extra}\n")
```

Wire it into a `@logger` passed via `logger_defs`, select it in the run config
(`loggers: <name>: {}`), and grep `/tmp/captured.log` for `trace_id`/`span_id` after
the run. Confirm both: present on records logged inside the span, and absent on
records logged outside it.

## 5. Record what was actually checked

Note the Dagster and opentelemetry-sdk versions used (`pip show dagster
opentelemetry-sdk`), whether the executor was genuinely multiprocess, and paste the
actual Jaeger span dump and captured log lines — not just "traces looked fine in the
UI." A screenshot of Jaeger's UI is not reproducible evidence; the API query output is.

## 6. Tear down

```sh
docker rm -f jaeger
```
