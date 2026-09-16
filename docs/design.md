# Design

**Status: design doc / early prototype. Not published yet.**

This is the rationale, prior-art comparison, and decisions behind dagster-otel --
what to read before changing the public API or the propagation/log-correlation
mechanisms. See the top-level [README](../README.md) for what the project is and how
to use it.

## The gap this fills

Dagster has no built-in OpenTelemetry support. Two long-standing, still-open upstream
issues track this:

- [dagster-io/dagster#11191](https://github.com/dagster-io/dagster/issues/11191) --
  "Implement a default telemetry provider parallel to the default logger (OTel /
  OpenTelemetry)" (opened 2022-12-16). The comment thread drifts into people wanting
  Prometheus metrics specifically (see
  [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter),
  a separate project of mine, for that). But it's also where the two pieces of prior
  art below surfaced.
- [dagster-io/dagster#12353](https://github.com/dagster-io/dagster/issues/12353) --
  "Support for OpenTelemetry traces and span/trace ids in log lines" (opened
  2023-02-15). Two asks in one issue: traces, and correlating trace/span IDs into log
  lines. Both are in scope -- see below; the log correlation piece turned out to be
  achievable without touching Dagster internals at all, via a public API nobody
  building prior art here seems to have used.

## Prior art, and why this project exists anyway

| | Approach | Status | Gap |
| --- | --- | --- | --- |
| [bradlangel/dagster-opentelemetry](https://github.com/bradlangel/dagster-opentelemetry) | (none) | Empty. One commit, "Initial project structure and design document," README only, 0 stars/forks. Never implemented. | -- |
| [Form-Energy/formenergy-observability](https://github.com/Form-Energy/formenergy-observability) | `@otel_op()` decorator that **replaces** `@op` (calls it internally); trace context propagated cross-process via Dagster's event log | Real, used in production at Form Energy for 3+ years, MIT licensed. Last commit 2023-11-21. | **No `@asset` support** (maintainer confirmed in [issue #1](https://github.com/Form-Energy/formenergy-observability/issues/1): "entrenched in ops," no plans to add it). The decorator also takes over the op definition itself -- see below. |
| [aaaaahaaaaa's monkeypatch](https://github.com/dagster-io/dagster/issues/11191#issuecomment-1899092782) | Monkeypatches `OpDefinition.compute_fn` and `dagster._core.execution.api.job_execution_iterator` at process start; zero code changes to existing jobs | Prototype in an issue comment, not a package | Patches private Dagster internals ("no doubt this is highly discouraged by the Dagster team," per the author). Author's own finding: trace context only nests correctly under the `in_process` executor -- "I don't see any way to pass the OTPL context between processes." No solution for multiprocess/k8s. |

None of the above address log/trace correlation (the other half of #12353) at all.

Verified in this session (2026-09-15, against a from-scratch venv: Dagster 1.13.22,
opentelemetry-sdk 1.44.0, local Jaeger with OTLP ingest):

- formenergy-observability's core trick -- publish the trace context as
  `AssetMaterialization` metadata, read it back via `context.instance.all_logs(...)`
  -- still works unmodified, 3 years after its last commit, across genuinely separate
  OS processes (`multiprocess` executor). aaaaahaaaaa's "no way to pass context between
  processes" turns out to already have a solution; it's just in a different repo.
- The same trick works for `@asset`, not just `@op`, with zero changes to the
  propagation logic -- only the outer decorator needs to call `@asset(...)` instead of
  `@op(...)`. This was expected once you know `@asset` wraps an `OpDefinition`
  internally (same `op_handle`, same `context.instance`), not a surprise, but it means
  the "no asset support" gap is a few lines of unwritten code, not a design limitation.

So the trace-context-via-event-log mechanism is sound and already proven. What's worth
rebuilding is the **decorator shape**.

## Design decision: stack under `@op`/`@asset`, don't replace it

formenergy-observability's `@otel_op()` owns the op definition:

```python
@otel_op()   # this calls @op(...) internally -- otel_op *is* the op decorator
def my_op(context):
    ...
```

To trace an op you already have, you replace its `@op` with `@otel_op()` -- meaning
the library, not Dagster, decides what makes this function an op. That's an awkward
place for a tracing library to sit.

Instead, `dagster-otel` provides a plain function decorator meant to stack **under**
Dagster's own `@op`/`@asset`, which keeps full, unmodified ownership of the op/asset
definition:

```python
from dagster_otel import traced

@op(...)          # Dagster's own decorator owns op-ness, unchanged
@traced()         # thin layer added underneath, does nothing Dagster-specific
def my_op(context, upstream: str) -> str:
    ...
```

Verified in this session: this composition preserves Dagster's type inference through
the wrapper (`functools.wraps` sets `__wrapped__`, which `inspect.signature()` follows
by default -- confirmed via `STEP_INPUT ... Type check passed` in the run log, not just
assumed), works for both `@op` and `@asset`, and produces correctly-nested cross-process
spans in Jaeger identically to formenergy-observability's op-owning version.

## Design decision: no monkeypatching

Rejected, even though it's the only approach that needs zero decorators on existing
code. Reasons:

- It patches private, undocumented Dagster internals
  (`dagster._core.execution.api.job_execution_iterator`, `OpDefinition.compute_fn`),
  which are far less stable than even Dagster's GraphQL API (see the compatibility
  notes in [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter#compatibility)
  for how much GraphQL itself already shifts between versions).
- Its own author calls it "highly discouraged by the Dagster team" and "use at your
  own risk."
- It uses `unittest.mock.patch` in production code, not a test.
- It doesn't solve cross-process propagation on its own -- combining it with the
  event-log trick would still be needed, at which point the only thing monkeypatching
  buys is "no decorator on each function," at the cost of an entire extra fragility
  axis.

The "no decorator on existing code" convenience monkeypatching offers is real, but
`opentelemetry-instrument`-style zero-code auto-instrumentation is a specific,
recognized OTel pattern (implemented via `BaseInstrumentor` + an
`opentelemetry_instrumentor` entry point) that this project is deliberately not
attempting -- which is also why this package is **not** named
`opentelemetry-instrumentation-dagster`; that name implies exactly the zero-code
contract this project opts out of.

One concrete consequence, checked against
[opentelemetry-python-contrib's CONTRIBUTING.md](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/CONTRIBUTING.md):
new instrumentation packages there are required to extend `BaseInstrumentor` and
support auto-instrumentation via entry points. So this design can never be merged into
the official contrib monorepo, regardless of license -- it's a structural mismatch, not
a licensing one. (The much lower-bar [OTel ecosystem registry](https://opentelemetry.io/ecosystem/registry/),
by contrast, lists projects under any license, MIT included, and isn't gated on the
`BaseInstrumentor` pattern. That's the realistic path to any official-ish visibility,
independent of license choice -- see [License](#license) below.)

## Log/trace correlation, via a public API

`context.log` (`DagsterLogManager`) is, concretely, a subclass of Python's standard
`logging.Logger` (`class DagsterLogManager(logging.Logger)`, see
`dagster/_core/log_manager.py`). That means every ordinary `logging` extension point
works on it, including `Logger.addFilter()` -- the standard library's own documented
mechanism for attaching contextual data to log records
([logging cookbook](https://docs.python.org/3/howto/logging-cookbook.html#using-filters-to-impart-contextual-information)).

Verified in this session: attaching a `logging.Filter` to `context.log` that stamps
`record.trace_id`/`record.span_id` from the currently-active OTel span, then calling
`context.log.info(...)`, produces exactly what you'd want:

```
CAPTURED msg='...hello from inside a span' extra={'trace_id': 'b87390af...', 'span_id': '64065b93...', 'dagster_meta': {...}}
CAPTURED msg='...hello after span exited'  extra={'dagster_meta': {...}}   # no trace_id/span_id -- correctly out of scope
```

Two things confirmed, not assumed: the ids only appear on records logged while the
span is active (scoping is correct, not a global stamp); and they survive all the way
through Dagster's own `DagsterLogHandler` into a **user-defined `@logger`** (Dagster's
own documented custom-logging extension point) via its existing `extra={...}`
forwarding (`DagsterLogHandler._extract_extra`) -- i.e. this reaches actual log output,
not just something internal to this library. A user-defined `@logger` that formats
`record.trace_id`/`record.span_id` (e.g. into JSON) gets real log/trace correlation,
entirely through public `logging` and Dagster APIs. No Dagster internals touched.

`traced()` will attach this filter automatically at span-start, so this happens for
free wherever `@traced()` is used -- no separate opt-in.

**Scope check, since it's easy to overstate this:** this only covers `context.log`
calls made during an op/asset step -- i.e. only what `@traced()` wraps. It does
**not** reach the Dagster daemon's own operational logs (scheduler/sensor tick
evaluation, heartbeats). Checked directly in `dagster/_daemon/daemon.py`:
`self._logger = get_default_daemon_logger(...)` resolves to a plain
`logging.getLogger(f"dagster.daemon.{name}")` -- a completely separate logger from
`DagsterLogManager`, with no `context` (a daemon tick isn't scoped to a run the way a
step is). So this doesn't answer requests like
[dagster-io/dagster#12495](https://github.com/dagster-io/dagster/discussions/12495)'s
`tha23rd`/`dorothychen` comments wanting daemon logs shipped to Datadog as JSON --
that's a shipping/formatting problem a log agent (Fluentd, Vector, etc.) already
solves for arbitrary process output, independent of anything this library does. What
this library actually adds is narrower and different in kind: embedding trace/span IDs
*into* the content of op/asset log lines specifically, which a log shipper can't do on
its own since it doesn't know about the active OTel span.

**Second scope gap found later, against a real `@dbt_assets` run (Issue #2,
2026-09-15), and fixed the same day:** `context.log` alone doesn't cover log lines an
*integration* emits on its own behalf. `dagster_dbt`'s own progress messages
("Running dbt command...", etc.) go through `get_dagster_logger()`
(`dagster_dbt/core/dbt_cli_invocation.py`: `logger = get_dagster_logger()`), a
separate, global `"dagster.builtin"` logger, not `context.log` -- confirmed by running
the real example (`examples/dagster_workspace/definitions.py`): those lines showed up
in a captured `@logger` with no `trace_id`/`span_id` at all until `_traced_span` also
filtered `get_dagster_logger()`. `get_dagster_logger()` is itself documented public
API (same status as `context.log`), so this isn't reaching into anything undocumented
-- and after the fix, the same dbt log lines carry the correct `trace_id`/`span_id`,
verified to match the real span in Jaeger exactly.

One caveat this introduces: unlike `context.log`, `get_dagster_logger()` returns one
shared, process-wide `Logger`, not something scoped to the current step. Not an issue
under Dagster's normal execution model (multiprocess/k8s give each step its own
process; `in_process` runs steps sequentially, not concurrently) -- but if steps ever
ran concurrently within one process, they could stamp each other's
`dagster.builtin`-routed log lines with the wrong span while both are active.

## Design decision: run tags, not `AssetMaterialization` events, as the transport

formenergy-observability (and this project's first working version) published the
trace context as `AssetMaterialization` event metadata. Verified directly (2026-09-15,
against the `dagster_otel_selftest` fixture): doing this registers the bookkeeping
`asset_key` (`__dagster_otel_root__`, or a subgraph path) in Dagster's **real asset
catalog** --

```python
instance.all_asset_keys()
# -> [AssetKey(['__dagster_otel_root__'])]   # for a job using ZERO real @assets
```

-- a fake entry shows up in the Dagster UI's asset list next to real data assets, even
for `@op`-only jobs with no assets at all. Not cosmetic-and-ignorable: the asset
catalog is what `dagster-prometheus-exporter` (this author's other project) and anyone
else scraping asset health metadata reads from; polluting it with internal bookkeeping
is a real correctness problem for downstream consumers, not just visual noise.

Switched the transport to Dagster **run tags** instead
(`instance.add_run_tags(run_id, {key: json_string})` / `run.tags`): same run-storage
database, same cross-process readability (verified: a freshly-opened `DagsterInstance`
sees a tag added by another), but tags are a flat string-to-string map on the run
itself -- nothing asset-shaped, nothing that touches `instance.all_asset_keys()`
(re-verified empty after the switch, both for an op-only run and an asset run that
correctly shows only the *real* assets, `root_asset`/`child_asset`, nothing extra).

Bonus: this deleted the whole "narrow a ~26-member `DagsterEvent.event_specific_data`
union down to the one variant with a `.materialization` attribute, then narrow
`MetadataValue` down to `JsonMetadataValue`" dance that `_find_trace_context` needed
for `AssetMaterialization` (event-log entries are typed as a broad union across every
DagsterEvent kind; run tags are just `Mapping[str, str]`, no union narrowing needed at
all). Simpler and avoids the pollution -- no tradeoff either way, once run tags turned
out to work.

## Non-goals

- **Zero-code instrumentation.** See above -- this is `@traced()` on the functions you
  want traced, not automatic.
- **Metrics.** [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter)
  already covers Dagster metrics via Prometheus. This project is traces only.

## API (implemented, self-tested -- see docs/design.md's verification notes above)

```python
from dagster_otel import traced

@op(...)
@traced()  # the first @traced() step to run in a run becomes its root automatically
def upstream_op(context) -> int:
    ...

@op(...)
@traced()  # picks up the published context automatically; no context lookup by hand
def downstream_op(context, x: int) -> int:
    ...

@asset(...)
@traced()  # identical decorator, works for assets too
def downstream_asset(context) -> None:
    ...
```

No `configure()` call, no `@resource`, no `required_resource_keys` -- `@traced()`
calls `configure()` itself (idempotently, see `_setup.py`) the first time it runs in a
process. `publish_trace_context` is still exported for cases `@traced()`'s auto-root
detection doesn't cover (e.g. deliberately marking a specific subgraph boundary), and
`configure()` is still exported for configuring eagerly rather than on first use, but
neither is required for ordinary use -- see
"Resolved: `@traced()` now handles the root case itself" above and "Design decision:
`@traced()` self-configures" below.

`configure()` honors the standard `OTEL_EXPORTER_OTLP_*` and `OTEL_SERVICE_NAME` env
vars rather than inventing bespoke config (formenergy-observability requires its own
`OTEL_EXPORTER_OTLP_HEADERS_JSON` env var for this; not repeating that here).

`@traced()` attaches the trace-context logging filter (see above) to `context.log` for
the duration of its span, so `record.trace_id`/`record.span_id` are available to any
`@logger` you define, e.g.:

```python
@logger
def json_logger(init_context):
    py_logger = logging.getLogger("my_pipeline")
    py_logger.addHandler(JsonFormattingHandler())  # reads record.trace_id/span_id
    return py_logger
```

## Sequencing: decorator-based now, auto-instrumentation later if it's ever needed

This project deliberately ships the decorator-based (`@traced()`) design first, not a
`BaseInstrumentor`-based auto-instrumentation package, even though only the latter
could ever be accepted into `opentelemetry-python-contrib` (see above -- that's a
structural requirement, unrelated to license).

Reasoning, worked through in the design chat: getting into the official contrib repo
is not itself the goal here -- a working, safe tracing story for this project's own
Dagster pipelines is. Auto-instrumentation would mean building essentially
aaaaahaaaaa's monkeypatch prototype (patching `OpDefinition.compute_fn` and
`dagster._core.execution.api.job_execution_iterator`) into a proper package, which
reopens every fragility concern already rejected in "Design decision: no
monkeypatching" above -- undocumented internals, no official support, an entire extra
package to keep working across Dagster versions. Note this is a separate *package*
patching Dagster from the outside at runtime, not a change to dagster-io/dagster's own
repo -- OTel's other instrumentation packages (`opentelemetry-instrumentation-flask`
etc.) don't touch Flask's source either, they patch it at process start from outside.

So: ship `dagster-otel` (this design) first, and only build a monkeypatch-based
auto-instrumentation package later, as a deliberate, separate, explicitly-riskier
addition, if there's ever an actual reason to want contrib listing or zero-code use
specifically (e.g. tracing jobs whose source isn't ours to edit). Not a default
follow-up step -- a decision to make again, with its own justification, if it comes up.

## Resolved: `@traced()` now handles the root case itself

`publish_trace_context` no longer needs a manually-written root step. `traced()` calls
it for every step unconditionally now (changed again as part of the Issue #5 rework
below -- each step needs to publish under its *own* key regardless of whether it found
a parent, since it's that publish that lets whatever depends on *it* find its real
parent). So the common case is just `@traced()` on every op/asset, including what
would be "the root" -- no special code path for it, and `publish_trace_context` stays
exported only for advanced cases (manually marking a boundary, etc.). This also
resolves the "root doesn't get the logging filter" gap noted earlier: the root's own
`@traced()` call installs the filter the same as every other step's.

## Design decision: `@traced()` self-configures, no `@resource` required (2026-09-15)

Raised in the design chat: needing `configure(service_name=...)` wired through a
`@resource` plus `required_resource_keys={"observability"}` on *every single* traced
op/asset, just to get a `TracerProvider` set up, is real boilerplate for what
`configure()` actually does (call `trace.set_tracer_provider()` once per process).

Fix: `_traced_span` calls `configure()` itself, before doing anything else. Safe
because `configure()` is already idempotent (see its docstring) -- if a `@resource` or
an earlier `@traced()` step in this same process already called it, this call is a
no-op; if nothing has, it configures from env vars via `Resource.create()`, which
(verified, unlike the plain `Resource(attributes=...)` constructor this project's
`configure()` used previously) reads `OTEL_SERVICE_NAME` automatically and adds
standard attributes for free (`telemetry.sdk.*`, `service.instance.id`).

Verified end-to-end (`no_resource_test` fixture): a job with **zero** `@resource`,
`required_resource_keys`, or `configure()` call anywhere, run with only
`OTEL_SERVICE_NAME`/`OTEL_EXPORTER_OTLP_ENDPOINT` set as env vars, produces a correctly
nested trace in Jaeger under the expected service name.

`configure()` takes **no arguments** -- also raised in the design chat, and correct:
its only caller (`_traced_span`) never had a Python-level value to pass in the first
place, so a `service_name`/`otlp_endpoint` parameter surface would exist for literally
nobody. Set `OTEL_SERVICE_NAME` / `OTEL_EXPORTER_OTLP_ENDPOINT` instead -- consistent
with this module's actual policy (defer to env vars, don't build a parallel config
surface) rather than in tension with it. `configure()` stays exported for the one
remaining real case: calling it eagerly (e.g. from a `@resource`) if you want
configuration to happen before any step runs rather than lazily on first `@traced()`
use.

**Side effect of making this automatic, caught and fixed the same day**: with no
working OTLP endpoint configured at all, `SimpleSpanProcessor`'s synchronous,
in-line export retries (`_MAX_RETRYS = 6`, exponential backoff) blocked **every single
`@traced()` step for ~7 seconds** before giving up -- confirmed by pointing
`OTEL_EXPORTER_OTLP_ENDPOINT` at a closed port and watching `STEP_SUCCESS` durations
jump from ~100ms to ~7s. Not a hang (`RUN_SUCCESS` still happens), but "forgot to stand
up a collector" silently turning into "the whole pipeline is now several seconds
slower per step" is a bad failure mode for something meant to be safe to just add.
Fixed with a 2-second default `timeout` on `OTLPSpanExporter`
(`_DEFAULT_OTLP_TIMEOUT_SECONDS` in `_setup.py`), applied **only** when the caller
hasn't set `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`/`OTEL_EXPORTER_OTLP_TIMEOUT` themselves
-- an explicit env var is still honored over this project's own default, same policy as
everywhere else. Reverified with the same unreachable-endpoint setup: ~1.2s per step
instead of ~7s.

## Multi-root and fan-in: fixed by keying on real dependencies (Issue #5, 2026-09-15)

**Original problem**, confirmed by direct testing (2026-09-15), not just reasoned
through: the original design keyed a published trace context by *subgraph path*
(`_trace_key_for`: `ROOT_TRACE_KEY` for a top-level step, else its enclosing
subgraph's path) -- correct for nesting depth, but unable to distinguish sibling
top-level steps. **Multiple independent roots** (two steps with no dependency between
them, e.g. `root_a` and `root_b` in the same job) raced on the shared `ROOT_TRACE_KEY`:
whichever one's published context happened to be visible first when the other looked
won, and the loser's branch attached to the *wrong* root. Reproduced against this
repo, **and against Form-Energy's original `formenergy-observability`** (same
`SpanName.ROOT`-keyed design) -- not a bug this reimplementation introduced, a limit
inherited from the prior art, apparently never noticed across 3+ years of use there.
**Fan-in** (one step depending on outputs from more than one independently-traced
upstream, e.g. `merge_op(root_a(), root_b())`) had the same root cause: OTel spans
have exactly one parent, and the old lookup only ever returned one context, so
`merge_op`'s span attached to whichever of `root_a`/`root_b` it happened to find, with
no trace-level indication it also depended on the other one at all.

**Fix**: key published contexts by each step's own *real* identity
(`_own_step_key`: `".".join(op_handle.path)`) instead of subgraph path, and look a
step's parent(s) up by asking Dagster's own execution plan what its *actual* upstream
step keys are (`StepInput.dependency_keys`, read via
`context._step_execution_context.step.step_inputs` --
`context.op_execution_context._step_execution_context...` for an
`AssetExecutionContext` -- confirmed against a real multiprocess-executor run that
this matches the same `".".join(op_handle.path)` naming `_own_step_key` uses).
Two independent roots now simply have disjoint dependency sets -- there's no shared
key left to race on. Every direct upstream that has published is returned
(`find_upstream_trace_contexts`), not just one: `_tracing.py` makes the
lexicographically-first the real OTel parent (deterministic, not lookup-order
dependent) and attaches the rest as `Link`s, the primitive OTel provides for exactly
this ("this span is also related to that one, but isn't its child") --
formenergy-observability's `ContextAwareTracer.start_new_linked_trace` uses the same
mechanism for a related purpose. `_seed_run_root_context`'s deterministic-`trace_id`
fallback (SHA-256 of `context.run_id`, unchanged) is kept for a genuine root or a step
whose only upstream(s) are untraced -- every step in a run still lands in the same
trace even when no real parent is found.

**Verified against real Dagster + Jaeger (2026-09-15)**, not just against
`tests/`'s toy fixtures:
- Multi-root (`root_a`/`root_b`, no dependency; `child_a`/`child_b` each depending on
  one): Jaeger shows `child_a` `CHILD_OF` `root_a` and `child_b` `CHILD_OF` `root_b`,
  every time -- no more race, no more wrong-sibling attachment.
- Fan-in (`merge_op(root_a(), root_b())`): Jaeger shows `merge_op` `CHILD_OF` `root_a`
  (deterministic primary parent -- `"root_a" < "root_b"`) and `FOLLOWS_FROM`
  (OTel's `Link` reference type) `root_b` -- both real dependencies now visible on the
  span, not just one.
- `examples/`'s real `@dbt_assets` jaffle_shop pipeline re-materialized clean after
  this change (regression check, single asset/step so doesn't exercise multi-root/
  fan-in itself, but confirms the rewrite didn't break the common single-parent path).

Residual limitation, not fixed by this and not expected to be: a step whose *direct*
upstream isn't `@traced()` (so never published anything to find) won't be looked up
transitively past it -- it falls back to `_seed_run_root_context` (same trace,
no real parent edge), same as a genuine root.

## Verified against a real `@dbt_assets` pipeline (Issue #2, 2026-09-15)

`examples/` (see its own README) wraps the real `jaffle_shop` dbt project (copied from
`dagster-prometheus-exporter`'s dev fixture of the same name) in `@traced()` and
materializes it for real, closing the gap between this project's unit tests (hand-written
toy ops/assets in `tests/`) and anything Dagster itself actually generates. Two things
confirmed:

- **Generator handling works against a real `@dbt_assets` function**, not just
  hand-written toy generators -- `jaffle_shop_dbt_assets` (`yield from
  dbt.cli(...).stream()`) produces a single, correctly-durationed span (~6.7s,
  matching the actual `dbt build` wall-clock time) in Jaeger.
- **`Definitions(loggers=...)` does reach `dagster asset materialize` runs**, once
  explicitly selected via run config (`loggers: <name>: {}`) -- same activation
  mechanism as `logger_defs` on a `@job`. Whether this answers
  [dagster-io/dagster#12495](https://github.com/dagster-io/dagster/discussions/12495)'s
  `zyd14` (who in 2024 found no way to do this) depends on whether `Definitions`
  gained this parameter after that comment or it was just missed -- not established
  here, only that it works on Dagster 1.13.22.

Building and running this example also surfaced two real bugs neither unit tests nor
hand-written toy fixtures had caught:

1. The `get_dagster_logger()` log-correlation gap described above.
2. **`ComputeFn`'s context parameter was a fixed `ExecutionContext` union, not
   generic** -- caught by pyright (not mypy) on `jaffle_shop_dbt_assets`, which is
   typed with the *specific* `AssetExecutionContext`, not the union. Real op/asset
   code is normally typed with the specific context type it expects, not the union
   this library's own type alias used internally; a fixed-union parameter type
   rejects a narrower one by ordinary function-parameter contravariance. Fixed by
   making the context parameter generic (`C = TypeVar("C", bound=ExecutionContext)`)
   so `traced()` accepts and preserves whichever specific context type (or the
   union) the wrapped function actually declares.

Neither of these came up in `tests/`, since its fakes and toy functions were never
typed as narrowly as real Dagster code is, and never exercised an integration's own
logging. This is the concrete case for why Issue #2 (and #3, #4 -- verification
against real Dagster behavior generally) matters beyond "more test coverage": each of
these fixes came from behavior no amount of *unit* testing this library's own code in
isolation would have surfaced.

## Verified against a real `k8s_job_executor` cluster (Issue #3, 2026-09-15)

Confirmed by direct testing, not just reasoning that `context.instance` is the same
abstraction regardless of node: a `kind` cluster (single node), Postgres-backed run
storage/event log/schedule storage (`fs_io_manager`'s default local-disk storage also
needed a shared PVC -- unrelated to this library, just a prerequisite for
`k8s_job_executor` to run a job to completion at all, since each step gets a fresh
container with no filesystem of its own), an in-cluster Jaeger, and the multi-root +
fan-in op graph from the Issue #5 verification (`root_a`/`root_b` independent,
`child_a`/`child_b` each depending on one, `merge_op` depending on both), run with
`@job(executor_def=k8s_job_executor)`.

`kubectl get pods` confirmed each of the 5 ops ran in its own separate Kubernetes Job/
pod (`dagster-step-<hash>`, not just separate OS processes on one host as
`multiprocess` gives). Jaeger showed the identical, correct trace shape as the
multiprocess verification, now across real pods:

```
root_a     CHILD_OF 0000000000000001        (deterministic-seed root)
root_b     CHILD_OF 0000000000000001
child_a    CHILD_OF root_a
child_b    CHILD_OF root_b
merge_op   CHILD_OF root_a, FOLLOWS_FROM root_b
```

Confirms all three things Issue #3 asked about: trace context published by one step's
pod is found by a downstream step's pod (via `context.instance` -- Postgres run
storage, reachable identically from any pod); Jaeger shows correctly nested spans (and
correct fan-in `Link`s) across pods, not just processes; and the deterministic
multi-root `trace_id` fallback (`_seed_run_root_context`) computes the identical
`trace_id` independently in each pod's own process, same as across multiprocess's
separate host processes -- confirmed by `root_a`/`root_b` landing in the same trace
despite running in unrelated pods with no shared memory.

Setup is preserved under `dev/kubernetes/` (see its own README for exact repro
commands) rather than only in a scratch directory -- not wired into CI (a full `kind`
cluster spin-up per-PR is heavy for a one-off verification, unlike
`dagster-prometheus-exporter`'s scheduled kind e2e test, which guards a chart/exporter
against its own regressions), but rerunnable by hand if `dagster`/`dagster-k8s`/
`dagster-postgres` get bumped and this needs reconfirming.

## Verified against a real retry-from-failure run (Issue #4, 2026-09-15)

Confirmed by direct testing, not just the fake-instance unit tests
(`test_run_id_and_ancestors_walks_parent_chain`,
`test_find_trace_context_falls_back_to_ancestor_run`): a two-step job (`root_op` ->
`failing_op`, both `@traced()`), `failing_op` deliberately failing on its first
attempt (gated by a marker file) and succeeding on a second, executed via Dagster's
real reexecution API (`execute_job(..., reexecution_options=
ReexecutionOptions.from_failure(original_run_id, instance))` -- the same mechanism
the Dagster UI's "Re-execute from failure" button uses).

Confirmed the retry run's step selection actually exercises what this needs tested:
Dagster reused `root_op`'s output from the original run rather than re-running it --
the retry run's own event log shows only `failing_op` ever started. That matters
because it means `failing_op`'s `@traced()` call in the retry run *cannot* find a
trace context published within its own run -- `root_op`'s `publish_trace_context`
call only ever happened in the original run's process, which is exactly the case
`_run_id_and_ancestors` (walking `parent_run_id`) exists for.

Jaeger confirmed all three spans across both runs landed in one trace, correctly
nested:

```
root_op                CHILD_OF 0000000000000001    (original run)
failing_op (attempt 1) CHILD_OF root_op              (original run, the failed attempt)
failing_op (attempt 2) CHILD_OF root_op              (retry run, parent_run_id = original)
```

Both `failing_op` spans -- the failed first attempt *and* the successful retry, two
different Dagster runs, two different processes, no shared memory -- correctly
parented under `root_op`'s span from the original run. `retry_run.parent_run_id`
matched the original run's ID exactly as `_run_id_and_ancestors` assumes.

## Bare `@traced` support, matching `@op`/`@asset` (Issue #19, 2026-09-16)

Confirmed `@op` (and `@asset`) support bare use with no parens (`@op` alone, not just
`@op(...)`) -- verified directly, not assumed. `@traced()` stacks right underneath
either, so a user reasonably might try `@traced` by analogy and, before this fix, hit
a genuinely bad failure mode: no exception anywhere, just a silently wrong result.
`@traced` bare is `my_op = traced(my_op)` -- `span_name` (the sole parameter) receives
the function itself, and the `wrapper` function `traced()` returns became `my_op`'s
new value directly, an *unconfigured decorator*, not the traced original function.

Fixed by overloading `traced()` itself on a genuinely decidable axis: `Callable` (bare
use) vs. `str | None` (called use) are non-overlapping argument types at the
`traced(...)` call site itself -- unlike the generator/plain-return split (still
resolved one level down, on the returned decorator's `__call__`, per the existing
design; see `_tracing.py`'s module docstring for why that one can't be decided at
`traced()`'s own call site). Implementation detail that cost real debugging time:
mypy's overload-implementation compatibility check failed
(`Overloaded function implementation does not accept all possible parameters of
signature 1/2`) until the `func` parameter in the bare-use overloads was marked
positional-only (`func: ComputeFn[C, P, R], /`) -- without it, mypy required the
single implementation to also accept a `func=` keyword, which conflicts with the
called-form overload's `span_name` keyword parameter occupying that same position.

Verified against real Dagster + Jaeger: a `root_op`/`child_op` job with bare
`@traced` (no parens) on both produces the identical correct trace shape (`child_op`
`CHILD_OF` `root_op`) as the called form.

## Dagster context as span attributes (Issue #9, 2026-09-16)

Confirmed via a real Jaeger span (2026-09-15, during the Issue #9 investigation) that
spans carried no custom attributes at all before this -- everything visible was OTel
SDK boilerplate (`otel.scope.name`, `span.kind`) or process-level `telemetry.sdk.*`
metadata. A trace told you *when* and *in what parent/child (or Link) shape* a step
ran, but nothing about *which run*, *which job*, or *which retry attempt* it belonged
to without cross-referencing Dagster's own UI/event log by hand.

Four attributes added, all `@public`-documented properties already available on
`context` (no extra Dagster call needed): `dagster.run_id`, `dagster.job_name`,
`dagster.step_key` (`_own_step_key(context)` -- the same identity `_propagation.py`
already keys published trace contexts by), and `dagster.retry_number`. Not a name
set picked in isolation: researched while comparing against other orchestrators'
OTel support (2026-09-15), and Airflow's own native tracing (`dag_id`/`task_id`/
`run_id`/`try_number` attributes on its task spans) and Leoflow's ADR 0010 (a
from-scratch Airflow-compatible orchestrator, `leoflow.dag_id`/`leoflow.task_id`/
`leoflow.run_id`/`leoflow.try_number`) both independently converge on essentially
this same attribute set.

`asset_key` deliberately not included: `context.asset_key` raises
`DagsterInvariantViolationError` for a `multi_asset` with more than one output asset
(confirmed in the property's own source) -- handling that safely needs
`selected_asset_keys` (plural) instead, more design work than "cheap to add" covers.
Tracked as a follow-up, not done half-right here.

Verified against real Dagster + Jaeger, including a `RetryPolicy`-triggered
same-run retry (see Issue #14): both the failed first attempt and the successful
retry carried the correct, distinct `dagster.retry_number` (`0` then `1`), alongside
identical `dagster.run_id`/`dagster.job_name`/`dagster.step_key`. This doesn't fix
Issue #14 (the two attempts are still unrelated sibling spans, no `Link` between
them) but does make them distinguishable from each other by attribute, which is a
real partial improvement toward that issue even before it's fully addressed.

## Open questions

None currently tracked -- multi-root/fan-in (#5), k8s_job_executor (#3), and
retry-from-failure (#4) were the three open verification questions, and all three are
now confirmed against real Dagster, not just reasoned through. See
[#11](https://github.com/HirofumiTsuda/dagster-otel/issues/11) for what's still
missing before a release, not this library's own behavior.

## License

MIT -- matches [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter)
(my other Dagster project) and formenergy-observability (which this design builds on
ideas from, not copied code -- see [attribution](#prior-art-and-why-this-project-exists-anyway)
above). Originally set to Apache-2.0 on the assumption that might matter for eventual
upstreaming into opentelemetry-python-contrib; turned out not to (see above), so no
reason to diverge from MIT.
