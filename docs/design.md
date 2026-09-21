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
(`test_ancestor_runs_walks_parent_chain`,
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
`_ancestor_runs` (walking `parent_run_id`) exists for.

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
matched the original run's ID exactly as `_ancestor_runs` assumes.

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
identical `dagster.run_id`/`dagster.job_name`/`dagster.step_key`. At the time this
was written, the two attempts were still unrelated sibling spans with no `Link`
between them -- distinguishable by attribute, not yet by trace structure. Issue #14
(below) later closed that second gap.

## No real exporter installed unless export is actually configured (Issue #16, 2026-09-16)

Found while comparing against Prefect's OTel docs, which describe OTel calls staying
a true no-op ("very performant, no overhead") until the user configures something.
`configure()` didn't do that: confirmed `OTLPSpanExporter()` defaults to
`localhost:4317` even with **zero** `OTEL_*` env vars set, so `configure()` built a
real `TracerProvider` + `OTLPSpanExporter` + `SimpleSpanProcessor` regardless --
someone who `pip install`s this and tries `@traced()` with no setup at all got a real
gRPC connection attempt on every single step, forever, for a collector they never
asked for (bounded to ~2s per step by the existing timeout fix above, but still a
real, silent, avoidable cost).

Fixed by gating whether a real exporter gets attached at all
(`_export_configured()`): `OTEL_SDK_DISABLED=true` (the spec's own kill switch) always
wins; otherwise gated on `OTEL_EXPORTER_OTLP_ENDPOINT`/`..._TRACES_ENDPOINT`
presence. When neither signal says "export somewhere," `configure()` still installs a
`TracerProvider` (so `traced()`'s propagation logic keeps working unmodified -- every
invariant it relies on, like `publish_trace_context`'s span_id check, only depends on
whether a span was *created*, not whether it was ever *exported*) but attaches no span
processor at all. Verified directly: a bare `TracerProvider` with zero span
processors still produces genuinely valid, recording spans with real non-zero
`span_id`/`trace_id` -- nothing about span creation or cross-process propagation
changes, only whether anything ever tries to *send* a span anywhere.

Known, deliberately-not-solved tradeoff: someone running a real OTel Collector as a
sidecar at the literal default `localhost:4317`, relying on the SDK's own built-in
default instead of setting the endpoint env var explicitly, now gets silent
no-export instead of real export. Chose the safer default of the two failure modes --
a surprise network stall on every step for a brand-new user vs. a one-line env var
for a more deliberate deployment (already standard OTel advice: set the endpoint
explicitly) -- rather than trying to detect that narrower case.

Verified against real Dagster, with Jaeger deliberately stopped to test against a
genuinely unreachable default endpoint, not just an untested one:

| Env vars | Jaeger running? | Per-step time |
| --- | --- | --- |
| none | down | 93ms / 151ms -- no network attempt |
| `OTEL_EXPORTER_OTLP_ENDPOINT` set | down | 1.0s / 1.2s -- real export attempted, bounded by the existing 2s timeout |
| `OTEL_EXPORTER_OTLP_ENDPOINT` set | up | 107ms / 161ms, spans correctly received in Jaeger -- the happy path still works unmodified |

## RetryPolicy attempts linked to each other (Issue #14, 2026-09-16)

An op-level `RetryPolicy` retry re-executes the same step as a fresh process, with
its own fresh `_traced_span` call -- confirmed (#9) each attempt gets a genuinely
separate span, distinguishable only by the `dagster.retry_number` attribute, with no
relationship to the previous attempt in the trace structure itself.

Fixed by reusing the exact mechanism already built for fan-in (#5): every attempt
publishes its trace context under the identical `_own_step_key(context)` -- the same
key every attempt of a given step uses, since a retry isn't a new step, it's the same
one running again. That means a later attempt can look its *own* key up (via the same
`_find_context_for_step_key` fan-in already uses) *before* publishing its own context
over it, and find exactly the immediately-preceding attempt's context --
`find_previous_attempt_context`, gated on `context.retry_number > 0` so a first
attempt (nothing preceded it) is skipped. Whatever's found becomes an additional
`Link`, alongside (not replacing) whatever `Link`s/parent the step's real upstream
dependencies already produce -- the retry relationship is additive information, not a
substitute for the real dependency-graph parent.

Verified against a real `RetryPolicy`-triggered retry (`root_op -> flaky_op`,
`flaky_op` failing once and succeeding on retry) against real Dagster + Jaeger:

```
root_op           CHILD_OF <root>
flaky_op (try 0)  CHILD_OF root_op
flaky_op (try 1)  CHILD_OF root_op, FOLLOWS_FROM flaky_op (try 0)
```

Both attempts correctly keep `root_op` as their real parent (the actual dependency
relationship, unchanged by any of this); the retry additionally links back to the
attempt it retried. A viewer can now see both facts from the trace alone: what this
step actually depends on, and that it needed a retry to succeed.

## Per-dbt-node spans via `traced_dbt()` (Issue #8, 2026-09-16)

`@traced()` alone gives `@dbt_assets` exactly one span for the whole step, however
many dbt nodes it actually materializes internally (confirmed: 15 nodes, 1 span, in
the jaffle_shop example) -- correct given how `@traced()` works, but coarser than
necessary, since `dbt.cli(...).stream()`'s `DbtEventIterator` already yields one
event per node.

Two things this issue's own open questions asked about, both answered by direct
investigation before writing any code:

- **Does each event carry real timing, or would a per-node span be a zero-duration
  marker?** Confirmed real: every `Output`/`AssetCheckResult` event carries an
  `"Execution Duration"` metadata value (dbt's own measured seconds), letting a span
  be backdated (`start_time = event_arrival_time - duration`) to something accurate,
  via OTel's explicit `tracer.start_span(..., start_time=...)` +
  `span.end(end_time=...)` API rather than `start_as_current_span()` (the node
  already finished by the time Python sees the event -- there's nothing to be
  "current" for while it runs).
- **Does this need `dagster-dbt` as an import, justifying a separate optional
  module/extra?** No -- `Output`/`AssetCheckResult` are core `dagster` types
  (already a required dependency); a dbt node's metadata just happens to carry
  `"Execution Duration"`/`"unique_id"` keys by `dagster_dbt`'s own convention.
  `dagster_otel.dbt` is a separate *module* (so `traced_dbt`'s name is explicit and
  discoverable, and a non-dbt op that happens to reuse those exact metadata key
  names for unrelated reasons never gets surprise spans it didn't ask for), not
  because of any dependency requirement -- see that module's own docstring.

Went further than the issue's original "flat, one span per node" sketch: `Output`'s
`output_name` resolves to the real Dagster `AssetKey` via the `@public`
`context.asset_key_for_output(...)`, and `AssetCheckResult.asset_key` already carries
one directly -- so spans are keyed by Dagster's own asset model, not dbt's internal
`unique_id`, and a check's span nests as a child of its own asset's span (tracked by
the most recently seen span per `asset_key` during iteration, matching dbt's own
build-then-test execution order -- not dependent on any ordering guarantee, just
confirmed to hold in practice). This incidentally resolves, for exactly this
multi-asset case, the `dagster.asset_key` attribute Issue #9 deliberately deferred
(`context.asset_key` itself raises for a `@dbt_assets` function's multiple outputs;
a specific event's own asset_key is not ambiguous the same way).

`traced_dbt()` requires zero changes to the wrapped function's body -- still exactly
`yield from dbt.cli(...).stream()` -- reusing `traced()`'s own generator-handling
branch internally rather than duplicating it (see `dagster_otel/dbt.py`).

Verified against the real jaffle_shop example + Jaeger:

```
jaffle_shop_dbt_assets                    (the step, unchanged)
  raw_customers                             (real AssetKey, real ~60ms duration)
    not_null_raw_customers_first_name       (real ~44ms duration)
    not_null_raw_customers_id
    not_null_raw_customers_last_name
    unique_raw_customers_id
  stg_customers
    not_null_stg_customers_customer_id
    ...
  customers
    ...
```

16 spans total (1 step + 3 assets + 12 checks), matching jaffle_shop's real node
count exactly, each with an accurate duration matching what dbt itself reported.

`traced_dbt()` also supports bare use (`@traced_dbt`, no parens), via the identical
`@overload` dispatch pattern `traced()` uses for the same purpose (see that
function's own module docstring) -- added after initially shipping only the called
form, once asked directly whether bare use worked. It didn't: confirmed the same
silent-breakage failure mode `traced()` had before Issue #19 (bare use rebound the
decorated name to the unconfigured inner decorator function, not the traced
original), fixed the same way, verified against real Dagster + Jaeger the same way
(identical 16-span trace either form).

## Externally-provided trace context to seed a run (Issue #13, 2026-09-16)

Everything so far handles propagation *within* a run (step to step, via run tags).
Nothing let a whole run itself nest under a trace something *outside* Dagster
started -- a CI/CD pipeline, a scheduler, another OTel-instrumented system triggering
a Dagster run and wanting its steps to show up under *its own* trace, not start a
fresh unrelated one. Idea originally surfaced while researching Airflow's native OTel
support, which lets a `dag_run`'s `conf` carry reserved tracing-control keys set at
trigger time.

Dagster runs can already be launched with `tags={...}` (CLI `--tags`, the Python API,
`RunRequest(tags=...)` for sensors, etc.), and tags are already exactly this
library's own propagation transport. So the shape: `EXTERNAL_TRACE_CONTEXT_TAG_KEY`
(`dagster_otel/external_trace_context`, exported from the top-level package) is a
tag key *this library never writes itself* -- the launching caller sets it directly,
carrying the same W3C traceparent carrier shape `publish_trace_context` writes. Every
root step in a run (the ones that would otherwise fall back to
`_seed_run_root_context`'s deterministic seed) checks for this tag first via
`find_external_trace_context`, activating it instead when present.

A real gotcha caught only by testing against an actual retry, not assumed: **Dagster
does not copy a run's tags forward to a retry-from-failure run** -- confirmed
directly (`execute_job(tags={"custom": "value"})` then
`ReexecutionOptions.from_failure(...)`: the retry run's own tags came back `{}`,
completely empty). So `find_external_trace_context`, like every other cross-run
lookup here, has to walk the ancestor-run chain (`_ancestor_runs`) rather than checking only the
current run's own tags -- without that, a retried run's root steps would silently
fall back to the deterministic seed instead of still nesting under the original
external caller.

Verified against real Dagster + Jaeger: a real "external caller" span (simulating a
CI/CD pipeline) with its `traceparent` passed via the tag at `execute_job(tags=...)`
launch time, a two-step job (`root_op` -> `child_op`):

```
external-ci-pipeline-step
  root_op    CHILD_OF external-ci-pipeline-step
    child_op CHILD_OF root_op
```

`root_op` (a genuine root, no real Dagster-internal upstream) correctly became a
child of the external span; `child_op`'s propagation *within* the run kept working
completely unmodified. A real gotcha caught while first probing this manually (not
part of the design, a mistake in the probe script): the op body runs in its own
subprocess under `multiprocess` (the default executor), so `configure()` has to run
in that subprocess too, same as `@traced()` always does -- forgetting it (writing a
standalone probe that skipped `@traced()`/`configure()` entirely) silently no-op'd
span creation, `span.parent` came back `None` even though the tag lookup itself
worked correctly. Not a gap in the actual design, a reminder this only works because
`@traced()` already calls `configure()` itself every time.

## Ancestor-run chain fetched once per step, not once per lookup (Issue #35, 2026-09-16)

`_ancestor_runs(context)` (formerly `_run_id_and_ancestors`, which returned only run
ids) walks the retry-from-failure `parent_run_id` chain and fetches each `DagsterRun`
from run storage exactly once now, returning the run objects themselves rather than
discarding them and making every caller re-fetch. `find_upstream_trace_contexts`,
`find_external_trace_context`, and `find_previous_attempt_context` all take an
optional `runs` parameter -- `_traced_span` (`_tracing.py`) fetches `_ancestor_runs`
once per step and passes the same list into all three, instead of each independently
re-walking and re-fetching the identical chain.

Not a correctness fix -- purely redundant round-trips to run storage. Before this,
a single `_traced_span()` call with U real upstream dependencies on a run with N
ancestor hops (a retry chain) issued on the order of `(U + 1) * 2N` `get_run_by_id`
calls where `N` suffices: `find_upstream_trace_contexts` alone re-walked and
re-fetched the whole chain once per upstream key, on top of the chain already being
fetched twice per walk (once to discover `parent_run_id`, discarded; once more by
each caller to read `.tags`). Every function's own default (`runs=None` recomputes
internally via `_ancestor_runs`) keeps every existing call site -- and every test
that calls these directly without a `runs` argument -- working unmodified; only
`_traced_span`, which actually has several lookups to share the fetch across, passes
`runs` through explicitly.

## Dynamic-mapped steps: `_own_step_key` must use the real step key (Issue #45, 2026-09-16)

`_own_step_key` used `".".join(context.op_handle.path)` -- correct for an ordinary
step, but a step produced by Dagster's dynamic graph mapping (`DynamicOut`/`.map()`)
has a real execution-plan key shaped `f"{node_handle}[{mapping_key}]"`
(`StepHandle.parse_from_key`'s own regex, confirmed by reading Dagster's source),
which `op_handle` never carries at all. Every parallel invocation of one mapped op
therefore computed the identical `_own_step_key`, so `publish_trace_context` had `N`
concurrent instances racing to overwrite the same run tag, and a downstream
`.collect()` step's real `dependency_keys` (which *do* include `[mapping_key]`) could
never match what got published under the bracket-less key -- the same "looks fine,
silently wrong" shape as the original Issue #5 bug, reintroduced through a different
code path that keying-by-`dependency_keys` didn't close, since the *publishing* side
(not the upstream-lookup side) was still keying by `op_handle.path`.

Fixed by using `context.get_step_execution_context().step.key`
(`ExecutionStep.key` / `StepHandle.to_key()`) instead. Confirmed by reading Dagster's
own source (not assumed) that this is behavior-preserving for the ordinary case --
`StepHandle.key` defaults to `str(node_handle)`, and `NodeHandle.__str__` builds
`".".join(path components)` the same way `.path` does -- while producing the real
`[mapping_key]`-suffixed string for a mapped step, matching exactly what
`StepInput.dependency_keys` already returns for a downstream dependency on one.

Verified against a real multiprocess job (`fan_out` -> `DynamicOut` -> `.map
(process_file)` -> `.collect()` -> `collect_results`, all `@traced()`) + real
Jaeger:

```
fan_out                 CHILD_OF 0000000000000001   (deterministic-seed root)
process_file[a]          CHILD_OF fan_out
process_file[b]          CHILD_OF fan_out
process_file[c]          CHILD_OF fan_out
collect_results          CHILD_OF process_file[a], FOLLOWS_FROM process_file[b],
                          FOLLOWS_FROM process_file[c]
```

`collect_results` found all three real mapped-instance parents (one primary + two
`Link`s, the same fan-in mechanism Issue #5 built) -- before the fix, all three
`process_file` instances would have raced on the single key `"process_file"`, and
`collect_results`'s lookups for `"process_file[a]"`/`"[b]"`/`"[c]"` would never have
matched it, silently falling back to a fresh disconnected root instead.

## `traced_dbt()` handles op-based `dbt.cli()` usage too (Issue #47, 2026-09-16)

`traced_dbt()`'s span-creation code only matched `Output` (asset span) and
`AssetCheckResult` (check span) -- the events `dagster_dbt` yields for `@dbt_assets`
usage. But a plain `@op` calling `dbt.cli(...).stream()` directly (not `@dbt_assets`)
is a real, separately-documented `dagster_dbt` API
(`DbtCliInvocation.to_default_asset_events`'s own docstring literally describes both
cases), and it yields *different* event types for the identical underlying dbt run:
confirmed by reading `dagster_dbt`'s own translation
(`dagster_dbt/core/dbt_cli_event.py`, installed via `uv sync --group dbt`) --
`AssetMaterialization` instead of `Output` for models/seeds/snapshots, and either
`AssetCheckEvaluation` (same shape as `AssetCheckResult`: `asset_key`/`check_name`/
`passed`/`metadata`, just the op-mode name for it) or `AssetObservation` (dagster_dbt's
own fallback, for a test with no determinable `AssetCheckKey` -- also used, less
commonly, in *asset* mode for a test excluded by Dagster's own check selection) for
tests. None of `AssetMaterialization`/`AssetCheckEvaluation`/`AssetObservation` were
handled, so op-based usage silently produced zero per-node spans despite `DbtEvent`'s
own type alias already declaring `AssetMaterialization`/`AssetObservation` as
supported -- the type signature promised more than the runtime delivered.

`AssetCheckEvaluation` wasn't part of the original issue's own investigation (or the
prior `DbtEvent` type alias) -- found by reading `to_default_asset_events`'s actual
return-type annotation directly against the installed `dagster_dbt` version, not
assumed from the issue text alone.

Fixed by branching on `AssetMaterialization` (keyed by `event.asset_key` directly --
simpler than `Output`, no `context.asset_key_for_output()` lookup needed) and on
`AssetCheckResult | AssetCheckEvaluation | AssetObservation` together for the check
span, with `AssetObservation` (which has neither `check_name` nor `passed`) getting a
generic span name (`"dbt_observation"`) and no pass/fail status set, instead of one
invented.

Verified against a real op-based job (`@op def run_dbt_build(context, dbt):
yield from dbt.cli(["build"], manifest=..., context=context).stream()`, `@traced_dbt()`,
same jaffle_shop project as the `@dbt_assets` verification) + real Jaeger: 16 spans
(1 step + 3 assets + 12 checks), identical shape to the `@dbt_assets` result, built
entirely from `AssetMaterialization`/`AssetCheckEvaluation` events this time instead
of `Output`/`AssetCheckResult`. Two real setup gotchas hit along the way, neither a
gap in the actual design: (1) `dbt.cli()` needs `manifest=` passed explicitly for
op-based usage -- in asset mode this is resolved automatically from the `@dbt_assets`
definition's own manifest via `context.has_assets_def`, which doesn't exist for a
plain op; (2) the op itself still needs to yield a real `Output` for its own
declared output (`Output(None)` after the `yield from`) -- dbt's events don't
satisfy Dagster's own step-output contract, that's a separate concern from what this
library adds.

## `dagster.asset_keys` span attribute, including `@multi_asset` (Issue #37, 2026-09-17)

Issue #9 deliberately left `dagster.asset_key` out of the four `dagster.*` attributes
it added, because `context.asset_key` raises `DagsterInvariantViolationError` for a
`@multi_asset` with more than one output asset -- exactly the case this follow-up
needed to handle, not a nice-to-have.

Fixed with `context.selected_asset_keys` (`@public`, a `frozenset[AssetKey]`) instead
of `context.asset_key` -- works uniformly for a plain `@op` (empty set, confirmed by
reading the property's own source: it returns `set()` when `not self.has_assets_def`,
never raises), a single-output `@asset`, and a `@multi_asset` alike, no branching
needed on context shape. Comma-joined into one `dagster.asset_keys` string attribute
(sorted, since a set has no stable order), not OTel's native sequence-attribute
support -- probed directly against real Jaeger (2026-09-17) that a native list
attribute round-trips through OTLP as a JSON-array-*shaped string* anyway
(`'["a","b"]'`, not a real array in the UI), so a plain comma-joined string renders
just as cleanly and reads better. Omitted entirely (not set to `""`) when there's
nothing to report, same "don't invent a value" stance as every other lookup in this
library.

Verified against a real `@multi_asset` with two outputs (`zeta_asset`, `alpha_asset`
-- deliberately out-of-alphabetical-order asset names, to actually exercise the
sort) + real Jaeger: the span's `dagster.asset_keys` attribute reads
`"alpha_asset,zeta_asset"` -- both present, correctly sorted, on exactly the case
`context.asset_key` itself cannot handle.

## Verified against a real Grafana Tempo backend, through a real OTel Collector (Issue #42, 2026-09-20)

Every prior verification in this doc used Jaeger. `configure()`'s whole premise --
`OTLPSpanExporter()` with no `endpoint=`/`headers=`/`credentials=`, so anything
speaking OTLP should work purely via env vars -- had never actually been checked
against a second backend.

Added `tempo` and `otel-collector` services to `docker-compose.yaml` (`dev/tempo.yaml`
adapted from Grafana's own official single-binary example, `dev/otel-collector-
config.yaml` a plain `otlp` receiver forwarding to Tempo). Deliberately routed through
a real OTel Collector rather than pointing `OTEL_EXPORTER_OTLP_ENDPOINT` straight at
Tempo -- this project had never been verified against an actual Collector hop either,
and a real deployment fronting multiple instrumented services with one Collector is a
more realistic shape than every prior Jaeger check exercised.

Two things confirmed, both against the real `examples/` jaffle_shop `@dbt_assets`
pipeline (not a hand-written toy trace):

- A manual span round-trips through `dagster-otel -> Collector -> Tempo` correctly
  first (`GET /api/traces/<id>` on Tempo's query API returned the right resource
  attributes and span), before trying anything more complex.
- The real pipeline produces the exact same 16-span `step -> asset -> check` shape
  already documented for Jaeger: `jaffle_shop_dbt_assets` (root) parenting
  `raw_customers`/`stg_customers`/`customers`, each in turn parenting its own dbt test
  spans (4 checks each). Confirmed via Tempo's search API
  (`serviceStats.jaffle_shop_tempo_verification.spanCount: 16`) and by walking the
  full trace's parent/child edges directly, not just trusting a span count.

Tempo's `grafana/tempo:latest` image (pulled 2026-09-20) logs `live_store`/
`partition`-related lines on startup that don't appear in older single-binary-mode
docs -- newer Tempo versions have a different internal ingestion architecture than
the classic local-storage-only single binary. Didn't investigate further since
`storage.trace.backend: local` (this config) still worked end-to-end regardless; flag
this in case a future Tempo version genuinely requires something this config doesn't
provide (a Kafka-compatible queue was mentioned in some current Tempo docs, though not
needed here).

No Grafana instance in this setup yet (Tempo only exposes an API, no UI of its own) --
unlike the Jaeger case, no accompanying UI screenshot for the README. Adding Grafana
on top (with a Tempo datasource) would be a natural addition when
[#56](https://github.com/HirofumiTsuda/dagster-otel/issues/56)'s combined
exporter+traces demo adds Prometheus too, rather than doing it twice.

## Combined demo: traces + exporter metrics through one Collector, in Grafana (Issue #56, 2026-09-20)

Follow-up to the Tempo verification above, which flagged this as the natural next
step rather than adding Grafana twice.

Two sibling projects (this one, and
[dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter))
had never been demonstrated working together before, despite being a natural pair.
`dev/otel-collector-config.yaml` now has two independent pipelines, not one:

- **Traces** (unchanged from the Tempo verification above): `otlp` receiver ->
  `otlp/tempo` exporter.
- **Metrics** (new): a `prometheus` receiver scrapes
  `dagster-prometheus-exporter`'s `/metrics` (same thing a standalone Prometheus
  server would do), then a `prometheusremotewrite` exporter forwards to the
  Prometheus this demo also runs.

Tempo has no metrics ingestion (traces-only, see above), so this isn't literally
"one Collector, one backend" the way #56's original SigNoz-shaped diagram pictured
-- the Collector is still the thing both signal types pass through, which is the
part that actually matters here, and Grafana (new in this stack, with both a
Prometheus and a Tempo datasource provisioned) is where a human actually looks at
both together.

New pieces:

- `dev/jaffle-shop-dev.Dockerfile` -- runs `examples/dagster_workspace` (the
  jaffle_shop pipeline verified against Jaeger and Tempo above) as a persistent
  `dagster dev` webserver, not the one-shot `dagster asset materialize`
  `examples/README.md`'s basic walkthrough uses. Needed because
  `dagster-prometheus-exporter` scrapes a live GraphQL endpoint, which a one-shot
  command never exposes. Needed a new `demo` dependency group
  (`dagster-webserver`, pinned to the same 1.13.22 `dagster` itself resolves to)
  -- this library itself never needs a running webserver, so it stayed out of the
  base dependencies, same reasoning as the existing `dbt` group.
- `exporter` service: the sibling project's own published image
  (`ghcr.io/hirofumitsuda/dagster-prometheus-exporter:latest`), pointed at the
  jaffle-shop container's GraphQL endpoint. Zero changes needed to that project,
  exactly as #56 anticipated -- it's referenced as an external image, not vendored
  or built from source here.
- `prometheus`/`grafana` services, mirroring `dagster-prometheus-exporter`'s own
  dev-stack conventions (same author, same shape) rather than inventing a
  different one.

Verified against a real materialization, not just each piece in isolation:
launched a real run via GraphQL against the jaffle-shop webserver, and confirmed
both signal types landed from that one run --

```
$ curl -s --get http://localhost:3200/api/search --data-urlencode 'q={}' | ...
"jaffle_shop_combined_demo": {"spanCount": 16}   # same step->asset->check shape as before

$ curl -s http://localhost:9090/api/v1/query?query=dagster_last_run_info | ...
{"status": "success"}   # the exporter's own metric, having passed through the Collector's
                         # prometheus receiver -> prometheusremotewrite -> this Prometheus
```

Both datasources confirmed provisioned and reachable via Grafana's own API
(`GET /api/datasources`), not just assumed from the YAML.

`dev/grafana/dashboards/combined-demo-dashboard.json` (auto-provisioned into a
"dagster-otel" folder) ships three Prometheus-backed panels (active runs, latest
run status, last run duration) and one native Grafana **traces** panel querying
Tempo directly via TraceQL (`{resource.service.name="jaffle_shop_combined_demo"}`)
-- not a link out to Tempo's own UI (it has none), an actual in-dashboard trace
list. Built and validated against a real running Grafana instance via its HTTP API
(`POST /api/dashboards/db`, then queried each panel's data via `POST
/api/ds/query` to confirm real values came back) before writing the file, rather
than hand-authoring dashboard JSON and hoping the schema was right.

The datasource `uid`s are pinned explicitly in `dev/grafana/provisioning/
datasources/datasource.yml` (`prometheus` for Prometheus,
`tempo` for Tempo) specifically because the dashboard JSON references
them by uid -- leaving Grafana to auto-generate one per fresh install would silently
break the dashboard's datasource links every time the stack is torn down and
recreated. Verified by actually doing that: removed the running Grafana container
and recreated it from a clean state, confirmed both the pinned uids and the
dashboard (still resolving real trace data) came back identically.

One real debugging catch worth noting: a manual span sent straight from the host to
the Tempo/Collector path (via the host-mapped port) landed and searched
immediately, but the *first* check of the real materialization's trace (queried
right after the run finished) came back empty -- re-querying moments later found it
with the full 16 spans. Tempo's search index has some lag after ingestion; don't
treat an immediate empty search as "it didn't arrive."

### Confirmed limitation: Tempo's live-store ring can self-evict during idle periods (2026-09-20)

Left the stack running (idle, no new traces) for roughly 35 minutes after the
verification above and came back to `{}`-filtered search returning zero traces --
including the one already confirmed present. Tempo's own logs explained why:

```
level=warn caller=basic_lifecycler_delegates.go:147 msg="auto-forgetting instance
from the ring because it is unhealthy for a long time" instance=<container-id>
last_heartbeat="..." forget_period=2m0s
```

Tempo 3.x's "live-store" ingestion path (the `ring=live-store`/`livestore-partitions`
log lines flagged as unfamiliar in the Tempo-verification section above) tracks
ring membership even for a single, monolithic instance -- and this instance's own
heartbeat lapsed for long enough (evidence points to host resource contention from
other work happening in parallel, not a fixed/reproducible interval) that Tempo's
own health-check machinery evicted it from its own ring. Confirmed this is not a
`:latest`-vs-pinned-version issue: pinning to `3.0.0` (the version the upstream
single-binary example itself defaults to) shows the identical ring/live-store log
lines and the identical behavior.

Separately (and initially conflated with the above before checking): recreating the
`tempo` container alone, without restarting `otel-collector`, breaks the
collector's already-established gRPC connection to Tempo's old container IP --
ordinary container-lifecycle behavior, not Tempo-specific, but worth remembering
when iterating on this stack (`docker compose restart otel-collector` after
recreating `tempo`).

No fix attempted for the ring self-eviction itself -- root-causing Tempo 3.x's
live-store health-check tuning is a deeper rabbit hole than this demo needs. If
Grafana's dashboard shows no trace data, re-materializing the pipeline is the
practical workaround (confirmed: a fresh run after the eviction landed and
searched normally). Worth revisiting if this turns out to bite real usage rather
than just an idle demo stack.

### Three more gotchas, found walking a real user through actually viewing this (2026-09-20)

The first two are genuinely Grafana/Tempo-side (confirmed by directly querying
Tempo's own API in parallel with what the browser was doing, matching real request
logs at the exact timestamps). **The third turned out not to be** -- see below --
and is the one that actually explained the dashboard's traces panel showing "No
data found in response" the whole time, not the two Grafana-side items, which were
real but insufficient on their own to fix it.

- **Grafana's anonymous auth is Viewer-only in this version**, regardless of
  `GF_AUTH_ANONYMOUS_ORG_ROLE` -- confirmed via Grafana's own startup log:
  `"auth.anonymous.org_role is deprecated, only viewer role is supported"`. Combined
  with `GF_AUTH_DISABLE_LOGIN_FORM=true` (no way to log in as anything else), this
  left no path to Editor/Admin capabilities at all. Fixed by keeping the login form
  enabled with a fixed `admin`/`admin` account instead of relying on anonymous
  access alone. Also had `GF_FEATURE_TOGGLES_ENABLE=<names>` (plural, space-separated)
  silently do nothing -- Grafana logs it as deprecated too; one
  `GF_FEATURE_TOGGLES_<NAME>=true` var per toggle is what this version actually
  reads.
- **Tempo's tag-*values* index (what populates the Search tab's "Service Name"
  dropdown) is empty for data this fresh**, even though the same data is fully
  findable by a direct query -- confirmed: `GET /api/v2/search/tag/resource.service.
  name/values` returned `{"tagValues": []}` at the same moment `GET /api/search?q=
  {resource.service.name="..."}`  returned the real trace. Whatever populates that
  values index appears to lag behind (or depend on a compacted-block state the
  live-store fast path doesn't need) separately from search itself being
  immediately consistent. Practical takeaway: use the **TraceQL** tab (free-text
  query, confirmed working) rather than the **Search** tab's dropdown-driven
  filters for anything recently ingested.
- **Not actually a Tempo/Grafana issue: this project's own root spans carry a fake,
  non-empty `parentSpanId`.** After fixing both items above, the dashboard's
  **traces** panel (the visualization that needs to structurally resolve a real
  root span, unlike Explore's flatter Table view, which worked throughout) still
  showed no data. Traced to `_seed_run_root_context()` in `_propagation.py`: its
  placeholder `span_id=0x1`, meant as "never a real span, only trace_id matters,"
  gets exported as a literal `parentSpanId` on the resulting root span --
  `AAAAAAAAAAE=` (base64) decodes to `0000000000000001`, and no span with that ID
  exists anywhere in the trace. Tempo's own search summary reflects this
  accurately: `"rootServiceName": "<root span not yet received>"` on every trace
  this project has produced through this code path, not just this demo's. **Fixed
  in [Issue #63](https://github.com/HirofumiTsuda/dagster-otel/issues/63)** --
  `_DeterministicRunIdGenerator` (`_setup.py`), a custom OTel `IdGenerator`, gets
  the deterministic `trace_id` without ever activating a parent context at all, so
  the SDK's own `parent_span_id`-recording logic has nothing to attach to. Verified
  after the fix landed: `rootServiceName` resolves correctly, and the dashboard's
  traces panel renders the trace.
- **Two more, purely browser/session-side, found confirming the fix above through
  the actual dashboard UI (not just the API) with a real person watching:**
  - Grafana's session token has a rotation mechanism that can end up needing a
    fresh login to recover from -- confirmed via server logs at the exact moment
    of a failed panel load: `error="[session.token.rotate] token needs to be
    rotated"`, `POST /api/ds/query status=401`. Frontend shows this as "No data
    found in response" (a data-shaped message for what's actually an auth
    failure), so it's easy to misdiagnose as a query/backend problem. Logging out
    and back in resolves it, but note Grafana's post-login redirect can land on
    the home page, not back on the dashboard you were viewing -- re-navigate to it
    explicitly rather than assuming a re-login alone fixes a panel that's no
    longer even on screen.
  - The query editor's **"Table view"** toggle is a per-viewer UI preference, not
    part of the saved dashboard -- turning it on for the traces panel produces the
    same "No data found in response", with nothing in the dashboard JSON
    responsible for it (confirmed: fetched the live dashboard's JSON directly,
    nothing table-view-related is stored on the panel or its targets). No fix
    possible in the dashboard definition itself; noted in the traces panel's own
    `description` field instead, the one thing that *is* persisted and visible to
    whoever hits this next.

### The actual fix: the traces panel was the wrong panel type all along (2026-09-20)

The two session/toggle items above were both real and both reproduced, but neither
one was the actual, reproducible cause -- they explained specific *instances* of "No
data found in response," not why it kept recurring. Walking through it again with a
fresh login and the toggle confirmed off, the panel still showed no data, with the
browser's Network tab showing the `ds_type=tempo` request wasn't even being sent
(only the three `ds_type=prometheus` requests for the other panels were). Root
cause, found along the way:

- **The `grafana` container's `dev/grafana/dashboards` bind mount had come up
  empty.** `docker compose exec grafana ls /var/lib/grafana/dashboards/` showed
  nothing, while the same path on the host had the real file -- Docker had bound an
  empty directory at container-creation time (this directory was created after the
  container's first `docker compose up`) and, on this Docker Desktop/WSL2 setup,
  never picked up the host directory's contents afterward. `docker compose up -d
  --force-recreate grafana` fixed it (confirmed: the file appeared inside the
  container immediately after). Worth remembering for any bind-mounted directory
  created after its container's first start, not just this one.
- **Once the panel could actually load, "No data found in response" turned out to
  be structurally correct, not a bug at all.** The dashboard's target is a TraceQL
  **search** query (`{resource.service.name="..."}`, matching however many traces
  fit the filter) -- confirmed by reading the panel's own `/api/ds/query` response
  directly: its frame schema carries `"meta": {"preferredVisualisationType":
  "table"}`. Tempo itself is saying this result is table-shaped. Grafana's native
  **traces** panel renders a single trace's span tree (parent/child span
  hierarchy) -- structurally incompatible with a multi-trace search result,
  regardless of session state or UI toggles. **Fixed** by changing the panel's
  `type` from `"traces"` to `"table"` in `combined-demo-dashboard.json` -- the
  Trace ID column keeps its drill-down link (`internal.query` in the field config)
  into the full waterfall view, so nothing is lost, it's just a click away instead
  of inline. Verified against a real run's data (screenshots in
  `examples/README.md`).

This is also the reason the "Table view" toggle and session-rotation items above
*looked* like they explained the problem: both are real, independent ways to get
the exact same "No data found in response" message on any panel, so each one was a
plausible-looking, and wrong, explanation for a problem that was actually
structural. The lesson generalizes: prefer reading the actual API response over
reasoning from the frontend's (often reused, generic) error message.

## Open questions

None currently tracked -- multi-root/fan-in (#5), k8s_job_executor (#3), and
retry-from-failure (#4) were the three open verification questions, and all three are
now confirmed against real Dagster, not just reasoned through. See
[#11](https://github.com/HirofumiTsuda/dagster-otel/issues/11) for what's still
missing before a release, not this library's own behavior.

## `AssetExecutionContext.run_id` deprecation (Issue #68, 2026-09-21)

Found via [`opentelemetry-instrumentation-dagster`](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster)'s
own test suite (a sibling project depending on this one, with no upper pin
beyond `dagster >= 1.5`, so it resolved a newer `dagster` than this repo's
own `uv.lock` -- 1.13.23, not 1.13.22) surfacing `DeprecationWarning`s on
every `context.run_id` read reachable from an `AssetExecutionContext`.
`OpExecutionContext.run_id` is unaffected -- only `AssetExecutionContext`'s
own convenience properties (`run_id`, `dagster_run`, `run_config`,
`run_tags`, `has_tag`) are individually `@deprecated`, confirmed by reading
`asset_execution_context.py` directly, not guessed from the warning text
alone.

Fixed by switching every site (`_tracing.py`'s span attribute,
`_propagation.py`'s three internal lookups) to `context.run.run_id` instead
-- `.run` itself carries no deprecation on either context class, and (checked
by downloading the `dagster==1.5.0` wheel directly rather than assuming)
existed at that version too, so it's safe across this library's whole
declared `dagster >= 1.5` floor, not just the versions currently pinned in
`uv.lock`. Verified against real `dagster==1.13.23` (the exact version that
originally surfaced the warning) with `-W error::DeprecationWarning` --
materializes cleanly, no warning raised.

## `AssetCheckExecutionContext` support (Issue #72, 2026-09-21)

Found while scoping auto-instrumentation support for `@asset_check` in the
sibling project ([opentelemetry-instrumentation-dagster#16](https://github.com/HirofumiTsuda/opentelemetry-instrumentation-dagster/issues/16)):
applying `@traced()` to a real `@asset_check` function crashed at runtime --
`AttributeError: 'AssetCheckExecutionContext' object has no attribute
'job_name'`. `ExecutionContext` (`_types.py`) only covered `OpExecutionContext
| AssetExecutionContext`; `AssetCheckExecutionContext` is a genuinely
different shape (checked `dagster/_core/execution/context/
asset_check_execution_context.py` directly, not guessed from the crash
alone): no `.job_name` (only `.job_def`), no `.op_handle` (not that this
library still reads that one -- see below), no `.selected_asset_keys` (only
`.selected_asset_check_keys`, a `frozenset[AssetCheckKey]`, a different key
type entirely). Does have `.log`, `.run`, `.retry_number`, `.instance`, and
`.get_step_execution_context()` -- same shape as the other two for those.

Fixed:

- `ExecutionContext` widened to all three. `dagster.job_name` now reads
  uniformly from `context.job_def.name` across all three context types
  instead of `context.job_name` -- confirmed against a real run that
  `.job_def.name` gives the identical value `.job_name` already did for
  `OpExecutionContext`, so this isn't a behavior change for existing
  `@op`/`@asset` users, just a different (available-everywhere) path to the
  same value.
- `dagster.asset_keys` (existing attribute) vs. a new `dagster.asset_check_keys`
  attribute: a real `isinstance(context, AssetCheckExecutionContext)` branch
  in `_tracing.py`, not a unifiable property access -- `AssetCheckKey` isn't
  an `AssetKey`, so there's no single attribute name both shapes could share
  the way `job_def.name` could.
- `_own_step_key`/`_upstream_step_keys` (`_propagation.py`) needed no changes
  at all -- despite their own docstrings still discussing `.op_handle`
  (historical, from Issue #45; `.op_handle` isn't actually read anywhere in
  this codebase anymore), both already use `context.get_step_execution_
  context().step.key`/`.step_inputs`, which is present and correct on
  `AssetCheckExecutionContext` too (confirmed against a real run: step key
  `"my_asset_my_check"`, matching Dagster's own step-naming for asset checks).
- `dbt.py`'s `traced_dbt()` is bound to a new, narrower alias
  (`AssetOrOpExecutionContext = OpExecutionContext | AssetExecutionContext`,
  not the widened `ExecutionContext`) -- `context.asset_key_for_output(...)`
  (needed to resolve a dbt-yielded `Output`'s real `AssetKey`) isn't on
  `AssetCheckExecutionContext` at all, and `@dbt_assets`/op-based `dbt.cli()`
  usage never actually produces one anyway (dbt_assets is multi_asset-shaped,
  not asset_check-shaped) -- caught by mypy the moment `ExecutionContext`
  widened, not by a runtime failure.

Verified against a real `@asset_check` execution + real Jaeger, not just the
type checker: a `@traced()`-decorated `verify_check` (checking a
`@traced()`-decorated `verify_asset`) produced two spans, both with correct
`dagster.job_name`/`dagster.run_id`/`dagster.retry_number`/`dagster.step_key`;
`verify_asset`'s span carries `dagster.asset_keys = verify_asset`, and
`verify_check`'s carries `dagster.asset_check_keys = verify_asset:verify_check`
(via `AssetCheckKey.to_user_string()`) -- not `dagster.asset_keys`, confirming
the `isinstance` branch actually fires for the right context type in
practice, not just in principle.

## `traced_sensor()`/`traced_schedule()`: tick evaluation tracing (Issue #38, 2026-09-21)

Everything above traces a *run's* steps. None of it touches a `@sensor`/`@schedule`
tick's own evaluation -- what decides whether a run happens at all, running outside
any op/asset compute context, before any run_id exists. A slow, erroring, or
non-obviously-skipping tick was invisible to the trace backend entirely; only whatever
run it eventually launched (if any) showed up, with no link back to the tick that
caused it.

**Shape, confirmed via live introspection rather than assumed:**

- A tick has no run_id and no `context.instance`-backed state this library's existing
  propagation lookups (`_propagation.py`) key on at all -- `SensorEvaluationContext`
  has no `run_id`, only `.sensor_name`/`.cursor`/`.instance`/`.log`/
  `.last_completion_time` (checked via `dir()`). So a tick needs none of `traced()`'s
  cross-process deterministic-trace_id-seeding machinery
  (`_ancestor_runs`/`_seed_run_root_context`) -- unlike a step, a tick evaluation
  happens in exactly one process/one function call, so each tick is simply a fresh,
  ordinary root span.
- `ScheduleEvaluationContext` has no public name accessor at all -- confirmed via
  `dir(dagster.ScheduleEvaluationContext)` (no `schedule_name` in the public listing)
  and by reading `schedule_definition.py` directly: only a private `_schedule_name`
  `__slots__` entry, never exposed via a property. `traced_schedule()`'s
  `dagster.schedule_name` attribute falls back to the span's own name (`span_name or
  func.__name__`) instead -- matching Dagster's own default schedule naming (a
  schedule's name defaults to its decorated function's name unless overridden via
  `@schedule(name=...)`), so this agrees with the real schedule name for the common
  case even though it can't be read from the context.
- `context.scheduled_execution_time` is `@public` but **raises** `CheckError` rather
  than returning `None` when the context was built without one (confirmed live via
  `build_schedule_context()` with no argument) -- not just a test artifact, since
  nothing about the type signature (`-> datetime`, not `datetime | None`) or a real
  evaluation call site guarantees a caller always supplies one. `_schedule_attributes`
  wraps the read in a narrow `try/except Exception` rather than importing
  `CheckError` from its actual home (`dagster_shared.check.functions`, not reachable
  from the public `dagster` namespace at all -- `hasattr(dagster, "check")` is
  `False`).
- Reuses the existing `EXTERNAL_TRACE_CONTEXT_TAG_KEY` mechanism (Issue #13, built for
  an *external* caller to nest a run under its own trace) but originating from
  *inside* Dagster this time: any `RunRequest` a tick returns/yields (bare, inside a
  plain `list`/`tuple`, or inside `SensorResult.run_requests`) gets the tag injected
  into its `tags` via `RunRequest._replace(tags=...)` (`RunRequest` is immutable,
  NamedTuple-based -- confirmed live that `._replace()` works). The launched run's own
  root step then finds this via the *already-existing*
  `find_external_trace_context()` lookup, unmodified -- no changes needed on that side
  at all.
- Real bug caught by the unit tests before this ever ran against anything real:
  `SkipReason` is *also* NamedTuple-based (a real `tuple`/`Sequence` under the hood,
  single `skip_message` field) -- an `isinstance(value, Sequence)` check (the first
  draft of the "is this a bare sequence of RunRequests" branch) silently shredded a
  returned `SkipReason("...")` into a one-element `list`, corrupting the return value
  Dagster itself would then receive. Fixed by checking `type(value) in (list, tuple)`
  instead of `isinstance(value, Sequence)` -- matches only a genuine plain `list`/
  `tuple`, not any other NamedTuple-shaped member of the return union (this also
  avoids needing to enumerate `DagsterRunReaction` by name, which isn't even
  importable from the public `dagster` namespace).
- `SensorResult(run_requests=None, ...)` normalizes to `run_requests=[]` in real
  Dagster (confirmed live) -- the `is None` branch in `_tag_tick_result` is currently
  unreachable in practice, kept anyway since it matches the field's own declared
  `Sequence[RunRequest] | None` type and costs nothing.

A separate module (`_sensors.py`), not folded into `_tracing.py`: a tick function's
signature/semantics (`SensorEvaluationContext`/`ScheduleEvaluationContext`, returns
`RunRequest`/`SkipReason`/`SensorResult`/`None`/a plain dict, not an op's `Output`) are
different enough from an op/asset compute function's that reusing `traced()`'s
`ComputeFn`/propagation/`dagster.*`-attribute machinery wouldn't fit cleanly -- same
"separate, explicitly-named decorator" reasoning `traced_dbt()` already uses.

**Verified against a real `@sensor` + real `@schedule` + real Jaeger** (not just unit
tests), via `SensorDefinition.evaluate_tick()`/`ScheduleDefinition.evaluate_tick()`
(Dagster's own public tick-evaluation API) against a real `DagsterInstance.ephemeral()`,
then actually launching the returned `RunRequest` via `execute_in_process(tags=...)`:

- Both tick spans (`verify_sensor`, `verify_schedule`) came back as genuine trace
  roots (empty `references`), each carrying its own `dagster.sensor_name` /
  (`dagster.schedule_name` + `dagster.scheduled_execution_time`).
- Querying Jaeger's `/api/traces/<traceID>` for each tick's own trace_id showed the
  *launched run's* `verify_asset` step span present in the *same trace*, with a real
  `CHILD_OF` reference back to the tick span -- confirming the
  `EXTERNAL_TRACE_CONTEXT_TAG_KEY` injection→`find_external_trace_context()` lookup
  round-trip actually works when the tag originates from `traced_sensor()`/
  `traced_schedule()`, not just from Issue #13's original external-caller case.

## License

MIT -- matches [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter)
(my other Dagster project) and formenergy-observability (which this design builds on
ideas from, not copied code -- see [attribution](#prior-art-and-why-this-project-exists-anyway)
above). Originally set to Apache-2.0 on the assumption that might matter for eventual
upstreaming into opentelemetry-python-contrib; turned out not to (see above), so no
reason to diverge from MIT.
