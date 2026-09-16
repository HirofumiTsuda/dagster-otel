"""Cross-process OpenTelemetry trace context propagation for Dagster steps.

Dagster's multiprocess and k8s executors run each step in its own process (often on
a different node entirely), so there is no shared memory to pass an OTel trace context
through directly. This module instead uses Dagster's own run storage -- which every
step process can already read and write via `context.instance`, regardless of executor
or node -- as the transport: every step publishes its own trace context as a run tag,
keyed by its own step key, and every downstream step looks up its *real* upstream
step(s) by the same key.

This piggybacks on a problem Dagster has already solved for itself (making state visible
to every step of a run, on any executor) instead of re-solving cross-process/cross-node
communication from scratch.

Design and the core trick (publish trace context as run-scoped state, readable by every
step) verified against the approach and prior art in
https://github.com/Form-Energy/formenergy-observability, which shipped the same idea
for @op using AssetMaterialization events as the carrier, keyed by *subgraph path*
rather than each step's own identity (see "Keyed by real step dependencies" below for
why that changed). This module generalizes it to also work unmodified for @asset (both
share the same OpExecutionContext-like surface under the hood), drops the op-defining
decorator in favor of a plain function decorator (see _tracing.py) that can stack under
Dagster's own @op/@asset, so this library never owns the op/asset definition, and uses
run tags (see below) as the transport instead of AssetMaterialization events.

**Why run tags, not AssetMaterialization events (as formenergy-observability does)**:
verified directly (2026-09-15) that publishing via AssetMaterialization registers the
bookkeeping asset_key (e.g. `__dagster_otel_root__`) in Dagster's real asset catalog
(`instance.all_asset_keys()`) -- a fake entry shows up next to real data assets in the
Dagster UI, for jobs that use *no* assets at all. `instance.add_run_tags(run_id, tags)` /
`run.tags` carries the same JSON payload through the same run-storage database, is
readable from a freshly-opened `DagsterInstance` the same way (verified), and adds
nothing to the asset catalog (verified: `instance.all_asset_keys()` stays empty).

**Keyed by real step dependencies, not subgraph path (2026-09-15, Issue #5)**: the
original design here (like formenergy-observability's) keyed published contexts by
*subgraph path* -- correct for nesting depth, but unable to distinguish sibling
top-level steps. Confirmed reproducible bug: two independent roots (`root_a`,
`root_b`, no dependency between them) raced on the same shared key, and whichever
published second silently "won" -- a downstream step of `root_a` could end up parented
under `root_b` instead. Reproduced against this module *and* formenergy-observability
(same architecture, same bug -- see docs/design.md), not something this
reimplementation introduced.

Fixed properly (not just bounded) by keying on each step's *real* dependencies, read
from Dagster's own execution plan (`StepInput.dependency_keys`, verified against both
`OpExecutionContext` and `AssetExecutionContext`, under the real multiprocess
executor) instead of approximating from subgraph nesting. Every step now publishes
under its own step key, and looks itself up by asking "what are my real upstream step
keys" rather than "what's the nearest enclosing subgraph." Two independent roots
simply have disjoint dependency sets now -- there's no shared key left to race on.
Fan-in (a step with more than one direct upstream) is also handled properly: every
upstream that has published is returned, not just one, so `_tracing.py` can make the
first the real parent and the rest `Link`s.

Reads dependency info via `context.get_step_execution_context()` -- defined on both
`OpExecutionContext` and `AssetExecutionContext` (the latter delegates to the former),
so no branching is needed between the two context shapes here, unlike elsewhere in
this module. Not `@public` (no versioned-API guarantee) and its docstring says
`:meta private:`, but it's also not underscore-prefixed, and its own docstring says
exactly what this is doing with it: "Allows advanced users (e.g. framework authors) to
punch through to the underlying step execution context." A step's own dependency keys
specifically (`StepExecutionContext.step.step_inputs[i].dependency_keys`) still has no
`@public` accessor of its own, so this remains an accepted-risk internal-API dependence
overall (see docs/design.md's "no monkeypatching" section) -- a rename/move here is a
loud AttributeError at call time, not a silent behavior change.

Residual limitation (unrelated to any of the above, still tracked in Issue #5): an
upstream step that *isn't* `@traced()` (or hasn't run yet when looked up, though
Dagster's own execution order should prevent that for a real dependency) simply won't
be found -- this only looks at *direct* dependencies, not transitively past an
untraced one. `_seed_run_root_context` remains the fallback for that case, same as for
a genuine root: every step derives the same trace_id deterministically from
`context.run_id`, so even a step that can't find any real parent still lands in the
*same trace* as the rest of the run.
"""

import hashlib
import json
from collections.abc import Sequence

from dagster import DagsterRun
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from dagster_otel._types import ExecutionContext

#: Run tag key prefix. Full tag key is this plus the publishing step's own step key
#: (see _own_step_key) -- e.g. "dagster_otel/trace_context/root_a".
_TAG_PREFIX = "dagster_otel/trace_context/"

#: Run tag key a *caller* sets (before/while triggering a run -- e.g. `tags={
#: EXTERNAL_TRACE_CONTEXT_TAG_KEY: json.dumps(carrier)}` at execute_job()/CLI launch
#: time), not something this library ever writes itself, to nest a whole run's trace
#: under whatever triggered it (Issue #13) -- a CI/CD pipeline, a scheduler, another
#: OTel-instrumented system. Distinct from _TAG_PREFIX (which is per-step, written by
#: every @traced() step) -- this is per-run, written at most once, by something
#: outside Dagster entirely.
EXTERNAL_TRACE_CONTEXT_TAG_KEY = "dagster_otel/external_trace_context"


def _ancestor_runs(context: ExecutionContext) -> list[DagsterRun]:
    """The current run, then any ancestor runs (oldest last), each fetched from run
    storage exactly once.

    A retry-from-failure creates a new run whose `parent_run_id` points at the run it
    retried, and may re-execute only a subset of steps. If the retried run doesn't
    re-run the step that published the trace context, it's only findable by walking
    up to the parent run's event log.

    Callers that need to check several things against "this run, and (for retries)
    where it came from" -- find_upstream_trace_contexts, find_external_trace_context,
    find_previous_attempt_context all do -- should call this once and pass the result
    to each, rather than letting each independently re-walk and re-fetch the same
    chain (Issue #35: a fan-in step with U upstreams on a run with N ancestor hops
    was doing on the order of `(U + 1) * 2N` `get_run_by_id` calls where `N` suffices,
    since the original version of this walk fetched every run just to read
    `parent_run_id`, then discarded it, leaving every caller to fetch the same runs
    again to read `.tags`).
    """
    runs: list[DagsterRun] = []
    run_id: str | None = context.run_id
    while run_id is not None:
        run = context.instance.get_run_by_id(run_id)
        if run is None:
            break
        runs.append(run)
        run_id = run.parent_run_id
    return runs


def _own_step_key(context: ExecutionContext) -> str:
    """This step's own identifier, in the same string form Dagster's own dependency
    info (StepInput.dependency_keys, see _upstream_step_keys) uses to reference it.

    `context.get_step_execution_context().step.key` (`ExecutionStep.key`, a
    `StepHandle`/`ResolvedFromDynamicStepHandle.to_key()`), not
    `".".join(context.op_handle.path)` (Issue #45) -- the two agree for an ordinary,
    non-mapped step (verified: `StepHandle.key` defaults to `str(node_handle)`, and
    `NodeHandle.__str__` builds the identical dotted-path string `.path` does), but
    diverge for a step produced by Dagster's dynamic graph mapping (`DynamicOut`/
    `.map()`): `op_handle` has no `mapping_key` on it at all, while the real,
    per-mapped-instance step key is `f"{node_handle}[{mapping_key}]"`
    (`StepHandle.parse_from_key`'s own regex). Using `op_handle.path` made every
    parallel invocation of one mapped op compute the identical key -- concurrent
    instances raced to overwrite the same run tag, and a downstream collect step's
    `dependency_keys` (which *do* include `[mapping_key]`) could never match what
    was published, silently falling back to the deterministic root seed instead of
    finding its real parent. `step.key` is exactly the `[mapping_key]`-suffixed
    string `dependency_keys` already uses, so both sides of the lookup now agree.
    """
    return context.get_step_execution_context().step.key


def _upstream_step_keys(context: ExecutionContext) -> frozenset[str]:
    """The step_keys of every step whose output this step directly depends on, read
    from Dagster's own execution plan. Empty for a step with no dependencies (a
    genuine root).

    `get_step_execution_context()` is defined identically on both OpExecutionContext
    and AssetExecutionContext (the latter delegates to the former) -- no branching on
    context shape needed here, unlike elsewhere in this module."""
    keys: set[str] = set()
    for step_input in context.get_step_execution_context().step.step_inputs:
        keys.update(step_input.dependency_keys)
    return frozenset(keys)


def publish_trace_context(context: ExecutionContext) -> None:
    """Publish the current span's trace context, under this step's own key, for
    downstream steps to find.

    Call this from every `@traced()` step (see _tracing.py -- it does this for you),
    not just ones that turn out to have no parent: a step's *own* publish is what lets
    the steps that depend on *it* find their real parent.
    """
    span = trace.get_current_span()
    if span is None or span.get_span_context().span_id == 0:
        raise RuntimeError(
            "publish_trace_context() called with no active span -- call it inside a "
            "`with tracer.start_as_current_span(...)` block."
        )
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)

    tag_key = _TAG_PREFIX + _own_step_key(context)
    context.instance.add_run_tags(context.run_id, {tag_key: json.dumps(carrier)})


def _find_context_for_step_key(
    runs: Sequence[DagsterRun], step_key: str
) -> dict[str, str] | None:
    """The trace context a specific step published, if any -- searching the given
    runs (see _ancestor_runs) in order."""
    tag_key = _TAG_PREFIX + step_key
    for run in runs:
        tag_value = run.tags.get(tag_key)
        if tag_value is not None:
            result: dict[str, str] = json.loads(tag_value)
            return result
    return None


def find_upstream_trace_contexts(
    context: ExecutionContext, runs: Sequence[DagsterRun] | None = None
) -> list[dict[str, str]]:
    """The trace contexts of this step's real, direct upstream dependencies that have
    themselves published one -- in deterministic (sorted step_key) order, so which one
    `_tracing.py` treats as the real parent (vs. a Link, for fan-in) is stable rather
    than dependent on lookup order.

    Empty if this step has no dependencies (a genuine root) or none of its direct
    dependencies have published (e.g. an untraced upstream, or -- shouldn't happen
    given Dagster's own execution order, but not asserted against here -- looked up
    before an upstream has run). Callers should treat empty the same as "no parent
    found," not an error.

    :param runs: This run and its ancestors (see `_ancestor_runs`), if the caller
        already fetched them for another lookup in the same step -- computed here if
        not given. `_tracing.py` passes this through explicitly so one step's worth
        of `_traced_span()` calls this exactly once, not once per lookup (Issue #35).
    """
    if runs is None:
        runs = _ancestor_runs(context)
    return [
        carrier
        for key in sorted(_upstream_step_keys(context))
        if (carrier := _find_context_for_step_key(runs, key)) is not None
    ]


def find_external_trace_context(
    context: ExecutionContext, runs: Sequence[DagsterRun] | None = None
) -> dict[str, str] | None:
    """The trace context an external caller published before/while triggering this
    run, if any (Issue #13) -- e.g. a CI/CD pipeline or another OTel-instrumented
    system that wants this whole run's trace nested under its own, not starting a
    fresh unrelated one.

    Unlike every other lookup in this module, this reads a tag this library never
    writes itself -- the caller sets `EXTERNAL_TRACE_CONTEXT_TAG_KEY` directly (e.g.
    via `tags={EXTERNAL_TRACE_CONTEXT_TAG_KEY: json.dumps(carrier)}` at launch time),
    before any `@traced()` step runs. Confirmed against real Dagster + Jaeger that a
    tag set this way (via `execute_job(tags=...)` and CLI `--tags` alike) is visible
    from `context.instance.get_run_by_id(context.run_id).tags` by the time any step
    starts -- the same read path every other propagation lookup here already uses.

    Walks the same ancestor-run chain as `_find_context_for_step_key` -- checked
    directly against a real retry-from-failure run (2026-09-15) that Dagster does
    *not* copy a run's tags forward to a retry by default (confirmed: a custom tag
    set on the original run was simply absent from `instance.get_run_by_id(retry_run_id)
    .tags`, an empty dict). So a retried run needs the same parent-run walk every
    other cross-run lookup here already does, to still find what the *original*
    launch was seeded with -- without it, only the original run's own root steps
    would ever see the external context, and a retry would silently fall back to the
    deterministic run_id seed instead.

    :param runs: See `find_upstream_trace_contexts`'s `runs` parameter -- same
        share-the-fetch purpose.
    """
    if runs is None:
        runs = _ancestor_runs(context)
    for run in runs:
        tag_value = run.tags.get(EXTERNAL_TRACE_CONTEXT_TAG_KEY)
        if tag_value is not None:
            result: dict[str, str] = json.loads(tag_value)
            return result
    return None


def find_previous_attempt_context(
    context: ExecutionContext, runs: Sequence[DagsterRun] | None = None
) -> dict[str, str] | None:
    """The trace context this same step published on a previous attempt, if this is
    an op-level `RetryPolicy` retry (`context.retry_number > 0`) and that attempt
    actually published one (Issue #14).

    Looked up *before* this attempt's own `publish_trace_context()` call overwrites
    it: a RetryPolicy-triggered retry re-executes the same step as a fresh process
    (confirmed against a real retry), but publishes under the exact same
    `_own_step_key(context)` as every other attempt of it -- there's nothing else to
    disambiguate attempts by in the tag key itself. Whatever's currently there when a
    later attempt starts is necessarily the immediately-preceding attempt's publish,
    since every attempt (successful or not -- publish happens before the wrapped
    function body runs, so even a subsequently-failing attempt still publishes)
    overwrites the same key in order.

    None for the first attempt (nothing preceded it) or if the previous attempt never
    published (untraced, or Dagster's execution order not guaranteeing what this
    assumes -- not asserted against here, treated as "no relationship found" like
    every other lookup in this module).

    :param runs: See `find_upstream_trace_contexts`'s `runs` parameter -- same
        share-the-fetch purpose. Not fetched at all when `context.retry_number == 0`,
        since nothing here is looked up in that case anyway.
    """
    if context.retry_number == 0:
        return None
    if runs is None:
        runs = _ancestor_runs(context)
    return _find_context_for_step_key(runs, _own_step_key(context))


def _activate_trace_context(carrier: dict[str, str]) -> None:
    """Make a previously-published trace context the active one in this process.

    New spans started after this call will be children of the trace the carrier came
    from, even though it was published from a different process (or node).
    """
    ctx = TraceContextTextMapPropagator().extract(carrier)
    otel_context.attach(ctx)


def carrier_to_span_context(carrier: dict[str, str]) -> trace.SpanContext:
    """Recover just the SpanContext from a published carrier, without activating it
    as the current context -- for building an OTel `Link` (fan-in's non-parent
    upstreams), which needs a SpanContext, not a full context activation."""
    ctx = TraceContextTextMapPropagator().extract(carrier)
    return trace.get_current_span(ctx).get_span_context()


def _seed_run_root_context(context: ExecutionContext) -> None:
    """Activate a context carrying a trace_id derived deterministically from the run
    ID, for a step that found no real parent to attach to (see module docstring for
    the cases this covers). Every step that calls this for the same run computes the
    identical trace_id independently, with no shared state and no race.

    The seeded span context is a NonRecordingSpan (Dagster's own run storage, not
    this, remains the source of truth for real parent/child spans -- see
    publish_trace_context/find_upstream_trace_contexts) with `is_remote=True`, the
    same shape OTel's own propagators use for context received from outside the
    process. Its span_id is a fixed placeholder (never a real span), only trace_id
    matters here.
    """
    trace_id = int.from_bytes(hashlib.sha256(context.run_id.encode()).digest()[:16], "big")
    seed = NonRecordingSpan(
        SpanContext(
            trace_id=trace_id,
            span_id=0x1,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
    )
    otel_context.attach(trace.set_span_in_context(seed))
