"""Cross-process OpenTelemetry trace context propagation for Dagster steps.

Dagster's multiprocess and k8s executors run each step in its own process (often on
a different node entirely), so there is no shared memory to pass an OTel trace context
through directly. This module instead uses Dagster's own run storage -- which every
step process can already read and write via `context.instance`, regardless of executor
or node -- as the transport: the root step publishes the trace context as a run tag,
and every other step looks it up from there.

This piggybacks on a problem Dagster has already solved for itself (making state visible
to every step of a run, on any executor) instead of re-solving cross-process/cross-node
communication from scratch.

Design and the core trick (publish trace context as run-scoped state, readable by every
step) verified against the approach and prior art in
https://github.com/Form-Energy/formenergy-observability, which shipped the same idea
for @op using AssetMaterialization events as the carrier. This module generalizes it to
also work unmodified for @asset (both share the same OpExecutionContext-like surface
under the hood), drops the op-defining decorator in favor of a plain function decorator
(see _tracing.py) that can stack under Dagster's own @op/@asset, so this library never
owns the op/asset definition, and uses run tags (see below) as the transport instead of
AssetMaterialization events.

**Why run tags, not AssetMaterialization events (as formenergy-observability does)**:
verified directly (2026-09-15) that publishing via AssetMaterialization registers the
bookkeeping asset_key (e.g. `__dagster_otel_root__`) in Dagster's real asset catalog
(`instance.all_asset_keys()`) -- a fake entry shows up next to real data assets in the
Dagster UI, for jobs that use *no* assets at all. `instance.add_run_tags(run_id, tags)` /
`run.tags` carries the same JSON payload through the same run-storage database, is
readable from a freshly-opened `DagsterInstance` the same way (verified), and adds
nothing to the asset catalog (verified: `instance.all_asset_keys()` stays empty). Also
simpler: no need to filter/narrow a ~26-member DagsterEvent union to find the one
event kind with a `.materialization` attribute (see git history if that filtering is
ever needed again for another purpose).

Known limitation, confirmed reproducible against both this module and the
Form-Energy prior art it's based on (same architecture, same bug -- see
docs/design.md): a job/run with more than one independent root (no dependency
between them) can race on ROOT_TRACE_KEY, since whichever root's step happens to
run first "wins" and every other independent root silently attaches to it instead of
getting its own. `_seed_run_root_context` (below) bounds the damage: every step
derives the same trace_id deterministically from `context.run_id`, so a step that
can't find a real parent still lands in the *same trace* as the rest of the run
instead of a completely disconnected one. Parent-span attribution among independent
roots is still not guaranteed correct -- only the trace_id is.
"""

import hashlib
import json

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from dagster_otel._types import ExecutionContext

#: Trace key used for the top-level/root span of a run, when the publishing step isn't
#: inside a named subgraph.
ROOT_TRACE_KEY = "__dagster_otel_root__"

#: Run tag key prefix. Full tag key is this plus the trace key (see _trace_key_for) --
#: e.g. "dagster_otel/trace_context/__dagster_otel_root__".
_TAG_PREFIX = "dagster_otel/trace_context/"


def _run_id_and_ancestors(context: ExecutionContext) -> list[str]:
    """The current run's ID, then any ancestor run IDs (oldest last).

    A retry-from-failure creates a new run whose `parent_run_id` points at the run it
    retried, and may re-execute only a subset of steps. If the retried run doesn't
    re-run the step that published the trace context, it's only findable by walking
    up to the parent run's event log.
    """
    run_id = context.run_id
    run_ids = [run_id]
    while True:
        run = context.instance.get_run_by_id(run_id)
        if not run or not run.parent_run_id:
            break
        run_id = run.parent_run_id
        run_ids.append(run_id)
    return run_ids


def _trace_key_for(context: ExecutionContext) -> str:
    """The event-log key this step should publish/look up its trace context under.

    Dagster represents subgraphs as dotted paths in `op_handle.path`. A step at the
    top level of the graph uses ROOT_TRACE_KEY; a step nested in a subgraph uses the
    subgraph's path, so sibling subgraphs (and root-level steps outside them) don't
    collide.
    """
    path = context.op_handle.path
    return ROOT_TRACE_KEY if len(path) <= 1 else ".".join(path[:-1])


def publish_trace_context(context: ExecutionContext) -> None:
    """Publish the current span's trace context for other steps in this run to find.

    Call this exactly once per run (or once per subgraph, for a step at that
    subgraph's root) -- typically from whichever step runs first. Every other step
    that uses @traced() will look this up automatically.
    """
    span = trace.get_current_span()
    if span is None or span.get_span_context().span_id == 0:
        raise RuntimeError(
            "publish_trace_context() called with no active span -- call it inside a "
            "`with tracer.start_as_current_span(...)` block."
        )
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)

    tag_key = _TAG_PREFIX + _trace_key_for(context)
    context.instance.add_run_tags(context.run_id, {tag_key: json.dumps(carrier)})


def _find_trace_context(context: ExecutionContext) -> dict[str, str] | None:
    """Look up the trace context published by publish_trace_context(), if any.

    Falls back from this step's subgraph up to the run root, and searches ancestor
    runs for retries. Returns None if nothing has been published yet -- callers should
    treat that as "no context to attach to" rather than an error, since a step that
    runs before the publishing step (or a job that never calls publish_trace_context
    at all) is a normal, valid state, not a bug.
    """
    # All non-empty prefixes of the enclosing-subgraph path, longest (nearest) first
    # -- e.g. ["a", "b", "c"] -> ["a.b.c", "a.b", "a"] -- then the run root as the
    # final fallback.
    path = context.op_handle.path[:-1]
    trace_keys = [".".join(path[:i]) for i in range(len(path), 0, -1)]
    trace_keys.append(ROOT_TRACE_KEY)

    for run_id in _run_id_and_ancestors(context):
        run = context.instance.get_run_by_id(run_id)
        if run is None:
            continue
        for key in trace_keys:
            tag_value = run.tags.get(_TAG_PREFIX + key)
            if tag_value is not None:
                result: dict[str, str] = json.loads(tag_value)
                return result
    return None


def _activate_trace_context(carrier: dict[str, str]) -> None:
    """Make a previously-published trace context the active one in this process.

    New spans started after this call will be children of the trace the carrier came
    from, even though it was published from a different process (or node).
    """
    ctx = TraceContextTextMapPropagator().extract(carrier)
    otel_context.attach(ctx)


def _seed_run_root_context(context: ExecutionContext) -> None:
    """Activate a context carrying a trace_id derived deterministically from the run
    ID, for a step that found no real parent to attach to (see module docstring for
    why this exists -- it's a safety net against the multi-root collision, not a fix
    for it). Every step that calls this for the same run computes the identical
    trace_id independently, with no shared state and no race: unlike ROOT_TRACE_KEY,
    there's nothing to publish first.

    The seeded span context is a NonRecordingSpan (Dagster's own event log, not this,
    remains the source of truth for real parent/child spans -- see
    publish_trace_context/_find_trace_context) with `is_remote=True`, the same shape
    OTel's own propagators use for context received from outside the process. Its
    span_id is a fixed placeholder (never a real span), only trace_id matters here.
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
