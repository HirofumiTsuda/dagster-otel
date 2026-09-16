"""`traced_dbt()`: per-dbt-node spans for `@dbt_assets` (Issue #8).

@dbt_assets(manifest=...)
@traced_dbt()
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()  # unmodified

A separate, explicitly-named decorator from `traced()` -- not merged into it, even
though nothing here actually needs `dagster-dbt` as an import (`Output`/
`AssetCheckResult` are core `dagster` types, already a required dependency; a dbt
node's metadata just happens to carry `"Execution Duration"`/`"unique_id"` keys by
`dagster_dbt`'s own convention). Baking this into `traced()` itself -- silently
sniffing every yielded value's metadata for those key names -- would mean a
non-dbt op that happens to use the same metadata key names for its own unrelated
reasons gets surprise extra spans it never asked for. An explicitly-named decorator
keeps that opt-in instead of implicit, at the cost of one more name to know about --
not a cost this codebase treats as free (see `traced()`'s own module docstring for
the same reasoning applied to bare-vs-called dispatch), but the collision risk here
was judged the bigger one. Revisit (deprecate this, fold the behavior into `traced()`)
if that judgment turns out wrong in practice.

Confirmed via a real jaffle_shop `@dbt_assets` materialization + Jaeger (2026-09-16):

- Every `Output`/`AssetCheckResult` event carries a real `"Execution Duration"`
  metadata value (dbt's own measured seconds), so these spans get accurate durations,
  not zero-width completion markers.
- `Output.output_name` resolves to the real Dagster `AssetKey` via
  `context.asset_key_for_output(...)` (a `@public` method); `AssetCheckResult.asset_key`
  is directly on the event already. Spans are keyed by these -- Dagster's own asset
  model, not dbt's internal `unique_id` -- resolving, for exactly this multi-asset
  case, the `dagster.asset_key` attribute Issue #9 deliberately deferred (`context.
  asset_key` itself is ambiguous/raises for a `@dbt_assets` function's multiple
  outputs; a specific event's own asset_key is not).
- `AssetCheckResult` events for a given asset consistently arrive right after that
  asset's `Output` event (matching dbt's own build-then-test execution order) --
  confirmed nesting check spans as children of their asset's span works correctly
  this way, keyed by tracking the most recently seen span per asset_key, not by
  waiting for or requiring any particular event ordering guarantee from dbt/Dagster.
"""
import time
from collections.abc import Callable, Generator
from functools import wraps
from typing import Any, Concatenate, ParamSpec, TypeVar, overload

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    FloatMetadataValue,
    Output,
)
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, Span, Status, StatusCode

from dagster_otel._tracing import traced
from dagster_otel._types import ExecutionContext

_tracer = trace.get_tracer("dagster_otel.dbt")

C = TypeVar("C", bound=ExecutionContext)
P = ParamSpec("P")
#: What `dbt.cli(...).stream()` actually yields -- matches (without importing)
#: dagster_dbt's own DbtDagsterEventType union (module docstring: none of these need
#: dagster-dbt as an import, they're all core dagster event types already).
DbtEvent = Output | AssetMaterialization | AssetCheckResult | AssetObservation
Y = TypeVar("Y", bound=DbtEvent)

#: A @dbt_assets compute function -- always a generator that neither receives values
#: via `.send()` nor returns one (Dagster requires `yield from
#: dbt.cli(...).stream()`, nothing more elaborate) -- unlike traced()'s ComputeFn,
#: which also covers plain-return ops/assets and doesn't fix those two params to
#: None. No generator/plain-return overload split needed here for the same reason.
DbtComputeFn = Callable[Concatenate[C, P], Generator[Y, None, None]]


def _emit_asset_span(asset_key: AssetKey, duration_seconds: float) -> Span:
    """One span per dbt model/seed/snapshot (an `Output` event), keyed by the real
    Dagster AssetKey. Ended immediately -- the node already finished by the time
    Python sees the event, so there's nothing to be "current" for while it runs;
    this uses the explicit start_time/end_time span API rather than
    start_as_current_span(), backdating start_time to when dbt says the node
    actually started. Returned (not discarded) so a later AssetCheckResult for the
    same asset_key can still nest under it as a Link-free real child, even though
    this span has already ended -- an ended span's SpanContext stays valid to parent
    other spans, only the span itself can no longer be added to.
    """
    end_time = time.time_ns()
    start_time = end_time - int(duration_seconds * 1e9)
    span = _tracer.start_span(asset_key.to_user_string(), start_time=start_time)
    span.set_attribute("dagster.asset_key", asset_key.to_user_string())
    span.end(end_time=end_time)
    return span


def _emit_check_span(event: AssetCheckResult, duration_seconds: float, parent: Span | None) -> None:
    """One span per dbt test (an `AssetCheckResult` event), nested under its asset's
    own span if that asset's Output event was seen earlier in this same iteration
    (the normal case -- dbt runs a model then its tests) -- under whatever's
    otherwise ambient (ordinarily the step's own @traced() span) if not.

    `AssetCheckResult.asset_key`/`.check_name` are declared `| None` (resolvable from
    surrounding spec context when omitted, per the class's own docstring) even though
    events actually produced by dagster_dbt's translation always populate both in
    practice -- handled defensively (a fallback name, an attribute only set when
    present) rather than assumed, since nothing here has verified dagster_dbt always
    populates them for every dbt node type.
    """
    end_time = time.time_ns()
    start_time = end_time - int(duration_seconds * 1e9)

    parent_context = None
    if parent is not None:
        parent_context = trace.set_span_in_context(NonRecordingSpan(parent.get_span_context()))

    check_name = event.check_name or "dbt_check"
    span = _tracer.start_span(check_name, start_time=start_time, context=parent_context)
    if event.asset_key is not None:
        span.set_attribute("dagster.asset_key", event.asset_key.to_user_string())
    span.set_attribute("dagster.check_name", check_name)
    # A failed dbt test doesn't raise a Python exception, so it never hits
    # start_as_current_span()'s own automatic exception/error-status behavior (see
    # docs/design.md) the rest of this library gets for free -- set explicitly here.
    if event.passed is False:
        span.set_status(Status(StatusCode.ERROR, f"dbt check failed: {check_name}"))
    span.end(end_time=end_time)


def _traced_dbt_decorator(
    span_name: str | None,
) -> Callable[[DbtComputeFn[C, P, Y]], DbtComputeFn[C, P, Y]]:
    """The actual `@traced_dbt(...)`-called-form decorator -- also what a bare
    `@traced_dbt` reduces to underneath (with `span_name=None`), see `traced_dbt()`
    below. Same split, same reason, as `traced()`/`_traced_decorator()` in
    `_tracing.py`."""

    def wrapper(func: DbtComputeFn[C, P, Y]) -> DbtComputeFn[C, P, Y]:
        # Reuses traced()'s own generator-handling branch entirely -- the step's own
        # span, propagation, dagster.* attributes, log filter: all of it, unmodified.
        traced_func = traced(span_name)(func)

        @wraps(func)
        def inner(context: C, *args: P.args, **kwargs: P.kwargs) -> Generator[Y, None, None]:
            # AssetCheckResult.asset_key is itself `AssetKey | None` (see
            # _emit_check_span's docstring), hence the Optional key here too.
            asset_spans: dict[AssetKey | None, Span] = {}
            for event in traced_func(context, *args, **kwargs):
                metadata = dict(event.metadata) if event.metadata else {}
                duration = metadata.get("Execution Duration")
                # isinstance-narrowed, not just "is not None": MetadataValue.value is
                # a broad union across every metadata shape (text/int/json/table/...)
                # -- only a real FloatMetadataValue's .value is actually (optionally)
                # a float; still Optional even then (FloatMetadataValue itself allows
                # a None value), hence the second check below.
                duration_seconds = (
                    duration.value if isinstance(duration, FloatMetadataValue) else None
                )
                if duration_seconds is not None and isinstance(event, Output):
                    asset_key = context.asset_key_for_output(event.output_name)
                    asset_spans[asset_key] = _emit_asset_span(asset_key, duration_seconds)
                elif duration_seconds is not None and isinstance(event, AssetCheckResult):
                    _emit_check_span(event, duration_seconds, asset_spans.get(event.asset_key))
                yield event

        return inner

    return wrapper


@overload
def traced_dbt(func: DbtComputeFn[C, P, Y], /) -> DbtComputeFn[C, P, Y]: ...
@overload
def traced_dbt(
    span_name: str | None = None,
) -> Callable[[DbtComputeFn[C, P, Y]], DbtComputeFn[C, P, Y]]: ...
def traced_dbt(span_name: Any = None) -> Any:
    """Like `traced()`, plus a child span per dbt node (model/seed/test/etc.),
    keyed by the real Dagster asset_key/check_name dbt's build already produces --
    see module docstring for why this is a separate decorator, not folded into
    `traced()` itself.

    Usable bare, like `traced()` (and `@op`/`@asset`) themselves -- see `traced()`'s
    own module docstring for why bare use is decidable at this call site and what
    the failure mode was before that was true there:

        @dbt_assets(manifest=...)
        @traced_dbt
        def my_other_dbt_assets(context, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()

    No changes needed to the wrapped function's body either way -- it can still be
    exactly `yield from dbt.cli(...).stream()`:

        @dbt_assets(manifest=...)
        @traced_dbt()
        def my_dbt_assets(context, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()

    An event without both "Execution Duration" and "unique_id" metadata (not
    verified against every event type `dbt.cli(...).stream()` can yield -- only
    `Output` and `AssetCheckResult` were checked) is passed through with no span
    created for it, rather than raising -- better to silently skip a span than break
    materialization over telemetry.

    :param span_name: Passed through to the underlying `traced()` call for the
        step's own (outer) span. Only meaningful with the called form -- with bare
        `@traced_dbt`, this parameter instead receives the function being decorated
        (see `traced()`'s module docstring for the same shape). See `traced()`'s own
        docstring for `span_name` itself.
    """
    if span_name is None or isinstance(span_name, str):
        return _traced_dbt_decorator(span_name)
    # Bare `@traced_dbt` (no parens): span_name is actually the function itself.
    func = span_name
    return _traced_dbt_decorator(None)(func)
