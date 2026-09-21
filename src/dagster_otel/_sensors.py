"""traced_sensor()/traced_schedule(): OTel spans for sensor/schedule tick evaluation
(Issue #38) -- what decides whether a run happens, not the run itself.

@sensor(...)
@traced_sensor()
def my_sensor(context: SensorEvaluationContext):
    ...

@schedule(cron_schedule="...")
@traced_schedule()
def my_schedule(context: ScheduleEvaluationContext):
    ...

A tick isn't a step of a run -- unlike `traced()`/`traced_dbt()`, there's no run_id
yet at evaluation time, and so no upstream-step trace context to look up
(`_propagation.py`'s whole lookup chain is keyed on `context.instance` + a run's own
tags, neither of which exist for a tick). So each tick is simply a fresh root span (a
plain `_tracer.start_as_current_span()` call, no `_ancestor_runs`/
`_seed_run_root_context`/deterministic-trace_id machinery at all) -- considerably
simpler than `traced()`'s own span-opening logic, confirmed unnecessary here precisely
*because* a tick is a single process/single function call, not something that can be
re-executed across processes the way a step can.

If the tick launches a run (yields/returns a `RunRequest`, directly, inside a
`Sequence[RunRequest]`, or inside `SensorResult.run_requests`), that `RunRequest` is
tagged with `EXTERNAL_TRACE_CONTEXT_TAG_KEY` -- the same mechanism Issue #13 built for
an external caller to nest a whole run under its own trace, reused here but
originating from *inside* Dagster instead. The launched run's own trace then nests
under this tick's span, so "why did/didn't this run fire" is answerable from the trace
backend directly instead of needing to cross-reference Dagster's own tick/run UI by
hand (see Issue #38's own "Why").

Not every return shape carries a `RunRequest` to tag, though: a schedule's own eval
function can return a plain `dict`/`RunConfig` directly instead of a `RunRequest`
(Dagster then builds the `RunRequest` internally, past the point this decorator ever
sees it) -- confirmed via `inspect.signature(dagster.schedule)`. Nothing here can
inject a tag in that case; return `RunRequest(run_config=...)` explicitly instead (a
normal, already-documented Dagster pattern) to opt into the link.

`ScheduleEvaluationContext` has no public name accessor at all -- only a private
`_schedule_name` slot (confirmed via `dir(dagster.ScheduleEvaluationContext)` and by
reading `schedule_definition.py` directly), unlike `SensorEvaluationContext.
sensor_name`, which *is* public. So the `dagster.schedule_name` span attribute for
`traced_schedule()` falls back to the wrapped function's own name (`span_name or
func.__name__`) instead -- matching Dagster's own default naming (a schedule's name
defaults to its decorated function's name unless overridden via `@schedule(name=...)`),
so this agrees with the real schedule name for the common case, even though it can't
read it from the context itself the way `traced_sensor()` reads `sensor_name`.

A separate module (not folded into `_tracing.py`): a tick function's signature/
semantics (`SensorEvaluationContext`/`ScheduleEvaluationContext`, returns
`RunRequest`/`SkipReason`/`SensorResult`/`None`/a plain dict, not an op's `Output`) are
different enough from an op/asset compute function's that reusing `traced()`'s
`ComputeFn`/propagation/`dagster.*`-attribute machinery wouldn't fit cleanly -- see
Issue #38's own "Shape" section.
"""

import inspect
import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any, Concatenate, ParamSpec, Protocol, TypeVar, overload

from dagster import RunRequest, ScheduleEvaluationContext, SensorEvaluationContext, SensorResult
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from dagster_otel._propagation import EXTERNAL_TRACE_CONTEXT_TAG_KEY
from dagster_otel._setup import configure

_tracer = trace.get_tracer("dagster_otel")

P = ParamSpec("P")
R = TypeVar("R")
Y = TypeVar("Y")


def _tag_run_request(rr: RunRequest, carrier: dict[str, str]) -> RunRequest:
    """A copy of `rr` with `EXTERNAL_TRACE_CONTEXT_TAG_KEY` added to its tags --
    `RunRequest` is immutable (NamedTuple-based, confirmed via a live `._replace()`
    call), so this can't just mutate `.tags` in place. `rr.tags or {}` since `.tags`
    is itself `Optional[Dict[str, Any]]` -- a RunRequest with no tags of its own
    shouldn't need one set explicitly by the caller just to be traceable.
    """
    carrier_json = json.dumps(carrier)
    return rr._replace(tags={**(rr.tags or {}), EXTERNAL_TRACE_CONTEXT_TAG_KEY: carrier_json})


def _tag_tick_result(value: Any, carrier: dict[str, str]) -> Any:
    """Injects the current tick span's trace context into every `RunRequest`
    reachable from a tick function's return/yield value, so any run it launches nests
    under this tick's span -- see module docstring.

    Passes anything else through unchanged: a `SkipReason`, a `DagsterRunReaction`, a
    schedule's plain `dict`/`RunConfig`, or `None` isn't something this can tag at
    all -- only a `RunRequest` (bare, inside a plain `list`/`tuple`, or inside
    `SensorResult.run_requests`) is.

    The "inside a sequence" branch below checks `type(value) in (list, tuple)`, not
    `isinstance(value, Sequence)` -- confirmed live (2026-09-21) that `SkipReason` is
    itself a NamedTuple (so also a real `tuple`/`Sequence` under the hood, single
    `skip_message` field): the broader `isinstance` check silently shredded a returned
    `SkipReason("...")` into a one-element `list`, corrupting the return value instead
    of passing it through -- caught by `test_traced_sensor_preserves_skip_reason`
    before this ever reached real code. `RunRequest`/`SensorResult` are also
    NamedTuple-based (same reason `isinstance` alone doesn't disambiguate them either),
    handled by their own `isinstance` checks above this one; `type(value) in (list,
    tuple)` only matches an actual plain `list`/`tuple`, not any NamedTuple subclass,
    without needing to enumerate every other NamedTuple-shaped type in the return union
    (e.g. `DagsterRunReaction`, not even importable from the public `dagster`
    namespace) by name.
    """
    if isinstance(value, RunRequest):
        return _tag_run_request(value, carrier)
    if isinstance(value, SensorResult):
        if value.run_requests is None:
            return value
        return value._replace(
            run_requests=[_tag_run_request(rr, carrier) for rr in value.run_requests]
        )
    if type(value) in (list, tuple):
        return type(value)(
            _tag_run_request(item, carrier) if isinstance(item, RunRequest) else item
            for item in value
        )
    return value


@contextmanager
def _tick_span(name: str, attributes: dict[str, Any]) -> Generator[dict[str, str], None, None]:
    """Opens this tick's span (a fresh root -- see module docstring), sets the given
    `dagster.*` attributes, and yields a carrier of *this* span's context for the
    caller to inject into any `RunRequest`(s) the tick produces.

    Same idempotent auto-`configure()` as `traced()`/`traced_dbt()` -- a sensor/
    schedule daemon process is a genuinely different process from any step's, so it
    needs its own first-use configuration the same way.
    """
    configure()
    with _tracer.start_as_current_span(name) as span:
        for key, attr_value in attributes.items():
            span.set_attribute(key, attr_value)
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)
        yield carrier


def _traced_tick_decorator(
    span_name: str | None, context_attributes: Callable[[Any, str], dict[str, Any]]
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Shared implementation behind both `traced_sensor()` and `traced_schedule()` --
    identical span-opening/tagging logic either way, differing only in which
    `dagster.*` attributes get set (`context_attributes`, called with the resolved
    span name so `traced_schedule()`'s fallback -- see module docstring -- can use it).

    Same generator-vs-plain-return split as `traced()`/`traced_dbt()`, decided once at
    decoration time from `inspect.isgeneratorfunction` -- a multi-run sensor/schedule
    (`yield RunRequest(...)` more than once) is exactly the generator case this exists
    for, same reasoning as `_tracing.py`'s own module docstring.
    """

    def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        name = span_name or func.__name__

        if inspect.isgeneratorfunction(func):

            @wraps(func)
            def inner(context: Any, *args: Any, **kwargs: Any) -> Any:
                with _tick_span(name, context_attributes(context, name)) as carrier:
                    for item in func(context, *args, **kwargs):
                        yield _tag_tick_result(item, carrier)

        else:

            @wraps(func)
            def inner(context: Any, *args: Any, **kwargs: Any) -> Any:
                with _tick_span(name, context_attributes(context, name)) as carrier:
                    return _tag_tick_result(func(context, *args, **kwargs), carrier)

        return inner

    return wrapper


def _sensor_attributes(context: SensorEvaluationContext, name: str) -> dict[str, Any]:
    return {"dagster.sensor_name": context.sensor_name}


def _schedule_attributes(context: ScheduleEvaluationContext, name: str) -> dict[str, Any]:
    # dagster.schedule_name: see module docstring for why this reads `name` (the
    # span's own name) rather than the context -- ScheduleEvaluationContext has no
    # public accessor for it at all.
    attributes: dict[str, Any] = {"dagster.schedule_name": name}
    # context.scheduled_execution_time is `@public` but, confirmed live (2026-09-21),
    # *raises* CheckError rather than returning None when this context was built with
    # none (e.g. `build_schedule_context()` with no argument -- a real, reachable case,
    # not just a test artifact: nothing about the type signature or a real evaluation
    # call site guarantees a caller always supplies one). There's no public
    # non-raising way to check first, and CheckError itself isn't importable from the
    # public `dagster` namespace (only from the internal `dagster_shared.check`
    # module) -- so this treats "raised" the same as "not set" via a narrow
    # try/except, rather than reaching into that internal path just to catch a more
    # specific type.
    try:
        scheduled_execution_time = context.scheduled_execution_time
    except Exception:
        scheduled_execution_time = None
    if scheduled_execution_time is not None:
        attributes["dagster.scheduled_execution_time"] = scheduled_execution_time.isoformat()
    return attributes


SensorTickFn = Callable[Concatenate[SensorEvaluationContext, P], R]
ScheduleTickFn = Callable[Concatenate[ScheduleEvaluationContext, P], R]


class _TracedSensorDecorator(Protocol):
    """The callable `traced_sensor(...)` returns. Overloaded on its __call__, not on
    `traced_sensor` itself -- same reasoning as `traced()`'s own `_TracedDecorator`
    (`_tracing.py`)."""

    @overload
    def __call__(
        self, func: SensorTickFn[P, Generator[Y, None, None]]
    ) -> SensorTickFn[P, Generator[Y, None, None]]: ...
    @overload
    def __call__(self, func: SensorTickFn[P, R]) -> SensorTickFn[P, R]: ...


class _TracedScheduleDecorator(Protocol):
    """Schedule counterpart of `_TracedSensorDecorator` above."""

    @overload
    def __call__(
        self, func: ScheduleTickFn[P, Generator[Y, None, None]]
    ) -> ScheduleTickFn[P, Generator[Y, None, None]]: ...
    @overload
    def __call__(self, func: ScheduleTickFn[P, R]) -> ScheduleTickFn[P, R]: ...


@overload
def traced_sensor(
    func: SensorTickFn[P, Generator[Y, None, None]], /
) -> SensorTickFn[P, Generator[Y, None, None]]: ...
@overload
def traced_sensor(func: SensorTickFn[P, R], /) -> SensorTickFn[P, R]: ...
@overload
def traced_sensor(span_name: str | None = None) -> _TracedSensorDecorator: ...
def traced_sensor(span_name: Any = None) -> Any:
    """Wraps a `@sensor` tick-evaluation function in an OTel span (Issue #38).

    Usable bare, like `traced()`/`traced_dbt()` (and `@sensor` itself) -- see
    `traced()`'s own module docstring (`_tracing.py`) for why bare use is decidable at
    this call site:

        @sensor(...)
        @traced_sensor()
        def my_sensor(context: SensorEvaluationContext):
            ...

        @sensor(...)
        @traced_sensor
        def my_other_sensor(context: SensorEvaluationContext):
            ...

    Each tick is a fresh root span (see module docstring -- a tick has no run_id or
    upstream step to attach to, unlike `traced()`). Any `RunRequest` the tick
    returns/yields (bare, in a `Sequence`, or inside a `SensorResult`) gets
    `EXTERNAL_TRACE_CONTEXT_TAG_KEY` injected into its tags, so the run it launches (if
    any) nests under this tick's span in the trace backend.

    Sets `dagster.sensor_name` on the span.

    :param span_name: Span name. Defaults to the wrapped function's name. Only
        meaningful with the called form -- with bare `@traced_sensor`, this parameter
        instead receives the function being decorated (see `traced()`'s module
        docstring for the same shape).
    """
    if span_name is None or isinstance(span_name, str):
        return _traced_tick_decorator(span_name, _sensor_attributes)
    func = span_name
    return _traced_tick_decorator(None, _sensor_attributes)(func)


@overload
def traced_schedule(
    func: ScheduleTickFn[P, Generator[Y, None, None]], /
) -> ScheduleTickFn[P, Generator[Y, None, None]]: ...
@overload
def traced_schedule(func: ScheduleTickFn[P, R], /) -> ScheduleTickFn[P, R]: ...
@overload
def traced_schedule(span_name: str | None = None) -> _TracedScheduleDecorator: ...
def traced_schedule(span_name: Any = None) -> Any:
    """Wraps a `@schedule` tick-evaluation function in an OTel span (Issue #38).

    Usable bare, like `traced_sensor()` above:

        @schedule(cron_schedule="0 * * * *")
        @traced_schedule()
        def my_schedule(context: ScheduleEvaluationContext):
            ...

    Same fresh-root-span, `RunRequest`-tagging behavior as `traced_sensor()` -- see
    that function's docstring and this module's docstring for the details, including
    why a schedule returning a plain `dict`/`RunConfig` (instead of an explicit
    `RunRequest`) can't be tagged.

    Sets `dagster.schedule_name` (falls back to the span name -- see module docstring
    for why) and, when available, `dagster.scheduled_execution_time` on the span.

    :param span_name: See `traced_sensor()`'s own `span_name` docstring -- identical
        shape here.
    """
    if span_name is None or isinstance(span_name, str):
        return _traced_tick_decorator(span_name, _schedule_attributes)
    func = span_name
    return _traced_tick_decorator(None, _schedule_attributes)(func)
