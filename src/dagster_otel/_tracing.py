"""The public `traced()` decorator: stacks under @op/@asset, does nothing Dagster-specific.

@op(...)
@traced()
def my_op(context, x: int) -> int:
    ...

Handles both plain-return and generator compute functions. This matters concretely
for @dbt_assets, which Dagster requires to be defined as a generator
(`yield from dbt.cli(...).stream()`) -- a naive wrapper that always does
`return func(context, *args, **kwargs)` would hand Dagster an unconsumed generator
object instead of running it, and a wrapper that always does `yield from func(...)`
would make even a plain-return op's wrapper a generator function too (Python decides
"is this a generator function" from whether `yield` appears anywhere in its body, at
compile time, not from what actually executes at runtime) -- so *calling* it would
just construct a generator without running anything, silently breaking the plain case.

The fix: decide the wrapper's shape once, at decoration time, from
`inspect.isgeneratorfunction(func)` on the *undecorated* function -- not by calling the
function and inspecting what it returns (the approach prior art here takes, which only
works because it normalizes every return value into `yield Output(...)`).

Typing note: a generator function's declared return type must be `Generator[...]`-
shaped (or a supertype), not a bare TypeVar -- both mypy and pyright enforce this,
pyright more strictly (mypy accepts a `# type: ignore`; pyright's equivalent needs a
separate, differently-named suppression, i.e. one silences a checker the other still
flags). Overloading `traced(span_name: str | None = ...)` itself on that split doesn't
work -- mypy rejects it (`Overloaded function signature 2 will never be matched`),
because `span_name` is identical across both overloads, so there's nothing at a
`traced(...)` call site to disambiguate on. The decision only becomes knowable once
the *returned* decorator is applied to an actual `func` -- one step later. So the
generator-vs-plain-return split is overloaded on that returned decorator's `__call__`,
via a `Protocol` (`_TracedDecorator` below), not `traced()`'s own signature.

`traced()` itself is overloaded on a *different* axis: bare use (`@traced`, no
parens -- matches `@op`/`@asset` themselves, which both support bare use; verified
against real Dagster that `@op` alone, no call, works) vs. called use (`@traced()` or
`@traced("name")`). Unlike the generator/plain-return split, this one *is* decidable
at the `traced(...)` call site itself -- a bare `@traced` calls `traced(my_op)`,
handing the wrapped function itself as the first positional argument, so `Callable`
vs. `str | None` are genuinely different, non-overlapping argument types mypy can
dispatch on. Confirmed (2026-09-16) that without this, bare `@traced` didn't raise
anything -- it silently rebound the decorated name to the *unconfigured inner
decorator function*, not the traced original, since `span_name` just receives the
function object and `span_name or func.__name__` degrades into "the function is
already truthy" territory downstream. A footgun worth a real fix, not a docs note,
given `@op`/`@asset` train users to expect bare use directly above this decorator.
"""

import inspect
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any, Concatenate, ParamSpec, Protocol, TypeVar, overload

from dagster import get_dagster_logger
from opentelemetry import trace
from opentelemetry.trace import Link

from dagster_otel._logging import TraceContextFilter
from dagster_otel._propagation import (
    _activate_trace_context,
    _ancestor_runs,
    _own_step_key,
    _seed_run_root_context,
    carrier_to_span_context,
    find_external_trace_context,
    find_previous_attempt_context,
    find_upstream_trace_contexts,
    publish_trace_context,
)
from dagster_otel._setup import configure
from dagster_otel._types import ExecutionContext

_tracer = trace.get_tracer("dagster_otel")

C = TypeVar("C", bound=ExecutionContext)
P = ParamSpec("P")
R = TypeVar("R")
Y = TypeVar("Y")
S = TypeVar("S")
Rt = TypeVar("Rt")

#: An op/asset compute function: first positional arg is the execution context,
#: everything after that is whatever the wrapped function itself declares.
#:
#: The context parameter is generic (bound=ExecutionContext), not just
#: ExecutionContext outright -- caught by pyright (not mypy) against a real example:
#: real op/asset code is normally typed with the *specific* context type it expects
#: (`AssetExecutionContext`, not the `OpExecutionContext | AssetExecutionContext`
#: union), and a fixed-Union parameter type rejects a narrower one by function
#: parameter contravariance. Generic C lets `traced()` accept and preserve whichever
#: specific context type -- or the union -- the wrapped function actually declares.
ComputeFn = Callable[Concatenate[C, P], R]


class _TracedDecorator(Protocol):
    """The callable `traced(...)` returns. Overloaded on its __call__, not on
    `traced` itself -- see module docstring for why."""

    @overload
    def __call__(
        self, func: ComputeFn[C, P, Generator[Y, S, Rt]]
    ) -> ComputeFn[C, P, Generator[Y, S, Rt]]: ...
    @overload
    def __call__(self, func: ComputeFn[C, P, R]) -> ComputeFn[C, P, R]: ...


@contextmanager
def _traced_span(context: ExecutionContext, name: str) -> Generator[None, None, None]:
    # Idempotent (see configure()'s docstring) -- a no-op if a @resource or another
    # @traced() step already configured this process. If nothing has, this is what
    # lets `@traced()` alone be enough: no @resource/required_resource_keys wiring
    # needed just to get a TracerProvider set up. Uses env-var defaults
    # (OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT) since no explicit args are
    # available at this call site.
    configure()

    # This run and (for retry-from-failure) its ancestor runs, fetched once and
    # reused by every lookup below that might need it (Issue #35) -- each of
    # find_upstream_trace_contexts/find_external_trace_context/
    # find_previous_attempt_context otherwise independently re-walks and re-fetches
    # the identical chain.
    runs = _ancestor_runs(context)

    # Every real, direct upstream dependency that has itself published a trace
    # context (see _propagation.py's module docstring for why "real dependency",
    # not "nearest enclosing subgraph"). Ordered deterministically: the first becomes
    # this span's actual parent, any rest become Links -- fan-in (e.g. a merge step
    # depending on two independent roots) is then visible as extra Links on the span
    # rather than silently collapsing onto whichever upstream happened to be found.
    upstream_contexts = find_upstream_trace_contexts(context, runs)
    if upstream_contexts:
        primary_context, *secondary_contexts = upstream_contexts
        _activate_trace_context(primary_context)
        links = [Link(carrier_to_span_context(c)) for c in secondary_contexts]
    else:
        # No real parent found -- a genuine root, or every direct upstream is
        # untraced (see _propagation.py's "Residual limitation"). Issue #13: an
        # external caller (CI/CD, a scheduler, another OTel-instrumented system) may
        # have seeded this whole run to nest under its own trace -- checked before
        # falling all the way back to the deterministic run_id seed, so every root
        # step in the run activates the *same* real external parent when one was
        # provided, the same way every root currently activates the same synthetic
        # seed when one wasn't. Don't fall through to a fresh, randomly-generated
        # trace_id in either case -- that's what caused the confirmed multi-root
        # collision (see _propagation.py module docstring): every step with no real
        # parent derives the *same* trace_id (external or deterministic-seeded)
        # instead, so independent branches still end up in one trace together.
        external_context = find_external_trace_context(context, runs)
        if external_context is not None:
            _activate_trace_context(external_context)
        else:
            _seed_run_root_context(context)
        links = []

    # Issue #14: an op-level RetryPolicy retry re-executes the same step as a fresh
    # process/span, with no relationship to the failed (or otherwise-retried)
    # previous attempt otherwise -- confirmed against a real retry: two genuinely
    # unrelated sibling spans, distinguishable only by the dagster.retry_number
    # attribute (see #9), not by trace structure. This doesn't change who the real
    # parent is (still the step's actual upstream dependency, or the deterministic
    # root seed) -- it adds a Link to the previous attempt alongside that, the same
    # primitive already used for fan-in.
    previous_attempt_context = find_previous_attempt_context(context, runs)
    if previous_attempt_context is not None:
        links = [*links, Link(carrier_to_span_context(previous_attempt_context))]

    log_filter = TraceContextFilter()
    # context.log alone misses log lines integrations emit on their own behalf --
    # verified against a real @dbt_assets run: dagster_dbt's own progress messages
    # ("Running dbt command...", etc.) go through `get_dagster_logger()`
    # (`dagster_dbt/core/dbt_cli_invocation.py`: `logger = get_dagster_logger()`), a
    # separate, global `"dagster.builtin"` logger -- not context.log -- even though
    # both end up formatted identically via the same DagsterLogHandler (attached to
    # both as one of DagsterLogManager's managed_loggers). A filter added to
    # context.log alone never sees records that originate on a different Logger, so
    # those lines came through with no trace_id/span_id at all. get_dagster_logger()
    # is documented public API, same status as context.log, so tagging it too is in
    # scope the same way -- not reaching into anything undocumented.
    #
    # Caveat: unlike context.log, get_dagster_logger() is one shared, process-wide
    # Logger, not scoped to this step. Fine under Dagster's normal execution model
    # (multiprocess/k8s give each step its own process; in_process runs steps
    # sequentially, not concurrently) -- but if that ever changes, concurrent
    # @traced() steps in one process could stamp each other's dagster.builtin-routed
    # log lines with the wrong span while both are active.
    dagster_builtin_log = get_dagster_logger()

    with _tracer.start_as_current_span(name, links=links) as span:
        # Dagster context as span attributes (Issue #9) -- confirmed via a real
        # Jaeger span (2026-09-15) that nothing here was previously attached: every
        # attribute on a span was either OTel SDK boilerplate (otel.scope.name,
        # span.kind) or process-level telemetry.sdk.* metadata, nothing identifying
        # which run/job/step/attempt a span even belonged to without
        # cross-referencing Dagster's own UI/event log by hand. All four are
        # `@public`-documented properties, no extra Dagster calls needed.
        #
        # asset_key deliberately not included yet: context.asset_key raises
        # DagsterInvariantViolationError for a multi_asset with more than one
        # output asset (confirmed in the property's own docstring/source) --
        # handling that safely needs `selected_asset_keys` (plural) instead, more
        # design work than "cheap to add" covers; tracked as a follow-up on #9
        # rather than done half-right here.
        span.set_attribute("dagster.run_id", context.run_id)
        span.set_attribute("dagster.job_name", context.job_name)
        span.set_attribute("dagster.step_key", _own_step_key(context))
        span.set_attribute("dagster.retry_number", context.retry_number)

        # Published unconditionally now, not just when this step turns out to have
        # no parent (the old subgraph-keyed design's behavior): every step publishes
        # under its own step key (see _propagation.py), because it's each step's own
        # publish that lets whatever depends on *it* find its real parent. A step
        # with no real parent still ends up as the (deterministic-seed) root of the
        # trace -- it just gets there via _seed_run_root_context above rather than a
        # special case here.
        publish_trace_context(context)
        context.log.addFilter(log_filter)
        dagster_builtin_log.addFilter(log_filter)
        try:
            yield
        finally:
            context.log.removeFilter(log_filter)
            dagster_builtin_log.removeFilter(log_filter)


def _traced_decorator(span_name: str | None) -> _TracedDecorator:
    """The actual `@traced(...)`-called-form decorator -- also what a bare `@traced`
    reduces to underneath (with `span_name=None`), see `traced()` below."""

    def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        name = span_name or func.__name__

        if inspect.isgeneratorfunction(func):

            @wraps(func)
            def inner(context: ExecutionContext, *args: Any, **kwargs: Any) -> Any:
                with _traced_span(context, name):
                    yield from func(context, *args, **kwargs)

        else:

            @wraps(func)
            def inner(context: ExecutionContext, *args: Any, **kwargs: Any) -> Any:
                with _traced_span(context, name):
                    return func(context, *args, **kwargs)

        return inner

    return wrapper


@overload
def traced(
    func: ComputeFn[C, P, Generator[Y, S, Rt]], /
) -> ComputeFn[C, P, Generator[Y, S, Rt]]: ...
@overload
def traced(func: ComputeFn[C, P, R], /) -> ComputeFn[C, P, R]: ...
@overload
def traced(span_name: str | None = None) -> _TracedDecorator: ...
def traced(span_name: Any = None) -> Any:
    """Wraps a Dagster op/asset compute function in an OTel span.

    Usable bare, like `@op`/`@asset` themselves:

        @op(...)
        @traced()
        def my_op(context, x: int) -> int:
            ...

        @op(...)
        @traced
        def my_other_op(context) -> None:
            ...

    Looks up the trace context published by publish_trace_context() (falling back up
    to the run root, and across ancestor runs for retries) and activates it, so the
    span opened here is a child of that trace even if this step is running in a
    different process. If no trace context has been published (yet, or ever, for this
    job), just opens an unparented span -- a normal state, not an error.

    Also attaches a logging filter to context.log for the duration of the span, so any
    context.log.* call made inside gets trace_id/span_id attributes forwarded to any
    @logger you've configured (see _logging.py).

    :param span_name: Span name. Defaults to the wrapped function's name. Only
        meaningful with the called form (`@traced()`/`@traced("name")`) -- with bare
        `@traced`, this parameter instead receives the function being decorated (see
        module docstring).
    """
    if span_name is None or isinstance(span_name, str):
        return _traced_decorator(span_name)
    # Bare `@traced` (no parens): span_name is actually the function itself.
    func = span_name
    return _traced_decorator(None)(func)
