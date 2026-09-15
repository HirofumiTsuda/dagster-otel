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
flags). Rather than suppress either: overloading `traced()` itself doesn't work --
mypy rejects it (`Overloaded function signature 2 will never be matched`), because
`span_name` is identical across both overloads, so there's nothing at a `traced(...)`
call site to disambiguate on. The decision only becomes knowable once the *returned*
decorator is applied to an actual `func` -- one step later. So what's overloaded here
is that returned decorator's `__call__`, via a `Protocol` (`_TracedDecorator` below),
not `traced()`'s own signature.
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
    _seed_run_root_context,
    carrier_to_span_context,
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

    # Every real, direct upstream dependency that has itself published a trace
    # context (see _propagation.py's module docstring for why "real dependency",
    # not "nearest enclosing subgraph"). Ordered deterministically: the first becomes
    # this span's actual parent, any rest become Links -- fan-in (e.g. a merge step
    # depending on two independent roots) is then visible as extra Links on the span
    # rather than silently collapsing onto whichever upstream happened to be found.
    upstream_contexts = find_upstream_trace_contexts(context)
    if upstream_contexts:
        primary_context, *secondary_contexts = upstream_contexts
        _activate_trace_context(primary_context)
        links = [Link(carrier_to_span_context(c)) for c in secondary_contexts]
    else:
        # No real parent found -- a genuine root, or every direct upstream is
        # untraced (see _propagation.py's "Residual limitation"). Don't fall through
        # to a fresh, randomly-generated trace_id -- that's what caused the confirmed
        # multi-root collision (see _propagation.py module docstring): every step
        # derives the same trace_id deterministically from the run ID instead, so
        # independent branches still end up in one trace even when none of them can
        # find a real parent.
        _seed_run_root_context(context)
        links = []

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

    with _tracer.start_as_current_span(name, links=links):
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


def traced(span_name: str | None = None) -> _TracedDecorator:
    """Wraps a Dagster op/asset compute function in an OTel span.

    Looks up the trace context published by publish_trace_context() (falling back up
    to the run root, and across ancestor runs for retries) and activates it, so the
    span opened here is a child of that trace even if this step is running in a
    different process. If no trace context has been published (yet, or ever, for this
    job), just opens an unparented span -- a normal state, not an error.

    Also attaches a logging filter to context.log for the duration of the span, so any
    context.log.* call made inside gets trace_id/span_id attributes forwarded to any
    @logger you've configured (see _logging.py).

    :param span_name: Span name. Defaults to the wrapped function's name.
    """

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
