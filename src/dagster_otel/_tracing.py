"""The public `traced()` decorator: stacks under @op/@asset, does nothing Dagster-specific.

@op(...)
@traced()
def my_op(context, x: int) -> int:
    ...

Handles plain-return, generator, coroutine (`async def`) and async-generator compute
functions (the async two: Issue #93, see `_traced_decorator`). This matters concretely
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

import contextvars
import inspect
from collections.abc import AsyncGenerator, Callable, Coroutine, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any, Generic, ParamSpec, Protocol, TypeVar, cast, overload

from dagster import OpExecutionContext, get_dagster_logger
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

P = ParamSpec("P")
R = TypeVar("R")
Y = TypeVar("Y")
S = TypeVar("S")
Rt = TypeVar("Rt")
T = TypeVar("T")

#: The overloads below type the compute function as a plain `Callable[P, ...]`, not
#: `Callable[Concatenate[Context, P], ...]`: since Issue #94 the context parameter is
#: optional (see `_traced_decorator()`), so requiring one in the signature would
#: reject Dagster's own context-less form at type-check time just as the old wrapper
#: did at run time. `P` still captures and preserves whatever the function declares,
#: including a specific context type such as `AssetExecutionContext` -- the reason the
#: old `ComputeFn` alias used a generic context parameter rather than the fixed
#: `ExecutionContext` union (see docs/design.md).


class _TracedDecorator(Protocol):
    """The callable `traced(...)` returns. Overloaded on its __call__, not on
    `traced` itself -- see module docstring for why."""

    @overload
    def __call__(
        self, func: Callable[P, Generator[Y, S, Rt]]
    ) -> Callable[P, Generator[Y, S, Rt]]: ...
    @overload
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...


@contextmanager
def _traced_span(context: ExecutionContext, name: str | None) -> Generator[None, None, None]:
    # Issue #95: with no explicit span name, name the span after the node Dagster is
    # actually running, resolved here at run time -- not `func.__name__` at decoration
    # time, which `traced()` (applied underneath `@op`/`@asset`) can't correct for
    # `@op(name=...)`, `@asset(key=...)`/`key_prefix=`/`name=`, `@multi_asset(name=...)`,
    # an asset check's `<asset>_<check>` op, or an aliased op
    # (`my_op.alias("a")`). `op_handle.name` rather than the `@public` `op_def.name`:
    # the definition name is the same for every alias of one op, which is the same
    # "one function, many steps, one span name" problem this fixes; the handle's name is
    # the alias. `op_handle` is `:meta private:` in Dagster's docs -- see Issue #100 for
    # that dependency. It's the leaf name (`inner_op`, not `sub.inner_op`); the full
    # path is already on the span as `dagster.step_key`.
    if name is None:
        name = context.op_execution_context.op_handle.name
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
        # asset_key(s) (Issue #37, follow-up on #9): context.asset_key itself raises
        # DagsterInvariantViolationError for a multi_asset with more than one output
        # asset -- context.selected_asset_keys (plural, `@public`, a
        # frozenset[AssetKey]) works uniformly for a plain op (empty set, since
        # has_assets_def is False), a single-output @asset, and a multi_asset alike,
        # no branching needed. Comma-joined into one string attribute, not OTel's
        # native sequence-attribute support: probed directly against real Jaeger
        # (2026-09-17) that a native list attribute round-trips through OTLP as a
        # JSON-array-shaped *string* anyway (`["a","b"]`, not a real array in the
        # UI) -- a plain comma-joined string renders just as well and reads cleaner,
        # matching how OTel semantic conventions themselves usually flatten
        # array-shaped attributes into a single string when broad backend support
        # isn't guaranteed. Sorted for determinism (a set has no stable order of its
        # own); omitted entirely (not set to an empty string) when there's nothing to
        # report, same "don't invent a value" stance as the other lookups here.
        #
        # job_name (Issue #72): AssetCheckExecutionContext has no `.job_name` at all
        # (only `.job_def`) -- confirmed against real Dagster that `.job_def.name`
        # gives the identical value `.job_name` does on the other two context types,
        # so this reads uniformly from `.job_def.name` for all three instead of
        # branching.
        #
        # asset_check_keys (Issue #72): AssetCheckExecutionContext has no
        # `.selected_asset_keys` -- it has `.selected_asset_check_keys` instead (a
        # frozenset[AssetCheckKey], `asset_key:check_name` shaped via
        # AssetCheckKey.to_user_string(), so kept on its own attribute rather than
        # merged into `dagster.asset_keys`). Both keys are read from the underlying
        # OpExecutionContext, which has both as `@public` properties for every step
        # kind (op, asset, asset check -- confirmed against real Dagster 1.13.22), so
        # there's no branching on the context type. This also reports an asset's
        # inline `check_specs` checks, which the earlier isinstance branch skipped.
        # `.op_execution_context` isn't decorated `@public` itself, but it's what
        # Dagster's own AssetExecutionContext deprecation messages tell users to call
        # ("Use context.op_execution_context.{attr} instead"), and on
        # OpExecutionContext it's just `self`.
        op_context = context.op_execution_context
        span.set_attribute("dagster.run_id", context.run.run_id)
        span.set_attribute("dagster.job_name", context.job_def.name)
        span.set_attribute("dagster.step_key", _own_step_key(context))
        span.set_attribute("dagster.retry_number", context.retry_number)
        asset_keys = sorted(k.to_user_string() for k in op_context.selected_asset_keys)
        if asset_keys:
            span.set_attribute("dagster.asset_keys", ",".join(asset_keys))
        asset_check_keys = sorted(k.to_user_string() for k in op_context.selected_asset_check_keys)
        if asset_check_keys:
            span.set_attribute("dagster.asset_check_keys", ",".join(asset_check_keys))

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


class _run_in_context(Generic[T]):  # noqa: N801 -- used like a function, `await _run_in_context(...)`
    """Awaitable that runs `coro` with every one of its steps inside `ctx`.

    The same thing an asyncio Task does for its own Context (`context.run(coro.send,
    ...)` per step), just for a Context chosen here rather than the Task's. Only used
    to keep one Context across an async generator's items, see `_traced_decorator`.
    """

    def __init__(self, ctx: contextvars.Context, coro: Coroutine[Any, Any, T]) -> None:
        self._ctx = ctx
        self._coro = coro

    def __await__(self) -> Generator[Any, Any, T]:
        send_value: Any = None
        thrown: BaseException | None = None
        while True:
            try:
                if thrown is None:
                    yielded = self._ctx.run(self._coro.send, send_value)
                else:
                    yielded = self._ctx.run(self._coro.throw, thrown)
            except StopIteration as stop:
                return cast(T, stop.value)
            try:
                send_value, thrown = (yield yielded), None
            except BaseException as exc:  # noqa: BLE001 -- forwarded into the coroutine
                send_value, thrown = None, exc


#: Set on every wrapper `traced()`/`traced_dbt()` return (Issue #96), so a function
#: that's already traced isn't wrapped again. One marker shared by both: an explicit
#: `@traced()` on a `@dbt_assets` body (one coarse span) must also stop an outer
#: `traced_dbt()`, and vice versa. `functools.wraps` copies `__dict__`, so a
#: third-party decorator stacked on a traced function carries the marker too, which
#: is what we want: the function inside is still traced exactly once.
_TRACED_MARKER = "__dagster_otel_traced__"


def _is_traced(func: Callable[..., Any]) -> bool:
    return getattr(func, _TRACED_MARKER, False) is True


W = TypeVar("W", bound=Callable[[Any], Any])


def _idempotent(wrap: W) -> W:
    """Makes a `func -> wrapped func` step of a tracing decorator idempotent (Issue
    #96): an already-traced `func` comes back unchanged, and anything `wrap` returns
    gets the marker. Applied to both `traced()`'s and `traced_dbt()`'s wrapper, so the
    check and the marking live in exactly one place.

    Wrapping an already-traced function again gave two spans per step, and the inner
    layer's publish_trace_context() overwrote the outer's, so downstream steps
    parented onto the inner span. That's the normal state under
    opentelemetry-instrumentation-dagster, which applies traced() on top of every
    compute function, including ones the user already decorated. The innermost
    (user-written) decorator wins, including its explicit span name."""

    @wraps(wrap)
    def guarded(func: Callable[..., Any]) -> Callable[..., Any]:
        if _is_traced(func):
            return func
        wrapped = wrap(func)
        setattr(wrapped, _TRACED_MARKER, True)
        return wrapped

    return cast(W, guarded)


def _traced_decorator(span_name: str | None) -> _TracedDecorator:
    """The actual `@traced(...)`-called-form decorator -- also what a bare `@traced`
    reduces to underneath (with `span_name=None`), see `traced()` below."""

    @_idempotent
    def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        name = span_name
        # Issue #94: a context-less compute function (`@asset def x(): ...`, Dagster's
        # own canonical form) used to fail at run time with `x() missing 1 required
        # positional argument: 'context'` -- Dagster reads the original signature
        # through `@wraps`, sees no context parameter, and calls the wrapper without
        # one. The wrapper now forwards exactly what Dagster passed, and takes the
        # context from wherever Dagster actually put it.

        def context_of(args: tuple[Any, ...]) -> ExecutionContext:
            # Follows how Dagster calls a compute function (`invoke_compute_fn` in
            # dagster/_core/execution/plan/compute_generator.py, 1.13.22):
            # `fn(context, **args_to_pass) if context_arg_provided else
            # fn(**args_to_pass)`. Inputs, config and resources always go by keyword,
            # so a positional argument is present exactly when Dagster passed a
            # context. Deciding from what was actually passed, rather than predicting
            # Dagster's own "does this function take a context" rule from the
            # signature, can't drift from that rule if Dagster ever changes it. It
            # does still depend on the calling convention itself; see Issue #98.
            return args[0] if args else OpExecutionContext.get()

        if inspect.isgeneratorfunction(func):

            @wraps(func)
            def inner(*args: Any, **kwargs: Any) -> Any:
                with _traced_span(context_of(args), name):
                    yield from func(*args, **kwargs)

        elif inspect.iscoroutinefunction(func):

            @wraps(func)
            async def coroutine_inner(*args: Any, **kwargs: Any) -> Any:
                with _traced_span(context_of(args), name):
                    return await func(*args, **kwargs)

            return coroutine_inner

        elif inspect.isasyncgenfunction(func):

            async def traced_agen(*args: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
                with _traced_span(context_of(args), name):
                    async for item in func(*args, **kwargs):
                        yield item

            @wraps(func)
            async def async_generator_inner(*args: Any, **kwargs: Any) -> Any:
                # Dagster drives an async generator one item at a time, each in a
                # fresh asyncio Task (`gen_from_async_gen`:
                # `event_loop.run_until_complete(async_gen.__anext__())`), and every
                # Task runs in its own copy of the contextvars Context. Iterated
                # naively, `_traced_span`'s OTel context would be current only until
                # the first `yield` -- confirmed against real Dagster 1.13.22: a span
                # started after it had no parent, and the final detach logged
                # "Failed to detach context" (a token from a different Context). So
                # every step of `traced_agen` runs in one Context pinned here instead.
                pinned = contextvars.copy_context()
                agen = traced_agen(*args, **kwargs)
                try:
                    while True:
                        try:
                            item = await _run_in_context(pinned, agen.__anext__())
                        except StopAsyncIteration:
                            return
                        yield item
                finally:
                    await _run_in_context(pinned, agen.aclose())

            return async_generator_inner

        else:

            @wraps(func)
            def inner(*args: Any, **kwargs: Any) -> Any:
                with _traced_span(context_of(args), name):
                    return func(*args, **kwargs)

        return inner

    return wrapper


@overload
def traced(func: Callable[P, Generator[Y, S, Rt]], /) -> Callable[P, Generator[Y, S, Rt]]: ...
@overload
def traced(func: Callable[P, R], /) -> Callable[P, R]: ...
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

    Works on plain, generator, `async def` and async-generator compute functions alike.

    The compute function doesn't have to take a `context` parameter: one without it
    (`@asset def x(): ...`) is traced the same way, with the context fetched from
    Dagster itself (`OpExecutionContext.get()`).

    Looks up the trace context published by publish_trace_context() (falling back up
    to the run root, and across ancestor runs for retries) and activates it, so the
    span opened here is a child of that trace even if this step is running in a
    different process. If no trace context has been published (yet, or ever, for this
    job), just opens an unparented span -- a normal state, not an error.

    Also attaches a logging filter to context.log for the duration of the span, so any
    context.log.* call made inside gets trace_id/span_id attributes forwarded to any
    @logger you've configured (see _logging.py).

    :param span_name: Span name. Defaults to the name of the op/asset/check node
        Dagster is running (its `op_handle.name`, e.g. `warehouse__customers` for
        `@asset(key=["warehouse", "customers"])`, or the alias for `my_op.alias("a")`),
        not the Python function's name. Only meaningful with the called form
        (`@traced()`/`@traced("name")`) -- with bare `@traced`, this parameter instead
        receives the function being decorated (see module docstring).
    """
    if span_name is None or isinstance(span_name, str):
        return _traced_decorator(span_name)
    # Bare `@traced` (no parens): span_name is actually the function itself.
    func = span_name
    return _traced_decorator(None)(func)
