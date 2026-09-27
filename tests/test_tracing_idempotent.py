"""Issue #96: `traced()`/`traced_dbt()` applied to an already-traced function return
it unchanged, so a step gets exactly one span and downstream steps parent onto it.

The end-to-end case runs through real Dagster (`materialize()`), since what broke
was parentage across steps via published run tags; the rest are identity checks on
the decorators themselves."""

import functools
from collections.abc import Callable
from typing import Any

import dagster as dg
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dagster_otel import traced
from dagster_otel.dbt import traced_dbt


def test_double_traced_asset_gets_one_span_and_downstream_parents_onto_it(
    spans: InMemorySpanExporter,
) -> None:
    @dg.asset
    @traced()
    @traced("custom_name")
    def a() -> int:
        return 1

    @dg.asset
    @traced()
    def b(a: int) -> int:
        return a + 1

    assert dg.materialize([a, b]).success

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert sorted(finished) == ["b", "custom_name"]
    parent = finished["b"].parent
    assert parent is not None and finished["custom_name"].context is not None
    assert parent.span_id == finished["custom_name"].context.span_id


def test_traced_returns_an_already_traced_function_unchanged() -> None:
    def fn(context) -> None:  # type: ignore[no-untyped-def]
        pass

    once = traced("inner")(fn)
    assert traced()(once) is once
    assert traced(once) is once  # bare form
    assert traced("outer")(once) is once  # the inner, explicit name wins


def test_tracing_does_not_mark_the_original_function() -> None:
    """`@wraps` copies the original's `__dict__` into the wrapper, not the other way
    round -- the undecorated function can still be traced separately."""

    def fn(context) -> None:  # type: ignore[no-untyped-def]
        pass

    first = traced()(fn)
    second = traced()(fn)
    assert first is not fn and second is not fn and first is not second


def test_traced_and_traced_dbt_share_the_marker() -> None:
    def dbt_body(context):  # type: ignore[no-untyped-def]
        yield from ()

    coarse = traced()(dbt_body)
    assert traced_dbt()(coarse) is coarse

    per_node = traced_dbt()(dbt_body)
    assert traced()(per_node) is per_node
    assert traced_dbt()(per_node) is per_node


def test_third_party_wraps_decorator_carries_the_marker() -> None:
    """A decorator built with `functools.wraps` on top of a traced function copies
    the marker; the function inside is still traced exactly once."""

    def passthrough(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def inner(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return inner

    def fn(context) -> None:  # type: ignore[no-untyped-def]
        pass

    wrapped = passthrough(traced()(fn))
    assert traced()(wrapped) is wrapped
