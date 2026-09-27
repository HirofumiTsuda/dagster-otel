"""Issue #93: `@traced()` on `async def` compute functions (coroutines and async
generators).

Run through real Dagster (`materialize()`), not `conftest.make_context()`: what
broke is how Dagster itself drives the wrapper -- it inspects the function kind
(`inspect.iscoroutinefunction`/`isasyncgenfunction`) and, for an async generator,
steps it one item at a time, each in a fresh asyncio Task
(`gen_from_async_gen`). Calling a wrapper directly with a fake context goes through
neither."""

import asyncio
import logging

import dagster as dg
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from dagster_otel import traced

_user_tracer = trace.get_tracer("test.user")


def _span(spans: InMemorySpanExporter, name: str):  # type: ignore[no-untyped-def]
    matching = [s for s in spans.get_finished_spans() if s.name == name]
    assert len(matching) == 1, [s.name for s in spans.get_finished_spans()]
    return matching[0]


def _duration_seconds(span) -> float:  # type: ignore[no-untyped-def]
    return (span.end_time - span.start_time) / 1e9


def test_coroutine_asset_runs_and_span_covers_the_await(spans: InMemorySpanExporter) -> None:
    @dg.asset
    @traced()
    async def coro_asset(context: dg.AssetExecutionContext) -> int:
        await asyncio.sleep(0.2)
        with _user_tracer.start_as_current_span("coro_child"):
            pass
        return 1

    result = dg.materialize([coro_asset])

    assert result.success
    assert result.output_for_node("coro_asset") == 1
    span, child = _span(spans, "coro_asset"), _span(spans, "coro_child")
    assert _duration_seconds(span) >= 0.2
    assert child.parent is not None and span.context is not None
    assert child.parent.span_id == span.context.span_id


def test_context_less_coroutine_asset(spans: InMemorySpanExporter) -> None:
    @dg.asset
    @traced()
    async def no_ctx_coro() -> int:
        await asyncio.sleep(0)
        return 1

    assert dg.materialize([no_ctx_coro]).success
    _span(spans, "no_ctx_coro")


def test_async_generator_keeps_its_span_current_across_yields(
    spans: InMemorySpanExporter, caplog: pytest.LogCaptureFixture
) -> None:
    """Dagster runs each item of an async generator in a fresh Task (a copy of the
    contextvars Context), so without pinning one Context the span stops being
    current after the first yield and the final detach fails."""

    @dg.multi_asset(outs={"first": dg.AssetOut(), "second": dg.AssetOut()})
    @traced()
    async def two_outputs(context: dg.AssetExecutionContext):  # type: ignore[no-untyped-def]
        await asyncio.sleep(0.1)
        yield dg.Output(1, output_name="first")
        with _user_tracer.start_as_current_span("between_yields"):
            pass
        await asyncio.sleep(0.1)
        yield dg.Output(2, output_name="second")
        with _user_tracer.start_as_current_span("after_last_yield"):
            pass

    with caplog.at_level(logging.ERROR, logger="opentelemetry.context"):
        result = dg.materialize([two_outputs])

    assert result.success
    span = _span(spans, "two_outputs")
    assert _duration_seconds(span) >= 0.2
    for child_name in ("between_yields", "after_last_yield"):
        child = _span(spans, child_name)
        assert child.parent is not None and span.context is not None
        assert child.parent.span_id == span.context.span_id
    assert "Failed to detach context" not in caplog.text


def test_async_downstream_parents_onto_upstream(spans: InMemorySpanExporter) -> None:
    @dg.asset
    @traced()
    async def upstream_async() -> int:
        return 1

    @dg.asset
    @traced()
    async def downstream_async(upstream_async: int) -> int:
        return upstream_async + 1

    result = dg.materialize([upstream_async, downstream_async])

    assert result.output_for_node("downstream_async") == 2
    up, down = _span(spans, "upstream_async"), _span(spans, "downstream_async")
    assert down.parent is not None and up.context is not None
    assert down.parent.span_id == up.context.span_id


@pytest.mark.parametrize("kind", ["coroutine", "async_generator"])
def test_async_exception_is_recorded_on_the_span(spans: InMemorySpanExporter, kind: str) -> None:
    if kind == "coroutine":

        @dg.asset(name="failing_async")
        @traced("failing_async")
        async def failing() -> int:
            await asyncio.sleep(0)
            raise ValueError("boom")

    else:

        @dg.asset(name="failing_async")
        @traced("failing_async")
        async def failing():  # type: ignore[no-untyped-def]
            yield dg.Output(1)
            raise ValueError("boom")

    result = dg.materialize([failing], raise_on_error=False)

    assert not result.success
    span = _span(spans, "failing_async")
    assert span.status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in span.events)
