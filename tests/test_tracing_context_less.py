"""Issue #94: `@traced()` on a compute function that takes no `context` parameter.

Run through real Dagster (`materialize()` / `execute_in_process()`), not
`conftest.make_context()`: the bug lives in how Dagster itself decides whether to
pass a context -- it inspects the wrapper's signature, which `@wraps` makes report
the original function's parameters -- and a fake context called directly never goes
through that decision at all."""

import dagster as dg
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dagster_otel import traced


def _span(spans: InMemorySpanExporter, name: str):  # type: ignore[no-untyped-def]
    matching = [s for s in spans.get_finished_spans() if s.name == name]
    assert len(matching) == 1, [s.name for s in spans.get_finished_spans()]
    return matching[0]


def test_context_less_asset_runs_and_is_traced(spans: InMemorySpanExporter) -> None:
    @dg.asset
    @traced()
    def no_ctx_asset() -> int:
        return 1

    result = dg.materialize([no_ctx_asset])

    assert result.success
    assert result.output_for_node("no_ctx_asset") == 1
    span = _span(spans, "no_ctx_asset")
    assert span.attributes is not None
    assert span.attributes["dagster.asset_keys"] == "no_ctx_asset"


def test_context_less_generator_asset_runs_and_is_traced(spans: InMemorySpanExporter) -> None:
    @dg.asset
    @traced()
    def no_ctx_gen_asset():  # type: ignore[no-untyped-def]
        yield dg.Output(1)

    assert dg.materialize([no_ctx_gen_asset]).success
    _span(spans, "no_ctx_gen_asset")


def test_context_less_asset_with_input_parents_onto_upstream(spans: InMemorySpanExporter) -> None:
    """Inputs are passed by keyword, so nothing shifts when the context is absent --
    and the downstream span still finds its upstream's published trace context."""

    @dg.asset
    @traced()
    def upstream() -> int:
        return 1

    @dg.asset
    @traced()
    def downstream(upstream: int) -> int:
        return upstream + 1

    result = dg.materialize([upstream, downstream])

    assert result.output_for_node("downstream") == 2
    up, down = _span(spans, "upstream"), _span(spans, "downstream")
    assert down.parent is not None and up.context is not None
    assert down.parent.span_id == up.context.span_id


def test_context_less_op_runs_and_is_traced(spans: InMemorySpanExporter) -> None:
    @dg.op
    @traced()
    def no_ctx_op() -> None:
        pass

    job = dg.GraphDefinition(name="no_ctx_job", node_defs=[no_ctx_op]).to_job()
    assert job.execute_in_process().success
    assert _span(spans, "no_ctx_op").attributes["dagster.job_name"] == "no_ctx_job"  # type: ignore[index]


def test_context_less_asset_check_reports_its_check_key(spans: InMemorySpanExporter) -> None:
    """`OpExecutionContext.get()` inside an asset check returns the plain op context,
    which still carries the step's `selected_asset_check_keys`. The span is named
    after the check's op (`<asset>_<check>`, Issue #95)."""

    @dg.asset
    def checked() -> int:
        return 1

    @dg.asset_check(asset=checked)
    @traced()
    def no_ctx_check() -> dg.AssetCheckResult:
        return dg.AssetCheckResult(passed=True)

    assert dg.materialize([checked, no_ctx_check]).success
    span = _span(spans, "checked_no_ctx_check")
    assert span.attributes is not None
    assert span.attributes["dagster.asset_check_keys"] == "checked:no_ctx_check"


def test_underscore_context_name_is_still_passed_through(spans: InMemorySpanExporter) -> None:
    """`_` is one of the names Dagster accepts for the context parameter; the wrapper
    must hand it the real context rather than treating the function as context-less."""

    @dg.asset
    @traced()
    def underscore_ctx(_) -> int:  # type: ignore[no-untyped-def]
        assert isinstance(_, dg.AssetExecutionContext)
        return 1

    assert dg.materialize([underscore_ctx]).success
    _span(spans, "underscore_ctx")


def test_inline_check_specs_are_reported_on_the_asset_span(spans: InMemorySpanExporter) -> None:
    """An asset's own `check_specs` checks show up in `dagster.asset_check_keys` on
    the asset's span, alongside `dagster.asset_keys` -- both read from the op context."""

    @dg.asset(check_specs=[dg.AssetCheckSpec("inline_chk", asset="with_inline_check")])
    @traced()
    def with_inline_check():  # type: ignore[no-untyped-def]
        yield dg.Output(1)
        yield dg.AssetCheckResult(passed=True)

    assert dg.materialize([with_inline_check]).success
    span = _span(spans, "with_inline_check")
    assert span.attributes is not None
    assert span.attributes["dagster.asset_keys"] == "with_inline_check"
    assert span.attributes["dagster.asset_check_keys"] == "with_inline_check:inline_chk"
