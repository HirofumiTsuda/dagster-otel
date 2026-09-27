"""Issue #95: with no explicit span name, `@traced()` names the span after the node
Dagster is actually running (`op_handle.name`, resolved at run time), not the Python
function's name.

Run through real Dagster: the names come from Dagster's own naming rules (`key=`,
`key_prefix=`, `name=`, asset-check op names, `.alias()`), which a fake context can't
reproduce. The alias case is also what pins the `op_handle` dependency (Issue #100) --
`op_def.name` would give the definition name for every alias."""

import dagster as dg
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dagster_otel import traced


def _span_names(spans: InMemorySpanExporter) -> list[str]:
    return sorted(s.name for s in spans.get_finished_spans())


def test_asset_naming_arguments(spans: InMemorySpanExporter) -> None:
    def make(table: str) -> dg.AssetsDefinition:
        @dg.asset(key=["warehouse", table])
        @traced()
        def _asset() -> int:
            return 1

        return _asset

    @dg.asset(key_prefix=["raw"])
    @traced()
    def orders() -> int:
        return 1

    @dg.asset(name="renamed_asset")
    @traced()
    def fn_asset() -> int:
        return 1

    @dg.multi_asset(name="my_multi", outs={"m1": dg.AssetOut(), "m2": dg.AssetOut()})
    @traced()
    def multi_fn():  # type: ignore[no-untyped-def]
        yield dg.Output(1, "m1")
        yield dg.Output(2, "m2")

    assert dg.materialize([make("customers"), make("payments"), orders, fn_asset, multi_fn]).success
    assert _span_names(spans) == [
        "my_multi",
        "raw__orders",
        "renamed_asset",
        "warehouse__customers",
        "warehouse__payments",
    ]


def test_asset_check_op_name(spans: InMemorySpanExporter) -> None:
    @dg.asset(key_prefix=["raw"])
    def orders() -> int:
        return 1

    @dg.asset_check(asset=orders, name="named_check")
    @traced()
    def fn_check() -> dg.AssetCheckResult:
        return dg.AssetCheckResult(passed=True)

    assert dg.materialize([orders, fn_check]).success
    assert _span_names(spans) == ["raw__orders_named_check"]


def test_op_name_alias_and_nested_graph(spans: InMemorySpanExporter) -> None:
    @dg.op(name="renamed_op")
    @traced()
    def some_fn() -> None:
        pass

    @dg.op
    @traced()
    def base_op() -> None:
        pass

    @dg.op
    @traced()
    def inner_op() -> None:
        pass

    @dg.graph
    def sub() -> None:
        inner_op()

    @dg.job
    def naming_job() -> None:
        some_fn()
        base_op.alias("alias_a")()
        base_op.alias("alias_b")()
        sub()

    assert naming_job.execute_in_process().success
    assert _span_names(spans) == ["alias_a", "alias_b", "inner_op", "renamed_op"]


def test_explicit_span_name_still_wins(spans: InMemorySpanExporter) -> None:
    @dg.asset(key=["warehouse", "customers"])
    @traced("custom")
    def _asset() -> int:
        return 1

    assert dg.materialize([_asset]).success
    assert _span_names(spans) == ["custom"]
