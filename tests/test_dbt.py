"""Tests for dagster_otel.dbt: per-dbt-node spans (Issue #8).

Real dagster_dbt is never used here -- fake Output/AssetCheckResult events
(genuine dagster core objects, constructed by hand with the same metadata shape
dagster_dbt's real translation produces) stand in for a real dbt.cli(...).stream(),
same "verified separately against something real" split as the rest of this test
suite -- see the real jaffle_shop verification recorded in docs/design.md for that.
"""

from collections.abc import Generator

from conftest import FakeInstance, make_context
from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    MetadataValue,
    Output,
)

from dagster_otel._types import AssetOrOpExecutionContext
from dagster_otel.dbt import traced_dbt

DbtEvent = Output | AssetCheckResult


def _make_dbt_context(
    instance: FakeInstance, run_id: str, op_path: list[str]
) -> AssetOrOpExecutionContext:
    """make_context() plus the one extra method traced_dbt() needs that plain
    @traced() doesn't: asset_key_for_output(), used to resolve an Output event's
    output_name to a real AssetKey. Real Dagster's own implementation does the same
    output_name -> AssetKey lookup via the op's AssetsDefinition; this fake just
    treats the output_name as the asset key directly, which is exactly jaffle_shop's
    real mapping (confirmed against a real run -- see docs/design.md).

    Narrower return type than plain make_context() -- traced_dbt() is bound to
    AssetOrOpExecutionContext, not the full 3-way ExecutionContext (Issue #72:
    AssetCheckExecutionContext has no asset_key_for_output at all, and dbt_assets/
    op-based dbt.cli() usage never actually produces one anyway)."""
    context = make_context(instance, run_id, op_path)
    context.asset_key_for_output = lambda output_name: AssetKey(output_name)  # type: ignore[method-assign, union-attr]
    return context  # type: ignore[return-value]


def test_traced_dbt_creates_asset_and_nested_check_spans(spans) -> None:
    @traced_dbt()
    def my_dbt_assets(context) -> Generator[DbtEvent, None, None]:
        yield Output(
            value=None,
            output_name="stg_customers",
            metadata={
                "Execution Duration": MetadataValue.float(0.05),
                "unique_id": MetadataValue.text("model.jaffle_shop.stg_customers"),
            },
        )
        yield AssetCheckResult(
            passed=True,
            check_name="not_null_stg_customers_id",
            asset_key=AssetKey("stg_customers"),
            metadata={
                "Execution Duration": MetadataValue.float(0.02),
                "unique_id": MetadataValue.text("test.jaffle_shop.not_null_stg_customers_id"),
            },
        )

    context = _make_dbt_context(FakeInstance(), "run-1", ["my_dbt_assets"])
    events: list[DbtEvent] = list(my_dbt_assets(context))

    # Pass-through: events come back unmodified, same order, for Dagster to still see.
    assert len(events) == 2
    assert isinstance(events[0], Output)
    assert isinstance(events[1], AssetCheckResult)

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert set(finished) == {"my_dbt_assets", "stg_customers", "not_null_stg_customers_id"}

    step_span = finished["my_dbt_assets"]
    asset_span = finished["stg_customers"]
    check_span = finished["not_null_stg_customers_id"]

    # step -> asset -> check, three real levels of nesting, not three siblings.
    assert asset_span.parent is not None
    assert asset_span.parent.span_id == step_span.context.span_id
    assert check_span.parent is not None
    assert check_span.parent.span_id == asset_span.context.span_id

    assert asset_span.attributes["dagster.asset_key"] == "stg_customers"
    assert check_span.attributes["dagster.asset_key"] == "stg_customers"
    assert check_span.attributes["dagster.check_name"] == "not_null_stg_customers_id"


def test_traced_dbt_handles_op_based_dbt_events(spans) -> None:
    """Issue #47: a plain @op calling dbt.cli(...).stream() (not @dbt_assets) gets
    AssetMaterialization/AssetCheckEvaluation instead of Output/AssetCheckResult for
    the identical underlying dbt run -- must produce the same step -> asset -> check
    span tree, not silently skip every event."""

    @traced_dbt()
    def my_dbt_op(context) -> Generator[AssetMaterialization | AssetCheckEvaluation, None, None]:
        yield AssetMaterialization(
            asset_key=AssetKey("stg_customers"),
            metadata={"Execution Duration": MetadataValue.float(0.05)},
        )
        yield AssetCheckEvaluation(
            passed=True,
            check_name="not_null_stg_customers_id",
            asset_key=AssetKey("stg_customers"),
            metadata={"Execution Duration": MetadataValue.float(0.02)},
        )

    context = make_context(FakeInstance(), "run-1", ["my_dbt_op"])
    events = list(my_dbt_op(context))

    assert len(events) == 2
    finished = {s.name: s for s in spans.get_finished_spans()}
    assert set(finished) == {"my_dbt_op", "stg_customers", "not_null_stg_customers_id"}

    step_span = finished["my_dbt_op"]
    asset_span = finished["stg_customers"]
    check_span = finished["not_null_stg_customers_id"]
    assert asset_span.parent is not None
    assert asset_span.parent.span_id == step_span.context.span_id
    assert check_span.parent is not None
    assert check_span.parent.span_id == asset_span.context.span_id
    assert check_span.attributes["dagster.check_name"] == "not_null_stg_customers_id"


def test_traced_dbt_asset_observation_gets_generic_span_no_status(spans) -> None:
    """AssetObservation -- dagster_dbt's own fallback for a test with no
    determinable check identity -- has neither check_name nor passed, so it must
    get a generic span name and no pass/fail status, not a crash or an invented
    verdict."""

    @traced_dbt()
    def my_dbt_op(context) -> Generator[AssetObservation, None, None]:
        yield AssetObservation(
            asset_key=AssetKey("stg_customers"),
            metadata={"Execution Duration": MetadataValue.float(0.01)},
        )

    context = make_context(FakeInstance(), "run-1", ["my_dbt_op"])
    list(my_dbt_op(context))

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert "dbt_observation" in finished
    observation_span = finished["dbt_observation"]
    assert observation_span.attributes["dagster.asset_key"] == "stg_customers"
    assert observation_span.status.status_code.name != "ERROR"


def test_traced_dbt_check_span_gets_error_status_on_failure(spans) -> None:
    @traced_dbt()
    def failing_dbt_assets(context) -> Generator[AssetCheckResult, None, None]:
        yield AssetCheckResult(
            passed=False,
            check_name="not_null_customers_id",
            asset_key=AssetKey("customers"),
            metadata={"Execution Duration": MetadataValue.float(0.01)},
        )

    context = _make_dbt_context(FakeInstance(), "run-1", ["failing_dbt_assets"])
    list(failing_dbt_assets(context))

    finished = {s.name: s for s in spans.get_finished_spans()}
    check_span = finished["not_null_customers_id"]
    assert check_span.status.status_code.name == "ERROR"


def test_traced_dbt_skips_events_without_duration_metadata(spans) -> None:
    """An event without "Execution Duration" is passed through untouched, no span
    created for it -- not verified against every DbtDagsterEventType variant, so
    this shouldn't ever raise on one that lacks it."""

    @traced_dbt()
    def sparse_dbt_assets(context) -> Generator[Output, None, None]:
        yield Output(value=None, output_name="no_metadata_here", metadata={})

    context = _make_dbt_context(FakeInstance(), "run-1", ["sparse_dbt_assets"])
    events: list[Output] = list(sparse_dbt_assets(context))

    assert len(events) == 1
    finished = {s.name for s in spans.get_finished_spans()}
    assert finished == {"sparse_dbt_assets"}  # just the step span, no asset span


def test_traced_dbt_bare_preserves_asset_span_behavior(spans) -> None:
    """Bare @traced_dbt (no parens) -- must behave identically to @traced_dbt().
    Regression test for the same silent-breakage bug @traced() had before Issue #19
    (see _tracing.py's module docstring): bare use used to rebind the decorated name
    to the unconfigured inner decorator function instead of the traced original."""

    @traced_dbt
    def my_dbt_assets(context) -> Generator[Output, None, None]:
        yield Output(
            value=None,
            output_name="stg_customers",
            metadata={"Execution Duration": MetadataValue.float(0.05)},
        )

    context = _make_dbt_context(FakeInstance(), "run-1", ["my_dbt_assets"])
    events: list[Output] = list(my_dbt_assets(context))

    assert len(events) == 1
    finished = {s.name for s in spans.get_finished_spans()}
    assert finished == {"my_dbt_assets", "stg_customers"}
