"""Tests for dagster_otel._tracing: the public @traced() decorator."""

from conftest import FakeInstance, make_context

from dagster_otel import traced


def test_traced_preserves_plain_return_value() -> None:
    @traced()
    def my_op(context, x: int) -> int:
        return x * 2

    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert my_op(ctx, 21) == 42


def test_traced_preserves_generator_yields() -> None:
    """The case @dbt_assets needs -- see _tracing.py's module docstring for why this
    isn't just "the same as plain functions but with yield"."""

    @traced()
    def my_asset(context):
        yield "a"
        yield "b"
        yield "c"

    ctx = make_context(FakeInstance(), "run-1", ["my_asset"])
    assert list(my_asset(ctx)) == ["a", "b", "c"]


def test_traced_default_span_name_is_function_name(spans) -> None:
    @traced()
    def my_named_op(context) -> None:
        pass

    my_named_op(make_context(FakeInstance(), "run-1", ["my_named_op"]))

    names = [s.name for s in spans.get_finished_spans()]
    assert "my_named_op" in names


def test_traced_custom_span_name(spans) -> None:
    @traced(span_name="a_custom_name")
    def my_op(context) -> None:
        pass

    my_op(make_context(FakeInstance(), "run-1", ["my_op"]))

    names = [s.name for s in spans.get_finished_spans()]
    assert "a_custom_name" in names
    assert "my_op" not in names


def test_traced_root_auto_publishes_and_child_nests_under_it(spans) -> None:
    """The behavior added in response to "manually publishing from a root step is
    annoying": the first @traced() step in a run just becomes the root, no
    publish_trace_context() call needed anywhere."""
    instance = FakeInstance()

    @traced()
    def root_op(context) -> str:
        return "root_done"

    @traced()
    def child_op(context, upstream: str) -> str:
        return f"{upstream}+child"

    root_ctx = make_context(instance, "run-1", ["root_op"])
    root_result = root_op(root_ctx)
    assert root_result == "root_done"

    child_ctx = make_context(instance, "run-1", ["child_op"])
    child_result = child_op(child_ctx, root_result)
    assert child_result == "root_done+child"

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert "root_op" in finished
    assert "child_op" in finished
    root_span = finished["root_op"]
    child_span = finished["child_op"]
    # Real parent, not just the same trace_id via the deterministic-seed fallback.
    assert child_span.parent is not None
    assert child_span.parent.span_id == root_span.context.span_id
    assert child_span.context.trace_id == root_span.context.trace_id


def test_traced_two_independent_runs_get_different_traces(spans) -> None:
    """Sanity check on the run-scoping itself: steps from two unrelated runs should
    never end up in the same trace just because they happen to run in the same
    process (e.g. two runs executed back-to-back in tests, or via in_process
    executor)."""

    @traced()
    def solo_op(context) -> None:
        pass

    solo_op(make_context(FakeInstance(), "run-A", ["solo_op"]))
    solo_op(make_context(FakeInstance(), "run-B", ["solo_op"]))

    finished = spans.get_finished_spans()
    assert len(finished) == 2
    assert finished[0].context.trace_id != finished[1].context.trace_id


def test_traced_attaches_and_removes_logging_filter() -> None:
    """context.log shouldn't be left with a dangling filter after the step ends --
    otherwise every subsequent log call in that process (a long-lived resource,
    unrelated steps sharing a logger, ...) would keep getting stamped."""

    @traced()
    def my_op(context) -> None:
        assert len(context.log.filters) == 1

    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert len(ctx.log.filters) == 0
    my_op(ctx)
    assert len(ctx.log.filters) == 0


def test_traced_also_filters_get_dagster_logger() -> None:
    """Integrations (dagster_dbt confirmed by example) log via get_dagster_logger(),
    a separate global logger from context.log -- verified against a real
    @dbt_assets run (examples/dagster_workspace/definitions.py) that those lines
    came through with no trace_id/span_id until this was added. Regression-test the
    mechanism here without needing dbt: filtering get_dagster_logger() should work
    the same way context.log's filtering does."""
    from dagster import get_dagster_logger

    @traced()
    def my_op(context) -> None:
        assert len(get_dagster_logger().filters) == 1

    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert len(get_dagster_logger().filters) == 0
    my_op(ctx)
    assert len(get_dagster_logger().filters) == 0


def test_traced_removes_filter_even_if_func_raises() -> None:
    @traced()
    def failing_op(context) -> None:
        raise ValueError("boom")

    ctx = make_context(FakeInstance(), "run-1", ["failing_op"])
    try:
        failing_op(ctx)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")
    assert len(ctx.log.filters) == 0


def test_traced_op_and_asset_share_propagation(spans) -> None:
    """@traced() doesn't care whether it's wrapping something destined to become an
    @op or an @asset -- op_handle-based lookup works the same either way (verified
    against real Dagster separately; this just checks dagster_otel's own logic
    doesn't special-case either)."""
    instance = FakeInstance()

    @traced()
    def root_asset(context) -> None:
        pass

    @traced()
    def child_op(context) -> None:
        pass

    root_asset(make_context(instance, "run-1", ["root_asset"]))
    child_op(make_context(instance, "run-1", ["child_op"]))

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert finished["child_op"].parent.span_id == finished["root_asset"].context.span_id
