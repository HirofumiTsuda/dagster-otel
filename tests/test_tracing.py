"""Tests for dagster_otel._tracing: the public @traced() decorator."""

from conftest import FakeInstance, make_context

from dagster_otel import traced


def test_traced_preserves_plain_return_value() -> None:
    @traced()
    def my_op(context, x: int) -> int:
        return x * 2

    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert my_op(ctx, 21) == 42


def test_traced_bare_preserves_plain_return_value() -> None:
    """Bare @traced (no parens) -- must behave identically to @traced(), matching
    @op/@asset's own bare-use support. Regression test for the silent-breakage bug
    (Issue #19): bare use used to rebind the decorated name to the unconfigured inner
    decorator function instead of the traced original, with no exception anywhere."""

    @traced
    def my_op(context, x: int) -> int:
        return x * 2

    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert my_op(ctx, 21) == 42


def test_traced_bare_preserves_generator_yields() -> None:
    @traced
    def my_asset(context):
        yield "a"
        yield "b"

    ctx = make_context(FakeInstance(), "run-1", ["my_asset"])
    assert list(my_asset(ctx)) == ["a", "b"]


def test_traced_bare_default_span_name_is_function_name(spans) -> None:
    @traced
    def my_named_op(context) -> None:
        pass

    my_named_op(make_context(FakeInstance(), "run-1", ["my_named_op"]))

    names = [s.name for s in spans.get_finished_spans()]
    assert "my_named_op" in names


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

    child_ctx = make_context(instance, "run-1", ["child_op"], deps=["root_op"])
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
    child_op(make_context(instance, "run-1", ["child_op"], deps=["root_asset"]))

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert finished["child_op"].parent.span_id == finished["root_asset"].context.span_id


def test_traced_fan_in_gets_real_parent_and_link_for_second_upstream(spans) -> None:
    """Issue #5: a step depending on two independent upstreams should be parented
    under one of them (deterministically, the lexicographically-first step_key) and
    carry a Link to the other -- not silently attach to whichever upstream happened
    to be found first, and not lose the second dependency's relationship entirely."""
    instance = FakeInstance()

    @traced()
    def root_a(context) -> None:
        pass

    @traced()
    def root_b(context) -> None:
        pass

    @traced()
    def merge_op(context, a=None, b=None) -> None:
        pass

    root_a(make_context(instance, "run-1", ["root_a"]))
    root_b(make_context(instance, "run-1", ["root_b"]))
    merge_op(make_context(instance, "run-1", ["merge_op"], deps=["root_a", "root_b"]))

    finished = {s.name: s for s in spans.get_finished_spans()}
    merge_span = finished["merge_op"]
    root_a_span = finished["root_a"]
    root_b_span = finished["root_b"]

    # "root_a" sorts before "root_b" -- deterministic primary parent.
    assert merge_span.parent is not None
    assert merge_span.parent.span_id == root_a_span.context.span_id
    assert merge_span.context.trace_id == root_a_span.context.trace_id

    # The other real dependency isn't lost -- it's a Link instead of the parent.
    assert len(merge_span.links) == 1
    assert merge_span.links[0].context.span_id == root_b_span.context.span_id


def test_traced_sets_dagster_context_span_attributes(spans) -> None:
    """Issue #9: run_id/job_name/step_key/retry_number should be on the span itself,
    not just derivable by cross-referencing Dagster's own UI/event log."""

    @traced()
    def my_op(context) -> None:
        pass

    ctx = make_context(
        FakeInstance(),
        "run-1",
        ["outer", "my_op"],
        job_name="my_job",
        retry_number=2,
    )
    my_op(ctx)

    (span,) = spans.get_finished_spans()
    assert span.attributes["dagster.run_id"] == "run-1"
    assert span.attributes["dagster.job_name"] == "my_job"
    assert span.attributes["dagster.step_key"] == "outer.my_op"
    assert span.attributes["dagster.retry_number"] == 2


def test_traced_retry_gets_link_to_previous_attempt(spans) -> None:
    """Issue #14: a RetryPolicy-triggered retry's span should carry a Link back to
    the previous attempt's span, even though the real parent (here: none, a root)
    stays whatever it actually is -- the retry relationship is additive, not a
    replacement for the real dependency-graph parent."""
    instance = FakeInstance()

    @traced()
    def flaky_op(context) -> None:
        pass

    first_attempt_ctx = make_context(instance, "run-1", ["flaky_op"], retry_number=0)
    flaky_op(first_attempt_ctx)

    retry_ctx = make_context(instance, "run-1", ["flaky_op"], retry_number=1)
    flaky_op(retry_ctx)

    finished = spans.get_finished_spans()
    assert len(finished) == 2
    first_span, retry_span = finished

    # Both attempts are still roots (no real upstream dependency here) -- the retry
    # relationship doesn't change who the real parent is.
    assert retry_span.parent is None or retry_span.parent.span_id == 0x1

    # But the retry carries a Link back to the first attempt's span specifically.
    assert len(retry_span.links) == 1
    assert retry_span.links[0].context.span_id == first_span.context.span_id
    # And the first attempt itself has no such link -- nothing preceded it.
    assert len(first_span.links) == 0
