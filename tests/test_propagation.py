"""Tests for dagster_otel._propagation: the run-tags-based cross-process trace context
transport, and the key scheme _find_trace_context/_trace_key_for use to fall back from
a step's own subgraph up to the run root."""

from conftest import FakeInstance, make_context
from opentelemetry import trace

from dagster_otel._propagation import (
    ROOT_TRACE_KEY,
    _find_trace_context,
    _run_id_and_ancestors,
    _seed_run_root_context,
    _trace_key_for,
    publish_trace_context,
)


def test_trace_key_for_top_level_op_is_root() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert _trace_key_for(ctx) == ROOT_TRACE_KEY


def test_trace_key_for_nested_op_is_enclosing_subgraph_path() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["outer", "inner", "my_op"])
    assert _trace_key_for(ctx) == "outer.inner"


def test_find_trace_context_prefix_order_nearest_first() -> None:
    """A step nested three levels deep should check its immediate subgraph first,
    then progressively shallower ones, then the run root last."""
    instance = FakeInstance()
    # Publish under the *outermost* subgraph key ("a") only.
    publisher_ctx = make_context(instance, "run-1", ["a", "unrelated_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(publisher_ctx)

    # A step nested at a.b.c.my_op should still find the "a"-level publish, by
    # falling all the way back through "a.b.c" -> "a.b" -> "a".
    deep_ctx = make_context(instance, "run-1", ["a", "b", "c", "my_op"])
    found = _find_trace_context(deep_ctx)
    assert found is not None


def test_find_trace_context_prefers_nearest_match() -> None:
    """If both an outer and a nearer subgraph published, the nearer one wins."""
    instance = FakeInstance()

    outer_ctx = make_context(instance, "run-1", ["a", "outer_publisher"])
    with trace.get_tracer("test").start_as_current_span("outer"):
        publish_trace_context(outer_ctx)
        outer_trace_id = trace.get_current_span().get_span_context().trace_id

    inner_ctx = make_context(instance, "run-1", ["a", "b", "inner_publisher"])
    with trace.get_tracer("test").start_as_current_span("inner"):
        publish_trace_context(inner_ctx)
        inner_trace_id = trace.get_current_span().get_span_context().trace_id

    assert outer_trace_id != inner_trace_id  # sanity: two genuinely different spans

    child_ctx = make_context(instance, "run-1", ["a", "b", "my_op"])
    found = _find_trace_context(child_ctx)
    assert found is not None
    # traceparent carrier format: "00-<trace_id hex>-<span_id hex>-<flags>"
    assert format(inner_trace_id, "032x") in found["traceparent"]
    assert format(outer_trace_id, "032x") not in found["traceparent"]


def test_find_trace_context_none_when_nothing_published() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert _find_trace_context(ctx) is None


def test_publish_requires_an_active_span() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    try:
        publish_trace_context(ctx)
    except RuntimeError as e:
        assert "no active span" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_run_id_and_ancestors_walks_parent_chain() -> None:
    instance = FakeInstance()
    instance.create_run("grandparent")
    instance.create_run("parent", parent_run_id="grandparent")
    instance.create_run("child", parent_run_id="parent")
    ctx = make_context(instance, "child", ["my_op"])

    assert _run_id_and_ancestors(ctx) == ["child", "parent", "grandparent"]


def test_run_id_and_ancestors_single_run_has_no_ancestors() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert _run_id_and_ancestors(ctx) == ["run-1"]


def test_find_trace_context_falls_back_to_ancestor_run() -> None:
    """A retried run that doesn't re-run the publishing step should still find the
    trace context published in the original (parent) run."""
    instance = FakeInstance()
    instance.create_run("original")
    instance.create_run("retry", parent_run_id="original")

    publisher_ctx = make_context(instance, "original", ["root_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(publisher_ctx)

    # This step only exists in the retry run, which never published anything itself.
    retry_ctx = make_context(instance, "retry", ["child_op"])
    assert _find_trace_context(retry_ctx) is not None


def test_seed_run_root_context_is_deterministic_per_run() -> None:
    ctx_a1 = make_context(FakeInstance(), "run-A", ["op1"])
    ctx_a2 = make_context(FakeInstance(), "run-A", ["op2"])
    ctx_b = make_context(FakeInstance(), "run-B", ["op1"])

    _seed_run_root_context(ctx_a1)
    trace_id_a1 = trace.get_current_span().get_span_context().trace_id

    _seed_run_root_context(ctx_a2)
    trace_id_a2 = trace.get_current_span().get_span_context().trace_id

    _seed_run_root_context(ctx_b)
    trace_id_b = trace.get_current_span().get_span_context().trace_id

    assert trace_id_a1 == trace_id_a2  # same run_id -> same trace_id, no coordination
    assert trace_id_a1 != trace_id_b  # different run_id -> different trace_id
