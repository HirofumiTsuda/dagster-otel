"""Tests for dagster_otel._propagation: the run-tags-based cross-process trace context
transport, keyed by each step's own real dependency graph (see the module's docstring,
"Keyed by real step dependencies, not subgraph path", for why -- subgraph-path keying
let two independent roots collide)."""

import json

from conftest import FakeInstance, make_context
from opentelemetry import trace
from opentelemetry.trace import Link

from dagster_otel._propagation import (
    EXTERNAL_TRACE_CONTEXT_TAG_KEY,
    _ancestor_runs,
    _own_step_key,
    _seed_run_root_context,
    _upstream_step_keys,
    carrier_to_span_context,
    find_external_trace_context,
    find_previous_attempt_context,
    find_upstream_trace_contexts,
    publish_trace_context,
)
from dagster_otel._setup import _current_run_id


def test_own_step_key_is_dotted_op_path() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["outer", "inner", "my_op"])
    assert _own_step_key(ctx) == "outer.inner.my_op"


def test_own_step_key_uses_real_step_key_for_dynamic_mapping() -> None:
    """Issue #45: a dynamic-mapped step's real key includes `[mapping_key]`, which
    `op_handle.path` never carries -- `_own_step_key` must use the real
    `ExecutionStep.key` (here, the fake context's `step_key` param), not reconstruct
    one from `op_path`."""
    ctx = make_context(FakeInstance(), "run-1", ["process_file"], step_key="process_file[a.txt]")
    assert _own_step_key(ctx) == "process_file[a.txt]"


def test_find_upstream_trace_contexts_distinguishes_parallel_mapped_instances() -> None:
    """The actual bug Issue #45 fixes: before, every parallel invocation of one
    mapped op collapsed to the identical `op_handle.path`-based key and raced to
    overwrite the same run tag. Two mapped instances now publish under distinct
    real step keys, so a downstream step depending on one specific instance finds
    exactly that one -- not whichever instance happened to publish last."""
    instance = FakeInstance()

    instance_a = make_context(instance, "run-1", ["process_file"], step_key="process_file[a.txt]")
    with trace.get_tracer("test").start_as_current_span("process_file[a.txt]"):
        publish_trace_context(instance_a)
        trace_id_a = trace.get_current_span().get_span_context().trace_id

    instance_b = make_context(instance, "run-1", ["process_file"], step_key="process_file[b.txt]")
    with trace.get_tracer("test").start_as_current_span("process_file[b.txt]"):
        publish_trace_context(instance_b)

    assert trace_id_a != trace.get_current_span().get_span_context().trace_id

    # A downstream step depending on exactly one mapped instance (the real
    # dependency_keys shape, "op_name[mapping_key]") must find only that instance.
    collect_ctx = make_context(instance, "run-1", ["collect_op"], deps=["process_file[a.txt]"])
    found = find_upstream_trace_contexts(collect_ctx)
    assert len(found) == 1
    assert format(trace_id_a, "032x") in found[0]["traceparent"]


def test_upstream_step_keys_empty_for_no_deps() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["root_op"])
    assert _upstream_step_keys(ctx) == frozenset()


def test_upstream_step_keys_reflects_real_dependencies() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["merge_op"], deps=["root_a", "root_b"])
    assert _upstream_step_keys(ctx) == frozenset({"root_a", "root_b"})


def test_find_upstream_trace_contexts_empty_for_root() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["root_op"])
    assert find_upstream_trace_contexts(ctx) == []


def test_find_upstream_trace_contexts_empty_when_upstream_untraced() -> None:
    """A real dependency that never called publish_trace_context() (not @traced(), or
    -- shouldn't happen given Dagster's own execution order -- not yet run) simply
    isn't found; this is a normal state; not an error."""
    ctx = make_context(FakeInstance(), "run-1", ["child_op"], deps=["untraced_root"])
    assert find_upstream_trace_contexts(ctx) == []


def test_find_upstream_trace_contexts_finds_real_parent() -> None:
    instance = FakeInstance()
    root_ctx = make_context(instance, "run-1", ["root_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(root_ctx)
        root_trace_id = trace.get_current_span().get_span_context().trace_id

    child_ctx = make_context(instance, "run-1", ["child_op"], deps=["root_op"])
    found = find_upstream_trace_contexts(child_ctx)
    assert len(found) == 1
    # traceparent carrier format: "00-<trace_id hex>-<span_id hex>-<flags>"
    assert format(root_trace_id, "032x") in found[0]["traceparent"]


def test_find_upstream_trace_contexts_ignores_unrelated_publishers() -> None:
    """Two independent roots must not collide just because they published in the same
    run -- the bug this whole redesign (Issue #5) exists to fix. Each key is now the
    step's own identity, not a shared subgraph key, so there's nothing to collide on."""
    instance = FakeInstance()

    root_a_ctx = make_context(instance, "run-1", ["root_a"])
    with trace.get_tracer("test").start_as_current_span("root_a"):
        publish_trace_context(root_a_ctx)
        trace_id_a = trace.get_current_span().get_span_context().trace_id

    root_b_ctx = make_context(instance, "run-1", ["root_b"])
    with trace.get_tracer("test").start_as_current_span("root_b"):
        publish_trace_context(root_b_ctx)

    assert trace_id_a != trace.get_current_span().get_span_context().trace_id

    # child_a depends only on root_a -- must find exactly root_a's context, never
    # root_b's, regardless of which published first or second.
    child_a_ctx = make_context(instance, "run-1", ["child_a"], deps=["root_a"])
    found = find_upstream_trace_contexts(child_a_ctx)
    assert len(found) == 1
    assert format(trace_id_a, "032x") in found[0]["traceparent"]


def test_find_upstream_trace_contexts_fan_in_returns_all_in_sorted_order() -> None:
    """A step depending on two published upstreams gets both back, ordered
    deterministically by step_key -- _tracing.py relies on this order to pick a
    stable primary parent (first) vs. Link candidates (rest)."""
    instance = FakeInstance()

    root_b_ctx = make_context(instance, "run-1", ["root_b"])
    with trace.get_tracer("test").start_as_current_span("root_b"):
        publish_trace_context(root_b_ctx)
        trace_id_b = trace.get_current_span().get_span_context().trace_id

    root_a_ctx = make_context(instance, "run-1", ["root_a"])
    with trace.get_tracer("test").start_as_current_span("root_a"):
        publish_trace_context(root_a_ctx)
        trace_id_a = trace.get_current_span().get_span_context().trace_id

    merge_ctx = make_context(instance, "run-1", ["merge_op"], deps=["root_a", "root_b"])
    found = find_upstream_trace_contexts(merge_ctx)
    assert len(found) == 2
    # "root_a" sorts before "root_b" -- primary parent is deterministic even though
    # root_b was published first.
    assert format(trace_id_a, "032x") in found[0]["traceparent"]
    assert format(trace_id_b, "032x") in found[1]["traceparent"]


def test_find_upstream_trace_contexts_fan_in_partial_publish() -> None:
    """If only one of two real upstreams published (the other untraced), just that
    one comes back -- not an error, not padded with a placeholder."""
    instance = FakeInstance()

    root_a_ctx = make_context(instance, "run-1", ["root_a"])
    with trace.get_tracer("test").start_as_current_span("root_a"):
        publish_trace_context(root_a_ctx)

    merge_ctx = make_context(instance, "run-1", ["merge_op"], deps=["root_a", "untraced_root_b"])
    found = find_upstream_trace_contexts(merge_ctx)
    assert len(found) == 1


def test_carrier_to_span_context_recovers_published_span() -> None:
    instance = FakeInstance()
    ctx = make_context(instance, "run-1", ["root_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(ctx)
        expected = trace.get_current_span().get_span_context()

    found = find_upstream_trace_contexts(
        make_context(instance, "run-1", ["child_op"], deps=["root_op"])
    )
    span_context = carrier_to_span_context(found[0])
    assert span_context.trace_id == expected.trace_id
    assert span_context.span_id == expected.span_id
    # Also usable to build a Link, which is the actual reason this exists
    # (fan-in's non-primary upstreams -- see _tracing.py).
    Link(span_context)


def test_publish_requires_an_active_span() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    try:
        publish_trace_context(ctx)
    except RuntimeError as e:
        assert "no active span" in str(e)
    else:
        raise AssertionError("expected RuntimeError")


def test_ancestor_runs_walks_parent_chain() -> None:
    instance = FakeInstance()
    instance.create_run("grandparent")
    instance.create_run("parent", parent_run_id="grandparent")
    instance.create_run("child", parent_run_id="parent")
    ctx = make_context(instance, "child", ["my_op"])

    assert [run.run_id for run in _ancestor_runs(ctx)] == ["child", "parent", "grandparent"]


def test_ancestor_runs_single_run_has_no_ancestors() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["my_op"])
    assert [run.run_id for run in _ancestor_runs(ctx)] == ["run-1"]


def test_ancestor_runs_fetched_once_when_shared_across_lookups() -> None:
    """Issue #35: a caller doing several lookups for the same step (as
    _traced_span() does) should fetch the ancestor chain once and pass it through,
    not let each lookup independently re-walk/re-fetch run storage."""
    instance = FakeInstance()
    instance.create_run("original")
    instance.create_run("retry", parent_run_id="original")

    calls = 0
    real_get_run_by_id = instance.get_run_by_id

    def counting_get_run_by_id(run_id: str):
        nonlocal calls
        calls += 1
        return real_get_run_by_id(run_id)

    instance.get_run_by_id = counting_get_run_by_id  # type: ignore[method-assign]

    root_ctx = make_context(instance, "original", ["root_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(root_ctx)

    retry_ctx = make_context(instance, "retry", ["child_op"], deps=["root_op"], retry_number=1)
    calls = 0  # only count what happens from here on -- make_context() itself does a
    # get_run_by_id() check to decide whether to auto-create the run.
    runs = _ancestor_runs(retry_ctx)
    assert calls == 2  # "retry" then "original" -- exactly the chain, fetched once

    calls = 0
    find_upstream_trace_contexts(retry_ctx, runs)
    find_external_trace_context(retry_ctx, runs)
    find_previous_attempt_context(retry_ctx, runs)
    assert calls == 0  # every lookup reused the passed-in runs, no further fetches


def test_find_upstream_trace_contexts_falls_back_to_ancestor_run() -> None:
    """A retried run that doesn't re-run the publishing step should still find the
    trace context published by that step in the original (parent) run."""
    instance = FakeInstance()
    instance.create_run("original")
    instance.create_run("retry", parent_run_id="original")

    publisher_ctx = make_context(instance, "original", ["root_op"])
    with trace.get_tracer("test").start_as_current_span("root"):
        publish_trace_context(publisher_ctx)

    # This step only exists in the retry run, which never published anything itself.
    retry_ctx = make_context(instance, "retry", ["child_op"], deps=["root_op"])
    assert len(find_upstream_trace_contexts(retry_ctx)) == 1


def test_seed_run_root_context_sets_current_run_id() -> None:
    """_seed_run_root_context itself just records which run this is (Issue #63) --
    _DeterministicRunIdGenerator (tests/test_setup.py) is what turns that into a
    deterministic trace_id, no OTel context activated here at all any more."""
    ctx_a1 = make_context(FakeInstance(), "run-A", ["op1"])
    ctx_a2 = make_context(FakeInstance(), "run-A", ["op2"])
    ctx_b = make_context(FakeInstance(), "run-B", ["op1"])

    _seed_run_root_context(ctx_a1)
    assert _current_run_id.get() == "run-A"

    _seed_run_root_context(ctx_a2)
    assert _current_run_id.get() == "run-A"

    _seed_run_root_context(ctx_b)
    assert _current_run_id.get() == "run-B"


def test_find_previous_attempt_context_none_on_first_attempt() -> None:
    """retry_number=0 (the default) is the first attempt -- nothing preceded it."""
    ctx = make_context(FakeInstance(), "run-1", ["my_op"], retry_number=0)
    assert find_previous_attempt_context(ctx) is None


def test_find_previous_attempt_context_none_when_first_attempt_never_published() -> None:
    """A retry looked up before the first attempt ever published anything (shouldn't
    happen given Dagster's own execution order, but treated as "not found," not an
    error, same as every other lookup in this module)."""
    ctx = make_context(FakeInstance(), "run-1", ["my_op"], retry_number=1)
    assert find_previous_attempt_context(ctx) is None


def test_find_previous_attempt_context_finds_the_prior_attempt() -> None:
    instance = FakeInstance()

    first_attempt_ctx = make_context(instance, "run-1", ["my_op"], retry_number=0)
    with trace.get_tracer("test").start_as_current_span("attempt-0"):
        publish_trace_context(first_attempt_ctx)
        first_trace_id = trace.get_current_span().get_span_context().trace_id

    retry_ctx = make_context(instance, "run-1", ["my_op"], retry_number=1)
    found = find_previous_attempt_context(retry_ctx)
    assert found is not None
    assert format(first_trace_id, "032x") in found["traceparent"]


def test_find_external_trace_context_none_when_not_set() -> None:
    ctx = make_context(FakeInstance(), "run-1", ["root_op"])
    assert find_external_trace_context(ctx) is None


def test_find_external_trace_context_finds_it() -> None:
    """A caller setting EXTERNAL_TRACE_CONTEXT_TAG_KEY at launch time (before any
    @traced() step runs) is exactly what this simulates -- add_run_tags() directly,
    not via publish_trace_context() (which this library never calls for this key)."""
    instance = FakeInstance()
    instance.create_run("run-1")
    carrier = {"traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    instance.add_run_tags("run-1", {EXTERNAL_TRACE_CONTEXT_TAG_KEY: json.dumps(carrier)})

    ctx = make_context(instance, "run-1", ["root_op"])
    assert find_external_trace_context(ctx) == carrier


def test_find_external_trace_context_found_via_ancestor_run() -> None:
    """Confirmed against a real retry-from-failure run (see this function's own
    docstring) that Dagster does *not* copy a run's tags forward to a retry --
    the retry run's own tags start empty. So a retry still needs the ancestor walk
    to find what the *original* run was seeded with."""
    instance = FakeInstance()
    instance.create_run("original")
    instance.create_run("retry", parent_run_id="original")
    carrier = {"traceparent": "00-" + "c" * 32 + "-" + "d" * 16 + "-01"}
    instance.add_run_tags("original", {EXTERNAL_TRACE_CONTEXT_TAG_KEY: json.dumps(carrier)})

    # The retry run's own tags are empty -- nothing set directly on it.
    retry_ctx = make_context(instance, "retry", ["root_op"])
    assert find_external_trace_context(retry_ctx) == carrier
