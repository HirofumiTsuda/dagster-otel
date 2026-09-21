"""Shared test fixtures.

No real Dagster instance/run is used anywhere here -- these are lightweight stand-ins
providing only the attributes dagster_otel actually touches (`op_handle.path`,
`run.run_id`, `instance.get_run_by_id`/`add_run_tags`, `log`,
`get_step_execution_context().step.step_inputs[*].dependency_keys`). See the
verify-tracing skill for why that's a deliberate scope limit, not an oversight: these
tests can prove the Python logic is internally consistent, not that Dagster still
behaves the way this library assumes it does -- that needs a real instance
(verify-tracing/verify-dagster-version-compat skills), which these tests don't
replace.
"""

import logging
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import AssetKey
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dagster_otel._setup import _DeterministicRunIdGenerator
from dagster_otel._types import ExecutionContext


@pytest.fixture(scope="session", autouse=True)
def _otel_test_provider() -> InMemorySpanExporter:
    """Install one TracerProvider(InMemorySpanExporter) for the whole test session.

    OTel's set_tracer_provider() only takes effect on the *first* call per process
    (later calls are a no-op, see dagster_otel._setup.configure's docstring) -- so this
    has to happen exactly once, before anything else (including dagster_otel's own
    auto-configure() calls) tries to install a provider. That's also why this is fine:
    when @traced() calls the real configure() during a test, it's a guaranteed no-op,
    and every span still lands in this in-memory exporter instead of attempting a real
    OTLP network call.

    Uses _DeterministicRunIdGenerator, same as configure() itself, not the SDK's
    default -- otherwise tests exercising the deterministic-trace_id-for-parentless-
    steps behavior (Issue #63) would see plain random trace_ids instead, since that
    behavior lives in the IdGenerator, not anything these tests directly call.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider(id_generator=_DeterministicRunIdGenerator())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture
def spans(_otel_test_provider: InMemorySpanExporter) -> InMemorySpanExporter:
    """The in-memory exporter, cleared before each test so tests don't see each
    other's spans."""
    _otel_test_provider.clear()
    return _otel_test_provider


@pytest.fixture(autouse=True)
def _reset_otel_context():
    """Detach back to an empty OTel context before each test.

    Without this, a test that attaches a context without a `with` block (e.g. calling
    _seed_run_root_context directly, not through _traced_span's context manager) would
    leave it attached for whichever test runs next -- harmless for correctness (later
    `with start_as_current_span(...)` calls still nest correctly on top of it), but
    makes tests' current-span assertions depend on execution order, which is worth
    avoiding even if nothing here currently trips over it.
    """
    token = otel_context.attach(otel_context.Context())
    yield
    otel_context.detach(token)


class FakeInstance:
    """Stands in for DagsterInstance. Backs run tags and run parentage with a plain
    dict, the same shape dagster_otel actually reads (run.run_id / run.tags /
    run.parent_run_id) -- see _propagation.py's publish_trace_context/
    find_upstream_trace_contexts/_ancestor_runs, the only things that touch
    `context.instance`."""

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}

    def create_run(self, run_id: str, parent_run_id: str | None = None) -> None:
        self._runs[run_id] = {"parent_run_id": parent_run_id, "tags": {}}

    def get_run_by_id(self, run_id: str) -> SimpleNamespace | None:
        data = self._runs.get(run_id)
        if data is None:
            return None
        return SimpleNamespace(
            run_id=run_id, parent_run_id=data["parent_run_id"], tags=dict(data["tags"])
        )

    def add_run_tags(self, run_id: str, new_tags: dict[str, str]) -> None:
        self._runs[run_id]["tags"].update(new_tags)


def make_context(
    instance: FakeInstance,
    run_id: str,
    op_path: list[str],
    deps: list[str] | None = None,
    job_name: str = "test_job",
    retry_number: int = 0,
    step_key: str | None = None,
    asset_keys: list[str] | None = None,
) -> ExecutionContext:
    """A fake op/asset execution context with just enough surface for dagster_otel:
    op_handle.path, run.run_id, job_def.name, retry_number, instance, log_event
    (unused since the run-tags switch, but harmless to keep), log (a real
    logging.Logger -- context.log is a genuine logging.Logger subclass in real
    Dagster too, see _logging.py's module docstring, so using a plain one here
    matches the real shape rather than faking it), get_step_execution_context().
    step.step_inputs[*].dependency_keys / .step.key, and selected_asset_keys -- the
    same public properties/methods (see _propagation.py's _upstream_step_keys/
    _own_step_key docstrings, and _tracing.py's asset_keys attribute comment) real
    Dagster defines identically on both OpExecutionContext and AssetExecutionContext,
    so this fake doesn't need to model the two context shapes differently either.

    Doesn't model AssetCheckExecutionContext (Issue #72) -- that context type has a
    genuinely different shape (`.job_def` but no `.job_name`, `.selected_asset_check_
    keys` but no `.selected_asset_keys`) that `isinstance()`-checking code (_tracing.
    py's asset_check_keys branch) can't be satisfied by a SimpleNamespace regardless
    of which attributes it's given; that behavior is verified against a real Dagster
    run in test_tracing.py's own asset_check test instead.

    :param deps: step_keys this step directly depends on, matching real Dagster's
        `StepInput.dependency_keys` -- e.g. `deps=["root_op"]` for a step whose only
        input comes from a step named `root_op`. Defaults to no dependencies (a root).
    :param job_name: matches real Dagster's `context.job_def.name` (what this library
        now reads uniformly across all three context types, not `.job_name` -- see
        _tracing.py's comment) -- only meaningful for tests asserting on the
        `dagster.job_name` span attribute.
    :param retry_number: matches real Dagster's `context.retry_number` (0 for the
        first attempt) -- only meaningful for tests asserting on the
        `dagster.retry_number` span attribute.
    :param step_key: matches real Dagster's `ExecutionStep.key` -- defaults to
        `".".join(op_path)`, the same value a non-mapped step's real `step.key`
        resolves to (verified in Issue #45). Pass a `"op_name[mapping_key]"`-shaped
        string to simulate a dynamic-mapped step, where this diverges from `op_path`.
    :param asset_keys: matches real Dagster's `context.selected_asset_keys` -- plain
        strings, one `AssetKey` each (matching how a real single-part AssetKey's own
        `to_user_string()` round-trips). Defaults to none (a plain op, matching real
        Dagster's own empty-set return for `has_assets_def=False`).
    """
    if instance.get_run_by_id(run_id) is None:
        instance.create_run(run_id)
    step_inputs = [SimpleNamespace(dependency_keys={dep}) for dep in (deps or [])]
    step_execution_context = SimpleNamespace(
        step=SimpleNamespace(step_inputs=step_inputs, key=step_key or ".".join(op_path))
    )
    return SimpleNamespace(  # type: ignore[return-value]
        op_handle=SimpleNamespace(path=op_path),
        run=SimpleNamespace(run_id=run_id),
        job_def=SimpleNamespace(name=job_name),
        retry_number=retry_number,
        instance=instance,
        log_event=lambda event: None,
        log=logging.getLogger(f"test.{run_id}.{'.'.join(op_path)}"),
        get_step_execution_context=lambda: step_execution_context,
        selected_asset_keys=frozenset(AssetKey(k) for k in (asset_keys or [])),
    )
