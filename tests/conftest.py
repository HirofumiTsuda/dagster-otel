"""Shared test fixtures.

No real Dagster instance/run is used anywhere here -- these are lightweight stand-ins
providing only the attributes dagster_otel actually touches (`op_handle.path`,
`run_id`, `instance.get_run_by_id`/`add_run_tags`, `log`). See the verify-tracing
skill for why that's a deliberate scope limit, not an oversight: these tests can prove
the Python logic is internally consistent, not that Dagster still behaves the way this
library assumes it does -- that needs a real instance (verify-tracing/
verify-dagster-version-compat skills), which these tests don't replace.
"""
import logging
from types import SimpleNamespace
from typing import Any

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

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
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
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
    dict, the same shape dagster_otel actually reads (run.tags / run.parent_run_id) --
    see _propagation.py's publish_trace_context/_find_trace_context/
    _run_id_and_ancestors, the only things that touch `context.instance`."""

    def __init__(self) -> None:
        self._runs: dict[str, dict[str, Any]] = {}

    def create_run(self, run_id: str, parent_run_id: str | None = None) -> None:
        self._runs[run_id] = {"parent_run_id": parent_run_id, "tags": {}}

    def get_run_by_id(self, run_id: str) -> SimpleNamespace | None:
        data = self._runs.get(run_id)
        if data is None:
            return None
        return SimpleNamespace(parent_run_id=data["parent_run_id"], tags=dict(data["tags"]))

    def add_run_tags(self, run_id: str, new_tags: dict[str, str]) -> None:
        self._runs[run_id]["tags"].update(new_tags)


def make_context(
    instance: FakeInstance,
    run_id: str,
    op_path: list[str],
) -> ExecutionContext:
    """A fake op/asset execution context with just enough surface for dagster_otel:
    op_handle.path, run_id, instance, log_event (unused since the run-tags switch, but
    harmless to keep), and log (a real logging.Logger -- context.log is a genuine
    logging.Logger subclass in real Dagster too, see _logging.py's module docstring,
    so using a plain one here matches the real shape rather than faking it)."""
    if instance.get_run_by_id(run_id) is None:
        instance.create_run(run_id)
    return SimpleNamespace(  # type: ignore[return-value]
        op_handle=SimpleNamespace(path=op_path),
        run_id=run_id,
        instance=instance,
        log_event=lambda event: None,
        log=logging.getLogger(f"test.{run_id}.{'.'.join(op_path)}"),
    )
