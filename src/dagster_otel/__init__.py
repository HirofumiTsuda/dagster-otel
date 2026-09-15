"""OpenTelemetry tracing for Dagster ops and assets.

See docs/design.md for the design rationale. Public API:

    from dagster_otel import configure, publish_trace_context, traced

    configure(service_name="my_pipeline")  # once per process, e.g. from a @resource

    @op(...)
    def root_op(context):
        with tracer.start_as_current_span("root"):
            publish_trace_context(context)  # once per run (or per subgraph)

    @op(...)
    @traced()  # Dagster's own @op still owns op-ness; this is a thin layer underneath
    def downstream_op(context, x: int) -> int:
        ...
"""

from dagster_otel._propagation import publish_trace_context
from dagster_otel._setup import configure
from dagster_otel._tracing import traced
from dagster_otel.version import __version__

__all__ = [
    "__version__",
    "configure",
    "publish_trace_context",
    "traced",
]
