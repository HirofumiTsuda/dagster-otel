"""OpenTelemetry tracing for Dagster ops and assets.

See docs/design.md for the design rationale, and README.md for a fuller usage
example. Public API:

    from dagster_otel import traced

    @op(...)          # Dagster's own @op still owns op-ness; @traced() is a thin
    @traced()          # layer underneath -- no @resource/required_resource_keys, no
    def my_op(context, x: int) -> int:  # manual "root" step needed either.
        ...

`configure()`/`publish_trace_context()` are exported for advanced cases (eager
configuration, manually marking a subgraph boundary) -- not required for ordinary
use, see their own docstrings. `EXTERNAL_TRACE_CONTEXT_TAG_KEY` is for nesting a
whole run's trace under an external caller's (Issue #13) -- see its own docstring in
`_propagation.py` for how to use it; most users won't need it.

For `@dbt_assets`, see `dagster_otel.dbt.traced_dbt()` instead of plain `@traced()`.
"""

from dagster_otel._propagation import EXTERNAL_TRACE_CONTEXT_TAG_KEY, publish_trace_context
from dagster_otel._setup import configure
from dagster_otel._tracing import traced
from dagster_otel.version import __version__

__all__ = [
    "EXTERNAL_TRACE_CONTEXT_TAG_KEY",
    "__version__",
    "configure",
    "publish_trace_context",
    "traced",
]
