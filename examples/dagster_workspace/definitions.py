"""Example: dagster-otel applied to a real @dbt_assets pipeline (jaffle_shop).

Verifies things Issue #2 (https://github.com/HirofumiTsuda/dagster-otel/issues/2) and
Issue #8 (https://github.com/HirofumiTsuda/dagster-otel/issues/8) ask about, against a
real dbt-backed asset graph rather than a hand-written toy:

1. `@traced()`'s generator-function handling (see `_tracing.py`'s module docstring)
   against a *real* `@dbt_assets` function -- Dagster requires this to be a generator
   (`yield from dbt.cli(...).stream()`), which is exactly the case that handling was
   written for, but until now only ever exercised against hand-written toy generators
   in `tests/`.
2. Whether/how a custom `@logger` can be attached to an asset materialization run at
   all -- the question raised in dagster-io/dagster#12495 by `zyd14`: "I'm not seeing
   an obvious way to pass loggers for assets and asset jobs." `Definitions` does in
   fact accept a `loggers=` parameter (see below) -- whether that's new since zyd14's
   2024-07-26 comment or was just missed isn't something this checked, only whether it
   actually works.
3. `traced_dbt()` (`dagster_otel.dbt`, Issue #8): a child span per dbt node (model/
   seed/test), keyed by the real Dagster asset_key/check_name, nested under this
   step's own span -- no changes needed to the function body below versus the plain
   `@traced()` version.

Run with (see examples/README.md for the full walkthrough):
    DAGSTER_HOME=... OTEL_SERVICE_NAME=jaffle_shop_example \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    dagster asset materialize -f examples/dagster_workspace/definitions.py --select '*'
"""

import logging
import os
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, logger
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from dagster_otel.dbt import traced_dbt

jaffle_shop_project = DbtProject(project_dir=Path(__file__).parent.parent / "jaffle_shop")
jaffle_shop_project.prepare_if_dev()


@dbt_assets(manifest=jaffle_shop_project.manifest_path)
@traced_dbt()
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_resource = DbtCliResource(project_dir=jaffle_shop_project)


class _DumpHandler(logging.Handler):
    """Writes every non-default LogRecord attribute to a file, if
    DAGSTER_OTEL_EXAMPLE_LOG_CAPTURE is set -- used to check from outside the process
    whether trace_id/span_id (stamped by dagster_otel's TraceContextFilter) actually
    reach this logger during an asset materialization run."""

    def emit(self, record: logging.LogRecord) -> None:
        path = os.environ.get("DAGSTER_OTEL_EXAMPLE_LOG_CAPTURE")
        if not path:
            return
        default_keys = set(logging.makeLogRecord({}).__dict__.keys())
        extra = {k: v for k, v in record.__dict__.items() if k not in default_keys}
        with open(path, "a") as f:
            f.write(f"msg={record.getMessage()!r} extra={extra}\n")


@logger
def capturing_logger(init_context):
    py_logger = logging.getLogger("dagster_otel_jaffle_shop_example")
    py_logger.setLevel(logging.INFO)
    py_logger.addHandler(_DumpHandler())
    return py_logger


defs = Definitions(
    assets=[jaffle_shop_dbt_assets],
    resources={"dbt": dbt_resource},
    loggers={"capturing_logger": capturing_logger},
)
