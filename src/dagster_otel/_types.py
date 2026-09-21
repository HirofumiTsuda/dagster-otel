"""Shared type aliases.

AssetExecutionContext and AssetCheckExecutionContext are not subclasses of
OpExecutionContext -- both wrap one by composition (`AssetExecutionContext.
__init__(self, op_execution_context: OpExecutionContext)`, `AssetCheckExecutionContext.
__init__(self, op_execution_context: OpExecutionContext)`, see dagster/_core/execution/
context/{asset_execution_context,asset_check_execution_context}.py), delegating the
properties/methods this library uses (`.log`, `.get_step_execution_context()`,
`.instance`, `.run`, `.job_def`) rather than inheriting them. There's no common base
class to type against instead, so every function here that accepts any of the three
takes this Union.

Not identical shapes, though -- confirmed against real Dagster (Issue #72):
AssetCheckExecutionContext has no `.job_name` (only `.job_def`, hence this library
reading `.job_def.name` uniformly across all three rather than `.job_name`, which the
other two also have but this one doesn't) and no `.selected_asset_keys` (only
`.selected_asset_check_keys`, a different attribute entirely -- see _tracing.py's
`isinstance` branch for why that can't be unified the same way).
"""

from dagster import AssetCheckExecutionContext, AssetExecutionContext, OpExecutionContext

ExecutionContext = OpExecutionContext | AssetExecutionContext | AssetCheckExecutionContext

#: What `traced()` accepts -- any of the three. `traced_dbt()` (dbt.py) deliberately
#: does NOT use this one -- it's narrower (see AssetOrOpExecutionContext below),
#: since `context.asset_key_for_output(...)` (needed to resolve a dbt-yielded
#: Output's real AssetKey) isn't on AssetCheckExecutionContext at all, and
#: @dbt_assets/op-based dbt.cli() usage never actually produces one anyway --
#: dbt_assets is multi_asset-shaped, not asset_check-shaped.
AssetOrOpExecutionContext = OpExecutionContext | AssetExecutionContext
