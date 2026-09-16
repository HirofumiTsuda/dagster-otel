"""Shared type aliases.

AssetExecutionContext is not a subclass of OpExecutionContext -- it wraps one by
composition (`AssetExecutionContext.__init__(self, op_execution_context:
OpExecutionContext)`, see dagster/_core/execution/context/asset_execution_context.py),
delegating the properties/methods this library uses (`.log`, `.op_handle`, `.instance`,
`.log_event`, `.run_id`) rather than inheriting them. There's no common base class to
type against instead, so every function here that accepts either takes this Union.
"""

from dagster import AssetExecutionContext, OpExecutionContext

ExecutionContext = OpExecutionContext | AssetExecutionContext
