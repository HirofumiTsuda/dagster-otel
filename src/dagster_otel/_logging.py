"""Correlate Dagster log lines with the active OTel span, via a public logging.Filter.

`context.log` (DagsterLogManager) is a subclass of the standard `logging.Logger`
(see dagster/_core/log_manager.py), so the standard library's own documented extension
point for attaching contextual data to log records -- Logger.addFilter() -- works on it
unmodified. See:
https://docs.python.org/3/howto/logging-cookbook.html#using-filters-to-impart-contextual-information

Verified in this session: trace_id/span_id attached this way survive Dagster's
DagsterLogHandler (which forwards a record's non-standard attributes as `extra` to any
user-defined @logger, see DagsterLogHandler._extract_extra), so a @logger that reads
record.trace_id/record.span_id gets real log/trace correlation. No Dagster internals
touched -- this is Logger.addFilter(), a fully public API.
"""
import logging

from opentelemetry import trace


class TraceContextFilter(logging.Filter):
    """Stamps record.trace_id / record.span_id from the currently active OTel span.

    Only stamps records logged while a valid span is active -- a record logged outside
    any span (or before the filter is attached) is left alone, not given zeroed/fake
    ids.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            record.trace_id = format(span_context.trace_id, "032x")
            record.span_id = format(span_context.span_id, "016x")
        return True
