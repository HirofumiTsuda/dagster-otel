"""Tests for dagster_otel._logging: the trace_id/span_id logging correlation filter."""

import logging

from opentelemetry import trace

from dagster_otel._logging import TraceContextFilter


def _make_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )


def test_filter_stamps_trace_id_and_span_id_inside_a_span() -> None:
    tracer = trace.get_tracer("test")
    filt = TraceContextFilter()

    with tracer.start_as_current_span("s") as span:
        record = _make_record()
        result = filt.filter(record)

        expected_trace_id = format(span.get_span_context().trace_id, "032x")
        expected_span_id = format(span.get_span_context().span_id, "016x")

    assert result is True  # never drops the record
    # trace_id/span_id are stamped dynamically, not real LogRecord attributes --
    # getattr rather than record.trace_id so this doesn't need a type: ignore.
    assert getattr(record, "trace_id", None) == expected_trace_id
    assert getattr(record, "span_id", None) == expected_span_id


def test_filter_does_not_stamp_outside_any_span() -> None:
    filt = TraceContextFilter()
    record = _make_record()

    filt.filter(record)

    assert not hasattr(record, "trace_id")
    assert not hasattr(record, "span_id")


def test_filter_scoping_is_span_local_not_global() -> None:
    """A record logged after the span has already ended must not be stamped --
    scoping is what makes the filter correlate the right log lines to the right
    span, not just tag everything from some point onward."""
    tracer = trace.get_tracer("test")
    filt = TraceContextFilter()

    with tracer.start_as_current_span("s"):
        inside_record = _make_record()
        filt.filter(inside_record)

    outside_record = _make_record()
    filt.filter(outside_record)

    assert hasattr(inside_record, "trace_id")
    assert not hasattr(outside_record, "trace_id")
