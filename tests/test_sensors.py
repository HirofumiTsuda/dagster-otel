"""Tests for dagster_otel._sensors: traced_sensor()/traced_schedule() (Issue #38).

Uses Dagster's own `build_sensor_context`/`build_schedule_context` test helpers
(real `SensorEvaluationContext`/`ScheduleEvaluationContext` instances, not fakes --
see conftest.py's own module docstring for why real Dagster objects are preferred
wherever practical) since neither needs a real DagsterInstance/run the way
`_tracing.py`'s propagation lookups do -- a tick, unlike a step, has no run_id or
`context.instance`-backed state this module ever touches at all (see `_sensors.py`'s
module docstring).
"""

import json

from dagster import (
    RunRequest,
    SensorResult,
    SkipReason,
    build_schedule_context,
    build_sensor_context,
)

from dagster_otel import EXTERNAL_TRACE_CONTEXT_TAG_KEY, traced_schedule, traced_sensor
from dagster_otel._propagation import carrier_to_span_context


def _carrier_trace_id(carrier_json: str) -> int:
    return carrier_to_span_context(json.loads(carrier_json)).trace_id


# --- traced_sensor() ---------------------------------------------------------------


def test_traced_sensor_preserves_skip_reason(spans) -> None:
    @traced_sensor()
    def my_sensor(context) -> SkipReason:
        return SkipReason("nothing to do")

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert isinstance(result, SkipReason)
    assert result.skip_message == "nothing to do"


def test_traced_sensor_bare_preserves_return_value(spans) -> None:
    @traced_sensor
    def my_sensor(context) -> SkipReason:
        return SkipReason("nothing to do")

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert isinstance(result, SkipReason)


def test_traced_sensor_default_span_name_and_sensor_name_attribute(spans) -> None:
    @traced_sensor()
    def my_named_sensor(context) -> None:
        return None

    my_named_sensor(build_sensor_context(sensor_name="my_named_sensor"))

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert "my_named_sensor" in finished
    assert finished["my_named_sensor"].attributes["dagster.sensor_name"] == "my_named_sensor"


def test_traced_sensor_tags_bare_run_request(spans) -> None:
    @traced_sensor()
    def my_sensor(context) -> RunRequest:
        return RunRequest(run_key="k1")

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert isinstance(result, RunRequest)
    carrier_json = result.tags[EXTERNAL_TRACE_CONTEXT_TAG_KEY]

    span = spans.get_finished_spans()[0]
    assert _carrier_trace_id(carrier_json) == span.context.trace_id


def test_traced_sensor_preserves_existing_run_request_tags(spans) -> None:
    @traced_sensor()
    def my_sensor(context) -> RunRequest:
        return RunRequest(run_key="k1", tags={"my_tag": "my_value"})

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert result.tags["my_tag"] == "my_value"
    assert EXTERNAL_TRACE_CONTEXT_TAG_KEY in result.tags


def test_traced_sensor_tags_sequence_of_run_requests(spans) -> None:
    @traced_sensor()
    def my_sensor(context) -> list[RunRequest]:
        return [RunRequest(run_key="k1"), RunRequest(run_key="k2")]

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert len(result) == 2
    assert all(EXTERNAL_TRACE_CONTEXT_TAG_KEY in rr.tags for rr in result)


def test_traced_sensor_tags_sensor_result_run_requests(spans) -> None:
    @traced_sensor()
    def my_sensor(context) -> SensorResult:
        return SensorResult(run_requests=[RunRequest(run_key="k1")])

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert isinstance(result, SensorResult)
    assert result.run_requests is not None
    assert EXTERNAL_TRACE_CONTEXT_TAG_KEY in result.run_requests[0].tags


def test_traced_sensor_sensor_result_with_no_run_requests_untouched(spans) -> None:
    """SensorResult(run_requests=None) normalizes to `[]` in real Dagster (confirmed
    live, 2026-09-21) -- there's no actual `None` case to hit `_tag_tick_result`'s own
    `is None` branch in practice, but nothing here should crash on an empty list of
    run_requests either."""

    @traced_sensor()
    def my_sensor(context) -> SensorResult:
        return SensorResult(run_requests=None, skip_reason="nothing")

    result = my_sensor(build_sensor_context(sensor_name="my_sensor"))
    assert result.run_requests == []


def test_traced_sensor_generator_tags_each_yielded_run_request(spans) -> None:
    """A multi-run sensor -- `yield RunRequest(...)` more than once, the generator
    case (_sensors.py's own module docstring)."""

    @traced_sensor()
    def my_multi_run_sensor(context):
        yield RunRequest(run_key="k1")
        yield RunRequest(run_key="k2")

    results = list(my_multi_run_sensor(build_sensor_context(sensor_name="my_multi_run_sensor")))
    assert len(results) == 2
    assert all(EXTERNAL_TRACE_CONTEXT_TAG_KEY in rr.tags for rr in results)

    span = spans.get_finished_spans()[0]
    for rr in results:
        assert _carrier_trace_id(rr.tags[EXTERNAL_TRACE_CONTEXT_TAG_KEY]) == span.context.trace_id


# --- traced_schedule() --------------------------------------------------------------


def test_traced_schedule_preserves_plain_run_config_dict(spans) -> None:
    """A schedule returning a plain dict (run config directly, not a RunRequest) --
    nothing to tag, but must still pass through untouched (_sensors.py's module
    docstring: not every return shape carries a RunRequest)."""

    @traced_schedule()
    def my_schedule(context) -> dict:
        return {"ops": {}}

    result = my_schedule(build_schedule_context())
    assert result == {"ops": {}}


def test_traced_schedule_default_span_name_and_schedule_name_attribute(spans) -> None:
    """ScheduleEvaluationContext has no public name accessor -- dagster.schedule_name
    falls back to the function's own name (_sensors.py's module docstring)."""

    @traced_schedule()
    def my_named_schedule(context) -> None:
        return None

    my_named_schedule(build_schedule_context())

    finished = {s.name: s for s in spans.get_finished_spans()}
    assert "my_named_schedule" in finished
    assert finished["my_named_schedule"].attributes["dagster.schedule_name"] == "my_named_schedule"


def test_traced_schedule_sets_scheduled_execution_time_attribute(spans) -> None:
    import datetime

    scheduled_time = datetime.datetime(2026, 9, 21, 12, 0, 0, tzinfo=datetime.timezone.utc)

    @traced_schedule()
    def my_schedule(context) -> None:
        return None

    my_schedule(build_schedule_context(scheduled_execution_time=scheduled_time))

    span = spans.get_finished_spans()[0]
    assert span.attributes["dagster.scheduled_execution_time"] == scheduled_time.isoformat()


def test_traced_schedule_tags_run_request(spans) -> None:
    @traced_schedule()
    def my_schedule(context) -> RunRequest:
        return RunRequest(run_key="k1")

    result = my_schedule(build_schedule_context())
    assert isinstance(result, RunRequest)
    carrier_json = result.tags[EXTERNAL_TRACE_CONTEXT_TAG_KEY]

    span = spans.get_finished_spans()[0]
    assert _carrier_trace_id(carrier_json) == span.context.trace_id


def test_traced_schedule_bare_preserves_return_value(spans) -> None:
    @traced_schedule
    def my_schedule(context) -> RunRequest:
        return RunRequest(run_key="k1")

    result = my_schedule(build_schedule_context())
    assert isinstance(result, RunRequest)
