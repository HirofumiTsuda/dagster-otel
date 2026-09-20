"""Tests for dagster_otel._setup: the OTel SDK bootstrap.

configure()'s own provider-installation behavior isn't covered here --
trace.set_tracer_provider() is real global process state, already claimed by the
session-scoped _otel_test_provider fixture (see conftest.py), so a unit test can't
observe what configure() would have installed without real network calls or
subprocess isolation. _export_configured() is tested directly as a pure function
instead; configure()'s actual no-real-exporter behavior (Issue #16) was verified
against a real Dagster run + Jaeger, not here -- see docs/design.md.
"""

import hashlib

import pytest

from dagster_otel._setup import (
    _build_otlp_exporter,
    _current_run_id,
    _DeterministicRunIdGenerator,
    _export_configured,
    _GrpcOTLPSpanExporter,
    _HttpOTLPSpanExporter,
    _trace_id_from_run_id,
)


@pytest.fixture(autouse=True)
def _clear_otel_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """None of these tests should be affected by whatever OTEL_* env vars happen to
    be set in the environment actually running the test suite. Uses monkeypatch (not
    bare os.environ mutation) throughout this file so every change -- here and in each
    test below -- is automatically undone at test teardown, not just the ones this
    fixture itself makes."""
    for var in (
        "OTEL_SDK_DISABLED",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    ):
        monkeypatch.delenv(var, raising=False)


def test_export_not_configured_by_default() -> None:
    assert _export_configured() is False


def test_export_configured_via_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    assert _export_configured() is True


def test_export_configured_via_traces_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317")
    assert _export_configured() is True


def test_sdk_disabled_wins_even_with_endpoint_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    assert _export_configured() is False


def test_sdk_disabled_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "True")
    assert _export_configured() is False


def test_otlp_protocol_defaults_to_grpc() -> None:
    assert isinstance(_build_otlp_exporter(timeout=None), _GrpcOTLPSpanExporter)


def test_otlp_protocol_grpc_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    assert isinstance(_build_otlp_exporter(timeout=None), _GrpcOTLPSpanExporter)


def test_otlp_protocol_http_protobuf(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert isinstance(_build_otlp_exporter(timeout=None), _HttpOTLPSpanExporter)


def test_otlp_traces_protocol_wins_over_general_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
    assert isinstance(_build_otlp_exporter(timeout=None), _HttpOTLPSpanExporter)


def test_otlp_protocol_unsupported_value_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")
    with pytest.raises(ValueError, match="http/json"):
        _build_otlp_exporter(timeout=None)


@pytest.fixture(autouse=True)
def _clear_current_run_id():
    """Same reasoning as _clear_otel_env above, for _current_run_id -- these tests
    shouldn't see whatever a previous test (in this file or another) left set."""
    token = _current_run_id.set(None)
    yield
    _current_run_id.reset(token)


def test_trace_id_from_run_id_matches_hash_formula() -> None:
    """The documented derivation, spelled out independently of the implementation --
    sha256(run_id)[:16 bytes], big-endian -- so anyone reading a trace_id in a real
    backend can independently confirm which run it came from."""
    expected = int.from_bytes(hashlib.sha256(b"run-A").digest()[:16], "big")
    assert _trace_id_from_run_id("run-A") == expected


def test_trace_id_from_run_id_is_deterministic() -> None:
    assert _trace_id_from_run_id("run-A") == _trace_id_from_run_id("run-A")


def test_trace_id_from_run_id_differs_per_run() -> None:
    assert _trace_id_from_run_id("run-A") != _trace_id_from_run_id("run-B")


def test_deterministic_id_generator_same_run_id_same_trace_id() -> None:
    """The actual Issue #63 property: two spans seeded from the same run_id (however
    many process/step boundaries apart) land in the same trace, without either one
    needing to know about the other."""
    generator = _DeterministicRunIdGenerator()
    _current_run_id.set("run-A")
    first = generator.generate_trace_id()
    _current_run_id.set("run-A")
    second = generator.generate_trace_id()
    assert first == second


def test_deterministic_id_generator_matches_run_id_hash() -> None:
    """Not just "the same twice" -- the actual documented derivation
    (sha256(run_id)[:16 bytes], big-endian), so anyone reading a trace_id in a real
    backend can independently confirm which run it came from."""
    generator = _DeterministicRunIdGenerator()
    _current_run_id.set("run-A")
    trace_id = generator.generate_trace_id()
    expected = int.from_bytes(hashlib.sha256(b"run-A").digest()[:16], "big")
    assert trace_id == expected


def test_deterministic_id_generator_different_run_id_different_trace_id() -> None:
    generator = _DeterministicRunIdGenerator()
    _current_run_id.set("run-A")
    trace_id_a = generator.generate_trace_id()
    _current_run_id.set("run-B")
    trace_id_b = generator.generate_trace_id()
    assert trace_id_a != trace_id_b


def test_deterministic_id_generator_falls_back_to_random_without_run_id() -> None:
    """No _current_run_id set at all (a genuinely independent trace, not seeded by
    _seed_run_root_context) -- ordinary random trace_ids, not some fixed fallback
    value that would incorrectly collide unrelated traces together."""
    generator = _DeterministicRunIdGenerator()
    assert _current_run_id.get() is None
    first = generator.generate_trace_id()
    second = generator.generate_trace_id()
    assert first != second


def test_deterministic_id_generator_span_id_is_random() -> None:
    """Only trace_id is deterministic -- span_id generation is untouched, delegated
    straight to the SDK's own RandomIdGenerator (two calls shouldn't collide)."""
    generator = _DeterministicRunIdGenerator()
    assert generator.generate_span_id() != generator.generate_span_id()
