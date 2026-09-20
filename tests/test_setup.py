"""Tests for dagster_otel._setup: the OTel SDK bootstrap.

configure()'s own provider-installation behavior isn't covered here --
trace.set_tracer_provider() is real global process state, already claimed by the
session-scoped _otel_test_provider fixture (see conftest.py), so a unit test can't
observe what configure() would have installed without real network calls or
subprocess isolation. _export_configured() is tested directly as a pure function
instead; configure()'s actual no-real-exporter behavior (Issue #16) was verified
against a real Dagster run + Jaeger, not here -- see docs/design.md.
"""

import pytest

from dagster_otel._setup import (
    _export_configured,
    _GrpcOTLPSpanExporter,
    _HttpOTLPSpanExporter,
    _resolve_otlp_exporter_class,
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
    assert _resolve_otlp_exporter_class() is _GrpcOTLPSpanExporter


def test_otlp_protocol_grpc_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    assert _resolve_otlp_exporter_class() is _GrpcOTLPSpanExporter


def test_otlp_protocol_http_protobuf(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert _resolve_otlp_exporter_class() is _HttpOTLPSpanExporter


def test_otlp_traces_protocol_wins_over_general_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
    assert _resolve_otlp_exporter_class() is _HttpOTLPSpanExporter


def test_otlp_protocol_unsupported_value_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json")
    with pytest.raises(ValueError, match="http/json"):
        _resolve_otlp_exporter_class()
