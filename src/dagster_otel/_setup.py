"""OpenTelemetry SDK bootstrap.

Kept deliberately small: honor the standard OTEL_* env vars
(https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/,
https://opentelemetry.io/docs/languages/sdk-configuration/general/) rather than
inventing our own config surface -- including service name. Verified:
`Resource.create()` (unlike the plain `Resource(attributes=...)` constructor) reads
`OTEL_SERVICE_NAME` automatically, and adds standard attributes for free
(`telemetry.sdk.*`, `service.instance.id`) that a bare `Resource(...)` doesn't.

`configure()` deliberately takes no arguments -- its only caller is `traced()`'s
internal auto-configure-on-first-use, which never had a Python-level value to pass in
the first place, and this module's whole point is deferring to env vars rather than
building a parallel config surface. Set OTEL_SERVICE_NAME / OTEL_EXPORTER_OTLP_ENDPOINT
(or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) instead of passing values here.
"""
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

#: Verified (2026-09-15): with an unreachable OTLP endpoint and the exporter's own
#: default timeout (10s, OTEL_EXPORTER_OTLP_TRACES_TIMEOUT's spec default), every
#: single @traced() step blocked for ~7s -- SimpleSpanProcessor exports synchronously
#: inline with the step, retrying (up to 6x, exponential backoff) before giving up.
#: Not a hang, but "forgot to point this at a collector" turning into "every step is
#: now several seconds slower" is a bad default for something meant to be safe to just
#: add. 2s (confirmed: caps the same failure at well under 1s) is used *only* when the
#: caller hasn't set a timeout via the standard env vars -- an explicit
#: OTEL_EXPORTER_OTLP_TRACES_TIMEOUT/OTEL_EXPORTER_OTLP_TIMEOUT is always honored
#: instead, same as this module's general policy of not overriding standard env vars.
_DEFAULT_OTLP_TIMEOUT_SECONDS = 2.0


def configure() -> None:
    """Set up a TracerProvider and OTLP exporter for the current process, from env
    vars only (OTEL_SERVICE_NAME, OTEL_EXPORTER_OTLP_ENDPOINT / _TRACES_ENDPOINT, etc.
    -- see module docstring).

    Called automatically by `traced()` the first time it runs in a given process, so
    **you don't need to call this yourself** for ordinary use. Exported for the one
    case that isn't ordinary: calling it eagerly (e.g. from a @resource) if you want
    configuration to happen before any step runs rather than lazily on first use.

    Safe to call more than once, from as many places as you like:
    `trace.set_tracer_provider()` is already idempotent by itself (a second call logs
    "Overriding of current TracerProvider is not allowed" and is a no-op, verified
    against opentelemetry-sdk 1.44.0) -- no need for this module to track "already
    configured" state of its own.

    Uses SimpleSpanProcessor rather than BatchSpanProcessor: Dagster's multiprocess
    and k8s executors mean a step's process usually exits shortly after its work is
    done, and BatchSpanProcessor can drop the final batch on exit. If span volume
    becomes a bottleneck for high-throughput jobs, revisit this.
    """
    has_explicit_timeout = bool(
        os.environ.get("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT")
        or os.environ.get("OTEL_EXPORTER_OTLP_TIMEOUT")
    )
    timeout = None if has_explicit_timeout else _DEFAULT_OTLP_TIMEOUT_SECONDS

    provider = TracerProvider(resource=Resource.create())
    provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(timeout=timeout)))
    trace.set_tracer_provider(provider)
