---
name: verify-dagster-version-compat
description: Check whether dagster-otel's propagation/logging mechanism still works against a Dagster version other than the one last verified. Builds a fresh venv pinned to a candidate version and runs the same checks as verify-tracing against it. Use when investigating version compatibility, not for routine changes to this library's own code -- see verify-tracing for that.
---

# Verify Dagster version compatibility

This library depends on **internal** Dagster Python APIs, not the GraphQL API --
`context.op_handle.path`, `context.instance.add_run_tags`/`get_run_by_id` (the
run-tag transport, see `_propagation.py`'s module docstring), `context
.get_step_execution_context().step.step_inputs[*].dependency_keys` (how a step
resolves its real upstream keys -- the single riskiest one: not `@public`, flagged
directly in that module's own docstring as an accepted-risk private-API dependence),
and the fact that `DagsterLogManager` subclasses `logging.Logger`
(`dagster/_core/log_manager.py`). None of this is a documented, versioned contract.
See docs/design.md's "Design decision: no monkeypatching" section for why this is a
real, accepted risk rather than an oversight -- but it means every Dagster release is
a candidate for silent breakage, more so than a library built only on GraphQL (compare
[dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter),
which only touches the comparatively more stable GraphQL surface, and still has to
track this). (Skill last synced to the current implementation 2026-09-16 -- if
`_propagation.py`'s own "what we depend on" docstring changes, re-check this file
against it.)

Do this in a scratch venv, never the project's own dev environment, and never commit a
version pin change from this skill without a deliberate decision to move it.

## 1. Build a scratch venv pinned to the candidate version

```sh
python3 -m venv /tmp/dagster-otel-compat-test
source /tmp/dagster-otel-compat-test/bin/activate
pip install -e '.[dagster,test]' "dagster==<candidate>"
python3 -c "import dagster; print(dagster.__version__)"  # confirm the pin actually took
```

## 2. Confirm the internal surfaces this library touches still exist and have the expected shape

Before running a full job, check the specific attributes/behavior directly --
faster to iterate on, and pinpoints exactly what changed if something did:

```sh
python3 -c "
import dagster
from dagster._core.log_manager import DagsterLogManager
import logging
print('DagsterLogManager subclasses logging.Logger:', issubclass(DagsterLogManager, logging.Logger))
"
```

Then, inside a real op/asset body (a minimal throwaway job/graph with at least two
dependent steps is fine), check:

- `context.op_handle.path` is still a sequence of strings.
- `context.instance.add_run_tags(context.run_id, {...})` still writes, and
  `context.instance.get_run_by_id(context.run_id).tags` still reads it back within
  the same run.
- `context.get_step_execution_context().step.step_inputs[*].dependency_keys` still
  resolves to the real upstream step keys (matching the string
  `".".join(upstream.op_handle.path)` produces for that step) -- this is the
  riskiest surface (not `@public`), and the one most likely to silently change shape
  across a Dagster release rather than raise.

## 3. Run the full verify-tracing checks against this venv

Same steps as the `verify-tracing` skill (Jaeger, multiprocess job/asset run, span
nesting check, log-correlation check) -- just with this scratch venv active instead of
the project's normal one. Don't skip straight to "it imported fine, so it's
compatible" -- `dagster-otel` failing here looks like a silent no-op (a step just
never gets a nested span, or never gets trace_id on its logs), not an exception, so
absence of an error proves nothing.

## 4. Record the result

Note the candidate Dagster version, whether each internal surface from step 2 still
matched, and whether the full trace + log correlation checks passed. If something
broke, the specific attribute/behavior that changed *is* the finding -- record it
verbatim (e.g. "context.op_handle.path is now X instead of a list of str") rather than
summarizing it away, since that's exactly what a future fix needs to target.

## 5. Clean up

```sh
deactivate
rm -rf /tmp/dagster-otel-compat-test
```
