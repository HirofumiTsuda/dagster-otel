"""Regression test for Issue #80 against a real `DagsterInstance` (sqlite-backed),
not `conftest.FakeInstance` -- the bug this covers lives entirely inside Dagster's
own `SqlRunStorage.add_run_tags()` (a read-modify-write race on the `runs.run_body`
blob), which a fake instance backed by a plain dict can't reproduce or catch. See the
verify-tracing skill's rationale for why this class of bug specifically needs a real
instance, not a mock."""

import tempfile
from collections.abc import Iterator

import pytest
from dagster import DagsterInstance, DagsterRun, job, op
from dagster._core.storage.runs.schema import RunsTable, RunTagsTable
from dagster._core.storage.runs.sql_run_storage import SqlRunStorage
from dagster._serdes import serialize_value
from sqlalchemy import select

from dagster_otel._propagation import _read_run_tag


@op
def _an_op() -> int:
    return 1


@job
def _a_job() -> None:
    _an_op()


@pytest.fixture
def real_instance() -> Iterator[DagsterInstance]:
    with tempfile.TemporaryDirectory() as tmpdir:
        with DagsterInstance.ephemeral(tempdir=tmpdir) as instance:
            yield instance


def _create_run(instance: DagsterInstance) -> DagsterRun:
    """Shared setup: create a real run for `_a_job` and confirm the row exists before
    racing writes against it."""
    run = instance.create_run_for_job(_a_job)
    assert instance.get_run_by_id(run.run_id) is not None
    return run


def _race_two_concurrent_publishers(instance: DagsterInstance, run_id: str) -> None:
    """Reproduces the exact race Issue #80 describes: two callers each read the run's
    tags *before either has written*, then each writes back a snapshot containing only
    their own new tag -- exactly what `SqlRunStorage.add_run_tags()` does internally,
    just done here with two independently-read stale snapshots to force the race
    deterministically instead of hoping two real concurrent processes collide.

    Confirmed directly against a real `kind` cluster (Issue #80) that this is exactly
    what happens when two `k8s_job_executor` step pods call `add_run_tags` around the
    same moment -- this reproduces the same failure locally, deterministically, in a
    fraction of a second, instead of needing a real multi-pod cluster for every test
    run.
    """
    storage = instance.run_storage
    assert isinstance(storage, SqlRunStorage)  # sanity: only makes sense against SqlRunStorage

    stale_run_b = storage._get_run_by_id(run_id)
    assert stale_run_b is not None

    # Caller A: a normal add_run_tags() call, reading current (empty) state fresh.
    instance.add_run_tags(run_id, {"dagster_otel/trace_context/root_a": "a-context"})

    # Caller B: replays add_run_tags()'s own read-modify-write logic, but starting
    # from `stale_run_b` -- read *before* caller A's write landed, same as two
    # genuinely concurrent pods would.
    all_tags = {**(stale_run_b.tags or {}), "dagster_otel/trace_context/root_b": "b-context"}
    with storage.connect() as conn:
        conn.execute(
            RunsTable.update()
            .where(RunsTable.c.run_id == run_id)
            .values(run_body=serialize_value(stale_run_b.with_tags(all_tags)))
        )
        conn.execute(
            RunTagsTable.insert(),
            [{"run_id": run_id, "key": "dagster_otel/trace_context/root_b", "value": "b-context"}],
        )


def test_race_actually_drops_a_tag_from_run_body(real_instance: DagsterInstance) -> None:
    """Confirms the race this module works around is real, not a misreading of
    Issue #80 -- if this ever stops failing, either this Dagster version fixed the
    underlying `add_run_tags` race (great -- but `_read_run_tag`'s SqlRunStorage
    branch would then be redundant, not wrong) or this test's reproduction no longer
    matches how `add_run_tags` behaves internally, either way worth knowing."""
    run = _create_run(real_instance)
    _race_two_concurrent_publishers(real_instance, run.run_id)

    fresh_run = real_instance.get_run_by_id(run.run_id)
    assert fresh_run is not None
    assert "dagster_otel/trace_context/root_a" not in fresh_run.tags
    assert "dagster_otel/trace_context/root_b" in fresh_run.tags


def test_read_run_tag_finds_the_tag_the_race_drops_from_run_body(
    real_instance: DagsterInstance,
) -> None:
    """The actual Issue #80 fix: _read_run_tag reads the `run_tags` index table
    directly for a SqlRunStorage-backed instance, so it finds root_a's context even
    though the race above dropped it from `run.tags`."""
    run = _create_run(real_instance)
    _race_two_concurrent_publishers(real_instance, run.run_id)

    fresh_run = real_instance.get_run_by_id(run.run_id)
    assert fresh_run is not None

    assert (
        _read_run_tag(real_instance, fresh_run, "dagster_otel/trace_context/root_a") == "a-context"
    )
    assert (
        _read_run_tag(real_instance, fresh_run, "dagster_otel/trace_context/root_b") == "b-context"
    )
    assert _read_run_tag(real_instance, fresh_run, "dagster_otel/trace_context/no_such_tag") is None


def test_run_tags_table_row_confirms_only_run_body_is_racy(
    real_instance: DagsterInstance,
) -> None:
    """Direct confirmation of Issue #80's Postgres-level evidence, reproduced here
    against sqlite instead: the `run_tags` index table itself is never missing
    anything (both INSERTs succeeded) -- only the separately-maintained `run_body`
    blob snapshot loses a write, because only that write path has no locking."""
    run = _create_run(real_instance)
    _race_two_concurrent_publishers(real_instance, run.run_id)

    storage = real_instance.run_storage
    assert isinstance(storage, SqlRunStorage)
    rows = storage.fetchall(select(RunTagsTable.c.key).where(RunTagsTable.c.run_id == run.run_id))
    keys = {r["key"] for r in rows}
    assert "dagster_otel/trace_context/root_a" in keys
    assert "dagster_otel/trace_context/root_b" in keys
