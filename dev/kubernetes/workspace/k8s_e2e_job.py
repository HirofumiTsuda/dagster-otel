"""Issue #3 verification: multi-root + fan-in, each step in its own k8s pod
(k8s_job_executor), instead of multiprocess's separate-processes-same-host."""

from dagster import Definitions, OpExecutionContext, job, op
from dagster_k8s import k8s_job_executor

from dagster_otel import traced


@op
@traced()
def root_a(context: OpExecutionContext) -> str:
    return "a"


@op
@traced()
def root_b(context: OpExecutionContext) -> str:
    return "b"


@op
@traced()
def child_a(context: OpExecutionContext, upstream: str) -> str:
    return "a_done"


@op
@traced()
def child_b(context: OpExecutionContext, upstream: str) -> str:
    return "b_done"


@op
@traced()
def merge_op(context: OpExecutionContext, a: str, b: str) -> str:
    context.log.info(f"merging {a} and {b}")
    return f"{a}+{b}"


@job(executor_def=k8s_job_executor)
def k8s_e2e_job():
    a = root_a()
    b = root_b()
    child_a(a)
    child_b(b)
    merge_op(a, b)


defs = Definitions(jobs=[k8s_e2e_job])
