# k8s_job_executor verification (Issue #3)

Reproduces the [Issue #3](https://github.com/HirofumiTsuda/dagster-otel/issues/3)
verification: trace context propagation across real, separate Kubernetes pods (one
per Dagster step, via `k8s_job_executor`), not just separate OS processes on one host
(`multiprocess`, verified elsewhere -- see `docs/design.md`).

Kept as manifests, not wired into CI -- a full `kind` cluster spin-up is heavy for
every PR and this is a one-off verification, not a regression test this library's
logic needs re-run continuously (unlike `dagster-prometheus-exporter`'s scheduled kind
e2e test, which guards against its own chart/exporter regressing). Rerun by hand if
`dagster`/`dagster-k8s`/`dagster-postgres` get bumped and this needs re-confirming.

`workspace/k8s_e2e_job.py` is the same multi-root + fan-in op graph used for the
Issue #5 verification (`root_a`/`root_b` independent roots, `child_a`/`child_b` each
depending on one, `merge_op` depending on both) -- exercises both multi-root and
fan-in trace attribution in one run.

## Reproduce locally

```sh
kind create cluster --name dagster-otel-e2e

docker build -f dev/kubernetes/Dockerfile -t dagster-otel-k8s-e2e:latest .
kind load docker-image dagster-otel-k8s-e2e:latest --name dagster-otel-e2e

kubectl apply -f dev/kubernetes/manifests.yaml
kubectl wait --for=condition=available deployment/postgres deployment/jaeger --timeout=90s

kubectl apply -f dev/kubernetes/runner-pod.yaml
kubectl wait --for=condition=ready pod/dagster-runner --timeout=60s 2>/dev/null || true
kubectl logs -f dagster-runner   # wait for RUN_SUCCESS

# Confirm each step ran as its own separate Job/pod, not just a separate process:
kubectl get pods   # expect 5 "dagster-step-<hash>" pods, all Completed

# View traces:
kubectl port-forward svc/jaeger 16686:16686
# then open http://localhost:16686, service "k8s_e2e_test"
```

Teardown: `kind delete cluster --name dagster-otel-e2e`.

## What each file is for

- `Dockerfile` -- built from the repo root (not this directory); installs this
  package plus `dagster-postgres`/`dagster-k8s` (versions pinned to match the
  `dagster` version this repo's `pyproject.toml` resolves), and bakes
  `dagster_home/dagster.yaml` into the image so every pod launched from it --
  the runner pod and every step pod `k8s_job_executor` creates from the same
  `job_image` -- has a working `DagsterInstance` config with no extra wiring.
- `dagster_home/dagster.yaml` -- Postgres-backed `run_storage`/`event_log_storage`/
  `schedule_storage` (a step pod's local disk isn't visible to any other pod, so the
  default sqlite-backed storage can't work here at all). Also configures a
  `K8sRunLauncher` that's never actually used to launch anything (the runner pod
  calls `dagster job execute` directly) -- required anyway because
  `dagster_k8s.executor`'s container-context merge logic reads defaults
  (`image_pull_policy` etc.) off `instance.run_launcher` unconditionally, so a
  `K8sRunLauncher` still has to be configured and constructible.
- `workspace/k8s_e2e_job.py` -- the op graph, `@job(executor_def=k8s_job_executor)`
  (required explicitly -- selecting `k8s_job_executor` via run config alone, without
  it being one of the job's declared executors, is rejected).
- `workspace/run_config.yaml` -- `execution.config` fields directly (no
  `k8s_job_executor:` wrapper key -- that wrapper is only for jobs whose
  `executor_def` is a *choice* between multiple executors, e.g. the
  `in_process`/`multiprocess` default). Sets `job_image` explicitly (with no
  `K8sRunLauncher` actually launching the run, there's no run/repository origin
  Dagster can infer an image from). Also mounts a shared PVC at
  `/dagster-home/storage` -- the default `fs_io_manager` writes op outputs to local
  disk, which a downstream step's pod can't see without one; works with
  `ReadWriteOnce` only because the `kind` cluster here is single-node.
- `manifests.yaml` -- Postgres, an in-cluster Jaeger (a pod's `OTEL_EXPORTER_OTLP_ENDPOINT`
  can't reach the host's own Jaeger container across the `kind` network, so this runs
  a second, disposable one inside the cluster), a `ServiceAccount`/`Role`/`RoleBinding`
  granting the permissions `k8s_job_executor` needs (create/watch Kubernetes Jobs,
  read pod status/logs), and the storage `PersistentVolumeClaim`.
- `runner-pod.yaml` -- the pod that actually runs `dagster job execute`; every op's
  compute itself happens in a separate step pod the executor launches, not here.
