# TaskForge benchmarks

This directory is the TF-012 performance harness. It drives the real HTTP API,
PostgreSQL repository, workers, schedulers, and Prometheus deployment. It does
not mock lifecycle transitions and it does not add arbitrary task execution.

## Safety and prerequisites

The orchestrator accepts only Compose project names matching
`taskforge-tf012-[a-z0-9-]+`. Reset removes volumes only for that isolated
project. Never point a benchmark at a database containing useful data.

Required tools are Docker with Compose v2 and Python 3.11 or newer. Go is not
required on the host: the load generator is built in Docker.

## Commands

```text
make benchmark-trust-smoke
make benchmark-release
make benchmark-tf012e2 E1_RESULTS=benchmarks/results/<trusted-e1-run>/results.json
make benchmark-tf012e4
make benchmark-tf012e5
make benchmark-tf012e6
```

Both trusted targets refuse a dirty working tree before building images or
creating results. `benchmark-release` (also `benchmark-all`) uses
`config/release.json`, runs regression commands first, and requires every trust
gate to pass. Direct legacy scenario targets are explicitly UNPUBLISHABLE
development diagnostics; they cannot generate a trusted verdict. `benchmark-dev`
uses the provenance-aware harness, permits a dirty tree, and records
`publication_status: UNPUBLISHABLE` in the run and every trial. Trusted
direct invocation supports `--keep` and `--output-dir`:

```text
python3 -m benchmarks.trusted --profile release --project taskforge-tf012-release
python3 -m benchmarks.trust benchmarks/results/<run-id>
python3 benchmarks/plot.py benchmarks/results/<timestamp>/results.json
python3 benchmarks/report.py benchmarks/results/<timestamp>/results.json
```

Warm-up is archived and excluded. Each trial has an isolated Prometheus reset
and fresh boundary scrapes. Every attempt archives its immutable queue-entry,
start, finish, retry, and recovery evidence before the database is destroyed.
The trust evaluator re-derives all raw measurements from CSV and validates
per-trial SHA-256 manifests. API submission rate is reported separately from
processing rate.

## Result schema

Each timestamped directory contains:

- `results.json`: exact source/image/harness provenance, environment,
  configurations, trial indexes, aggregate dispersion, regression records, and
  trust-gate results.
- `results.csv`: flattened measurements for external analysis.
- `trials/*`: immutable task/attempt CSV, boundary Prometheus snapshots,
  reconciliation, correctness, metadata, resource samples, and a manifest.
- `plots/`: reproducible SVG plots and a plot manifest.
- `report.md`: generated 23-section TF-012B trust report.
- `manifest.json`: top-level SHA-256 inventory of the complete result bundle.

Raw quantiles use archived timestamps and documented type-7 interpolation.
Prometheus histogram quantiles use per-series start/end bucket deltas. A
percentile is bucket-reconciled only when the raw timestamps and histogram
measure the same semantic interval. Queue, retry-lateness, and recovery-lag
meet that requirement. Claim duration has no raw timestamp pair, and execution
does not: the immutable database attempt interval spans claim and completion
work, while `taskforge_task_execution_duration_seconds` times only the handler.
Those operational histograms therefore require valid structure, stable targets,
and an exact observation count but are not compared to a different raw
percentile. Neither source is silently substituted for the other.

The TF-012E2 command is fixed to `test.sleep` with `duration_ms=50`, workers
1/4/8/16, three reset blocks, and 1,000 tasks per configuration by default. It
requires an external trusted TF-012E1 `results.json` for the mechanically derived
no-op speedup comparison. `python3 -m benchmarks.e2 --dry-run` prints the scoped
contract without starting Docker or executing tasks.

The TF-012E3 command is fixed to deterministic `test.cpu` with `200000` SHA-256
iterations, workers 1/4/8/16, three reset blocks, and 1,000 tasks per
configuration by default. It requires external trusted TF-012E1 and TF-012E2
`results.json` files and compares scaling shape without modifying either input.
`python3 -m benchmarks.e3 --dry-run` prints the CPU-only contract without
starting Docker or executing tasks. A future trusted run uses:

```text
make benchmark-tf012e3 E1_RESULTS=benchmarks/results/<trusted-e1-run>/results.json E2_RESULTS=benchmarks/results/<trusted-e2-run>/results.json
```

TF-012E4 is an API-submission-only path. It issues 2,000 normal keyless
`POST /tasks` requests at concurrency 1/10/25/50/100 across three independently
reset, randomized blocks. The measured window uses one API replica and zero task
processing workers, so successful rows intentionally remain `QUEUED` with zero
attempts. The scoped output is `tf-012e4-api-submission.md` plus exactly three
plots for submission throughput, request p95, and request p99. Submission
throughput is not worker processing throughput. Inspect the plan without Docker:

```text
make benchmark-tf012e4 DRY_RUN=1
python3 -m benchmarks.e4 --dry-run
```

TF-012E5 is a retry-storm-only path. Each of three independent reset trials
submits 1,000 `test.fail_n_then_succeed` tasks with `failures=1`, using 10 workers
and 3 schedulers. The durable contract requires exactly one FAILED attempt 1 and
one SUCCEEDED attempt 2 per task, with exact retry schedule/promotion counters
and immutable retry-lateness evidence. It uses the committed benchmark retry
configuration (100 ms base/max delay, zero jitter, 100 ms promotion interval).
The scoped output is `tf-012e5-retry-storm.md` plus exactly four retry plots.
Inspect the plan without Docker:

```text
make benchmark-tf012e5 DRY_RUN=1
python3 -m benchmarks.e5 --dry-run
```

TF-012E6 is a fixed worker-crash recovery path. Each of three independently
reset trials submits 1,000 bounded 500 ms `test.sleep` tasks with 20 workers and
3 schedulers, then deterministically hard-kills exactly 10 workers after every
selected worker owns active work. Each trial archives `failure_boundary.json` as
the authority for the affected task set, expected ABANDONED attempts, killed
worker identities, kill time, lease expirations, and the exact failure-aware
Prometheus churn allowlist. Worker-local counters lost with killed processes are
not treated as reconstructable; durable PostgreSQL history and scheduler-owned
recovery metrics are authoritative. The scoped output is
`tf-012e6-recovery-storm.md` plus exactly four recovery plots. Inspect the fixed
plan without Docker or failure injection:

```text
make benchmark-tf012e6 DRY_RUN=1
python3 -m benchmarks.e6 --dry-run
```

## Publication policy

PUBLIC configurations need at least three valid trials. Headline scaling uses
three independently reset blocks with recorded randomized worker order. The
trust gate does not invent statistical thresholds: it verifies the recorded
trial count, independent reset/warm-up boundaries, raw evidence, and regression
commands. A result is not publicly usable unless clean-source provenance, image
identity, correctness, raw data, latency, Prometheus reconciliation, repetition,
reproducibility, and regression gates all pass.

## Workloads

- `test.noop`: constant predefined success.
- `test.sleep`: existing bounded 1–60,000 ms sleep; profiles use 50 ms and can
  specify 10 or 100 ms without code changes.
- `test.cpu`: deterministic repeated SHA-256 with a bounded iteration count.
- `test.fail_n_then_succeed`: existing attempt-aware retry workload.

The current worker executes one handler at a time per process. Worker count is
therefore also the maximum in-flight execution count.
