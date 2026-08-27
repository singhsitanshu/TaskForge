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
Prometheus histogram quantiles use per-series start/end bucket deltas and are
allowed only when the raw value lies in the corresponding observed bucket.
Neither source is silently substituted for the other.

## Publication policy

PUBLIC configurations need at least three valid trials. Headline scaling uses
three independently reset blocks with randomized worker order, reports medians,
CV, and block drift, and fails above the committed thresholds. A result is not
publicly usable unless clean-source provenance, image identity, correctness,
raw data, latency, Prometheus reconciliation, repetition, reproducibility, and
regression gates all pass.

## Workloads

- `test.noop`: constant predefined success.
- `test.sleep`: existing bounded 1–60,000 ms sleep; profiles use 50 ms and can
  specify 10 or 100 ms without code changes.
- `test.cpu`: deterministic repeated SHA-256 with a bounded iteration count.
- `test.fail_n_then_succeed`: existing attempt-aware retry workload.

The current worker executes one handler at a time per process. Worker count is
therefore also the maximum in-flight execution count.
