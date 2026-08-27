#!/usr/bin/env python3
"""Render an auditable TF-012 Markdown report from results.json."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
from typing import Any


def number(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def median(items: list[dict[str, Any]], path: tuple[str, ...]) -> float | None:
    values = []
    for item in items:
        current: Any = item
        for part in path:
            current = current.get(part) if isinstance(current, dict) else None
        if isinstance(current, int | float):
            values.append(float(current))
    return statistics.median(values) if values else None


def selected(results: list[dict[str, Any]], scenario: str) -> list[dict[str, Any]]:
    return [item for item in results if item["scenario"] == scenario]


def prom_value(item: dict[str, Any], key: str) -> float | None:
    rows = item.get("prometheus_after", {}).get(key, [])
    if isinstance(rows, list) and rows:
        try:
            return float(rows[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None
    return None


def scaling_table(results: list[dict[str, Any]]) -> str:
    runs = selected(results, "noop_scaling")
    groups: dict[int, list[dict[str, Any]]] = {}
    for item in runs:
        groups.setdefault(int(item["workers"]), []).append(item)
    baseline = median(groups.get(1, []), ("raw", "processing_throughput_per_second"))
    lines = [
        "| Workers | Throughput | Speedup | Efficiency | Queue p95 | Claim p95 |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for workers, items in sorted(groups.items()):
        throughput = median(items, ("raw", "processing_throughput_per_second"))
        speedup = throughput / baseline if throughput is not None and baseline else None
        efficiency = speedup / workers if speedup is not None else None
        queue_p95 = median(items, ("raw", "queue_p95_seconds"))
        claim_values = [
            value for item in items if (value := prom_value(item, "claim_p95")) is not None
        ]
        claim_p95 = statistics.median(claim_values) if claim_values else None
        lines.append(
            f"| {workers} | {number(throughput)} tasks/s | {number(speedup)}x | "
            f"{number(efficiency)} | {number(queue_p95, 4)} s | {number(claim_p95, 4)} s |"
        )
    if not groups:
        lines.append("| n/a | Not run | n/a | n/a | n/a | n/a |")
    return "\n".join(lines)


def latency_table(results: list[dict[str, Any]]) -> str:
    lines = [
        "| Workload | Workers | Queue p50/p95/p99 (s) | Execution p50/p95/p99 (s) | Total p95 (s) |",
        "|---|---:|---|---|---:|",
    ]
    for scenario in ("noop_scaling", "io50_scaling", "cpu_scaling"):
        groups: dict[int, list[dict[str, Any]]] = {}
        for item in selected(results, scenario):
            groups.setdefault(int(item["workers"]), []).append(item)
        for workers, items in sorted(groups.items()):

            def raw(key: str, current_items: list[dict[str, Any]] = items) -> float | None:
                return median(current_items, ("raw", key))

            lines.append(
                f"| {scenario} | {workers} | {number(raw('queue_p50_seconds'), 4)} / "
                f"{number(raw('queue_p95_seconds'), 4)} / {number(raw('queue_p99_seconds'), 4)} | "
                f"{number(raw('execution_p50_seconds'), 4)} / {number(raw('execution_p95_seconds'), 4)} / "
                f"{number(raw('execution_p99_seconds'), 4)} | {number(raw('total_p95_seconds'), 4)} |"
            )
    if len(lines) == 2:
        lines.append("| Not run | n/a | n/a | n/a | n/a |")
    return "\n".join(lines)


def compact_table(items: list[dict[str, Any]], x_name: str, x_getter: Any) -> str:
    lines = [
        f"| {x_name} | Requests/tasks | Throughput | p95 API latency | Correct |",
        "|---:|---:|---:|---:|---|",
    ]
    for item in items:
        throughput = item.get("raw", {}).get("processing_throughput_per_second")
        if throughput is None:
            throughput = item.get("submission", {}).get("requests_per_second")
        lines.append(
            f"| {x_getter(item)} | {item.get('count', 'n/a')} | {number(throughput)} /s | "
            f"{number(item.get('submission', {}).get('latency_ms', {}).get('p95'))} ms | "
            f"{'yes' if item.get('correctness', {}).get('passed') else 'no'} |"
        )
    if not items:
        lines.append("| n/a | n/a | Not run | n/a | n/a |")
    return "\n".join(lines)


def trusted_scaling_table(document: dict[str, Any], scenario: str) -> str:
    lines = [
        "| Workers | Median Processing Throughput | Block Drift | CV | Queue p95 | Claim p95 |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for item in document.get("summaries", []):
        if item.get("scenario") != scenario:
            continue
        workers = str(item.get("variant", "")).removeprefix("w")
        lines.append(
            f"| {workers} | {number(item.get('throughput_median'))} tasks/s | "
            f"{number(item.get('between_block_drift'), 3)} | "
            f"{number(item.get('throughput_cv'), 3)} | "
            f"{number(item.get('queue_p95_median'), 4)} s | "
            f"{number(item.get('claim_p95_prometheus_median'), 4)} s |"
        )
    if len(lines) == 2:
        lines.append("| n/a | Not run | n/a | n/a | n/a | n/a |")
    return "\n".join(lines)


def trusted_summary_table(document: dict[str, Any], scenario: str, heading: str) -> str:
    api = scenario == "api_throughput"
    lines = [
        f"| {heading} | Median throughput | p50/p95/p99 API latency (ms) | CV | Trials |",
        "|---:|---:|---:|---:|---:|",
    ]
    for item in document.get("summaries", []):
        if item.get("scenario") == scenario:
            lines.append(
                f"| {item.get('variant')} | {number(item.get('throughput_median'))} /s | "
                f"{number(item.get('submission_latency_p50_ms_median')) if api else 'n/a'} / "
                f"{number(item.get('submission_latency_p95_ms_median')) if api else 'n/a'} / "
                f"{number(item.get('submission_latency_p99_ms_median')) if api else 'n/a'} | "
                f"{number(item.get('throughput_cv'), 3)} | {item.get('trials')} |"
            )
    if len(lines) == 2:
        lines.append("| n/a | Not run | n/a | n/a | n/a |")
    return "\n".join(lines)


def reconciliation_table(results: list[dict[str, Any]]) -> str:
    lines = [
        "| Metric | Raw/PostgreSQL | Prometheus | Difference | Result |",
        "|---|---:|---:|---:|---|",
    ]
    for item in results:
        label = f"{item['scenario']}/{item['variant']}/t{item.get('trial')}"
        reconciliation = item.get("prometheus_reconciliation", {})
        for name, value in reconciliation.get("counters", {}).items():
            lines.append(
                f"| {label} {name} | {number(value.get('raw'), 0)} | "
                f"{number(value.get('prometheus'), 0)} | {number(value.get('difference'), 0)} | "
                f"{value.get('status', 'FAIL')} |"
            )
        for name, value in reconciliation.get("histograms", {}).items():
            lines.append(
                f"| {label} {name} p95 | {number(value.get('raw'), 4)} | "
                f"{number(value.get('prometheus'), 4)} | "
                f"{number(value.get('absolute_difference'), 4)} | {value.get('status', 'FAIL')} |"
            )
    if len(lines) == 2:
        lines.append("| n/a | n/a | n/a | n/a | FAIL |")
    return "\n".join(lines)


def sustainable_range(document: dict[str, Any]) -> str:
    measured = []
    for item in document.get("summaries", []):
        if item.get("scenario") != "arrival_saturation":
            continue
        try:
            offered = float(str(item.get("variant", "")).removeprefix("r"))
            throughput = float(item["throughput_median"])
        except (KeyError, TypeError, ValueError):
            continue
        measured.append((offered, throughput))
    sustainable = sorted(rate for rate, throughput in measured if throughput >= rate * 0.95)
    failing = sorted(rate for rate, throughput in measured if throughput < rate * 0.95)
    if not sustainable:
        return "No tested offered rate met the 95% completion criterion."
    upper = max(sustainable)
    next_failure = next((rate for rate in failing if rate > upper), None)
    if next_failure is None:
        return (
            f"All tested rates through {number(upper, 0)} tasks/s met the 95% criterion; "
            "the upper saturation boundary was not observed."
        )
    return (
        f"The observed sustainable boundary lies between {number(upper, 0)} and "
        f"{number(next_failure, 0)} offered tasks/s on this host."
    )


def render_trusted(
    document: dict[str, Any], results_path: pathlib.Path, artifact_prefix: str
) -> str:
    results = document.get("results", [])
    public = [item for item in results if item.get("classification") == "PUBLIC"]
    trust = document.get("trust") or {}
    overall_pass = trust.get("overall", {}).get("result") == "PASS"
    verdict = (
        "PASS — BENCHMARKS TRUSTWORTHY"
        if overall_pass
        else "FAIL — BENCHMARKS STILL NOT TRUSTWORTHY"
    )
    recommendation = (
        "BENCHMARK BASELINE COMPLETE — PROCEED"
        if overall_pass
        else "MORE BENCHMARK REMEDIATION REQUIRED"
    )
    source = document.get("source", {})
    images = document.get("images", {})
    image_lines = (
        "\n".join(
            f"- {name}: `{value.get('image_id', 'n/a')}`; digests "
            f"`{', '.join(value.get('repo_digests', [])) or 'none (local image ID archived)'}`"
            for name, value in sorted(images.items())
        )
        or "- Not captured."
    )
    harness_files = document.get("harness", {}).get("files", [])
    harness_lines = (
        "\n".join(f"- `{item['path']}`: `{item['sha256']}`" for item in harness_files)
        or "- Not captured."
    )
    gate_lines = ["| Gate | Result | Evidence |", "|---|---|---|"]
    for key in (
        "source_provenance",
        "correctness",
        "raw_data",
        "latency",
        "prometheus",
        "repetition",
        "reproducibility",
        "regression",
    ):
        gate = trust.get(key, {"result": "FAIL"})
        detail = gate.get("errors") or gate.get("failures") or "gate conditions satisfied"
        if isinstance(detail, list):
            detail = "; ".join(str(value) for value in detail) or "gate conditions satisfied"
        gate_lines.append(f"| {key.replace('_', ' ')} | {gate.get('result')} | {detail} |")
    regression_lines = []
    for item in document.get("regression", {}).get("commands", []):
        regression_lines.append(
            f"- `{' '.join(item.get('command', []))}` → exit `{item.get('exit_code')}`"
        )
    api_table = trusted_summary_table(document, "api_throughput", "Concurrency")
    saturation_table = trusted_summary_table(document, "arrival_saturation", "Offered rate")
    retry_table = trusted_summary_table(document, "retry_storm", "Variant")
    recovery_table = trusted_summary_table(document, "recovery_storm", "Variant")
    valid_public = [item for item in public if item.get("valid")]
    invalid = [
        f"{item['scenario']}/{item['variant']}/t{item.get('trial')}"
        for item in results
        if not item.get("valid") or item.get("classification") != "PUBLIC"
    ]
    drift = [
        item
        for item in document.get("summaries", [])
        if item.get("scenario") in {"noop_scaling", "io50_scaling", "cpu_scaling"}
    ]
    drift_text = (
        "; ".join(
            f"{item['scenario']}/{item['variant']}: drift {number(item.get('between_block_drift'), 3)}, "
            f"CV {number(item.get('throughput_cv'), 3)}"
            for item in drift
        )
        or "No independent blocks completed."
    )
    return f"""# 1. TF-012B Verdict

{verdict}

# 2. Provenance

- Commit: `{source.get("git_commit_sha", "n/a")}`
- Clean tree: `{source.get("clean", False)}`
- Tree hash: `{source.get("git_tree_hash", "n/a")}`
- Harness: `{document.get("harness", {}).get("version", "n/a")}`

{image_lines}

Harness identity:

{harness_lines}

# 3. Raw Evidence Model

Every trial archives immutable `tasks.csv` and `attempts.csv`, raw-derived `summary.json`, correctness, resource samples, boundary Prometheus snapshots, reconciliation, metadata, and a SHA-256 manifest. Raw artifacts are authoritative and [{results_path.name}]({artifact_prefix}{results_path.name}) is derived indexing data.

# 4. Queue Wait Fix

`task_attempts.queue_entered_at` records the task's queue entry at atomic claim time; `scheduled_at_snapshot`, retry scheduling, and recovery evidence remain attached to that attempt. Queue wait is `attempt_started_at - queue_entered_at`. Missing evidence or any negative duration fails the latency gate. Retry and recovery repository tests exercise historical independence.

# 5. Prometheus Trial Isolation

Each trial clears task state, recreates measured worker/scheduler processes, resets the isolated Prometheus volume, waits for the exact healthy target set and a start scrape, then waits for a scrape strictly after trial end. Start/end per-series deltas reject resets, missing series, and unexpected target churn. Warm-up is explicitly excluded: `{document.get("warmup", {}).get("excluded")}`.

# 6. Prometheus Reconciliation

{reconciliation_table(public)}

# 7. Repetition Policy

PUBLIC scenarios require at least `{document.get("profile", {}).get("minimum_public_trials", 3)}` valid trials per configuration. EXPLORATORY rows are excluded from headline tables and cannot satisfy trust gates.

# 8. Reproducibility Design

Headline scaling uses `{document.get("profile", {}).get("required_blocks", 3)}` independently reset blocks. Worker-count order is deterministically randomized per workload and block from archived seed `{document.get("profile", {}).get("random_seed", "n/a")}`. Medians, CV, and between-block drift are reported; the trust gate verifies independent blocks without inventing statistical thresholds.

# 9. Noop Scaling Re-Test

{trusted_scaling_table(document, "noop_scaling")}

# 10. I/O Scaling Re-Test

{trusted_scaling_table(document, "io50_scaling")}

# 11. CPU Scaling Re-Test

{trusted_scaling_table(document, "cpu_scaling")}

# 12. API Re-Test

Submission throughput uses the load-generator request interval and is never substituted for processing throughput.

{api_table}

# 13. Saturation Re-Test

Sustainable capacity should be stated only as the range of offered rates whose repeated processing medians keep pace without queue growth; the data do not justify interpolation to a single exact threshold.

{sustainable_range(document)}

{saturation_table}

# 14. Retry Storm Re-Test

{retry_table}

Expected attempt totals, unique attempt numbers, retry promotions, immutable retry timing, terminal success, and zero stranded leases are checked per trial and reconciled above.

# 15. Recovery Storm Re-Test

{recovery_table}

Killed-worker attempts must become evidenced `ABANDONED` attempts, scheduler recovery counters/histograms must reconcile, every task must finish, and no lease may remain stranded.

# 16. Artifact Reproducibility

Top-level manifest: [manifest.json]({artifact_prefix}manifest.json). Per-trial manifest hashes are embedded in results. Reports and plots read only saved machine-readable artifacts; manual measurement values are not accepted. Byte-identical regeneration check: `{document.get("artifact_reproducibility", {}).get("passed", False)}`; files compared: `{", ".join(document.get("artifact_reproducibility", {}).get("artifacts_compared", [])) or "none"}`.

# 17. Trust Gates

{chr(10).join(gate_lines)}

# 18. Run-Level Drift

{drift_text}

CV and block drift remain descriptive measurements; publication trust comes from the recorded independent reset blocks and the other evidence gates.

# 19. Resume-Safe Results

`{len(valid_public)}` public trial results are independently correct, artifact-backed, and Prometheus-reconciled. Numeric conclusions are resume-safe only when the overall verdict is PASS; otherwise this section intentionally makes no public performance claim.

# 20. Results That Must NOT Be Used Publicly

{", ".join(invalid) if invalid else ("None." if overall_pass else "All numeric results until every trust gate passes.")}

# 21. Regression Suite

{chr(10).join(regression_lines) or "- Not completed."}

# 22. Changes Beyond TF-012B

Benchmark methodology and the minimal immutable queue-timing evidence fix only. Polling, pools, PostgreSQL tuning, claim batching, worker concurrency, and throughput-oriented indexes were not changed by TF-012B.

# 23. Recommendation

{recommendation}
"""


def render(document: dict[str, Any], results_path: pathlib.Path, artifact_prefix: str = "") -> str:
    if document.get("schema_version") == 2:
        return render_trusted(document, results_path, artifact_prefix)
    results = document["results"]
    environment = document.get("environment") or {}
    correctness_failures = [
        f"{item['scenario']}/{item['variant']}"
        for item in results
        if not item.get("correctness", {}).get("passed", False)
    ]
    noop = selected(results, "noop_scaling")
    bottleneck = "Insufficient scaling data."
    if noop:
        best = max(
            noop, key=lambda item: item.get("raw", {}).get("processing_throughput_per_second") or 0
        )
        bottleneck = (
            f"The best measured noop result was {number(best['raw'].get('processing_throughput_per_second'))} "
            f"tasks/s at {best['workers']} workers. Scaling efficiency and PostgreSQL CPU plots determine "
            "whether claim/complete transactions or host capacity dominate; this report does not infer causality "
            "from throughput alone."
        )
    stability = selected(results, "stability")
    full_baseline = (
        document.get("profile", {}).get("name") == "baseline" and document.get("suite") == "all"
    )
    verdict = (
        "PASS — BENCHMARK BASELINE COMPLETE"
        if (full_baseline and document.get("all_correctness_passed") and not document.get("errors"))
        else "FAIL — TF-012 REQUIRES FIXES"
    )
    plot_links = "\n".join(
        f"- [Plot {index}: {name}]({artifact_prefix}plots/{name})"
        for index, name in enumerate(
            (
                "01-processing-throughput.svg",
                "02-scaling-efficiency.svg",
                "03-queue-p95.svg",
                "04-claim-p95.svg",
                "05-api-throughput.svg",
                "06-arrival-saturation.svg",
                "07-scheduler-scaling.svg",
                "08-recovery.svg",
                "09-retry-lateness.svg",
                "10-postgres-cpu.svg",
            ),
            start=1,
        )
    )
    return f"""# 1. TF-012 Verdict

{verdict}

# 2. Benchmark Environment

- Captured: `{environment.get("captured_at", "n/a")}`
- Git SHA: `{environment.get("git_sha", "n/a")}`
- Host: `{environment.get("platform", "n/a")}`
- CPU: `{environment.get("host_cpu", "n/a")}` ({environment.get("host_logical_cpus", "n/a")} logical)
- Memory bytes: `{environment.get("host_memory_bytes", "n/a")}`
- Docker: `{environment.get("docker_version", "n/a")}`
- PostgreSQL: `{environment.get("postgresql", "n/a")}`
- Profile/suite: `{document.get("profile", {}).get("name")}` / `{document.get("suite")}`

# 3. Benchmark Harness

The Go load generator uses bounded concurrency, connection reuse, optional paced arrivals, and none/unique/same idempotency modes. The Python orchestrator owns an isolated `taskforge-tf012-*` Compose project, refuses broader reset targets, separates warm-up, queries PostgreSQL lifecycle timestamps, captures Prometheus and `docker stats`, and validates invariants after every run. Raw evidence: [{results_path.name}]({artifact_prefix}{results_path.name}) and [CSV]({artifact_prefix}results.csv).

# 4. Workloads

Predefined handlers only: constant `test.noop`, configurable `test.sleep` (10/50/100 ms supported), deterministic bounded SHA-256 `test.cpu`, and the existing attempt-aware retry handlers. No arbitrary execution was added.

# 5. Scaling Results

{scaling_table(results)}

# 6. Latency Results

{latency_table(results)}

# 7. API Submission Results

{compact_table(selected(results, "api_throughput"), "Concurrency", lambda item: item["submission"]["configuration"]["concurrency"])}

Same-key and unique-key contention are retained as separate `idempotency_contention` rows in the raw result.

# 8. Saturation Point

{compact_table(selected(results, "arrival_saturation"), "Offered tasks/s", lambda item: item.get("offered_rate_per_second"))}

Queue growth is reported per run under `backpressure`; no unbounded client-side queue is hidden by the harness.

# 9. Retry Storm

{compact_table(selected(results, "retry_storm"), "Variant", lambda item: item["variant"])}

# 10. Recovery Storm

{compact_table(selected(results, "recovery_storm"), "Killed workers (%)", lambda item: item.get("kill_percentage"))}

# 11. Worker Failure Scaling

Recovery cases kill 10/25/50 percent of worker processes in the baseline profile. Because each current worker executes one task at a time, the number of simultaneously stranded attempts equals killed workers, not submitted task count.

# 12. Scheduler Scaling

{compact_table(selected(results, "scheduler_retry_scaling"), "Schedulers", lambda item: item.get("schedulers"))}

# 13. Database/Resource Behavior

Each measured processing run contains time-series `docker stats` samples and aggregated CPU/memory maxima by API, PostgreSQL, worker, scheduler, and Prometheus service. PostgreSQL version and memory-related settings are in the environment manifest.

# 14. Large Queue / History Test

{compact_table(selected(results, "large_queue"), "Queued tasks", lambda item: item.get("queued_before_drain"))}

The retry storm supplies two-attempt history; database size after large-queue drain is recorded in bytes.

# 15. Stability Test

{compact_table(stability, "Configured seconds", lambda item: item.get("configured_duration_seconds"))}

# 16. Correctness Audit

Overall: `{"PASS" if document.get("all_correctness_passed") else "FAIL"}`. Failures: `{", ".join(correctness_failures) if correctness_failures else "none"}`. Checks include task/attempt counts, terminal convergence, duplicate `(task_id, attempt_number)`, ownership/status consistency, stale active state, abandoned recovery counts, and idempotency cardinality.

# 17. Prometheus vs Raw Data

Every processing run contains before/after Prometheus query responses and exact PostgreSQL percentile calculations. Prometheus histogram values are scrape-interval/bucket approximations; PostgreSQL values are the baseline source for lifecycle quantiles. Differences are expected near bucket boundaries and for runs shorter than the five-second scrape interval.

# 18. Plots Generated

{plot_links}

# 19. Baseline Bottleneck

{bottleneck}

# 20. Candidate Resume Facts

- Result schema: `{document.get("schema_version")}`; ticket `{document.get("tf_ticket")}`.
- Warm-up excluded: `{document.get("warmup", {}).get("excluded")}`.
- Completed at: `{document.get("completed_at")}`.
- All trial-level raw inputs, timing, counters, and samples are in JSON; flattened lifecycle results are in CSV.

# 21. Regression Suite

Regression commands and results must be appended after running `make test`, `make test-observability`, worker/scheduler `go test -race`, load-generator tests, formatting, and linting. Performance suites are opt-in and are not part of normal CI.

# 22. Known Benchmark Limitations

- This is a single Docker Desktop host, not a multi-node capacity result.
- A baseline verdict requires the `baseline` profile with the `all` suite; smoke/CI profiles deliberately report incomplete.
- The current worker has one in-flight handler per process, limiting recovery-storm simultaneous failures.
- Stability duration is profile-defined: `{document.get("profile", {}).get("stability_seconds", "n/a")}` seconds.
- Prometheus uses a five-second scrape interval, so raw SQL is authoritative for short trials.

# 23. Changes Beyond TF-012

None. Production defaults and task lifecycle semantics are unchanged; only an environment override for the existing poll interval and two predefined test handlers were added.

# 24. Recommended Next Step

Run the same committed baseline profile on a pinned Linux release host and store that result beside this Docker Desktop baseline before using throughput as a capacity target.
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    arguments = parser.parse_args()
    document = json.loads(arguments.results.read_text())
    output = arguments.output or arguments.results.parent / "report.md"
    relative_directory = os.path.relpath(arguments.results.parent, output.parent)
    artifact_prefix = "" if relative_directory == "." else relative_directory.rstrip("/") + "/"
    output.write_text(render(document, arguments.results, artifact_prefix))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
