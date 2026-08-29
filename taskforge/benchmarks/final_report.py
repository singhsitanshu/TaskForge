#!/usr/bin/env python3
"""Generate the authoritative TF-012 report from trusted E1-E6 artifacts."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from benchmarks.e1_artifacts import latency_rows, markdown_table, summary_rows, valid_results
from benchmarks.e3_artifacts import e3_latency_rows
from benchmarks.e4_artifacts import api_rows
from benchmarks.e5_artifacts import retry_aggregate
from benchmarks.e6_artifacts import recovery_aggregate, recovery_rows
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.trust import evaluate_run_directory

REPORT_PATH = BENCHMARKS / "reports" / "tf-012-final-benchmark-report.md"
SUMMARY_PATH = BENCHMARKS / "reports" / "tf-012-resume-summary.md"

DEFAULT_INPUTS = {
    "E1": BENCHMARKS
    / "results/20260827T222041075944Z_426bc56e28c9_tf-012e1-noop_d61998c47358/results.json",
    "E2": BENCHMARKS
    / "results/20260827T230700739885Z_180e3868286f_tf-012e2-io50_8a5ccfad766b/results.json",
    "E3": BENCHMARKS
    / "results/20260828T194454461406Z_053d2edf177d_tf-012e3-cpu_34a34bf2ab90/results.json",
    "E4": BENCHMARKS
    / "results/20260829T020943647937Z_0a18f7201af2_tf-012e4-api_900f16b326ab/results.json",
    "E5": BENCHMARKS
    / "results/20260829T021706562017Z_0a18f7201af2_tf-012e5-retry_4f90403256db/results.json",
    "E6": BENCHMARKS
    / "results/20260829T045324141937Z_7c5364935f6d_tf-012e6-recovery_d3f527061c06/results.json",
}
EXPECTED_TICKETS = {label: f"TF-012{label}" for label in DEFAULT_INPUTS}
SCENARIOS = {
    "E1": "noop_scaling",
    "E2": "io50_scaling",
    "E3": "cpu_scaling",
    "E4": "api_submission",
    "E5": "retry_storm",
    "E6": "recovery_storm",
}
EXPERIMENT_REPORTS = {
    "E1": "tf-012e1-noop-scaling.md",
    "E2": "tf-012e2-io50-scaling.md",
    "E3": "tf-012e3-cpu-scaling.md",
    "E4": "tf-012e4-api-submission.md",
    "E5": "tf-012e5-retry-storm.md",
    "E6": "tf-012e6-recovery-storm.md",
}


def number(value: Any, digits: int = 3) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}f}"


def integer(value: Any) -> str:
    return f"{int(value):,}"


def load_trusted_inputs(
    input_paths: dict[str, pathlib.Path] | None = None,
) -> dict[str, dict[str, Any]]:
    """Load only exact ticket inputs that independently pass standalone trust."""
    selected = input_paths or DEFAULT_INPUTS
    if set(selected) != set(DEFAULT_INPUTS):
        raise BenchmarkError("TF-012F requires exactly E1 through E6 inputs")
    return {label: load_trusted_input(label, selected[label]) for label in DEFAULT_INPUTS}


def load_trusted_input(label: str, results_path: pathlib.Path) -> dict[str, Any]:
    """Validate one exact accepted-ticket input without running benchmark work."""
    resolved = pathlib.Path(results_path).resolve()
    if label not in EXPECTED_TICKETS:
        raise BenchmarkError(f"unknown TF-012 experiment label: {label}")
    if resolved.name != "results.json" or not resolved.is_file():
        raise BenchmarkError(f"{label} input must be an existing results.json")
    trust = evaluate_run_directory(resolved.parent)
    if trust.get("overall", {}).get("result") != "PASS":
        raise BenchmarkError(f"{label} standalone trust is not PASS")
    document = json.loads(resolved.read_text())
    if document.get("tf_ticket") != EXPECTED_TICKETS[label]:
        raise BenchmarkError(f"{label} ticket identity does not match")
    if document.get("trust", {}).get("overall", {}).get("result") != "PASS":
        raise BenchmarkError(f"{label} recorded overall trust is not PASS")
    document["_results_path"] = resolved
    document["_standalone_trust"] = trust
    return document


def scaling_maps(
    documents: dict[str, dict[str, Any]],
) -> dict[str, dict[int, dict[str, Any]]]:
    return {
        label: {
            int(row["workers"]): row for row in summary_rows(documents[label], SCENARIOS[label])
        }
        for label in ("E1", "E2", "E3")
    }


def correctness_rows(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for label in DEFAULT_INPUTS:
        results = valid_results(documents[label], SCENARIOS[label])
        tasks = sum(int(item["correctness"]["actual_tasks"]) for item in results)
        attempts = sum(int(item["correctness"]["actual_attempts"]) for item in results)
        duplicates = sum(int(item["correctness"]["duplicate_attempts"]) for item in results)
        stranded = sum(int(item["correctness"]["stranded_leases"]) for item in results)
        if label == "E4":
            requests = sum(int(item["correctness"]["actual_http_requests"]) for item in results)
            successes = sum(int(item["correctness"]["successful_responses"]) for item in results)
            queued = sum(int(item["correctness"]["queued_tasks"]) for item in results)
            lost_failed = (requests - successes) + (tasks - queued)
            scale = f"{integer(requests)} requests / {integer(tasks)} queued tasks"
        else:
            succeeded = sum(int(item["correctness"]["succeeded_tasks"]) for item in results)
            lost_failed = tasks - succeeded
            scale = f"{integer(tasks)} tasks"
        rows.append(
            {
                "experiment": label,
                "scale": scale,
                "tasks": tasks,
                "attempts": attempts,
                "duplicates": duplicates,
                "lost_failed": lost_failed,
                "stranded": stranded,
                "trust": documents[label]["_standalone_trust"]["overall"]["result"],
            }
        )
    return rows


def counter_totals(document: dict[str, Any]) -> dict[str, dict[str, float]]:
    totals: dict[str, dict[str, float]] = {}
    for item in valid_results(document, SCENARIOS[document["tf_ticket"][-2:]]):
        for metric, evidence in item["prometheus_reconciliation"]["counters"].items():
            total = totals.setdefault(metric, {"raw": 0.0, "prometheus": 0.0, "difference": 0.0})
            for field in total:
                total[field] += float(evidence[field])
    return totals


def resume_findings(documents: dict[str, dict[str, Any]]) -> list[str]:
    scaling = scaling_maps(documents)
    e2_1 = scaling["E2"][1]
    e2_16 = scaling["E2"][16]
    e3_8 = scaling["E3"][8]
    e3_16 = scaling["E3"][16]
    e4 = api_rows(documents["E4"])
    best_api = max(e4, key=lambda row: row["submission_throughput_median"])
    e5 = retry_aggregate(documents["E5"])
    e6 = recovery_aggregate(documents["E6"])
    correctness = {row["experiment"]: row for row in correctness_rows(documents)}
    e6_results = valid_results(documents["E6"], SCENARIOS["E6"])
    e6_affected = sum(int(item["correctness"]["affected_tasks"]) for item in e6_results)
    e6_duplicates = sum(
        int(item["correctness"]["duplicate_recovery_abandonments"]) for item in e6_results
    )
    host_cpus = int(documents["E3"]["environment"]["host_logical_cpus"])
    return [
        (
            f"The synthetic 50 ms wait workload scaled {number(e2_16['speedup'], 3)}x "
            f"from 1 to 16 workers ({number(e2_1['processing_throughput_median'], 3)} "
            f"to {number(e2_16['processing_throughput_median'], 3)} tasks/s) at "
            f"{number(e2_16['parallel_efficiency'] * 100, 1)}% parallel efficiency."
        ),
        (
            f"API submission throughput reached its highest tested median at concurrency "
            f"{best_api['concurrency']}: {number(best_api['submission_throughput_median'], 2)} "
            f"requests/s, with {integer(correctness['E4']['tasks'])} persisted task rows and "
            "zero request errors."
        ),
        (
            f"{integer(correctness['E5']['tasks'])} retrying tasks produced exactly "
            f"{integer(correctness['E5']['attempts'])} ordered attempts with zero duplicates; "
            f"median-trial retry-lateness p95 was "
            f"{number(e5['retry_lateness_p95'] * 1000, 3)} ms."
        ),
        (
            f"TaskForge recovered {integer(e6_affected)} crash-abandoned attempts across "
            f"{len(e6_results)} "
            f"trials with median-trial p95 recovery lag "
            f"{number(e6['recovery_p95_median'] * 1000, 3)} ms after lease expiration and "
            f"{e6_duplicates} duplicate recovery effects."
        ),
        (
            f"The deterministic CPU workload reached {number(e3_8['speedup'], 3)}x speedup at "
            f"8 workers, then flattened to {number(e3_16['speedup'], 3)}x at 16 workers on "
            f"the {host_cpus}-logical-CPU host."
        ),
    ]


def resume_bullets(documents: dict[str, dict[str, Any]]) -> dict[str, str]:
    scaling = scaling_maps(documents)
    e2_1 = scaling["E2"][1]
    e2_16 = scaling["E2"][16]
    e3_8 = scaling["E3"][8]
    correctness = {row["experiment"]: row for row in correctness_rows(documents)}
    e6_results = valid_results(documents["E6"], SCENARIOS["E6"])
    affected = sum(int(item["correctness"]["affected_tasks"]) for item in e6_results)
    schedulers = int(documents["E6"]["profile"]["recovery_schedulers"])
    return {
        "primary": (
            "Built a provenance-checked TaskForge benchmark suite and demonstrated "
            f"{number(e2_16['speedup'], 2)}x processing-throughput scaling from 1 to 16 "
            f"workers ({number(e2_1['processing_throughput_median'], 2)} to "
            f"{number(e2_16['processing_throughput_median'], 2)} tasks/s, "
            f"{number(e2_16['parallel_efficiency'] * 100, 1)}% efficiency) on a synthetic "
            f"50 ms wait workload, completing {integer(correctness['E2']['tasks'])} tasks "
            "without duplicates or stranded leases."
        ),
        "backend": (
            "Validated TaskForge's durable retry and crash-recovery semantics under "
            f"{schedulers}-scheduler concurrency: {integer(correctness['E5']['tasks'])} retrying tasks "
            f"produced exactly {integer(correctness['E5']['attempts'])} ordered attempts, and "
            f"{integer(affected)} crash-abandoned attempts were requeued and completed with "
            "zero duplicate recovery effects."
        ),
        "performance": (
            "Designed trusted multi-workload benchmarks showing "
            f"{number(e2_16['speedup'], 2)}x wait-bound scaling to 16 workers and "
            f"{number(e3_8['speedup'], 2)}x CPU-bound scaling to 8 workers, isolating "
            "coordination overhead, concurrency, and host CPU saturation."
        ),
    }


def interview_summary(documents: dict[str, dict[str, Any]]) -> str:
    scaling = scaling_maps(documents)
    blocks = int(documents["E2"]["profile"]["required_blocks"])
    host_cpus = int(documents["E3"]["environment"]["host_logical_cpus"])
    return (
        "I benchmarked TaskForge with synthetic no-op, 50 ms wait, and deterministic CPU "
        f"workloads so each run exposed a different bottleneck. Each configuration used {blocks} "
        "independently reset blocks, immutable PostgreSQL task and attempt timestamps, and "
        "trial-only Prometheus deltas reconciled against exact durable counts. I also validated "
        "fail-once retries and hard-kill lease recovery for duplicates, missing work, and "
        "stranded ownership. The wait workload scaled "
        f"{number(scaling['E2'][16]['speedup'], 2)}x to 16 workers, while CPU scaling flattened "
        f"after 8 workers as the {host_cpus}-logical-CPU host became limiting. These are controlled local "
        "Docker results, not production capacity claims."
    )


def render_report(documents: dict[str, dict[str, Any]]) -> str:
    scaling = scaling_maps(documents)
    e1_latency = {int(row["workers"]): row for row in latency_rows(documents["E1"], "noop_scaling")}
    e3_latency = {int(row["workers"]): row for row in e3_latency_rows(documents["E3"])}
    e4 = api_rows(documents["E4"])
    e5 = retry_aggregate(documents["E5"])
    e6 = recovery_aggregate(documents["E6"])
    e6_trials = recovery_rows(documents["E6"])
    correctness = correctness_rows(documents)
    correctness_by_label = {row["experiment"]: row for row in correctness}
    findings = resume_findings(documents)
    bullets = resume_bullets(documents)
    best_api = max(e4, key=lambda row: row["submission_throughput_median"])
    e5_results = valid_results(documents["E5"], SCENARIOS["E5"])
    e5_failed_first = sum(int(item["correctness"]["attempt1_failed"]) for item in e5_results)
    e5_succeeded_second = sum(int(item["correctness"]["attempt2_succeeded"]) for item in e5_results)
    e5_abandoned = sum(int(item["correctness"]["abandoned_attempts"]) for item in e5_results)
    e6_results = valid_results(documents["E6"], SCENARIOS["E6"])
    e6_affected = sum(int(item["correctness"]["affected_tasks"]) for item in e6_results)
    e6_abandoned = sum(int(item["correctness"]["abandoned_attempts"]) for item in e6_results)
    e6_replacements = sum(
        int(item["correctness"]["recovered_replacement_successes"]) for item in e6_results
    )
    e6_duplicates = sum(
        int(item["correctness"]["duplicate_recovery_abandonments"]) for item in e6_results
    )
    lease_duration = documents["E6"]["timing_configuration"]["WORKER_TASK_LEASE_DURATION"]
    lease_display = (
        f"{lease_duration.removesuffix('s')} seconds"
        if str(lease_duration).endswith("s")
        else str(lease_duration)
    )
    recovery_schedulers = int(documents["E6"]["profile"]["recovery_schedulers"])
    host_cpus = int(documents["E3"]["environment"]["host_logical_cpus"])

    provenance_table = markdown_table(
        ["Experiment", "Run ID", "Commit", "Tree", "Trust", "Inputs"],
        [
            [
                label,
                str(document["run_id"]),
                str(document["source"]["git_commit_sha"])[:12],
                str(document["source"]["git_tree_hash"])[:12],
                str(document["_standalone_trust"]["overall"]["result"]),
                f"[results](../results/{document['run_id']}/results.json) / "
                f"[report]({EXPERIMENT_REPORTS[label]})",
            ]
            for label, document in documents.items()
        ],
    )
    environment_table = markdown_table(
        ["Experiment", "CPU", "Logical CPUs", "Memory GiB", "Platform"],
        [
            [
                label,
                str(document["environment"].get("host_cpu")),
                str(document["environment"].get("host_logical_cpus")),
                number(float(document["environment"].get("host_memory_bytes", 0)) / 2**30, 1),
                str(document["environment"].get("platform")),
            ]
            for label, document in documents.items()
        ],
    )
    image_table = markdown_table(
        ["Experiment", "API", "Worker", "Scheduler", "Load generator"],
        [
            [
                label,
                *(
                    str(document["images"][service]["image_id"])[7:19]
                    for service in ("api", "worker", "scheduler", "load_generator")
                ),
            ]
            for label, document in documents.items()
        ],
    )
    scaling_tables = {}
    for label in ("E1", "E2", "E3"):
        scaling_tables[label] = markdown_table(
            ["Workers", "Median tasks/s", "Speedup", "Parallel efficiency"],
            [
                [
                    str(workers),
                    number(scaling[label][workers]["processing_throughput_median"], 3),
                    number(scaling[label][workers]["speedup"], 3),
                    number(scaling[label][workers]["parallel_efficiency"] * 100, 1) + "%",
                ]
                for workers in (1, 4, 8, 16)
            ],
        )
    e1_table = markdown_table(
        ["Workers", "Median tasks/s", "Speedup", "Efficiency", "Queue p95 s", "Claim p95 s*"],
        [
            [
                str(workers),
                number(scaling["E1"][workers]["processing_throughput_median"], 3),
                number(scaling["E1"][workers]["speedup"], 3),
                number(scaling["E1"][workers]["parallel_efficiency"] * 100, 1) + "%",
                number(e1_latency[workers]["queue_p95"], 6),
                number(e1_latency[workers]["claim_p95"], 6),
            ]
            for workers in (1, 4, 8, 16)
        ],
    )
    e3_table = markdown_table(
        [
            "Workers",
            "Median tasks/s",
            "Speedup",
            "Attempt Lifecycle p95 s",
            "Handler p95 s*",
            "Claim p95 s*",
        ],
        [
            [
                str(workers),
                number(scaling["E3"][workers]["processing_throughput_median"], 3),
                number(scaling["E3"][workers]["speedup"], 3),
                number(e3_latency[workers]["attempt_lifecycle_p95"], 6),
                number(e3_latency[workers]["handler_p95"], 6),
                number(e3_latency[workers]["claim_p95"], 6),
            ]
            for workers in (1, 4, 8, 16)
        ],
    )
    cross_table = markdown_table(
        ["Workers", "No-op speedup", "50 ms wait speedup", "CPU speedup"],
        [
            [
                str(workers),
                number(scaling["E1"][workers]["speedup"], 3),
                number(scaling["E2"][workers]["speedup"], 3),
                number(scaling["E3"][workers]["speedup"], 3),
            ]
            for workers in (1, 4, 8, 16)
        ],
    )
    api_table = markdown_table(
        ["Concurrency", "Median submission req/s", "p50 ms", "p95 ms", "p99 ms"],
        [
            [
                str(row["concurrency"]),
                number(row["submission_throughput_median"], 3),
                number(row["request_p50_ms"], 3),
                number(row["request_p95_ms"], 3),
                number(row["request_p99_ms"], 3),
            ]
            for row in e4
        ],
    )
    recovery_table = markdown_table(
        [
            "Trial",
            "Affected",
            "ABANDONED",
            "Recovery p50 ms",
            "p95 ms",
            "p99 ms",
            "Kill-to-drain s",
        ],
        [
            [
                str(row["trial"]),
                str(row["affected_tasks"]),
                str(row["abandoned_attempts"]),
                number(float(row["recovery_p50"]) * 1000, 3),
                number(float(row["recovery_p95"]) * 1000, 3),
                number(float(row["recovery_p99"]) * 1000, 3),
                number(row["kill_to_drain"], 6),
            ]
            for row in e6_trials
        ],
    )
    correctness_table = markdown_table(
        [
            "Experiment",
            "Logical tasks/requests",
            "Attempts",
            "Duplicates",
            "Lost/failed",
            "Stranded",
            "Trust",
        ],
        [
            [
                row["experiment"],
                row["scale"],
                integer(row["attempts"]),
                integer(row["duplicates"]),
                integer(row["lost_failed"]),
                integer(row["stranded"]),
                row["trust"],
            ]
            for row in correctness
        ],
    )
    prom_rows = []
    for label in DEFAULT_INPUTS:
        totals = counter_totals(documents[label])
        reconciled = ", ".join(
            f"{name}={integer(values['raw'])} (difference {integer(values['difference'])})"
            for name, values in sorted(totals.items())
        )
        if label == "E6":
            raw_observations = sum(
                int(item["raw"]["recovery_lag_observations"])
                for item in valid_results(documents[label], SCENARIOS[label])
            )
            reconciled += f"; recovery histogram observations={integer(raw_observations)} exact"
        prom_rows.append([label, reconciled])
    prom_table = markdown_table(["Experiment", "Important exact reconciliation"], prom_rows)
    findings_text = "\n".join(f"{index}. {finding}" for index, finding in enumerate(findings, 1))

    return f"""# TF-012 Final Benchmark Report

## Executive Summary

TF-012 measured six trusted TaskForge workloads with deliberately different bottleneck
profiles. The central result is not one universal throughput number: no-op work exposes
coordination cost, synthetic waiting exposes concurrency, deterministic CPU work exposes
host compute limits, and API submission, retry, and recovery each exercise distinct paths.

{findings_text}

## Benchmark Trust Methodology

Every number below comes from an accepted E1-E6 run that independently passes the
standalone trust evaluator. Runs require clean committed source with exact Git commit/tree
provenance, Docker image identities, immutable PostgreSQL task/attempt evidence, hashed
manifests, independent reset blocks, recorded randomized/interleaved order where applicable,
and no cherry-picking. Trial-only Prometheus deltas must structurally validate and exact
counters must reconcile to durable evidence. Raw PostgreSQL timestamps remain authoritative
for lifecycle timing, while scenario-specific validators enforce task/attempt correctness.
Reports regenerate deterministically from saved artifacts.

{provenance_table}

Image fingerprint prefixes (full identities are in each linked `results.json`):

{image_table}

## Environment

{environment_table}

All measurements are single-host Docker results. Host and container scheduling are part of
the measured environment.

## E1 — Coordination-Bound No-op Scaling

{e1_table}

`*` Claim p95 is a Prometheus trial-delta histogram estimate. Queue timing is immutable
PostgreSQL evidence. Throughput improved substantially from 1 to 4 workers, then saturated
and declined at 16 as coordination dominated nearly zero-cost handler work. No-op throughput
is not representative application throughput.

## E2 — Wait-Bound Scaling

{scaling_tables["E2"]}

This is a synthetic 50 ms `test.sleep` workload, not real network or disk I/O. It scaled
nearly linearly through 16 workers, reaching {number(scaling["E2"][16]["speedup"], 3)}x
speedup and {number(scaling["E2"][16]["parallel_efficiency"] * 100, 1)}% parallel efficiency.

## E3 — CPU-Bound Scaling

{e3_table}

`Attempt Lifecycle` is immutable PostgreSQL `finished_at - started_at`. `Handler` and
`Claim` are separate Prometheus delta-histogram estimates; they are not the same timing
semantic and are never collapsed. Scaling was strong through 8 workers
({number(scaling["E3"][8]["speedup"], 3)}x), then flattened at 16
({number(scaling["E3"][16]["speedup"], 3)}x) as 16 workers oversubscribed the {
        host_cpus
    }-logical-CPU
host and handler/claim timing increased.

## E4 — API Submission Performance

{api_table}

These values are **submission throughput**, not worker processing throughput. The highest
tested median was {number(best_api["submission_throughput_median"], 3)} requests/s at
concurrency {best_api["concurrency"]}. Higher concurrency did not improve median submission
throughput on this host and increased tail latency. The accepted artifact records
{correctness_by_label["E4"]["scale"]}, with {integer(correctness_by_label["E4"]["attempts"])}
attempts because workers were intentionally disabled.

## E5 — Retry-Storm Performance

Across {len(e5_results)} independent trials, {integer(correctness_by_label["E5"]["tasks"])}
logical tasks produced exactly {integer(correctness_by_label["E5"]["attempts"])} attempts.
The evidence contains {integer(e5_failed_first)} FAILED first attempts and
{integer(e5_succeeded_second)} SUCCEEDED second attempts; duplicate identities,
ABANDONED attempts ({integer(e5_abandoned)}), and stranded leases were all zero.
The exact history validator confirms every task followed `FAILED -> SUCCEEDED`.

{
        markdown_table(
            ["Median measurement", "Value"],
            [
                [
                    "Processing throughput",
                    number(e5["processing_throughput_median"], 3) + " tasks/s",
                ],
                ["Retry-lateness p95", number(e5["retry_lateness_p95"] * 1000, 3) + " ms"],
                ["Attempt-2 queue p95", number(e5["attempt2_queue_p95"] * 1000, 3) + " ms"],
                ["Total task p95", number(e5["total_p95"] * 1000, 3) + " ms"],
            ],
        )
    }

This experiment demonstrates exact retry history under load, not a production failure-rate
distribution.

## E6 — Crash-Recovery Performance

{recovery_table}

Across {len(e6_results)} trials, {integer(e6_affected)} captured attempts were ABANDONED
({integer(e6_abandoned)} durable ABANDONED rows) and all {integer(e6_replacements)} had
exactly one later successful replacement. Duplicate recovery effects ({integer(e6_duplicates)})
and final failures ({integer(correctness_by_label["E6"]["lost_failed"])}) were zero. Median
trial recovery p95 was {number(e6["recovery_p95_median"] * 1000, 3)} ms **after lease
expiration**. Recovery lag is lease expiration to durable recovery, not worker kill to
recovery. The configured lease duration was {lease_display}; median kill-to-final-drain was
{number(e6["kill_to_drain_median"], 6)} seconds.

## Cross-Workload Analysis

{cross_table}

- No-op: coordination overhead dominates after the first scaling gain.
- 50 ms wait: parallelism dominates, so scaling remains nearly linear through 16 workers.
- CPU: throughput scales until host compute becomes limiting.

There is no evidence for a universal optimal worker count; it depends on workload cost and
resource profile.

## Correctness Summary

{correctness_table}

E4's tasks intentionally remained queued and therefore had zero attempts. E5's FAILED first
attempts are expected intermediate history, not lost tasks. E6's ABANDONED attempts are
expected captured crash effects with exact successful replacements.

## Prometheus Reconciliation Summary

{prom_table}

Histograms were validated for structure, boundaries, and observation counts without listing
every bucket. Lifecycle percentiles continue to use raw immutable PostgreSQL evidence.

## System Bottlenecks / Tradeoffs

1. PostgreSQL coordination is visible when handler work is nearly free.
2. When waiting dominates coordination, adding workers produces near-linear concurrency.
3. CPU-bound scaling eventually reaches host scheduling and compute limits.
4. API submission saturation and tail latency are independent of worker processing capacity.
5. Retry and crash recovery preserve exact durable attempt histories under tested concurrency.
6. {
        recovery_schedulers
    } scheduler replicas produced no duplicate recovery side effects in the controlled E6 run.

## Resume-Safe Findings

{findings_text}

These findings are scoped to the linked trusted artifacts and controlled local environment.

## Resume Recommendation

**Primary recommendation**

> {bullets["primary"]}

**Alternative backend/distributed-systems version**

> {bullets["backend"]}

**Alternative performance-focused version**

> {bullets["performance"]}

## Interview Talk Track

**How did you benchmark TaskForge?**

{interview_summary(documents)}

**Why did no-op stop scaling?**

The handler is almost free, so claim, completion, polling, and PostgreSQL coordination become
the workload. Four workers improved concurrency, but more workers added contention without
enough handler work to amortize it.

**Why did 50 ms work scale almost linearly?**

Each worker spends most of its time in a bounded wait, so independent workers overlap that
time. Coordination is small relative to the 50 ms handler duration, producing
{number(scaling["E2"][16]["speedup"], 3)}x speedup at 16 workers.

**Why did CPU flatten?**

The host exposed {host_cpus} logical CPUs. Eight workers still scaled strongly, but 16 workers
oversubscribed host compute; handler and claim latency increased while throughput improved
only modestly beyond 8 workers.

**How does crash recovery work?**

Workers own tasks through expiring leases. After a hard-killed worker stops renewing, one of
{
        recovery_schedulers
    } schedulers atomically marks the captured attempt ABANDONED and requeues the task. A
healthy or replacement worker creates the next attempt. E6 verified exact one-time recovery,
successful replacement, and zero duplicate effects. The reported {
        number(e6["recovery_p95_median"] * 1000, 3)
    } ms p95 starts after lease expiration, not at process death.

## Limitations

- Synthetic no-op, bounded wait, deterministic CPU, fail-once, and hard-kill workloads.
- One local Apple M4 Pro host under Docker Desktop; results are not production capacity claims.
- E4 measures submission only, without task processing during the measured window.
- E6 uses Docker failure injection and a fixed {lease_display} lease configuration.
- Three trials/blocks quantify repeatability for these runs but not broader hardware variance.

## Reproducibility

Run `python3 -m benchmarks.final_report` to revalidate all six standalone trust gates and
regenerate this report plus `tf-012-resume-summary.md`. The accepted inputs, exact run IDs,
commits, trees, manifests, raw CSV evidence, and per-experiment reports are linked above.
"""


def render_summary(documents: dict[str, dict[str, Any]]) -> str:
    findings = resume_findings(documents)
    primary = resume_bullets(documents)["primary"]
    lease_duration = documents["E6"]["timing_configuration"]["WORKER_TASK_LEASE_DURATION"]
    lease_display = (
        f"{lease_duration.removesuffix('s')} seconds"
        if str(lease_duration).endswith("s")
        else str(lease_duration)
    )
    findings_text = "\n".join(f"{index}. {finding}" for index, finding in enumerate(findings, 1))
    return f"""# TF-012 Resume Summary

## Key Results

{findings_text}

## Recommended Resume Bullet

> {primary}

## 30-Second Interview Summary

{interview_summary(documents)}

## Benchmark Caveats

- Synthetic workloads isolate bottlenecks but are not application traffic.
- Results come from one local Apple M4 Pro host under Docker Desktop.
- E4 is submission throughput, not worker processing throughput.
- E6 recovery p95 is measured after lease expiration; the configured lease was {lease_display}.
- Every claim is conditional on the linked E1-E6 standalone trust gates remaining PASS.
"""


def generate(
    input_paths: dict[str, pathlib.Path] | None = None,
    report_path: pathlib.Path = REPORT_PATH,
    summary_path: pathlib.Path = SUMMARY_PATH,
) -> tuple[pathlib.Path, pathlib.Path]:
    documents = load_trusted_inputs(input_paths)
    return write_reports(documents, report_path, summary_path)


def write_reports(
    documents: dict[str, dict[str, Any]],
    report_path: pathlib.Path,
    summary_path: pathlib.Path,
) -> tuple[pathlib.Path, pathlib.Path]:
    """Write byte-deterministic reports from already validated documents."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(documents))
    summary_path.write_text(render_summary(documents))
    return report_path, summary_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the trusted final TF-012 reports")
    for label in DEFAULT_INPUTS:
        parser.add_argument(f"--{label.lower()}", type=pathlib.Path, default=DEFAULT_INPUTS[label])
    parser.add_argument("--report", type=pathlib.Path, default=REPORT_PATH)
    parser.add_argument("--summary", type=pathlib.Path, default=SUMMARY_PATH)
    arguments = parser.parse_args()
    inputs = {label: getattr(arguments, label.lower()) for label in DEFAULT_INPUTS}
    generated = generate(inputs, arguments.report, arguments.summary)
    print(json.dumps({"generated": [str(path) for path in generated]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
