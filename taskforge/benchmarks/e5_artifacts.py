#!/usr/bin/env python3
"""Generate the scoped TF-012E5 retry-storm report and four plots."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

from benchmarks.e1_artifacts import markdown_table, number, prometheus_totals, valid_results
from benchmarks.plot import line_plot

SCENARIO = "retry_storm"
PLOT_NAMES = (
    "01-retry-processing-throughput.svg",
    "02-retry-lateness-p95.svg",
    "03-attempt2-queue-p95.svg",
    "04-retry-total-latency-p95.svg",
)


def retry_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    results = sorted(
        valid_results(document, SCENARIO), key=lambda item: (int(item["block"]), int(item["trial"]))
    )
    return [
        {
            "trial": int(item["trial"]),
            "block": int(item["block"]),
            "processing_throughput": float(item["raw"]["processing_throughput_per_second"]),
            "retry_lateness_p50": item["raw"].get("retry_lateness_p50_seconds"),
            "retry_lateness_p95": item["raw"].get("retry_lateness_p95_seconds"),
            "retry_lateness_p99": item["raw"].get("retry_lateness_p99_seconds"),
            "attempt2_queue_p50": item["raw"].get("attempt2_queue_p50_seconds"),
            "attempt2_queue_p95": item["raw"].get("attempt2_queue_p95_seconds"),
            "attempt2_queue_p99": item["raw"].get("attempt2_queue_p99_seconds"),
            "total_p50": item["raw"].get("total_p50_seconds"),
            "total_p95": item["raw"].get("total_p95_seconds"),
            "total_p99": item["raw"].get("total_p99_seconds"),
            "retry_batch_p95": item.get("prometheus_reconciliation", {})
            .get("histograms", {})
            .get("retry_batch", {})
            .get("prometheus_quantiles", {})
            .get("p95"),
        }
        for item in results
    ]


def retry_aggregate(document: dict[str, Any]) -> dict[str, Any]:
    rows = retry_rows(document)
    throughput = [row["processing_throughput"] for row in rows]
    mean = statistics.fmean(throughput) if throughput else None
    aggregate: dict[str, Any] = {
        "processing_throughput_median": statistics.median(throughput) if throughput else None,
        "processing_throughput_min": min(throughput) if throughput else None,
        "processing_throughput_max": max(throughput) if throughput else None,
        "processing_throughput_cv": (
            statistics.stdev(throughput) / mean
            if len(throughput) > 1 and mean
            else 0.0
            if throughput
            else None
        ),
    }
    for prefix in ("retry_lateness", "attempt2_queue", "total"):
        for quantile in ("p50", "p95", "p99"):
            values = [
                float(row[f"{prefix}_{quantile}"])
                for row in rows
                if row[f"{prefix}_{quantile}"] is not None
            ]
            aggregate[f"{prefix}_{quantile}"] = statistics.median(values) if values else None
    batch = [float(row["retry_batch_p95"]) for row in rows if row["retry_batch_p95"] is not None]
    aggregate["retry_batch_p95"] = statistics.median(batch) if batch else None
    return aggregate


def plot_specs(document: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    rows = retry_rows(document)
    return (
        (
            PLOT_NAMES[0],
            "Retry PROCESSING THROUGHPUT across Trials",
            "Tasks/second",
            [("Fail once", [(row["trial"], row["processing_throughput"]) for row in rows])],
        ),
        (
            PLOT_NAMES[1],
            "Retry Lateness p95 across Trials",
            "Seconds",
            [("Retry lateness p95", [(row["trial"], row["retry_lateness_p95"]) for row in rows])],
        ),
        (
            PLOT_NAMES[2],
            "Attempt-2 Queue Wait p95 across Trials",
            "Seconds",
            [("Attempt 2 queue p95", [(row["trial"], row["attempt2_queue_p95"]) for row in rows])],
        ),
        (
            PLOT_NAMES[3],
            "Retry Total Task Latency p95 across Trials",
            "Seconds",
            [("Total latency p95", [(row["trial"], row["total_p95"]) for row in rows])],
        ),
    )


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document, SCENARIO)
    rows = retry_rows(document)
    aggregate = retry_aggregate(document)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    profile = document.get("profile", {}) or {}
    retry_configuration = document.get("retry_configuration", {}) or profile.get(
        "retry_configuration", {}
    )
    gates = document.get("trust", {}) or {}
    throughput_table = markdown_table(
        ["Median tasks/s", "Min", "Max", "CV"],
        [
            [
                number(aggregate["processing_throughput_median"], 2),
                number(aggregate["processing_throughput_min"], 2),
                number(aggregate["processing_throughput_max"], 2),
                number(aggregate["processing_throughput_cv"], 4),
            ]
        ],
    )
    trial_table = markdown_table(
        ["Trial", "Processing tasks/s", "Retry p95", "Attempt-2 queue p95", "Total p95"],
        [
            [
                str(row["trial"]),
                number(row["processing_throughput"], 2),
                number(row["retry_lateness_p95"]),
                number(row["attempt2_queue_p95"]),
                number(row["total_p95"]),
            ]
            for row in rows
        ],
    )
    latency_table = markdown_table(
        ["Measurement", "p50", "p95", "p99"],
        [
            [
                label,
                number(aggregate[f"{prefix}_p50"]),
                number(aggregate[f"{prefix}_p95"]),
                number(aggregate[f"{prefix}_p99"]),
            ]
            for label, prefix in (
                ("Retry lateness", "retry_lateness"),
                ("Attempt-2 queue wait", "attempt2_queue"),
                ("Total task latency", "total"),
            )
        ],
    )
    correctness = {
        "tasks": sum(int(item["correctness"]["actual_tasks"]) for item in results),
        "attempts": sum(int(item["correctness"]["actual_attempts"]) for item in results),
        "attempt1_failed": sum(int(item["correctness"]["attempt1_failed"]) for item in results),
        "attempt2_succeeded": sum(
            int(item["correctness"]["attempt2_succeeded"]) for item in results
        ),
        "retry_schedules": sum(int(item["correctness"]["retry_schedules"]) for item in results),
        "retry_promotions": sum(int(item["correctness"]["retry_promotions"]) for item in results),
        "duplicates": sum(
            int(item["correctness"]["retry_duplicate_identities"]) for item in results
        ),
        "chain_mismatches": sum(
            int(item["correctness"].get("retry_chain_mismatches", 0)) for item in results
        ),
        "orphan_attempt_tasks": sum(
            int(item["correctness"].get("retry_orphan_attempt_tasks", 0)) for item in results
        ),
        "abandoned": sum(int(item["correctness"]["abandoned_attempts"]) for item in results),
        "stranded": sum(int(item["correctness"]["stranded_leases"]) for item in results),
    }
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    return f"""# TF-012E5 Retry Storm

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E5 trusted fail-once retry experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Clean source: `{source.get("clean")}`
- Platform: `{environment.get("platform")}`

## Methodology

Each independent trial submits `{profile.get("retry_tasks")}` deterministic
`test.fail_n_then_succeed` tasks with `failures=1`, using
`{profile.get("retry_workers")}` workers and `{profile.get("retry_schedulers")}` schedulers.
Every logical task must durably record FAILED attempt 1 and SUCCEEDED attempt 2.

## Retry Configuration

- Base delay: `{retry_configuration.get("base_delay")}`
- Maximum delay: `{retry_configuration.get("max_delay")}`
- Jitter: `{retry_configuration.get("jitter")}`
- Promotion interval: `{retry_configuration.get("promotion_interval")}`

## PROCESSING THROUGHPUT

{throughput_table}

{trial_table}

## Retry Lateness

Retry lateness is immutable attempt-2 queue-entry time minus its scheduled snapshot.

{latency_table}

## Attempt-2 Queue Wait

Attempt-2-only queue p50/p95/p99 appears separately above and is not blended with attempt 1.

## Total Latency

Total task creation-to-final-success p50/p95/p99 appears above.

## Scheduler Retry-Batch Duration

Prometheus retry-batch p95 median: `{number(aggregate["retry_batch_p95"])}` seconds.

## Attempt-History Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- Tasks / attempts: `{correctness["tasks"]}` / `{correctness["attempts"]}`
- Attempt-1 FAILED: `{correctness["attempt1_failed"]}`
- Attempt-2 SUCCEEDED: `{correctness["attempt2_succeeded"]}`
- Retry schedules / promotions: `{correctness["retry_schedules"]}` / `{correctness["retry_promotions"]}`
- Duplicate identities: `{correctness["duplicates"]}`
- Per-task chain mismatches / orphan attempt task IDs: `{correctness["chain_mismatches"]}` / `{correctness["orphan_attempt_tasks"]}`
- ABANDONED attempts: `{correctness["abandoned"]}`
- Stranded leases: `{correctness["stranded"]}`

## Prometheus Reconciliation

The worker completion counter records each durable attempt outcome; fail-once tasks
therefore expect two completion observations per logical task.

{markdown_table(["Metric", "Raw", "Prometheus", "Difference", "Result"], prometheus_totals(results))}

## Trust Gates

{gate_table}

## Plots

{plots}

## Caveats

- Synthetic deterministic fail-once behavior, not a production failure distribution.
- Uses the committed benchmark retry timing without mid-run changes.
- Single local Docker host; findings are resume-safe only when overall trust is `PASS`.
"""


def generate(
    results_path: pathlib.Path, report_path: pathlib.Path | None = None
) -> list[pathlib.Path]:
    document = json.loads(results_path.read_text())
    output_dir = results_path.parent
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for name, title, y_label, series in plot_specs(document):
        path = plots_dir / name
        line_plot(path, title, "Trial", y_label, series)
        generated.append(path)
    internal_report = output_dir / "tf-012e5-retry-storm.md"
    internal_report.write_text(render_report(document))
    generated.append(internal_report)
    if report_path is not None:
        prefix = f"../results/{document.get('run_id')}/"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_report(document, prefix))
    return generated


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--report", type=pathlib.Path)
    arguments = parser.parse_args()
    generate(arguments.results.resolve(), arguments.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
