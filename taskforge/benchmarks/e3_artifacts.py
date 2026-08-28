#!/usr/bin/env python3
"""Generate the scoped TF-012E3 CPU report and five plots."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from benchmarks.e1_artifacts import (
    markdown_table,
    median_measurement,
    number,
    prometheus_totals,
    summary_rows,
    valid_results,
)
from benchmarks.plot import grouped, line_plot

SCENARIO = "cpu_scaling"
PLOT_NAMES = (
    "01-cpu-processing-throughput.svg",
    "02-cpu-speedup.svg",
    "03-cpu-parallel-efficiency.svg",
    "04-cpu-execution-p95.svg",
    "05-e1-e2-e3-speedup.svg",
)


def resource_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Aggregate existing per-trial Docker sample summaries by worker count."""
    results = valid_results(document, SCENARIO)
    rows = []
    for workers in sorted({int(item["workers"]) for item in results}):
        items = [item for item in results if int(item["workers"]) == workers]
        rows.append(
            {
                "workers": workers,
                "worker_cpu_percent": median_measurement(
                    items, ("resources", "worker", "cpu_percent_mean")
                ),
                "postgres_cpu_percent": median_measurement(
                    items, ("resources", "postgres", "cpu_percent_mean")
                ),
            }
        )
    return rows


def e3_latency_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep PostgreSQL attempt lifecycle and Prometheus handler timing distinct."""
    results = valid_results(document, SCENARIO)
    rows = []
    for workers in sorted({int(item["workers"]) for item in results}):
        items = [item for item in results if int(item["workers"]) == workers]
        row: dict[str, Any] = {"workers": workers}
        for quantile in ("p50", "p95", "p99"):
            row[f"queue_{quantile}"] = median_measurement(
                items, ("raw", f"queue_{quantile}_seconds")
            )
            row[f"attempt_lifecycle_{quantile}"] = median_measurement(
                items, ("raw", f"execution_{quantile}_seconds")
            )
            row[f"total_{quantile}"] = median_measurement(
                items, ("raw", f"total_{quantile}_seconds")
            )
            row[f"handler_{quantile}"] = median_measurement(
                items,
                (
                    "prometheus_reconciliation",
                    "histograms",
                    "execution",
                    "prometheus_quantiles",
                    quantile,
                ),
            )
            row[f"claim_{quantile}"] = median_measurement(
                items,
                (
                    "prometheus_reconciliation",
                    "histograms",
                    "claim",
                    "prometheus_quantiles",
                    quantile,
                ),
            )
        rows.append(row)
    return rows


def plot_specs(document: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    """Return the five deterministic E3 plots with explicit latency semantics."""
    results = valid_results(document, SCENARIO)
    summaries = summary_rows(document, SCENARIO)
    latencies = e3_latency_rows(document)
    throughput = grouped(results, SCENARIO, "workers", "raw.processing_throughput_per_second")
    cpu_speedup = [(row["workers"], row["speedup"]) for row in summaries]
    efficiency = [(row["workers"], row["parallel_efficiency"]) for row in summaries]
    handler_p95 = [(row["workers"], row["handler_p95"]) for row in latencies]
    e1_speedup = [
        (workers, float(document.get("e1_comparison", {}).get("speedup", {})[str(workers)]))
        for workers in (1, 4, 8, 16)
    ]
    e2_speedup = [
        (workers, float(document.get("e2_comparison", {}).get("speedup", {})[str(workers)]))
        for workers in (1, 4, 8, 16)
    ]
    return (
        (PLOT_NAMES[0], "CPU processing throughput", "Tasks/second", [("CPU", throughput)]),
        (PLOT_NAMES[1], "CPU speedup", "Speedup vs 1 worker", [("CPU", cpu_speedup)]),
        (PLOT_NAMES[2], "CPU parallel efficiency", "Efficiency", [("CPU", efficiency)]),
        (
            PLOT_NAMES[3],
            "CPU Handler-Execution p95 vs Workers",
            "Seconds",
            [("Prometheus handler execution", handler_p95)],
        ),
        (
            PLOT_NAMES[4],
            "Trusted E1 vs E2 vs E3 speedup",
            "Speedup vs 1 worker",
            [("E1 no-op", e1_speedup), ("E2 50 ms", e2_speedup), ("E3 CPU", cpu_speedup)],
        ),
    )


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document, SCENARIO)
    summaries = summary_rows(document, SCENARIO)
    latencies = e3_latency_rows(document)
    resources = resource_rows(document)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    profile = document.get("profile", {}) or {}
    workload = profile.get("workload", {}) or {}
    e1 = document.get("e1_comparison", {}) or {}
    e2 = document.get("e2_comparison", {}) or {}
    gates = document.get("trust", {}) or {}

    throughput_table = markdown_table(
        ["Workers", "Median", "Min", "Max", "CV", "Speedup", "Efficiency"],
        [
            [
                str(row["workers"]),
                number(row["processing_throughput_median"], 2),
                number(row["processing_throughput_min"], 2),
                number(row["processing_throughput_max"], 2),
                number(row["processing_throughput_cv"], 4),
                number(row["speedup"], 3),
                number(row["parallel_efficiency"], 3),
            ]
            for row in summaries
        ],
    )
    latency_table = markdown_table(
        [
            "Workers",
            "Queue p95",
            "Attempt Lifecycle p95",
            "Handler p50",
            "Handler p95",
            "Claim p95",
            "Total p95",
        ],
        [
            [
                str(row["workers"]),
                number(row["queue_p95"]),
                number(row["attempt_lifecycle_p95"]),
                number(row["handler_p50"]),
                number(row["handler_p95"]),
                number(row["claim_p95"]),
                number(row["total_p95"]),
            ]
            for row in latencies
        ],
    )
    resource_table = markdown_table(
        ["Workers", "Worker CPU %*", "PostgreSQL CPU %*"],
        [
            [
                str(row["workers"]),
                number(row["worker_cpu_percent"], 2),
                number(row["postgres_cpu_percent"], 2),
            ]
            for row in resources
        ],
    )
    blocks_table = markdown_table(
        ["Workers", "Block 1", "Block 2", "Block 3", "Maximum shift"],
        [
            [
                str(row["workers"]),
                *(number(row["block_values"].get(block), 2) for block in (1, 2, 3)),
                number(row["maximum_block_shift"], 4),
            ]
            for row in summaries
        ],
    )
    cpu_speedup = {str(row["workers"]): row["speedup"] for row in summaries}
    comparison_table = markdown_table(
        ["Workers", "E1 no-op speedup", "E2 50 ms speedup", "E3 CPU speedup"],
        [
            [
                str(workers),
                number(e1.get("speedup", {}).get(str(workers)), 3),
                number(e2.get("speedup", {}).get(str(workers)), 3),
                number(cpu_speedup.get(str(workers)), 3),
            ]
            for workers in (1, 4, 8, 16)
        ],
    )
    block_orders = []
    for block in sorted({int(item["block"]) for item in results}):
        ordered = sorted(
            (item for item in results if int(item["block"]) == block),
            key=lambda item: int(item.get("order_index") or 0),
        )
        block_orders.append(
            f"- Block {block}: "
            + ", ".join(str(item["workers"]) for item in ordered)
            + f" workers (seed `{ordered[0].get('random_seed') if ordered else None}`)"
        )
    image_lines = [
        f"- {name}: `{value.get('image_id')}`; RepoDigests: "
        f"`{', '.join(value.get('repo_digests', [])) or 'none'}`"
        for name, value in sorted((document.get("images", {}) or {}).items())
    ]
    correctness = {
        "tasks": sum(int(item["correctness"]["actual_tasks"]) for item in results),
        "attempts": sum(int(item["correctness"]["actual_attempts"]) for item in results),
        "duplicates": sum(int(item["correctness"]["duplicate_attempts"]) for item in results),
        "stranded": sum(int(item["correctness"]["stranded_leases"]) for item in results),
        "failures": sum(
            int(item["correctness"]["actual_tasks"]) - int(item["correctness"]["succeeded_tasks"])
            for item in results
        ),
        "negative": sum(
            sum(int(value) for value in item["raw"]["negative_durations"].values())
            for item in results
        ),
    }
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    prometheus_table = markdown_table(
        ["Metric", "Raw", "Prometheus", "Difference", "Result"],
        prometheus_totals(results),
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    return f"""# TF-012E3 Deterministic CPU Scaling

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E3 trusted CPU experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Clean source: `{source.get("clean")}`
- E1 comparison run: `{e1.get("run_id")}`
- E1 results SHA-256: `{e1.get("results_sha256")}`
- E2 comparison run: `{e2.get("run_id")}`
- E2 results SHA-256: `{e2.get("results_sha256")}`

{chr(10).join(image_lines)}

## CPU Workload Parameter

- Task type: `{workload.get("task_type")}`
- Deterministic SHA-256 iterations per attempt: `{workload.get("iterations")}`
- Tasks per configuration: `{profile.get("cpu_tasks")}`

The fixed input seed and bounded iteration count are identical for every worker count.

## Methodology

Worker counts are 1, 4, 8, and 16 across three independently reset blocks. Every block performs excluded warm-up and uses a recorded seeded order. Trial processes are recreated, Prometheus boundaries are isolated, and immutable task/attempt rows plus existing Docker resource samples are archived.

{chr(10).join(block_orders)}

## CPU Scaling

{throughput_table}

## Latency

Values are seconds. Attempt Lifecycle is immutable PostgreSQL `finished_at - started_at`
evidence. Handler and Claim values are separate Prometheus trial-delta histogram
estimates; neither is a validation of the attempt-lifecycle percentile.

{latency_table}

## CPU Resource Behavior

{resource_table}

`*` Median across trials of each trial's mean aggregate service CPU from existing Docker resource samples. CPU values are analysis evidence, not trust gates. Host logical CPUs: `{environment.get("host_logical_cpus")}`.

## Independent Blocks — PROCESSING THROUGHPUT

{blocks_table}

## E1 / E2 / E3 Speedup Comparison

{comparison_table}

E1 and E2 values come from externally supplied artifacts that passed the existing standalone trust evaluator. Absolute throughput is not compared across workload types.

## Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- Tasks: `{correctness["tasks"]}`
- Attempts: `{correctness["attempts"]}`
- Duplicate attempt identities: `{correctness["duplicates"]}`
- Unexpected failures: `{correctness["failures"]}`
- Stranded leases: `{correctness["stranded"]}`
- Negative durations: `{correctness["negative"]}`

## Prometheus Reconciliation

{prometheus_table}

## Trust Gates

{gate_table}

## Plots

{plots}

## Caveats

- Synthetic deterministic CPU workload, not application business logic.
- Single local host with `{environment.get("host_logical_cpus")}` logical CPUs.
- Docker virtualization affects CPU scheduling and utilization evidence.
- Performance findings are resume-safe only when the overall gate is `PASS`.
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
        line_plot(path, title, "Workers", y_label, series)
        generated.append(path)
    internal_report = output_dir / "tf-012e3-cpu-scaling.md"
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
    generate(arguments.results, arguments.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
