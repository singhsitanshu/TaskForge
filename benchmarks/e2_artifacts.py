#!/usr/bin/env python3
"""Generate the scoped TF-012E2 50 ms report and five plots."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from benchmarks.e1_artifacts import (
    latency_rows,
    markdown_table,
    number,
    prometheus_totals,
    summary_rows,
    valid_results,
)
from benchmarks.plot import grouped, line_plot

SCENARIO = "io50_scaling"
PLOT_NAMES = (
    "01-io50-processing-throughput.svg",
    "02-io50-speedup.svg",
    "03-io50-parallel-efficiency.svg",
    "04-io50-queue-p95.svg",
    "05-noop-vs-io50-speedup.svg",
)


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document, SCENARIO)
    summaries = summary_rows(document, SCENARIO)
    latencies = latency_rows(document, SCENARIO)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    comparison = document.get("e1_comparison", {})
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
        ["Workers", "Queue p95", "Claim p95*", "Execution p50", "Execution p95", "Total p95"],
        [
            [
                str(row["workers"]),
                number(row["queue_p95"]),
                number(row["claim_p95"]),
                number(row["execution_p50"]),
                number(row["execution_p95"]),
                number(row["total_p95"]),
            ]
            for row in latencies
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
    e2_speedup = {str(row["workers"]): row["speedup"] for row in summaries}
    comparison_table = markdown_table(
        ["Workers", "E1 no-op speedup", "E2 50 ms speedup"],
        [
            [
                str(workers),
                number(comparison.get("speedup", {}).get(str(workers)), 3),
                number(e2_speedup.get(str(workers)), 3),
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
    gates = document.get("trust", {}) or {}
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    return f"""# TF-012E2 Synthetic 50 ms Scaling

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E2 trusted synthetic wait experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Clean source: `{source.get("clean")}`
- E1 comparison run: `{comparison.get("run_id")}`
- E1 results SHA-256: `{comparison.get("results_sha256")}`

## Environment

- Platform: `{environment.get("platform")}`
- CPU: `{environment.get("host_cpu")}`
- Logical cores: `{environment.get("host_logical_cpus")}`
- Memory bytes: `{environment.get("host_memory_bytes")}`
- Docker: `{environment.get("docker_version")}`
- PostgreSQL: `{environment.get("postgresql")}`

## Methodology

Exactly `{document.get("profile", {}).get("io_tasks")}` `test.sleep` tasks with `duration_ms=50` are submitted per configuration. This is a synthetic wait/I/O-like workload, not actual network or disk I/O. Worker counts are 1, 4, 8, and 16 across three independently reset blocks with excluded warm-up and isolated Prometheus boundaries.

{chr(10).join(block_orders)}

## PROCESSING THROUGHPUT

{throughput_table}

## Latency

Values are seconds. Claim p95 marked `*` is secondary Prometheus delta-histogram evidence. Raw execution p50/p95 provides the workload-duration check.

{latency_table}

## Independent Blocks — PROCESSING THROUGHPUT

{blocks_table}

## Trusted E1 Comparison

{comparison_table}

E1 values are read from and validated against the external trusted E1 result artifact identified above; they are not embedded constants.

## Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- Tasks: `{correctness["tasks"]}`
- Attempts: `{correctness["attempts"]}`
- Duplicate attempt identities: `{correctness["duplicates"]}`
- Unexpected failures: `{correctness["failures"]}`
- Stranded leases: `{correctness["stranded"]}`
- Negative durations: `{correctness["negative"]}`

## Prometheus Reconciliation

{markdown_table(["Metric", "Raw", "Prometheus", "Difference", "Result"], prometheus_totals(results))}

## Trust Gates

{gate_table}

## Plots

{plots}

## Caveats

- Synthetic bounded 50 ms wait, not actual network or disk I/O.
- Single local Docker host.
- Performance findings are resume-safe only when the overall gate is `PASS`.
"""


def generate(
    results_path: pathlib.Path, report_path: pathlib.Path | None = None
) -> list[pathlib.Path]:
    document = json.loads(results_path.read_text())
    results = valid_results(document, SCENARIO)
    summaries = summary_rows(document, SCENARIO)
    output_dir = results_path.parent
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    throughput = grouped(results, SCENARIO, "workers", "raw.processing_throughput_per_second")
    e2_speedup = [(row["workers"], row["speedup"]) for row in summaries]
    efficiency = [(row["workers"], row["parallel_efficiency"]) for row in summaries]
    queue = grouped(results, SCENARIO, "workers", "raw.queue_p95_seconds")
    e1_speedup = [
        (workers, float(document.get("e1_comparison", {}).get("speedup", {})[str(workers)]))
        for workers in (1, 4, 8, 16)
    ]
    specs = (
        (PLOT_NAMES[0], "50 ms PROCESSING THROUGHPUT", "Tasks/second", [("50 ms", throughput)]),
        (PLOT_NAMES[1], "50 ms speedup", "Speedup vs 1 worker", [("50 ms", e2_speedup)]),
        (PLOT_NAMES[2], "50 ms parallel efficiency", "Efficiency", [("50 ms", efficiency)]),
        (PLOT_NAMES[3], "50 ms queue p95", "Seconds", [("50 ms", queue)]),
        (
            PLOT_NAMES[4],
            "Trusted no-op vs 50 ms speedup",
            "Speedup vs 1 worker",
            [("E1 no-op", e1_speedup), ("E2 50 ms", e2_speedup)],
        ),
    )
    generated = []
    for name, title, y_label, series in specs:
        path = plots_dir / name
        line_plot(path, title, "Workers", y_label, series)
        generated.append(path)
    internal_report = output_dir / "tf-012e2-io50-scaling.md"
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
