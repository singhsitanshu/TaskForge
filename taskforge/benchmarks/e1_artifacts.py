#!/usr/bin/env python3
"""Generate the narrowly scoped TF-012E1 no-op report and five plots."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

from benchmarks.plot import grouped, line_plot, prometheus_points

PLOT_NAMES = (
    "01-noop-processing-throughput.svg",
    "02-noop-speedup.svg",
    "03-noop-parallel-efficiency.svg",
    "04-noop-queue-p95.svg",
    "05-noop-claim-p95.svg",
)


def valid_results(document: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        item
        for item in document.get("results", [])
        if item.get("scenario") == "noop_scaling"
        and item.get("classification") == "PUBLIC"
        and item.get("valid") is True
    ]


def summary_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    results = valid_results(document)
    by_worker: dict[int, list[dict[str, Any]]] = {}
    for item in results:
        by_worker.setdefault(int(item["workers"]), []).append(item)
    rows = []
    for workers, items in sorted(by_worker.items()):
        values = [float(item["raw"]["processing_throughput_per_second"]) for item in items]
        mean = statistics.fmean(values)
        median = statistics.median(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0
        block_values = {
            int(item["block"]): float(item["raw"]["processing_throughput_per_second"])
            for item in items
        }
        rows.append(
            {
                "workers": workers,
                "processing_throughput_median": median,
                "processing_throughput_min": min(values),
                "processing_throughput_max": max(values),
                "processing_throughput_cv": stdev / mean if mean else None,
                "block_values": block_values,
                "maximum_block_shift": (max(values) - min(values)) / median if median else None,
            }
        )
    baseline = next(
        (row["processing_throughput_median"] for row in rows if row["workers"] == 1), None
    )
    for row in rows:
        speedup = row["processing_throughput_median"] / baseline if baseline else None
        row["speedup"] = speedup
        row["parallel_efficiency"] = speedup / row["workers"] if speedup is not None else None
    return rows


def median_measurement(items: list[dict[str, Any]], path: tuple[str, ...]) -> float | None:
    values = []
    for item in items:
        current: Any = item
        for part in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(part)
        if isinstance(current, int | float):
            values.append(float(current))
    return statistics.median(values) if values else None


def latency_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    results = valid_results(document)
    rows = []
    for workers in sorted({int(item["workers"]) for item in results}):
        items = [item for item in results if int(item["workers"]) == workers]
        row: dict[str, Any] = {"workers": workers}
        for name in ("queue", "execution", "total"):
            for quantile in ("p50", "p95", "p99"):
                row[f"{name}_{quantile}"] = median_measurement(
                    items, ("raw", f"{name}_{quantile}_seconds")
                )
        for quantile in ("p50", "p95", "p99"):
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


def number(value: Any, digits: int = 4) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}f}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    return "\n".join(
        [
            "| " + " | ".join(headers) + " |",
            "|" + "|".join("---" for _ in headers) + "|",
            *("| " + " | ".join(row) + " |" for row in rows),
        ]
    )


def prometheus_totals(results: list[dict[str, Any]]) -> list[list[str]]:
    totals: dict[str, dict[str, Any]] = {}
    for item in results:
        for metric, evidence in (
            item.get("prometheus_reconciliation", {}).get("counters", {}).items()
        ):
            total = totals.setdefault(
                metric, {"raw": 0.0, "prometheus": 0.0, "difference": 0.0, "passed": True}
            )
            total["raw"] += float(evidence.get("raw", 0))
            total["prometheus"] += float(evidence.get("prometheus", 0))
            total["difference"] += float(evidence.get("difference", 0))
            total["passed"] = total["passed"] and evidence.get("status") == "PASS"
    return [
        [
            metric,
            number(values["raw"], 0),
            number(values["prometheus"], 0),
            number(values["difference"], 0),
            "PASS" if values["passed"] else "FAIL",
        ]
        for metric, values in sorted(totals.items())
    ]


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document)
    summaries = summary_rows(document)
    latencies = latency_rows(document)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    images = document.get("images", {})
    block_orders = []
    for block in sorted({int(item["block"]) for item in results}):
        ordered = sorted(
            (item for item in results if int(item["block"]) == block),
            key=lambda item: int(item.get("order_index") or 0),
        )
        seed = ordered[0].get("random_seed") if ordered else None
        block_orders.append(
            f"- Block {block}: "
            + ", ".join(str(item["workers"]) for item in ordered)
            + f" workers (seed `{seed}`)"
        )
    image_lines = [
        f"- {name}: `{value.get('image_id')}`; RepoDigests: "
        f"`{', '.join(value.get('repo_digests', [])) or 'none'}`"
        for name, value in sorted(images.items())
    ]
    throughput_table = markdown_table(
        ["Workers", "Median", "Min", "Max", "CV", "Speedup", "Efficiency", "Max shift"],
        [
            [
                str(row["workers"]),
                number(row["processing_throughput_median"], 2),
                number(row["processing_throughput_min"], 2),
                number(row["processing_throughput_max"], 2),
                number(row["processing_throughput_cv"], 4),
                number(row["speedup"], 3),
                number(row["parallel_efficiency"], 3),
                number(row["maximum_block_shift"], 4),
            ]
            for row in summaries
        ],
    )
    latency_table = markdown_table(
        ["Workers", "Queue p95", "Claim p95*", "Execution p95", "Total p95"],
        [
            [
                str(row["workers"]),
                number(row["queue_p95"]),
                number(row["claim_p95"]),
                number(row["execution_p95"]),
                number(row["total_p95"]),
            ]
            for row in latencies
        ],
    )
    raw_trial_table = markdown_table(
        [
            "Block",
            "Workers",
            "Processing tasks/s",
            "Queue p50/p95/p99",
            "Claim p50/p95/p99*",
            "Execution p50/p95/p99",
            "Total p50/p95/p99",
        ],
        [
            [
                str(item["block"]),
                str(item["workers"]),
                number(item["raw"].get("processing_throughput_per_second"), 2),
                "/".join(
                    number(item["raw"].get(f"queue_{quantile}_seconds"))
                    for quantile in ("p50", "p95", "p99")
                ),
                "/".join(
                    number(
                        item.get("prometheus_reconciliation", {})
                        .get("histograms", {})
                        .get("claim", {})
                        .get("prometheus_quantiles", {})
                        .get(quantile)
                    )
                    for quantile in ("p50", "p95", "p99")
                ),
                "/".join(
                    number(item["raw"].get(f"execution_{quantile}_seconds"))
                    for quantile in ("p50", "p95", "p99")
                ),
                "/".join(
                    number(item["raw"].get(f"total_{quantile}_seconds"))
                    for quantile in ("p50", "p95", "p99")
                ),
            ]
            for item in sorted(
                results, key=lambda value: (value["block"], value.get("order_index") or 0)
            )
        ],
    )
    blocks_table = markdown_table(
        ["Workers", "Block 1", "Block 2", "Block 3"],
        [
            [
                str(row["workers"]),
                *(number(row["block_values"].get(block), 2) for block in (1, 2, 3)),
            ]
            for row in summaries
        ],
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
    }
    gates = document.get("trust", {}) or {}
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    task_count = document.get("profile", {}).get("noop_tasks")
    return f"""# TF-012E1 No-op Scaling

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E1 trusted no-op experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Branch: `{source.get("git_branch")}`
- Clean source: `{source.get("clean")}`

{chr(10).join(image_lines)}

## Environment

- Platform: `{environment.get("platform")}`
- CPU: `{environment.get("host_cpu")}`
- Logical cores: `{environment.get("host_logical_cpus")}`
- Memory bytes: `{environment.get("host_memory_bytes")}`
- Docker: `{environment.get("docker_version")}`
- PostgreSQL: `{environment.get("postgresql")}`
- Go: `{environment.get("go_version")}`
- Python: `{environment.get("python_version")}`

## Methodology

Exactly `{task_count}` `test.noop` tasks were submitted for each measured configuration. Worker counts were 1, 4, 8, and 16. One observation per configuration was captured in each of three independently reset blocks. Each block performed excluded warm-up work before its seeded randomized order. Every trial recreated measured processes, reset Prometheus, waited for fresh boundary scrapes, and archived immutable task/attempt rows.

{chr(10).join(block_orders)}

## PROCESSING THROUGHPUT

Values are tasks/second and use representative medians of valid raw trials.

{throughput_table}

## Latency

Values are seconds. Queue, execution, and total latency come from immutable raw lifecycle timestamps. Claim latency marked `*` is secondary Prometheus delta-histogram evidence because PostgreSQL has no claim-transaction timestamp pair.

{latency_table}

### Per-trial raw measurements

{raw_trial_table}

## Independent Blocks — PROCESSING THROUGHPUT

{blocks_table}

## Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- Tasks: `{correctness["tasks"]}`
- Attempts: `{correctness["attempts"]}`
- Duplicate attempt identities: `{correctness["duplicates"]}`
- Unexpected failures/non-successes: `{correctness["failures"]}`
- Stranded leases/ownership: `{correctness["stranded"]}`

## Prometheus Reconciliation

{markdown_table(["Metric", "Raw", "Prometheus", "Difference", "Result"], prometheus_totals(results))}

Prometheus quantiles are secondary validation; raw immutable timestamps remain authoritative for benchmark latency.

## Trust Gates

{gate_table}

## Plots

{plots}

## Resume-Safe Findings

Performance numbers in this report are resume-safe only when the overall trust gate above is `PASS`. Representative medians, never best-case trials, are used.

## Caveats

- Single local hardware host.
- Docker-based environment, not a multi-node deployment.
- Synthetic no-op workload; it does not represent I/O-bound or CPU-bound handlers.

## Remaining TF-012 Work

I/O, CPU, API, retry, recovery, saturation, sensitivity, stability, and comprehensive final benchmark reporting remain separate tickets.
"""


def generate(
    results_path: pathlib.Path, report_path: pathlib.Path | None = None
) -> list[pathlib.Path]:
    document = json.loads(results_path.read_text())
    results = valid_results(document)
    output_dir = results_path.parent
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    throughput = grouped(results, "noop_scaling", "workers", "raw.processing_throughput_per_second")
    baseline = next((measured for workers, measured in throughput if workers == 1), None)
    speedup = [
        (workers, measured / baseline) for workers, measured in throughput if baseline is not None
    ]
    efficiency = [(workers, value / workers) for workers, value in speedup]
    queue = grouped(results, "noop_scaling", "workers", "raw.queue_p95_seconds")
    claim = prometheus_points(results, "noop_scaling", "workers", "claim_p95")
    specs = (
        (PLOT_NAMES[0], "No-op PROCESSING THROUGHPUT", "Tasks/second", throughput),
        (PLOT_NAMES[1], "No-op speedup", "Speedup vs 1 worker", speedup),
        (PLOT_NAMES[2], "No-op parallel efficiency", "Efficiency", efficiency),
        (PLOT_NAMES[3], "No-op queue p95", "Seconds", queue),
        (PLOT_NAMES[4], "No-op claim p95 (Prometheus delta)", "Seconds", claim),
    )
    generated = []
    for name, title, y_label, points in specs:
        path = plots_dir / name
        line_plot(path, title, "Workers", y_label, [("noop", points)])
        generated.append(path)
    internal_report = output_dir / "tf-012e1-noop-scaling.md"
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
