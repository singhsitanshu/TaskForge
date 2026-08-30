#!/usr/bin/env python3
"""Generate the scoped TF-012E4 API submission report and three plots."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

from benchmarks.e1_artifacts import markdown_table, number, prometheus_totals, valid_results
from benchmarks.plot import line_plot

SCENARIO = "api_submission"
PLOT_NAMES = (
    "01-api-submission-throughput.svg",
    "02-api-request-p95.svg",
    "03-api-request-p99.svg",
)


def response_class_total(submission: dict[str, Any], prefix: str) -> int:
    return sum(
        int(value)
        for status, value in (submission.get("status_counts", {}) or {}).items()
        if str(status).startswith(prefix)
    )


def api_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    results = valid_results(document, SCENARIO)
    rows = []
    for concurrency in sorted({int(item["api_concurrency"]) for item in results}):
        items = [item for item in results if int(item["api_concurrency"]) == concurrency]
        throughput = [float(item["raw"]["submission_throughput_per_second"]) for item in items]
        mean = statistics.fmean(throughput)
        rows.append(
            {
                "concurrency": concurrency,
                "submission_throughput_median": statistics.median(throughput),
                "submission_throughput_min": min(throughput),
                "submission_throughput_max": max(throughput),
                "submission_throughput_cv": (
                    statistics.stdev(throughput) / mean if len(throughput) > 1 and mean else 0.0
                ),
                "request_p50_ms": statistics.median(
                    float(item["submission"]["latency_ms"]["p50"]) for item in items
                ),
                "request_p95_ms": statistics.median(
                    float(item["submission"]["latency_ms"]["p95"]) for item in items
                ),
                "request_p99_ms": statistics.median(
                    float(item["submission"]["latency_ms"]["p99"]) for item in items
                ),
                "http_2xx": sum(response_class_total(item["submission"], "2") for item in items),
                "http_4xx": sum(response_class_total(item["submission"], "4") for item in items),
                "http_5xx": sum(response_class_total(item["submission"], "5") for item in items),
                "transport_errors": sum(
                    sum(int(value) for value in item["submission"].get("error_counts", {}).values())
                    for item in items
                ),
                "block_values": {
                    int(item["block"]): float(item["raw"]["submission_throughput_per_second"])
                    for item in items
                },
            }
        )
    return rows


def plot_specs(document: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    rows = api_rows(document)
    throughput = [(row["concurrency"], row["submission_throughput_median"]) for row in rows]
    p95 = [(row["concurrency"], row["request_p95_ms"]) for row in rows]
    p99 = [(row["concurrency"], row["request_p99_ms"]) for row in rows]
    return (
        (
            PLOT_NAMES[0],
            "API SUBMISSION THROUGHPUT vs Concurrency",
            "Requests/second",
            [("Keyless POST /tasks", throughput)],
        ),
        (
            PLOT_NAMES[1],
            "API Request p95 vs Concurrency",
            "Milliseconds",
            [("POST /tasks p95", p95)],
        ),
        (
            PLOT_NAMES[2],
            "API Request p99 vs Concurrency",
            "Milliseconds",
            [("POST /tasks p99", p99)],
        ),
    )


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document, SCENARIO)
    rows = api_rows(document)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    profile = document.get("profile", {}) or {}
    gates = document.get("trust", {}) or {}
    orders = []
    for block in sorted({int(item["block"]) for item in results}):
        ordered = sorted(
            (item for item in results if int(item["block"]) == block),
            key=lambda item: int(item.get("order_index") or 0),
        )
        orders.append(
            f"- Block {block}: "
            + ", ".join(f"c{item['api_concurrency']}" for item in ordered)
            + f" (seed `{ordered[0].get('random_seed') if ordered else None}`)"
        )
    throughput_table = markdown_table(
        ["Concurrency", "Median req/s", "Min", "Max", "CV"],
        [
            [
                str(row["concurrency"]),
                number(row["submission_throughput_median"], 2),
                number(row["submission_throughput_min"], 2),
                number(row["submission_throughput_max"], 2),
                number(row["submission_throughput_cv"], 4),
            ]
            for row in rows
        ],
    )
    latency_table = markdown_table(
        ["Concurrency", "p50 ms", "p95 ms", "p99 ms"],
        [
            [
                str(row["concurrency"]),
                number(row["request_p50_ms"], 3),
                number(row["request_p95_ms"], 3),
                number(row["request_p99_ms"], 3),
            ]
            for row in rows
        ],
    )
    blocks_table = markdown_table(
        ["Concurrency", "Block 1", "Block 2", "Block 3"],
        [
            [
                str(row["concurrency"]),
                *(number(row["block_values"].get(block), 2) for block in (1, 2, 3)),
            ]
            for row in rows
        ],
    )
    error_table = markdown_table(
        ["Concurrency", "2xx", "4xx", "5xx", "Transport"],
        [
            [
                str(row["concurrency"]),
                str(row["http_2xx"]),
                str(row["http_4xx"]),
                str(row["http_5xx"]),
                str(row["transport_errors"]),
            ]
            for row in rows
        ],
    )
    correctness = {
        "requests": sum(int(item["correctness"]["actual_http_requests"]) for item in results),
        "successes": sum(int(item["correctness"]["successful_responses"]) for item in results),
        "tasks": sum(int(item["correctness"]["actual_tasks"]) for item in results),
        "attempts": sum(int(item["correctness"]["actual_attempts"]) for item in results),
        "queued": sum(int(item["correctness"]["queued_tasks"]) for item in results),
    }
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    return f"""# TF-012E4 API Submission

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E4 trusted API submission experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Clean source: `{source.get("clean")}`
- Platform: `{environment.get("platform")}`

## Methodology

Each configuration issues `{profile.get("api_requests")}` normal keyless `POST /tasks`
requests through one API replica while measured task-processing workers are disabled.
Submitted tasks intentionally remain `QUEUED`; submission throughput is not worker
processing throughput.

Concurrency levels: `{", ".join(str(value) for value in profile.get("api_concurrency", []))}`.

{chr(10).join(orders)}

## SUBMISSION THROUGHPUT

{throughput_table}

## HTTP Latency

{latency_table}

## Block Variation

{blocks_table}

## Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- HTTP requests/successes: `{correctness["requests"]}` / `{correctness["successes"]}`
- PostgreSQL task rows: `{correctness["tasks"]}`
- Attempts created with workers disabled: `{correctness["attempts"]}`
- Expected queued tasks: `{correctness["queued"]}`

{error_table}

## Prometheus Reconciliation

{markdown_table(["Metric", "Raw", "Prometheus", "Difference", "Result"], prometheus_totals(results))}

## Trust Gates

{gate_table}

## Plots

{plots}

## Caveats

- Measures API submission only; it is not processing capacity.
- Synthetic `test.noop` task payload with no worker execution during the measured window.
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
        line_plot(path, title, "Concurrency", y_label, series)
        generated.append(path)
    internal_report = output_dir / "tf-012e4-api-submission.md"
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
