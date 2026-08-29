#!/usr/bin/env python3
"""Generate the scoped TF-012E6 recovery report and four plots."""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
from typing import Any

from benchmarks.e1_artifacts import markdown_table, number, prometheus_totals, valid_results
from benchmarks.plot import line_plot

SCENARIO = "recovery_storm"
PLOT_NAMES = (
    "01-recovery-lag-p95.svg",
    "02-kill-to-final-drain.svg",
    "03-affected-vs-abandoned.svg",
    "04-attempt-composition.svg",
)


def recovery_rows(document: dict[str, Any]) -> list[dict[str, Any]]:
    results = sorted(
        valid_results(document, SCENARIO), key=lambda item: (int(item["block"]), int(item["trial"]))
    )
    rows = []
    for item in results:
        raw = item["raw"]
        correctness = item["correctness"]
        boundary = item["failure_boundary"]
        rows.append(
            {
                "trial": int(item["trial"]),
                "block": int(item["block"]),
                "affected_tasks": int(correctness["affected_tasks"]),
                "abandoned_attempts": int(correctness["abandoned_attempts"]),
                "first_attempt_successes": int(correctness["first_attempt_successes"]),
                "recovered_replacement_successes": int(
                    correctness["recovered_replacement_successes"]
                ),
                "total_attempts": int(correctness["actual_attempts"]),
                "recovery_p50": raw.get("recovery_lag_p50_seconds"),
                "recovery_p95": raw.get("recovery_lag_p95_seconds"),
                "recovery_p99": raw.get("recovery_lag_p99_seconds"),
                "kill_to_drain": raw.get("kill_to_final_drain_seconds"),
                "processing_throughput": raw.get("processing_throughput_per_second"),
                "killed_dead": int(boundary["worker_liveness"]["killed_dead"]),
                "surviving_active": int(boundary["worker_liveness"]["surviving_active"]),
                "selection_seed": boundary.get("selection_seed"),
                "recovery_batch_p95": item.get("prometheus_reconciliation", {})
                .get("histograms", {})
                .get("recovery_batch", {})
                .get("prometheus_quantiles", {})
                .get("p95"),
            }
        )
    return rows


def recovery_aggregate(document: dict[str, Any]) -> dict[str, Any]:
    rows = recovery_rows(document)
    aggregate: dict[str, Any] = {}
    for field in (
        "affected_tasks",
        "abandoned_attempts",
        "first_attempt_successes",
        "recovered_replacement_successes",
        "total_attempts",
        "recovery_p50",
        "recovery_p95",
        "recovery_p99",
        "kill_to_drain",
        "processing_throughput",
        "recovery_batch_p95",
    ):
        values = [float(row[field]) for row in rows if row.get(field) is not None]
        aggregate[f"{field}_median"] = statistics.median(values) if values else None
    return aggregate


def plot_specs(document: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    rows = recovery_rows(document)
    return (
        (
            PLOT_NAMES[0],
            "Recovery Lag p95 by Trial",
            "Seconds",
            [("Recovery lag p95", [(row["trial"], row["recovery_p95"]) for row in rows])],
        ),
        (
            PLOT_NAMES[1],
            "Kill to Final Drain by Trial",
            "Seconds",
            [("Kill to drain", [(row["trial"], row["kill_to_drain"]) for row in rows])],
        ),
        (
            PLOT_NAMES[2],
            "Affected Tasks vs ABANDONED Attempts",
            "Count",
            [
                ("Affected", [(row["trial"], row["affected_tasks"]) for row in rows]),
                ("ABANDONED", [(row["trial"], row["abandoned_attempts"]) for row in rows]),
            ],
        ),
        (
            PLOT_NAMES[3],
            "Final Attempt Composition",
            "Tasks",
            [
                (
                    "First-attempt success",
                    [(row["trial"], row["first_attempt_successes"]) for row in rows],
                ),
                (
                    "ABANDONED to success",
                    [(row["trial"], row["recovered_replacement_successes"]) for row in rows],
                ),
            ],
        ),
    )


def render_report(document: dict[str, Any], artifact_prefix: str = "") -> str:
    results = valid_results(document, SCENARIO)
    rows = recovery_rows(document)
    aggregate = recovery_aggregate(document)
    source = document.get("source", {})
    environment = document.get("environment", {}) or {}
    profile = document.get("profile", {}) or {}
    timing = document.get("timing_configuration", {}) or profile.get("timing_configuration", {})
    gates = document.get("trust", {}) or {}
    results_table = markdown_table(
        [
            "Trial",
            "Affected",
            "ABANDONED",
            "Recovery p50",
            "p95",
            "p99",
            "Kill-to-drain",
            "Tasks/s",
        ],
        [
            [
                str(row["trial"]),
                str(row["affected_tasks"]),
                str(row["abandoned_attempts"]),
                number(row["recovery_p50"]),
                number(row["recovery_p95"]),
                number(row["recovery_p99"]),
                number(row["kill_to_drain"]),
                number(row["processing_throughput"], 2),
            ]
            for row in rows
        ],
    )
    composition_table = markdown_table(
        ["Trial", "First-attempt success", "ABANDONED to success", "Total attempts"],
        [
            [
                str(row["trial"]),
                str(row["first_attempt_successes"]),
                str(row["recovered_replacement_successes"]),
                str(row["total_attempts"]),
            ]
            for row in rows
        ],
    )
    boundary_table = markdown_table(
        ["Trial", "Seed", "Killed DEAD", "Survivors ACTIVE", "Affected"],
        [
            [
                str(row["trial"]),
                str(row["selection_seed"]),
                str(row["killed_dead"]),
                str(row["surviving_active"]),
                str(row["affected_tasks"]),
            ]
            for row in rows
        ],
    )
    gate_table = markdown_table(
        ["Gate", "Result"],
        [[name, str(value.get("result"))] for name, value in gates.items()],
    )
    plots = "\n".join(f"- [{name}]({artifact_prefix}plots/{name})" for name in PLOT_NAMES)
    return f"""# TF-012E6 Recovery Storm

## Verdict

{gates.get("overall", {}).get("result", "FAIL")} — TF-012E6 trusted worker-crash recovery experiment

## Provenance

- Run ID: `{document.get("run_id")}`
- Commit: `{source.get("git_commit_sha")}`
- Tree hash: `{source.get("git_tree_hash")}`
- Clean source: `{source.get("clean")}`
- Platform: `{environment.get("platform")}`

## Methodology

Each independent trial submits `{profile.get("recovery_tasks")}` bounded
`test.sleep` tasks (`{profile.get("recovery_sleep_ms")}` ms), starts
`{profile.get("recovery_workers")}` workers and `{profile.get("recovery_schedulers")}`
schedulers, then deterministically hard-kills exactly
`{profile.get("recovery_kill_workers")}` worker containers with active owned work.

## Lease and Recovery Configuration

{markdown_table(["Setting", "Value"], [[key, str(value)] for key, value in timing.items()])}

## Failure Boundary

Every trial archives `failure_boundary.json`. Its task IDs, attempt identities,
owners, lease expirations, selected containers, kill timestamp, and exact Prometheus
churn allowlist are the authority for the expected ABANDONED set.

{boundary_table}

## Affected-Task Counts

{results_table}

Median affected / ABANDONED: `{number(aggregate["affected_tasks_median"], 0)}` /
`{number(aggregate["abandoned_attempts_median"], 0)}`.

## Recovery Lag

Median p50/p95/p99: `{number(aggregate["recovery_p50_median"])}` /
`{number(aggregate["recovery_p95_median"])}` /
`{number(aggregate["recovery_p99_median"])}` seconds.

Raw immutable `recovered_at - recovered_lease_expires_at` is authoritative.

## Kill-to-Drain

Median kill-to-final-drain: `{number(aggregate["kill_to_drain_median"])}` seconds.

## Attempt Composition

{composition_table}

## Worker Liveness

Killed worker database identities must eventually classify DEAD and every untouched
starting worker must remain ACTIVE. Liveness is diagnostic; expired leases authorize
recovery.

## Scheduler Contention

Three schedulers may scan concurrently. Exact per-task history validation rejects
duplicate abandonment, requeue, or replacement attempt numbering.

## Correctness

- Valid trials: `{len(results)}` / `{len(document.get("results", []))}`
- Unexpected histories: `{sum(int(item["correctness"]["unexpected_recovery_histories"]) for item in results)}`
- Unaffected ABANDONED: `{sum(int(item["correctness"]["unaffected_abandoned_attempts"]) for item in results)}`
- Duplicate recoveries: `{sum(int(item["correctness"]["duplicate_recovery_abandonments"]) for item in results)}`
- Stranded leases: `{sum(int(item["correctness"]["stranded_leases"]) for item in results)}`

## Prometheus Reconciliation

Scheduler-owned recovery counters and histograms are authoritative. Worker-local
counters from killed processes are deliberately not claimed as reconstructable.

{markdown_table(["Metric", "Raw", "Prometheus", "Difference", "Result"], prometheus_totals(results))}

Recovery-batch p95 median: `{number(aggregate["recovery_batch_p95_median"])}` seconds.

## Trust Gates

{gate_table}

## Plots

{plots}

## Caveats

- Synthetic hard-kill injection on one local Docker host.
- Replacement workers restore the fixed 20-worker drain configuration.
- Worker liveness classification is diagnostic, not recovery authority.
- Stale-renewal acceptance has no standalone durable counter; exact captured-attempt
  abandonment and the recorded worker/scheduler regression tests provide the available evidence.
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
    internal_report = output_dir / "tf-012e6-recovery-storm.md"
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
