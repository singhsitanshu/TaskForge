#!/usr/bin/env python3
"""Run the publishable TF-012E2 synthetic 50 ms-only scaling experiment."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from benchmarks.e1_artifacts import summary_rows
from benchmarks.e2_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import ScopedExperiment, run_scoped_experiment
from benchmarks.trust import evaluate_run_directory, sha256_file

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e2-io50.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e2-io50-scaling.md"
EXPECTED_WORKERS = [1, 4, 8, 16]
E2_SPEC = ScopedExperiment(
    ticket="TF-012E2",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="io50_scaling",
    task_type="test.sleep",
    payload={"duration_ms": 50},
    count_key="io_tasks",
    default_project="taskforge-tf012-e2-io50",
    artifact_names=("tf-012e2-io50-scaling.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e2_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_e2_harness",
    ),
)


def configuration_contract(*, tasks: int | None = None, seed: int | None = None) -> dict[str, Any]:
    profile = json.loads(PROFILE_PATH.read_text())
    if tasks is not None:
        if tasks <= 0:
            raise BenchmarkError("--tasks must be positive")
        profile["io_tasks"] = tasks
    if seed is not None:
        profile["random_seed"] = seed
    if profile.get("scaling_workers") != EXPECTED_WORKERS:
        raise BenchmarkError("TF-012E2 workers must be exactly 1,4,8,16")
    if profile.get("required_blocks") != 3:
        raise BenchmarkError("TF-012E2 requires exactly three independent blocks")
    workload = profile.get("workload", {})
    if workload != {"task_type": "test.sleep", "duration_ms": 50}:
        raise BenchmarkError("TF-012E2 workload must be exactly test.sleep duration_ms=50")
    if profile.get("required_public_scenarios") != [E2_SPEC.scenario]:
        raise BenchmarkError("TF-012E2 profile contains an unrelated scenario")
    return profile


def load_trusted_e1(results_path: pathlib.Path) -> dict[str, Any]:
    if results_path.name != "results.json" or not results_path.is_file():
        raise BenchmarkError("--e1-results must identify an existing results.json")
    trust = evaluate_run_directory(results_path.parent)
    if trust.get("overall", {}).get("result") != "PASS":
        raise BenchmarkError("E1 comparison artifact does not pass the trust evaluator")
    document = json.loads(results_path.read_text())
    if document.get("tf_ticket") != "TF-012E1":
        raise BenchmarkError("comparison artifact is not a TF-012E1 run")
    rows = summary_rows(document, "noop_scaling")
    if [row["workers"] for row in rows] != EXPECTED_WORKERS:
        raise BenchmarkError("trusted E1 comparison does not contain workers 1,4,8,16")
    return {
        "run_id": document.get("run_id"),
        "commit": document.get("source", {}).get("git_commit_sha"),
        "tree": document.get("source", {}).get("git_tree_hash"),
        "results_sha256": sha256_file(results_path),
        "trust": "PASS",
        "speedup": {str(row["workers"]): row["speedup"] for row in rows},
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E2_SPEC.default_project)
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--e1-results", type=pathlib.Path)
    parser.add_argument("--tasks", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    profile = configuration_contract(tasks=arguments.tasks, seed=arguments.seed)
    if arguments.dry_run:
        print(
            json.dumps(
                {
                    "ticket": E2_SPEC.ticket,
                    "scenario": E2_SPEC.scenario,
                    "task_type": E2_SPEC.task_type,
                    "payload": E2_SPEC.payload,
                    "workers": profile["scaling_workers"],
                    "blocks": profile["required_blocks"],
                    "tasks": profile["io_tasks"],
                    "scenarios": profile["required_public_scenarios"],
                    "measured_trials": len(profile["scaling_workers"]) * profile["required_blocks"],
                    "will_execute": False,
                },
                indent=2,
            )
        )
        return 0
    if arguments.e1_results is None:
        raise BenchmarkError("--e1-results is required for the trusted comparison")
    comparison = load_trusted_e1(arguments.e1_results.resolve())
    overrides = {
        "io_tasks": profile["io_tasks"],
        "random_seed": profile["random_seed"],
    }
    return run_scoped_experiment(
        arguments,
        E2_SPEC,
        generate,
        profile_overrides=overrides,
        document_fields={"e1_comparison": comparison},
    )


if __name__ == "__main__":
    raise SystemExit(main())
