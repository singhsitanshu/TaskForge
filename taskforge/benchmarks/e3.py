#!/usr/bin/env python3
"""Run the publishable TF-012E3 deterministic CPU-only scaling experiment."""

from __future__ import annotations

import argparse
import json
import pathlib
from typing import Any

from benchmarks.e3_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import (
    ScopedExperiment,
    load_trusted_scaling_comparison,
    run_scoped_experiment,
)

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e3-cpu.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e3-cpu-scaling.md"
EXPECTED_WORKERS = [1, 4, 8, 16]
CPU_ITERATIONS = 200000
E3_SPEC = ScopedExperiment(
    ticket="TF-012E3",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="cpu_scaling",
    task_type="test.cpu",
    payload={"iterations": CPU_ITERATIONS},
    count_key="cpu_tasks",
    default_project="taskforge-tf012-e3-cpu",
    artifact_names=("tf-012e3-cpu-scaling.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e3_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_e3_harness",
    ),
)


def configuration_contract(*, tasks: int | None = None, seed: int | None = None) -> dict[str, Any]:
    profile = json.loads(PROFILE_PATH.read_text())
    if tasks is not None:
        if tasks <= 0:
            raise BenchmarkError("--tasks must be positive")
        profile["cpu_tasks"] = tasks
    if seed is not None:
        profile["random_seed"] = seed
    if profile.get("scaling_workers") != EXPECTED_WORKERS:
        raise BenchmarkError("TF-012E3 workers must be exactly 1,4,8,16")
    if profile.get("required_blocks") != 3:
        raise BenchmarkError("TF-012E3 requires exactly three independent blocks")
    workload = profile.get("workload", {})
    if workload != {"task_type": "test.cpu", "iterations": CPU_ITERATIONS}:
        raise BenchmarkError(
            f"TF-012E3 workload must be exactly test.cpu iterations={CPU_ITERATIONS}"
        )
    if E3_SPEC.payload != {"iterations": CPU_ITERATIONS}:
        raise BenchmarkError("TF-012E3 runner payload does not match the fixed profile workload")
    if profile.get("required_public_scenarios") != [E3_SPEC.scenario]:
        raise BenchmarkError("TF-012E3 profile contains an unrelated scenario")
    return profile


def load_trusted_e1(results_path: pathlib.Path) -> dict[str, Any]:
    return load_trusted_scaling_comparison(results_path, ticket="TF-012E1", scenario="noop_scaling")


def load_trusted_e2(results_path: pathlib.Path) -> dict[str, Any]:
    return load_trusted_scaling_comparison(results_path, ticket="TF-012E2", scenario="io50_scaling")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E3_SPEC.default_project)
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--e1-results", type=pathlib.Path)
    parser.add_argument("--e2-results", type=pathlib.Path)
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
                    "ticket": E3_SPEC.ticket,
                    "scenario": E3_SPEC.scenario,
                    "task_type": E3_SPEC.task_type,
                    "payload": E3_SPEC.payload,
                    "workers": profile["scaling_workers"],
                    "blocks": profile["required_blocks"],
                    "tasks": profile["cpu_tasks"],
                    "scenarios": profile["required_public_scenarios"],
                    "measured_trials": len(profile["scaling_workers"]) * profile["required_blocks"],
                    "will_execute": False,
                },
                indent=2,
            )
        )
        return 0
    if arguments.e1_results is None or arguments.e2_results is None:
        raise BenchmarkError("--e1-results and --e2-results are required")
    e1_comparison = load_trusted_e1(arguments.e1_results.resolve())
    e2_comparison = load_trusted_e2(arguments.e2_results.resolve())
    return run_scoped_experiment(
        arguments,
        E3_SPEC,
        generate,
        profile_overrides={
            "cpu_tasks": profile["cpu_tasks"],
            "random_seed": profile["random_seed"],
        },
        document_fields={
            "e1_comparison": e1_comparison,
            "e2_comparison": e2_comparison,
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())
