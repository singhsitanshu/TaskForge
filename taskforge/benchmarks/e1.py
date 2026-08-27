#!/usr/bin/env python3
"""Run the publishable TF-012E1 no-op-only scaling experiment."""

from __future__ import annotations

import argparse
import pathlib

from benchmarks.e1_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS
from benchmarks.scoped import ScopedExperiment, run_scoped_experiment

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e1-noop.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e1-noop-scaling.md"
E1_SPEC = ScopedExperiment(
    ticket="TF-012E1",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="noop_scaling",
    task_type="test.noop",
    payload={},
    count_key="noop_tasks",
    default_project="taskforge-tf012-e1-noop",
    artifact_names=("tf-012e1-noop-scaling.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e1_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_tools",
        "benchmarks.tests.test_e1_artifacts",
    ),
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E1_SPEC.default_project)
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--keep", action="store_true")
    return parser.parse_args()


def main() -> int:
    return run_scoped_experiment(parse_arguments(), E1_SPEC, generate)


if __name__ == "__main__":
    raise SystemExit(main())
