#!/usr/bin/env python3
"""Run the publishable TF-012E6 fixed worker-crash recovery experiment."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import time
from typing import Any

from benchmarks.e6_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import ScopedExperiment, run_scoped_experiment
from benchmarks.trusted import TrustedRun, warmup

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e6-recovery.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e6-recovery-storm.md"
E6_SPEC = ScopedExperiment(
    ticket="TF-012E6",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="recovery_storm",
    task_type="test.sleep",
    payload={"duration_ms": 500},
    count_key="recovery_tasks",
    default_project="taskforge-tf012-e6-recovery",
    artifact_names=("tf-012e6-recovery-storm.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e6_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_e6_harness",
    ),
)


def configuration_contract() -> dict[str, Any]:
    profile = json.loads(PROFILE_PATH.read_text())
    expected = {
        "recovery_tasks": 1000,
        "recovery_workers": 20,
        "recovery_schedulers": 3,
        "recovery_kill_workers": 10,
        "recovery_sleep_ms": 500,
        "required_blocks": 3,
    }
    for field, value in expected.items():
        if profile.get(field) != value:
            raise BenchmarkError(f"TF-012E6 requires {field}={value}")
    if profile.get("required_public_scenarios") != [E6_SPEC.scenario]:
        raise BenchmarkError("TF-012E6 profile contains an unrelated scenario")
    if profile.get("workload") != {
        "task_type": "test.sleep",
        "payload": {"duration_ms": 500},
        "max_attempts": 2,
    }:
        raise BenchmarkError("TF-012E6 requires the bounded 500 ms test.sleep workload")
    return profile


def run_recovery_trials(trusted: TrustedRun, specification: ScopedExperiment) -> None:
    profile = trusted.profile
    actual_timing = trusted.harness.trial_configuration()
    if any(
        actual_timing.get(key) != value for key, value in profile["timing_configuration"].items()
    ):
        raise BenchmarkError("committed harness timing differs from the fixed TF-012E6 contract")
    for block in range(1, int(profile["required_blocks"]) + 1):
        reset_started = dt.datetime.now(dt.UTC)
        trusted.harness.reset()
        trusted.harness.start()
        block_warmup = warmup(trusted.harness, int(profile["warmup_tasks"]))
        block_warmup["source"] = "excluded per-trial trusted recovery warmup"
        trusted.block_events.append(
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": reset_started.isoformat(),
                "ready_at": dt.datetime.now(dt.UTC).isoformat(),
                "warmup": block_warmup,
            }
        )
        trial_seed = int(profile["random_seed"]) + block * 100
        trusted.recovery_crash_trial(
            block=block,
            trial=block,
            workers=int(profile["recovery_workers"]),
            schedulers=int(profile["recovery_schedulers"]),
            count=int(profile["recovery_tasks"]),
            killed_workers=int(profile["recovery_kill_workers"]),
            sleep_ms=int(profile["recovery_sleep_ms"]),
            selection_seed=trial_seed,
        )
        time.sleep(float(profile.get("cooldown_seconds", 0)))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E6_SPEC.default_project)
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    profile = configuration_contract()
    if arguments.dry_run:
        print(
            json.dumps(
                {
                    "ticket": E6_SPEC.ticket,
                    "scenario": E6_SPEC.scenario,
                    "logical_tasks_per_trial": profile["recovery_tasks"],
                    "workers": profile["recovery_workers"],
                    "schedulers": profile["recovery_schedulers"],
                    "hard_killed_workers": profile["recovery_kill_workers"],
                    "task_type": E6_SPEC.task_type,
                    "duration_ms": profile["recovery_sleep_ms"],
                    "trials": profile["required_blocks"],
                    "timing_configuration": profile["timing_configuration"],
                    "scenarios": profile["required_public_scenarios"],
                    "will_execute": False,
                },
                indent=2,
            )
        )
        return 0
    return run_scoped_experiment(
        arguments,
        E6_SPEC,
        generate,
        execute_trials=run_recovery_trials,
        document_fields={"timing_configuration": profile["timing_configuration"]},
    )


if __name__ == "__main__":
    raise SystemExit(main())
