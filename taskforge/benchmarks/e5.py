#!/usr/bin/env python3
"""Run the publishable TF-012E5 fail-once retry-storm experiment."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import time
from typing import Any

from benchmarks.e5_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import ScopedExperiment, run_scoped_experiment
from benchmarks.trusted import TrustedRun, warmup

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e5-retry.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e5-retry-storm.md"
EXPECTED_RETRY_CONFIGURATION = {
    "base_delay": "100ms",
    "max_delay": "100ms",
    "jitter": "0",
    "promotion_interval": "100ms",
}
E5_SPEC = ScopedExperiment(
    ticket="TF-012E5",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="retry_storm",
    task_type="test.fail_n_then_succeed",
    payload={"failures": 1},
    count_key="retry_tasks",
    default_project="taskforge-tf012-e5-retry",
    artifact_names=("tf-012e5-retry-storm.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e5_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_e5_harness",
    ),
)


def configuration_contract() -> dict[str, Any]:
    profile = json.loads(PROFILE_PATH.read_text())
    if profile.get("retry_tasks") != 1000:
        raise BenchmarkError("TF-012E5 requires exactly 1000 logical tasks per trial")
    if profile.get("retry_workers") != 10 or profile.get("retry_schedulers") != 3:
        raise BenchmarkError("TF-012E5 requires exactly 10 workers and 3 schedulers")
    if profile.get("required_blocks") != 3:
        raise BenchmarkError("TF-012E5 requires exactly three independent trials")
    if profile.get("required_public_scenarios") != [E5_SPEC.scenario]:
        raise BenchmarkError("TF-012E5 profile contains an unrelated scenario")
    if profile.get("workload") != {
        "task_type": "test.fail_n_then_succeed",
        "failures": 1,
        "max_attempts": 2,
    }:
        raise BenchmarkError("TF-012E5 workload must be deterministic fail-once then succeed")
    if profile.get("retry_configuration") != EXPECTED_RETRY_CONFIGURATION:
        raise BenchmarkError("TF-012E5 retry timing must match the committed harness configuration")
    return profile


def run_retry_trials(trusted: TrustedRun, specification: ScopedExperiment) -> None:
    profile = trusted.profile
    count = int(profile["retry_tasks"])
    base_seed = int(profile["random_seed"])
    for block in range(1, int(profile["required_blocks"]) + 1):
        reset_started = dt.datetime.now(dt.UTC)
        trusted.harness.reset()
        trusted.harness.start()
        block_warmup = warmup(trusted.harness, int(profile["warmup_tasks"]))
        block_warmup["source"] = "excluded per-trial trusted retry warmup"
        trusted.block_events.append(
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": reset_started.isoformat(),
                "ready_at": dt.datetime.now(dt.UTC).isoformat(),
                "warmup": block_warmup,
            }
        )
        trial_seed = base_seed + block * 100 + sum(ord(value) for value in specification.scenario)
        trusted.processing_trial(
            scenario=specification.scenario,
            variant="fail-once",
            block=block,
            trial=block,
            workers=int(profile["retry_workers"]),
            schedulers=int(profile["retry_schedulers"]),
            task_type=specification.task_type,
            payload=specification.payload,
            count=count,
            concurrency=min(200, count),
            max_attempts=2,
            expected_attempts=count * 2,
            timeout=max(300, count),
            order_index=1,
            random_seed=trial_seed,
            retry_history_contract=True,
        )
        time.sleep(float(profile.get("cooldown_seconds", 0)))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E5_SPEC.default_project)
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
                    "ticket": E5_SPEC.ticket,
                    "scenario": E5_SPEC.scenario,
                    "task_type": E5_SPEC.task_type,
                    "payload": E5_SPEC.payload,
                    "logical_tasks_per_trial": profile["retry_tasks"],
                    "expected_attempts_per_trial": profile["retry_tasks"] * 2,
                    "workers": profile["retry_workers"],
                    "schedulers": profile["retry_schedulers"],
                    "trials": profile["required_blocks"],
                    "retry_configuration": profile["retry_configuration"],
                    "scenarios": profile["required_public_scenarios"],
                    "will_execute": False,
                },
                indent=2,
            )
        )
        return 0
    return run_scoped_experiment(
        arguments,
        E5_SPEC,
        generate,
        execute_trials=run_retry_trials,
        document_fields={"retry_configuration": profile["retry_configuration"]},
    )


if __name__ == "__main__":
    raise SystemExit(main())
