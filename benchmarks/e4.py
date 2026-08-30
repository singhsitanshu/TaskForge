#!/usr/bin/env python3
"""Run the publishable TF-012E4 API-submission-only experiment."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import random
import time
from typing import Any

from benchmarks.e4_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import ScopedExperiment, run_scoped_experiment
from benchmarks.trusted import TrustedRun, warmup

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e4-api.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e4-api-submission.md"
EXPECTED_CONCURRENCY = [1, 10, 25, 50, 100]
E4_SPEC = ScopedExperiment(
    ticket="TF-012E4",
    profile_path=PROFILE_PATH,
    public_report=PUBLIC_REPORT,
    scenario="api_submission",
    task_type="test.noop",
    payload={},
    count_key="api_requests",
    default_project="taskforge-tf012-e4-api",
    artifact_names=("tf-012e4-api-submission.md", *(f"plots/{name}" for name in PLOT_NAMES)),
    harness_paths=(pathlib.Path(__file__), BENCHMARKS / "e4_artifacts.py"),
    focused_test_modules=(
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_e4_harness",
    ),
)


def configuration_contract() -> dict[str, Any]:
    profile = json.loads(PROFILE_PATH.read_text())
    if profile.get("api_concurrency") != EXPECTED_CONCURRENCY:
        raise BenchmarkError("TF-012E4 concurrency must be exactly 1,10,25,50,100")
    if profile.get("api_requests") != 2000:
        raise BenchmarkError("TF-012E4 requires exactly 2000 requests per configuration")
    if profile.get("required_blocks") != 3:
        raise BenchmarkError("TF-012E4 requires exactly three independent blocks")
    if profile.get("api_replicas") != 1 or profile.get("api_submission_workers") != 0:
        raise BenchmarkError("TF-012E4 requires one API replica and zero measured workers")
    if profile.get("required_public_scenarios") != [E4_SPEC.scenario]:
        raise BenchmarkError("TF-012E4 profile contains an unrelated scenario")
    if profile.get("workload") != {
        "task_type": "test.noop",
        "payload": {},
        "key_mode": "none",
        "max_attempts": 1,
    }:
        raise BenchmarkError("TF-012E4 workload must be normal keyless test.noop submission")
    return profile


def run_api_blocks(trusted: TrustedRun, specification: ScopedExperiment) -> None:
    profile = trusted.profile
    base_seed = int(profile["random_seed"])
    for block in range(1, int(profile["required_blocks"]) + 1):
        reset_started = dt.datetime.now(dt.UTC)
        trusted.harness.reset()
        trusted.harness.start()
        block_warmup = warmup(trusted.harness, int(profile["warmup_tasks"]))
        block_warmup["source"] = "excluded per-block trusted API warmup"
        trusted.block_events.append(
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": reset_started.isoformat(),
                "ready_at": dt.datetime.now(dt.UTC).isoformat(),
                "warmup": block_warmup,
            }
        )
        concurrency_levels = list(profile["api_concurrency"])
        block_seed = base_seed + block * 100 + sum(ord(value) for value in specification.scenario)
        random.Random(block_seed).shuffle(concurrency_levels)
        for order_index, concurrency in enumerate(concurrency_levels, start=1):
            trusted.api_trial(
                int(concurrency),
                block,
                scenario=specification.scenario,
                block=block,
                order_index=order_index,
                random_seed=block_seed,
                key_mode="none",
            )
            time.sleep(float(profile.get("cooldown_seconds", 0)))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=E4_SPEC.default_project)
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
                    "ticket": E4_SPEC.ticket,
                    "scenario": E4_SPEC.scenario,
                    "requests_per_configuration": profile["api_requests"],
                    "concurrency": profile["api_concurrency"],
                    "blocks": profile["required_blocks"],
                    "api_replicas": profile["api_replicas"],
                    "measured_workers": profile["api_submission_workers"],
                    "key_mode": profile["workload"]["key_mode"],
                    "measured_trials": len(profile["api_concurrency"]) * profile["required_blocks"],
                    "scenarios": profile["required_public_scenarios"],
                    "will_execute": False,
                },
                indent=2,
            )
        )
        return 0
    return run_scoped_experiment(
        arguments,
        E4_SPEC,
        generate,
        execute_trials=run_api_blocks,
    )


if __name__ == "__main__":
    raise SystemExit(main())
