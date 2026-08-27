#!/usr/bin/env python3
"""Run the publishable TF-012E1 no-op-only scaling experiment."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import random
import sys
import time
from typing import Any

from benchmarks.e1_artifacts import PLOT_NAMES, generate
from benchmarks.run import BENCHMARKS, ROOT, BenchmarkError, Harness, run_command
from benchmarks.trust import (
    aggregate,
    create_manifest,
    evaluate_run_directory,
    evaluate_trust,
    sha256_file,
    write_json,
)
from benchmarks.trusted import (
    TrustedRun,
    create_run_directory,
    harness_identity,
    image_provenance,
    new_run_id,
    run_contract,
    run_regressions,
    warmup,
    write_results_csv,
)

PROFILE_PATH = BENCHMARKS / "config" / "tf-012e1-noop.json"
PUBLIC_REPORT = BENCHMARKS / "reports" / "tf-012e1-noop-scaling.md"


def e1_harness_identity() -> dict[str, Any]:
    identity = harness_identity()
    identity["version"] = f"{identity['version']}+TF-012E1"
    recorded = {item["path"] for item in identity["files"]}
    for path in (pathlib.Path(__file__), BENCHMARKS / "e1_artifacts.py", PROFILE_PATH):
        relative = path.resolve().relative_to(ROOT).as_posix()
        if relative not in recorded:
            identity["files"].append({"path": relative, "sha256": sha256_file(path)})
    identity["files"] = sorted(identity["files"], key=lambda item: item["path"])
    return identity


def focused_regression_record() -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "unittest",
        "benchmarks.tests.test_trust_gates",
        "benchmarks.tests.test_provenance",
        "benchmarks.tests.test_prometheus_deltas",
        "benchmarks.tests.test_tools",
    ]
    started = dt.datetime.now(dt.UTC)
    result = run_command(command, check=False, timeout=300)
    return {
        "category": "benchmark_harness",
        "command": command,
        "started_at": started.isoformat(),
        "finished_at": dt.datetime.now(dt.UTC).isoformat(),
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def run_e1_regressions() -> dict[str, Any]:
    regression = run_regressions("release")
    if regression.get("passed") is True:
        regression["commands"].append(focused_regression_record())
    regression["passed"] = bool(regression.get("commands")) and all(
        item.get("exit_code") == 0 for item in regression.get("commands", [])
    )
    return regression


def run_noop_blocks(trusted: TrustedRun) -> None:
    profile = trusted.profile
    count = int(profile["noop_tasks"])
    base_seed = int(profile["random_seed"])
    for block in range(1, int(profile["required_blocks"]) + 1):
        reset_started = dt.datetime.now(dt.UTC)
        trusted.harness.reset()
        trusted.harness.start()
        block_warmup = warmup(trusted.harness, int(profile["warmup_tasks"]))
        block_warmup["source"] = "excluded per-block test.noop warmup"
        trusted.block_events.append(
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": reset_started.isoformat(),
                "ready_at": dt.datetime.now(dt.UTC).isoformat(),
                "warmup": block_warmup,
            }
        )
        workers = list(profile["scaling_workers"])
        block_seed = base_seed + block * 100 + sum(ord(character) for character in "noop_scaling")
        random.Random(block_seed).shuffle(workers)
        for order_index, worker_count in enumerate(workers, start=1):
            trusted.processing_trial(
                scenario="noop_scaling",
                variant=f"w{worker_count}",
                block=block,
                trial=block,
                workers=int(worker_count),
                task_type="test.noop",
                payload={},
                count=count,
                concurrency=min(200, count),
                timeout=max(300, count * 0.25),
                order_index=order_index,
                random_seed=block_seed,
            )
            time.sleep(float(profile.get("cooldown_seconds", 0)))


def artifact_hashes(paths: list[pathlib.Path]) -> dict[str, str]:
    common = paths[0].parent.parent
    return {path.relative_to(common).as_posix(): sha256_file(path) for path in paths}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="taskforge-tf012-e1-noop")
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--keep", action="store_true")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    contract = run_contract(development=False)
    source = contract["source"]
    profile = json.loads(PROFILE_PATH.read_text())
    run_id = new_run_id(source, profile["name"])
    output_dir = arguments.output_dir or BENCHMARKS / "results" / run_id
    create_run_directory(output_dir)
    harness = Harness(profile, arguments.project, arguments.keep)
    trusted: TrustedRun | None = None
    document: dict[str, Any] = {
        "schema_version": 2,
        "tf_ticket": "TF-012E1",
        "run_id": run_id,
        "profile": profile,
        "publishable": True,
        "publication_status": "PUBLISHABLE",
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "source": source,
        "harness": e1_harness_identity(),
        "images": {},
        "environment": None,
        "regression": None,
        "warmup": {
            "excluded": True,
            "source": "per-block warmups archived in run_blocks",
        },
        "artifact_reproducibility": {
            "passed": False,
            "method": "two byte-identical generations from saved results.json",
        },
        "results": [],
        "run_blocks": [],
        "summaries": [],
        "trust": None,
        "errors": [],
        "completed_at": None,
    }
    results_path = output_dir / "results.json"
    try:
        harness.reset()
        harness.build()
        document["images"] = image_provenance(harness)
        document["regression"] = run_e1_regressions()
        if document["regression"].get("passed") is not True:
            raise BenchmarkError("recorded E1 regression suite failed")
        harness.start()
        document["environment"] = harness.environment()
        environment = document["environment"]
        trial_provenance = {
            "publishable": True,
            "publication_status": "PUBLISHABLE",
            "source": source,
            "harness": document["harness"],
            "images": document["images"],
            "machine": {
                key: environment.get(key)
                for key in (
                    "captured_at",
                    "platform",
                    "os",
                    "host_cpu",
                    "host_logical_cpus",
                    "host_memory_bytes",
                    "docker_version",
                    "docker_info",
                    "postgresql",
                    "go_version",
                    "python_version",
                )
            },
        }
        trusted = TrustedRun(harness, output_dir, profile, run_id, trial_provenance)
        run_noop_blocks(trusted)
        document["results"] = trusted.results
        document["run_blocks"] = trusted.block_events
        document["summaries"] = aggregate(trusted.results)
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
    except Exception as exc:  # noqa: BLE001 - retain every failed measured trial
        if trusted is not None:
            document["results"] = trusted.results
            document["run_blocks"] = trusted.block_events
            document["summaries"] = aggregate(trusted.results)
        document["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        write_json(results_path, document)
        raise
    finally:
        harness.close()

    expected_artifacts = ["tf-012e1-noop-scaling.md", *(f"plots/{name}" for name in PLOT_NAMES)]
    document["artifact_reproducibility"] = {
        "passed": True,
        "method": "two byte-identical generations from saved results.json",
        "artifacts_compared": expected_artifacts,
    }
    document["trust"] = evaluate_trust(document, output_dir)
    write_json(results_path, document)
    write_results_csv(output_dir / "results.csv", document["results"])
    first_paths = generate(results_path)
    first_hashes = artifact_hashes(first_paths)
    second_paths = generate(results_path)
    second_hashes = artifact_hashes(second_paths)
    if first_hashes != second_hashes:
        document["artifact_reproducibility"]["passed"] = False
        document["artifact_reproducibility"]["error"] = {
            "first": first_hashes,
            "second": second_hashes,
        }
        document["trust"] = evaluate_trust(document, output_dir)
        write_json(results_path, document)
        generate(results_path)
    create_manifest(output_dir)
    generate(results_path, PUBLIC_REPORT)
    final_trust = evaluate_run_directory(output_dir)
    print(json.dumps({"run_directory": str(output_dir), "trust": final_trust}, indent=2))
    return 0 if final_trust.get("overall", {}).get("result") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
