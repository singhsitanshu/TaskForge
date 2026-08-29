"""Shared trusted lifecycle for single-scenario scaling experiments."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import random
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

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

ArtifactGenerator = Callable[[pathlib.Path, pathlib.Path | None], list[pathlib.Path]]


@dataclass(frozen=True)
class ScopedExperiment:
    ticket: str
    profile_path: pathlib.Path
    public_report: pathlib.Path
    scenario: str
    task_type: str
    payload: dict[str, Any]
    count_key: str
    default_project: str
    artifact_names: tuple[str, ...]
    harness_paths: tuple[pathlib.Path, ...]
    focused_test_modules: tuple[str, ...]


ExperimentExecutor = Callable[[TrustedRun, ScopedExperiment], None]


def scoped_harness_identity(specification: ScopedExperiment) -> dict[str, Any]:
    identity = harness_identity()
    identity["version"] = f"{identity['version']}+{specification.ticket}"
    recorded = {item["path"] for item in identity["files"]}
    for path in (pathlib.Path(__file__), specification.profile_path, *specification.harness_paths):
        relative = path.resolve().relative_to(ROOT).as_posix()
        if relative not in recorded:
            identity["files"].append({"path": relative, "sha256": sha256_file(path)})
    identity["files"] = sorted(identity["files"], key=lambda item: item["path"])
    return identity


def focused_regression_record(specification: ScopedExperiment) -> dict[str, Any]:
    command = [sys.executable, "-m", "unittest", *specification.focused_test_modules]
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


def run_scoped_regressions(specification: ScopedExperiment) -> dict[str, Any]:
    regression = run_regressions("release")
    if regression.get("passed") is True:
        regression["commands"].append(focused_regression_record(specification))
    regression["passed"] = bool(regression.get("commands")) and all(
        item.get("exit_code") == 0 for item in regression.get("commands", [])
    )
    return regression


def run_scaling_blocks(trusted: TrustedRun, specification: ScopedExperiment) -> None:
    profile = trusted.profile
    count = int(profile[specification.count_key])
    base_seed = int(profile["random_seed"])
    for block in range(1, int(profile["required_blocks"]) + 1):
        reset_started = dt.datetime.now(dt.UTC)
        trusted.harness.reset()
        trusted.harness.start()
        block_warmup = warmup(trusted.harness, int(profile["warmup_tasks"]))
        block_warmup["source"] = "excluded per-block trusted warmup"
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
        block_seed = (
            base_seed + block * 100 + sum(ord(character) for character in specification.scenario)
        )
        random.Random(block_seed).shuffle(workers)
        for order_index, worker_count in enumerate(workers, start=1):
            trusted.processing_trial(
                scenario=specification.scenario,
                variant=f"w{worker_count}",
                block=block,
                trial=block,
                workers=int(worker_count),
                task_type=specification.task_type,
                payload=specification.payload,
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


def load_trusted_scaling_comparison(
    results_path: pathlib.Path,
    *,
    ticket: str,
    scenario: str,
    workers: tuple[int, ...] = (1, 4, 8, 16),
) -> dict[str, Any]:
    """Validate an external trusted scaling run and return its speedup shape."""
    if results_path.name != "results.json" or not results_path.is_file():
        raise BenchmarkError("comparison input must identify an existing results.json")
    trust = evaluate_run_directory(results_path.parent)
    if trust.get("overall", {}).get("result") != "PASS":
        raise BenchmarkError(f"{ticket} comparison artifact does not pass the trust evaluator")
    document = json.loads(results_path.read_text())
    if document.get("tf_ticket") != ticket:
        raise BenchmarkError(f"comparison artifact is not a {ticket} run")
    summaries = {
        int(str(item.get("variant", ""))[1:]): item
        for item in aggregate(document.get("results", []))
        if item.get("scenario") == scenario
        and str(item.get("variant", "")).startswith("w")
        and str(item.get("variant", ""))[1:].isdigit()
    }
    if sorted(summaries) != list(workers):
        raise BenchmarkError(
            f"trusted {ticket} comparison does not contain workers "
            + ",".join(str(value) for value in workers)
        )
    speedup = {str(value): summaries[value].get("speedup_vs_w1") for value in workers}
    if any(value is None for value in speedup.values()):
        raise BenchmarkError(f"trusted {ticket} comparison is missing scaling speedup")
    return {
        "run_id": document.get("run_id"),
        "commit": document.get("source", {}).get("git_commit_sha"),
        "tree": document.get("source", {}).get("git_tree_hash"),
        "results_sha256": sha256_file(results_path),
        "trust": "PASS",
        "scenario": scenario,
        "speedup": speedup,
    }


def run_scoped_experiment(
    arguments: Any,
    specification: ScopedExperiment,
    generate_artifacts: ArtifactGenerator,
    *,
    profile_overrides: dict[str, Any] | None = None,
    document_fields: dict[str, Any] | None = None,
    execute_trials: ExperimentExecutor | None = None,
) -> int:
    contract = run_contract(development=False)
    source = contract["source"]
    profile = json.loads(specification.profile_path.read_text())
    profile.update(profile_overrides or {})
    run_id = new_run_id(source, profile["name"])
    output_dir = arguments.output_dir or BENCHMARKS / "results" / run_id
    create_run_directory(output_dir)
    harness = Harness(profile, arguments.project, arguments.keep)
    trusted: TrustedRun | None = None
    document: dict[str, Any] = {
        "schema_version": 2,
        "tf_ticket": specification.ticket,
        "run_id": run_id,
        "profile": profile,
        "scenario_contract": {
            "scenario": specification.scenario,
            "task_type": specification.task_type,
            "payload": specification.payload,
            "workers": profile.get(
                "scaling_workers",
                profile.get("retry_workers", profile.get("api_submission_workers")),
            ),
            "schedulers": profile.get("retry_schedulers", 1),
            "api_concurrency": profile.get("api_concurrency"),
            "blocks": profile["required_blocks"],
            "task_count": profile[specification.count_key],
        },
        "publishable": True,
        "publication_status": "PUBLISHABLE",
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "source": source,
        "harness": scoped_harness_identity(specification),
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
        **(document_fields or {}),
    }
    results_path = output_dir / "results.json"
    try:
        harness.reset()
        harness.build()
        document["images"] = image_provenance(harness)
        document["regression"] = run_scoped_regressions(specification)
        if document["regression"].get("passed") is not True:
            raise BenchmarkError(f"recorded {specification.ticket} regression suite failed")
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
        (execute_trials or run_scaling_blocks)(trusted, specification)
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

    document["artifact_reproducibility"] = {
        "passed": True,
        "method": "two byte-identical generations from saved results.json",
        "artifacts_compared": list(specification.artifact_names),
    }
    document["trust"] = evaluate_trust(document, output_dir)
    write_json(results_path, document)
    write_results_csv(output_dir / "results.csv", document["results"])
    if document["trust"].get("overall", {}).get("result") != "PASS":
        document["publishable"] = False
        document["publication_status"] = "UNPUBLISHABLE"
        document["trust"] = evaluate_trust(document, output_dir)
        write_json(results_path, document)
        create_manifest(output_dir)
        print(json.dumps({"run_directory": str(output_dir), "trust": document["trust"]}, indent=2))
        return 1
    with tempfile.TemporaryDirectory(prefix="taskforge-scoped-artifacts-") as temporary:
        temporary_results = pathlib.Path(temporary) / "results.json"
        shutil.copyfile(results_path, temporary_results)
        first_paths = generate_artifacts(temporary_results, None)
        first_hashes = artifact_hashes(first_paths)
        second_paths = generate_artifacts(temporary_results, None)
        second_hashes = artifact_hashes(second_paths)
    if first_hashes != second_hashes:
        document["artifact_reproducibility"]["passed"] = False
        document["artifact_reproducibility"]["error"] = {
            "first": first_hashes,
            "second": second_hashes,
        }
        document["trust"] = evaluate_trust(document, output_dir)
        document["publishable"] = False
        document["publication_status"] = "UNPUBLISHABLE"
        document["trust"] = evaluate_trust(document, output_dir)
        write_json(results_path, document)
        create_manifest(output_dir)
        print(json.dumps({"run_directory": str(output_dir), "trust": document["trust"]}, indent=2))
        return 1
    generate_artifacts(results_path, None)
    create_manifest(output_dir)
    final_trust = evaluate_run_directory(output_dir)
    if final_trust.get("overall", {}).get("result") == "PASS":
        generate_artifacts(results_path, specification.public_report)
    else:
        shutil.rmtree(output_dir / "plots", ignore_errors=True)
        (output_dir / specification.artifact_names[0]).unlink(missing_ok=True)
        document["publishable"] = False
        document["publication_status"] = "UNPUBLISHABLE"
        document["trust"] = evaluate_trust(document, output_dir)
        write_json(results_path, document)
        create_manifest(output_dir)
        final_trust = evaluate_run_directory(output_dir)
    print(json.dumps({"run_directory": str(output_dir), "trust": final_trust}, indent=2))
    return 0 if final_trust.get("overall", {}).get("result") == "PASS" else 1
