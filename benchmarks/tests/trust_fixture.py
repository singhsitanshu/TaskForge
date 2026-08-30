"""Small synthetic TF-012D run directory used by trust-gate unit tests."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from benchmarks.trust import (
    COUNTER_METRICS,
    HISTOGRAM_METRICS,
    build_reconciliation,
    create_manifest,
    derive_raw,
    write_csv,
    write_json,
)
from benchmarks.trusted import ATTEMPT_FIELDS, TASK_FIELDS


def _sample(metric: dict[str, str], value: float) -> dict[str, Any]:
    return {"metric": metric, "value": [1_800_000_000, str(value)]}


def prometheus_snapshot(value: float) -> dict[str, Any]:
    targets = [
        {"job": "taskforge-api", "instance": "api:8000", "health": "up", "last_error": ""},
        {
            "job": "taskforge-worker",
            "instance": "worker:8080",
            "health": "up",
            "last_error": "",
        },
        {
            "job": "taskforge-scheduler",
            "instance": "scheduler:8080",
            "health": "up",
            "last_error": "",
        },
    ]
    replicas = [
        {"service": "api", "name": "api-1", "container_id": "api-id", "state": "running"},
        {
            "service": "worker",
            "name": "worker-1",
            "container_id": "worker-id",
            "state": "running",
        },
        {
            "service": "scheduler",
            "name": "scheduler-1",
            "container_id": "scheduler-id",
            "state": "running",
        },
    ]
    metrics: dict[str, list[dict[str, Any]]] = {}
    for name, metric in COUNTER_METRICS.items():
        labels = {"job": "taskforge-worker", "instance": "worker:8080"}
        if name == "api_requests":
            labels = {
                "job": "taskforge-api",
                "instance": "api:8000",
                "method": "POST",
                "route": "/tasks",
                "status_class": "2xx",
            }
        elif name == "api_submissions":
            labels = {
                "job": "taskforge-api",
                "instance": "api:8000",
                "outcome": "created",
            }
        metrics[metric] = [_sample(labels, value)]
    for bucket_metric in HISTOGRAM_METRICS.values():
        base = bucket_metric.removesuffix("_bucket")
        labels = {"job": "taskforge-worker", "instance": "worker:8080"}
        metrics[bucket_metric] = [
            _sample({**labels, "le": "1.0"}, value),
            _sample({**labels, "le": "+Inf"}, value),
        ]
        metrics[base + "_sum"] = [_sample(labels, value)]
        metrics[base + "_count"] = [_sample(labels, value)]
    metrics["process_start_time_seconds"] = [
        _sample({"job": "taskforge-worker", "instance": "worker:8080"}, 100),
        _sample({"job": "taskforge-scheduler", "instance": "scheduler:8080"}, 100),
    ]
    return {"targets": targets, "replicas": replicas, "metrics": metrics}


def rehash_trial(root: Path, trial: int) -> None:
    create_manifest(root / "trials" / f"trial-{trial}")


def build_trust_fixture(root: Path) -> dict[str, Any]:
    source = {
        "clean": True,
        "git_commit_sha": "a" * 40,
        "git_tree_hash": "b" * 40,
        "git_branch": "main",
        "git_describe": "v1.0.0",
    }
    images = {
        service: {
            "image_id": f"sha256:{service}",
            "repo_digests": [f"taskforge/{service}@sha256:1"],
        }
        for service in ("api", "worker", "scheduler", "load_generator")
    }
    provenance = {
        "publishable": True,
        "publication_status": "PUBLISHABLE",
        "source": source,
        "images": images,
        "machine": {"platform": "synthetic", "host_logical_cpus": 4},
    }
    results = []
    for trial in range(1, 4):
        directory = root / "trials" / f"trial-{trial}"
        directory.mkdir(parents=True)
        task_id = f"task-{trial}"
        tasks = [
            {
                "task_id": task_id,
                "task_created_at": "2026-01-01T00:00:00+00:00",
                "task_completed_at": "2026-01-01T00:00:02+00:00",
                "final_status": "SUCCEEDED",
                "attempt_count": "1",
                "task_type": "test.noop",
                "queue": f"q-{trial}",
            }
        ]
        attempts = [
            {
                "task_id": task_id,
                "attempt_id": f"attempt-{trial}",
                "attempt_number": "1",
                "status": "SUCCEEDED",
                "worker_id": "worker-1",
                "worker_label": "worker-1",
                "task_created_at": "2026-01-01T00:00:00+00:00",
                "attempt_leased_at": "2026-01-01T00:00:01+00:00",
                "attempt_started_at": "2026-01-01T00:00:01+00:00",
                "attempt_finished_at": "2026-01-01T00:00:02+00:00",
                "queue_entered_at": "2026-01-01T00:00:00+00:00",
                "scheduled_at_snapshot": "2026-01-01T00:00:00+00:00",
                "retry_scheduled_at": "",
                "recovered_lease_expires_at": "",
                "recovered_at": "",
                "recovery_action": "",
            }
        ]
        raw = derive_raw(tasks, attempts)
        correctness = {
            "expected_tasks": 1,
            "actual_tasks": 1,
            "expected_attempts": 1,
            "actual_attempts": 1,
            "terminal_tasks": 1,
            "succeeded_tasks": 1,
            "queued_tasks": 0,
            "duplicate_attempts": 0,
            "attempt_count_mismatches": 0,
            "stranded_leases": 0,
            "unexpected_attempt_states": 0,
            "abandoned_attempts": 0,
            "missing_queue_evidence": 0,
            "negative_queue_waits": 0,
            "expected_abandoned": 0,
            "terminal_expected": True,
            "passed": True,
        }
        start = prometheus_snapshot(10)
        end = prometheus_snapshot(11)
        reconciliation = build_reconciliation(raw, start, end, "noop_scaling")
        metadata = {
            "scenario": "noop_scaling",
            "variant": "w1",
            "classification": "PUBLIC",
            "block": trial,
            "trial": trial,
            "order_index": 1,
        }
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "correctness.json", correctness)
        write_json(directory / "prometheus_start.json", start)
        write_json(directory / "prometheus_end.json", end)
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        write_json(directory / "resource_samples.json", [])
        write_json(directory / "summary.json", {"raw": raw})
        create_manifest(directory)
        results.append(
            {
                "scenario": "noop_scaling",
                "variant": "w1",
                "classification": "PUBLIC",
                "block": trial,
                "trial": trial,
                "order_index": 1,
                "workers": 1,
                "schedulers": 1,
                "count": 1,
                "task_type": "test.noop",
                "configuration": {"poll_interval": "synthetic"},
                "provenance": copy.deepcopy(provenance),
                "raw": raw,
                "correctness": correctness,
                "prometheus_reconciliation": reconciliation,
                "artifacts": {"directory": f"trials/trial-{trial}"},
                "valid": True,
            }
        )
    return {
        "publishable": True,
        "publication_status": "PUBLISHABLE",
        "source": source,
        "images": images,
        "environment": {"platform": "synthetic"},
        "harness": {"version": "TF-012D/test", "files": [{"path": "trust.py", "sha256": "c"}]},
        "profile": {
            "minimum_public_trials": 3,
            "required_blocks": 3,
            "required_public_scenarios": ["noop_scaling"],
            "scaling_workers": [1],
        },
        "run_blocks": [
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": f"2026-01-0{block}T00:00:00+00:00",
                "ready_at": f"2026-01-0{block}T00:00:01+00:00",
                "warmup": {"excluded": True, "source": "synthetic block warmup"},
            }
            for block in range(1, 4)
        ],
        "regression": {
            "passed": True,
            "commands": [
                {"category": "api_integration", "command": ["pytest", "api/tests"], "exit_code": 0},
                {
                    "category": "worker",
                    "command": ["go", "test", "-race", "./worker/..."],
                    "exit_code": 0,
                },
                {
                    "category": "scheduler",
                    "command": ["go", "test", "-race", "./scheduler/..."],
                    "exit_code": 0,
                },
                {
                    "category": "benchmark_harness",
                    "command": ["python", "-m", "unittest", "benchmarks.tests"],
                    "exit_code": 0,
                },
            ],
        },
        "artifact_reproducibility": {"passed": True},
        "results": results,
    }
