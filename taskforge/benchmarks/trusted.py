#!/usr/bin/env python3
"""TF-012B publishable benchmark orchestrator with trust-by-construction artifacts."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import pathlib
import random
import re
import sys
import time
import urllib.request
import uuid
from typing import Any

from benchmarks.run import (
    BENCHMARKS,
    ROOT,
    BenchmarkError,
    Harness,
    ResourceSampler,
    container_hostnames,
    run_command,
    summarize_resources,
    worker_containers,
)
from benchmarks.trust import (
    HARNESS_VERSION,
    PROMETHEUS_METRICS,
    PUBLIC_SCENARIOS,
    aggregate,
    build_reconciliation,
    create_manifest,
    derive_raw,
    evaluate_trust,
    sha256_file,
    write_csv,
    write_json,
)

TASK_FIELDS = [
    "task_id",
    "task_created_at",
    "task_completed_at",
    "final_status",
    "attempt_count",
    "task_type",
    "queue",
]
ATTEMPT_FIELDS = [
    "task_id",
    "attempt_id",
    "attempt_number",
    "status",
    "worker_id",
    "worker_label",
    "task_created_at",
    "attempt_leased_at",
    "attempt_started_at",
    "attempt_finished_at",
    "queue_entered_at",
    "scheduled_at_snapshot",
    "retry_scheduled_at",
    "recovered_lease_expires_at",
    "recovered_at",
    "recovery_action",
]


def command_value(arguments: list[str]) -> str:
    return run_command(arguments, timeout=60).stdout.strip()


def source_provenance(*, require_clean: bool) -> dict[str, Any]:
    status = command_value(["git", "status", "--porcelain"])
    clean = not status
    if require_clean and not clean:
        raise BenchmarkError(
            "publishable benchmark requires a clean working tree; use benchmark-dev for an "
            "explicitly UNPUBLISHABLE development run\n" + status
        )
    branch = run_command(
        ["git", "branch", "--show-current"], check=False, timeout=30
    ).stdout.strip()
    describe = run_command(
        ["git", "describe", "--always", "--tags", "--dirty"], check=False, timeout=30
    ).stdout.strip()
    return {
        "git_commit_sha": command_value(["git", "rev-parse", "HEAD"]),
        "git_tree_hash": command_value(["git", "rev-parse", "HEAD^{tree}"]),
        "git_branch": branch or None,
        "git_describe": describe or None,
        "clean": clean,
        "git_status_porcelain": status,
    }


def run_contract(*, development: bool) -> dict[str, Any]:
    publishable = not development
    return {
        "publishable": publishable,
        "publication_status": "PUBLISHABLE" if publishable else "UNPUBLISHABLE",
        "source": source_provenance(require_clean=publishable),
    }


def new_run_id(source: dict[str, Any], profile_name: str) -> str:
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S%fZ")
    nonce = uuid.uuid4().hex[:12]
    return f"{timestamp}_{source['git_commit_sha'][:12]}_{profile_name}_{nonce}"


def create_run_directory(path: pathlib.Path) -> pathlib.Path:
    try:
        path.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise BenchmarkError(f"refusing to overwrite existing result directory: {path}") from exc
    return path


def harness_identity() -> dict[str, Any]:
    paths = [
        BENCHMARKS / "trusted.py",
        BENCHMARKS / "trust.py",
        BENCHMARKS / "run.py",
        BENCHMARKS / "report.py",
        BENCHMARKS / "plot.py",
        BENCHMARKS / "loadgen" / "main.go",
        BENCHMARKS / "loadgen" / "go.mod",
    ]
    return {
        "version": HARNESS_VERSION,
        "files": [
            {"path": path.relative_to(ROOT).as_posix(), "sha256": sha256_file(path)}
            for path in paths
        ],
    }


def image_provenance(harness: Harness) -> dict[str, Any]:
    names = {
        "api": f"{harness.project}-api",
        "worker": f"{harness.project}-worker",
        "scheduler": f"{harness.project}-scheduler",
        "load_generator": harness.image,
    }
    captured = {}
    for service, name in names.items():
        result = run_command(
            ["docker", "image", "inspect", name, "--format", "{{json .}}"],
            timeout=60,
        )
        item = json.loads(result.stdout)
        captured[service] = {
            "image_name": name,
            "image_id": item.get("Id"),
            "repo_digests": item.get("RepoDigests") or [],
            "repo_tags": item.get("RepoTags") or [],
            "created": item.get("Created"),
            "architecture": item.get("Architecture"),
            "os": item.get("Os"),
        }
    return captured


def regression_commands(profile_name: str) -> list[list[str]]:
    if profile_name == "trust-smoke":
        return [
            [sys.executable, "-m", "unittest", "benchmarks.tests.test_tools"],
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{BENCHMARKS / 'loadgen'}:/workspace",
                "-w",
                "/workspace",
                "golang:1.23-bookworm",
                "go",
                "test",
                "./...",
            ],
        ]
    return [
        [
            "docker",
            "compose",
            "-p",
            "taskforge-tf012-release-regression",
            "--profile",
            "test",
            "run",
            "--rm",
            "--build",
            "integration-tests",
        ],
        [
            "docker",
            "compose",
            "-p",
            "taskforge-tf012-release-regression",
            "--profile",
            "test",
            "run",
            "--rm",
            "--build",
            "claim-tests",
        ],
        [
            "docker",
            "compose",
            "-p",
            "taskforge-tf012-release-regression",
            "--profile",
            "test",
            "run",
            "--rm",
            "--build",
            "recovery-tests",
        ],
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{ROOT / 'worker'}:/workspace",
            "-w",
            "/workspace",
            "golang:1.23-bookworm",
            "go",
            "test",
            "-race",
            "./...",
        ],
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{ROOT / 'scheduler'}:/workspace",
            "-w",
            "/workspace",
            "golang:1.23-bookworm",
            "go",
            "test",
            "-race",
            "./...",
        ],
        [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{BENCHMARKS / 'loadgen'}:/workspace",
            "-w",
            "/workspace",
            "golang:1.23-bookworm",
            "go",
            "test",
            "./...",
        ],
        [sys.executable, "-m", "unittest", "benchmarks.tests.test_tools"],
        ["docker", "compose", "build", "web"],
    ]


def regression_category(command: list[str]) -> str | None:
    text = " ".join(command)
    if "integration-tests" in text:
        return "api_integration"
    if str(ROOT / "worker") in text:
        return "worker"
    if str(ROOT / "scheduler") in text:
        return "scheduler"
    if "benchmarks.tests" in text or str(BENCHMARKS / "loadgen") in text:
        return "benchmark_harness"
    return None


def run_regressions(profile_name: str) -> dict[str, Any]:
    records = []
    regression_env = os.environ.copy()
    regression_env.update(
        {
            "COMPOSE_PROJECT_NAME": "taskforge-tf012-release-regression",
            "POSTGRES_PORT": "35432",
            "REDIS_PORT": "36379",
            "API_PORT": "38000",
            "WEB_PORT": "33000",
            "PROMETHEUS_PORT": "39090",
            "GRAFANA_PORT": "33001",
        }
    )
    for command in regression_commands(profile_name):
        started = dt.datetime.now(dt.UTC)
        result = run_command(command, env=regression_env, check=False, timeout=1800)
        finished = dt.datetime.now(dt.UTC)
        record = {
            "category": regression_category(command),
            "command": command,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        records.append(record)
        if result.returncode != 0:
            break
    run_command(
        [
            "docker",
            "compose",
            "-p",
            "taskforge-tf012-release-regression",
            "down",
            "--volumes",
            "--remove-orphans",
        ],
        env=regression_env,
        check=False,
        timeout=180,
    )
    return {
        "passed": bool(records) and all(item["exit_code"] == 0 for item in records),
        "commands": records,
    }


def prometheus_targets(harness: Harness) -> list[dict[str, Any]]:
    url = harness.prometheus_url + "/api/v1/targets"
    with urllib.request.urlopen(url, timeout=5) as response:
        payload = json.load(response)
    targets = []
    for target in payload.get("data", {}).get("activeTargets", []):
        labels = target.get("labels", {})
        targets.append(
            {
                "job": labels.get("job"),
                "instance": labels.get("instance"),
                "health": target.get("health"),
                "last_scrape": target.get("lastScrape"),
                "last_error": target.get("lastError"),
                "scrape_url": target.get("scrapeUrl"),
            }
        )
    return sorted(targets, key=lambda item: (item.get("job") or "", item.get("instance") or ""))


def replica_identities(harness: Harness) -> list[dict[str, Any]]:
    output = harness.compose("ps", "--format", "json", timeout=60)
    replicas = []
    for line in output.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("Service") not in {"api", "worker", "scheduler"}:
            continue
        replicas.append(
            {
                "service": item.get("Service"),
                "name": item.get("Name"),
                "container_id": item.get("ID"),
                "state": item.get("State"),
                "health": item.get("Health"),
            }
        )
    return sorted(replicas, key=lambda item: (item.get("service") or "", item.get("name") or ""))


def expected_targets(workers: int, schedulers: int) -> dict[str, int]:
    return {
        "taskforge-api": 1,
        "taskforge-worker": workers,
        "taskforge-scheduler": schedulers,
    }


def wait_for_boundary_scrape(
    harness: Harness,
    workers: int,
    schedulers: int,
    *,
    after: dt.datetime | None = None,
    timeout: float = 45,
) -> list[dict[str, Any]]:
    expected = expected_targets(workers, schedulers)
    deadline = time.monotonic() + timeout
    last: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        try:
            last = prometheus_targets(harness)
        except Exception:  # noqa: BLE001 - final error retains last state
            time.sleep(0.25)
            continue
        counts: dict[str, int] = {}
        healthy = True
        fresh = True
        for target in last:
            job = target.get("job")
            counts[job] = counts.get(job, 0) + 1
            healthy = healthy and target.get("health") == "up" and not target.get("last_error")
            if after is not None:
                scraped = target.get("last_scrape")
                fresh = (
                    fresh
                    and bool(scraped)
                    and dt.datetime.fromisoformat(str(scraped).replace("Z", "+00:00")) > after
                )
        counts_match = set(counts) <= set(expected) and all(
            counts.get(job, 0) == count for job, count in expected.items()
        )
        if counts_match and healthy and fresh:
            return last
        time.sleep(0.25)
    raise BenchmarkError(
        f"Prometheus targets did not reach exact healthy fresh boundary: expected={expected} "
        f"after={after} targets={last}"
    )


def prometheus_snapshot(
    harness: Harness,
    targets: list[dict[str, Any]],
    *,
    boundary_after: dt.datetime,
) -> dict[str, Any]:
    metrics = harness.prometheus({name: name for name in PROMETHEUS_METRICS})
    sample_times = [
        float(row["value"][0])
        for rows in metrics.values()
        if isinstance(rows, list)
        for row in rows
        if isinstance(row, dict) and row.get("value")
    ]
    return {
        "captured_at": dt.datetime.now(dt.UTC).isoformat(),
        "sample_time_min": min(sample_times) if sample_times else None,
        "sample_time_max": max(sample_times) if sample_times else None,
        "boundary_after": boundary_after.isoformat(),
        "targets": targets,
        "replicas": replica_identities(harness),
        "metrics": metrics,
    }


def prepare_trial(harness: Harness, workers: int, schedulers: int) -> dict[str, Any]:
    harness.clear_tasks()
    if workers == 0:
        harness.scale(workers=0)
    else:
        harness.recreate_workers(workers)
    harness.recreate_schedulers(schedulers)
    boundary_after = dt.datetime.now(dt.UTC)
    harness.reset_prometheus()
    targets = wait_for_boundary_scrape(harness, workers, schedulers, after=boundary_after)
    return prometheus_snapshot(harness, targets, boundary_after=boundary_after)


def finish_trial_snapshot(
    harness: Harness, workers: int, schedulers: int, trial_end: dt.datetime
) -> dict[str, Any]:
    targets = wait_for_boundary_scrape(harness, workers, schedulers, after=trial_end)
    return prometheus_snapshot(harness, targets, boundary_after=trial_end)


def warmup(harness: Harness, count: int) -> dict[str, Any]:
    """Exercise the full submit/claim/complete path outside every measured interval."""
    queue = "tf012b-warmup-excluded"
    harness.clear_tasks()
    harness.recreate_workers(1)
    harness.recreate_schedulers(1)
    started = dt.datetime.now(dt.UTC)
    submission = harness.loadgen(
        operation="submit",
        count=count,
        concurrency=min(20, count),
        task_type="test.noop",
        payload="{}",
        queue=queue,
        max_attempts=1,
        key_mode="unique",
        key_prefix=queue,
    )
    harness.wait_for_tasks(queue, count, timeout=60)
    check = trusted_correctness(harness, queue, count, count)
    return {
        "excluded": True,
        "started_at": started.isoformat(),
        "completed_at": dt.datetime.now(dt.UTC).isoformat(),
        "task_count": count,
        "submission": submission,
        "correctness": check,
    }


def psql_rows(harness: Harness, query: str) -> list[dict[str, str]]:
    output = harness.compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "--set",
        "ON_ERROR_STOP=1",
        "--csv",
        "--username",
        harness.env.get("POSTGRES_USER", "taskforge"),
        "--dbname",
        harness.env.get("POSTGRES_DB", "taskforge"),
        "--command",
        query,
        timeout=120,
    )
    return list(csv.DictReader(output.splitlines()))


def capture_rows(harness: Harness, queue: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    escaped = queue.replace("'", "''")
    tasks = psql_rows(
        harness,
        f"""
        SELECT
            id::text AS task_id,
            created_at::text AS task_created_at,
            COALESCE(completed_at::text, '') AS task_completed_at,
            status::text AS final_status,
            attempt_count::text AS attempt_count,
            task_type,
            queue
        FROM tasks
        WHERE queue = '{escaped}'
        ORDER BY id
        """,
    )
    attempts = psql_rows(
        harness,
        f"""
        SELECT
            task.id::text AS task_id,
            attempt.id::text AS attempt_id,
            attempt.attempt_number::text AS attempt_number,
            attempt.status::text AS status,
            attempt.worker_id::text AS worker_id,
            worker.name AS worker_label,
            task.created_at::text AS task_created_at,
            attempt.leased_at::text AS attempt_leased_at,
            COALESCE(attempt.started_at::text, '') AS attempt_started_at,
            COALESCE(attempt.finished_at::text, '') AS attempt_finished_at,
            COALESCE(attempt.queue_entered_at::text, '') AS queue_entered_at,
            COALESCE(attempt.scheduled_at_snapshot::text, '') AS scheduled_at_snapshot,
            COALESCE(attempt.retry_scheduled_at::text, '') AS retry_scheduled_at,
            COALESCE(attempt.recovered_lease_expires_at::text, '') AS recovered_lease_expires_at,
            COALESCE(attempt.recovered_at::text, '') AS recovered_at,
            COALESCE(attempt.recovery_action, '') AS recovery_action
        FROM task_attempts AS attempt
        JOIN tasks AS task ON task.id = attempt.task_id
        JOIN workers AS worker ON worker.id = attempt.worker_id
        WHERE task.queue = '{escaped}'
        ORDER BY task.id, attempt.attempt_number
        """,
    )
    return tasks, attempts


def trusted_correctness(
    harness: Harness,
    queue: str,
    expected_tasks: int,
    expected_attempts: int,
    *,
    terminal: bool = True,
    expected_abandoned: int = 0,
) -> dict[str, Any]:
    escaped = queue.replace("'", "''")
    check = harness.json_sql(
        f"""
        WITH selected AS (SELECT * FROM tasks WHERE queue = '{escaped}'),
        selected_attempts AS (
            SELECT attempt.* FROM task_attempts AS attempt
            JOIN selected AS task ON task.id = attempt.task_id
        )
        SELECT json_build_object(
            'expected_tasks', {expected_tasks},
            'actual_tasks', (SELECT count(*) FROM selected),
            'expected_attempts', {expected_attempts},
            'actual_attempts', (SELECT count(*) FROM selected_attempts),
            'terminal_tasks', (SELECT count(*) FROM selected WHERE status IN ('SUCCEEDED','FAILED','CANCELLED')),
            'succeeded_tasks', (SELECT count(*) FROM selected WHERE status = 'SUCCEEDED'),
            'queued_tasks', (SELECT count(*) FROM selected WHERE status = 'QUEUED'),
            'duplicate_attempts', (SELECT count(*) FROM (
                SELECT task_id, attempt_number FROM selected_attempts
                GROUP BY task_id, attempt_number HAVING count(*) > 1
            ) AS duplicates),
            'attempt_count_mismatches', (SELECT count(*) FROM selected AS task WHERE task.attempt_count <>
                (SELECT count(*) FROM selected_attempts AS attempt WHERE attempt.task_id = task.id)),
            'stranded_leases', (SELECT count(*) FROM selected WHERE
                status IN ('LEASED','RUNNING') OR lease_expires_at IS NOT NULL OR claimed_by_worker_id IS NOT NULL),
            'unexpected_attempt_states', (SELECT count(*) FROM selected_attempts WHERE status IN ('LEASED','RUNNING')),
            'abandoned_attempts', (SELECT count(*) FROM selected_attempts WHERE status = 'ABANDONED'),
            'missing_queue_evidence', (SELECT count(*) FROM selected_attempts WHERE started_at IS NOT NULL AND queue_entered_at IS NULL),
            'negative_queue_waits', (SELECT count(*) FROM selected_attempts WHERE started_at < queue_entered_at)
        )
        """
    )
    check.update(
        {
            "expected_abandoned": expected_abandoned,
            "terminal_expected": terminal,
        }
    )
    state_ok = (
        check["terminal_tasks"] == expected_tasks and check["succeeded_tasks"] == expected_tasks
        if terminal
        else check["queued_tasks"] == expected_tasks and check["terminal_tasks"] == 0
    )
    check["passed"] = bool(
        check["actual_tasks"] == expected_tasks
        and check["actual_attempts"] == expected_attempts
        and state_ok
        and check["duplicate_attempts"] == 0
        and check["attempt_count_mismatches"] == 0
        and check["stranded_leases"] == 0
        and check["unexpected_attempt_states"] == 0
        and check["abandoned_attempts"] == expected_abandoned
        and check["missing_queue_evidence"] == 0
        and check["negative_queue_waits"] == 0
    )
    return check


class TrustedRun:
    def __init__(
        self,
        harness: Harness,
        output_dir: pathlib.Path,
        profile: dict[str, Any],
        run_id: str,
        provenance: dict[str, Any],
    ) -> None:
        self.harness = harness
        self.output_dir = output_dir
        self.profile = profile
        self.run_id = run_id
        self.provenance = provenance
        self.results: list[dict[str, Any]] = []
        self.block_events: list[dict[str, Any]] = []
        self.sequence = 0

    def classification(self, scenario: str) -> str:
        return "PUBLIC" if scenario in PUBLIC_SCENARIOS else "EXPLORATORY"

    def host_state(self) -> dict[str, Any]:
        try:
            load = list(os.getloadavg())
        except OSError:
            load = []
        containers = []
        output = self.harness.compose("ps", "--format", "json", timeout=60)
        for line in output.splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            containers.append(
                {
                    "name": item.get("Name"),
                    "service": item.get("Service"),
                    "state": item.get("State"),
                    "health": item.get("Health"),
                }
            )
        return {
            "captured_at": dt.datetime.now(dt.UTC).isoformat(),
            "load_average": load,
            "containers": containers,
        }

    def artifact_directory(
        self, scenario: str, variant: str, block: int, trial: int
    ) -> pathlib.Path:
        self.sequence += 1
        safe = "-".join(
            re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-") for value in (scenario, variant)
        )
        relative = pathlib.Path("trials") / f"{self.sequence:04d}-{safe}-b{block}-t{trial}"
        directory = self.output_dir / relative
        directory.mkdir(parents=True, exist_ok=False)
        return directory

    def save_trial(
        self,
        *,
        scenario: str,
        variant: str,
        block: int,
        trial: int,
        workers: int,
        schedulers: int,
        count: int,
        queue: str,
        task_type: str,
        payload: dict[str, Any],
        submission: dict[str, Any],
        tasks: list[dict[str, str]],
        attempts: list[dict[str, str]],
        raw: dict[str, Any],
        correctness_result: dict[str, Any],
        prom_start: dict[str, Any],
        prom_end: dict[str, Any],
        reconciliation: dict[str, Any],
        resource_samples: list[dict[str, Any]],
        metadata: dict[str, Any],
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        directory = self.artifact_directory(scenario, variant, block, trial)
        metadata.update(
            {
                "workers": workers,
                "schedulers": schedulers,
                "api_replicas": 1,
                "task_count": count,
                "task_type": task_type,
                "workload_payload": payload,
                "queue": queue,
                "important_timing_configuration": self.harness.trial_configuration(),
                "provenance": self.provenance,
            }
        )
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "prometheus_start.json", prom_start)
        write_json(directory / "prometheus_end.json", prom_end)
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        write_json(directory / "correctness.json", correctness_result)
        write_json(directory / "resource_samples.json", resource_samples)
        write_json(
            directory / "summary.json",
            {"raw": raw, "submission": submission, "extra": extra or {}},
        )
        manifest = create_manifest(directory)
        relative = directory.relative_to(self.output_dir).as_posix()
        classification = self.classification(scenario)
        valid = bool(
            correctness_result.get("passed")
            and reconciliation.get("status") == "PASS"
            and raw.get("missing_queue_evidence", 0) == 0
            and not any(raw.get("negative_durations", {}).values())
        )
        result = {
            "scenario": scenario,
            "variant": variant,
            "classification": classification,
            "block": block,
            "trial": trial,
            "workers": workers,
            "schedulers": schedulers,
            "api_replicas": 1,
            "count": count,
            "queue": queue,
            "task_type": task_type,
            "payload": payload,
            "submission": submission,
            "raw": raw,
            "correctness": correctness_result,
            "prometheus_reconciliation": reconciliation,
            "configuration": self.harness.trial_configuration(),
            "provenance": self.provenance,
            "resources": summarize_resources(resource_samples),
            "artifacts": {
                "directory": relative,
                "manifest": f"{relative}/manifest.json",
                "manifest_sha256": sha256_file(directory / "manifest.json"),
                "artifact_count": len(manifest["artifacts"]),
            },
            "valid": valid,
        }
        if extra:
            result.update(extra)
        self.results.append(result)
        return result

    def processing_trial(
        self,
        *,
        scenario: str,
        variant: str,
        block: int,
        trial: int,
        workers: int,
        schedulers: int = 1,
        task_type: str,
        payload: dict[str, Any],
        count: int,
        concurrency: int,
        rate: float = 0,
        max_attempts: int = 1,
        expected_attempts: int | None = None,
        timeout: float = 300,
        order_index: int | None = None,
        random_seed: int | None = None,
    ) -> dict[str, Any]:
        queue = f"tf012b-{scenario}-{variant}-b{block}-t{trial}"[:128]
        print(
            f"[{scenario}] variant={variant} block={block} trial={trial} workers={workers}",
            flush=True,
        )
        host_before = self.host_state()
        prom_start = prepare_trial(self.harness, workers, schedulers)
        trial_started = dt.datetime.now(dt.UTC)
        with ResourceSampler(self.harness) as resources:
            submission = self.harness.loadgen(
                operation="submit",
                count=count,
                concurrency=min(concurrency, count),
                rate=rate,
                task_type=task_type,
                payload=json.dumps(payload, separators=(",", ":")),
                queue=queue,
                max_attempts=max_attempts,
                key_mode="unique",
                key_prefix=queue,
            )
            self.harness.wait_for_tasks(queue, count, timeout=timeout)
        trial_end = dt.datetime.now(dt.UTC)
        prom_end = finish_trial_snapshot(self.harness, workers, schedulers, trial_end)
        tasks, attempts = capture_rows(self.harness, queue)
        raw = derive_raw(tasks, attempts)
        expected = count if expected_attempts is None else expected_attempts
        correct = trusted_correctness(self.harness, queue, count, expected)
        reconciliation = build_reconciliation(raw, prom_start, prom_end, scenario)
        metadata = {
            "run_id": self.run_id,
            "scenario": scenario,
            "variant": variant,
            "classification": self.classification(scenario),
            "block": block,
            "trial": trial,
            "order_index": order_index,
            "random_seed": random_seed,
            "trial_start": trial_started.isoformat(),
            "trial_end": trial_end.isoformat(),
            "prometheus_start_sample_time": prom_start.get("sample_time_max"),
            "prometheus_end_sample_time": prom_end.get("sample_time_max"),
            "host_before": host_before,
            "host_after": self.host_state(),
            "configuration": self.harness.trial_configuration(),
        }
        return self.save_trial(
            scenario=scenario,
            variant=variant,
            block=block,
            trial=trial,
            workers=workers,
            schedulers=schedulers,
            count=count,
            queue=queue,
            task_type=task_type,
            payload=payload,
            submission=submission,
            tasks=tasks,
            attempts=attempts,
            raw=raw,
            correctness_result=correct,
            prom_start=prom_start,
            prom_end=prom_end,
            reconciliation=reconciliation,
            resource_samples=resources.samples,
            metadata=metadata,
            extra={
                "arrival_rate": rate,
                "order_index": order_index,
                "random_seed": random_seed,
            },
        )

    def api_trial(self, concurrency: int, trial: int) -> dict[str, Any]:
        scenario = "api_throughput"
        variant = f"c{concurrency}"
        count = int(self.profile["api_requests"])
        queue = f"tf012b-api-c{concurrency}-t{trial}"
        print(f"[api] concurrency={concurrency} trial={trial}", flush=True)
        host_before = self.host_state()
        prom_start = prepare_trial(self.harness, 0, 1)
        trial_started = dt.datetime.now(dt.UTC)
        with ResourceSampler(self.harness) as resources:
            submission = self.harness.loadgen(
                operation="submit",
                count=count,
                concurrency=concurrency,
                task_type="test.noop",
                payload="{}",
                queue=queue,
                max_attempts=1,
                key_mode="unique",
                key_prefix=queue,
            )
        trial_end = dt.datetime.now(dt.UTC)
        prom_end = finish_trial_snapshot(self.harness, 0, 1, trial_end)
        tasks, attempts = capture_rows(self.harness, queue)
        raw = derive_raw(tasks, attempts)
        raw["submission_throughput_per_second"] = submission["requests_per_second"]
        raw["submission_latency_ms"] = submission.get("latency_ms", {})
        correct = trusted_correctness(
            self.harness, queue, count, 0, terminal=False, expected_abandoned=0
        )
        reconciliation = build_reconciliation(raw, prom_start, prom_end, scenario)
        metadata = {
            "run_id": self.run_id,
            "scenario": scenario,
            "variant": variant,
            "classification": "PUBLIC",
            "block": trial,
            "trial": trial,
            "trial_start": trial_started.isoformat(),
            "trial_end": trial_end.isoformat(),
            "prometheus_start_sample_time": prom_start.get("sample_time_max"),
            "prometheus_end_sample_time": prom_end.get("sample_time_max"),
            "host_before": host_before,
            "host_after": self.host_state(),
            "configuration": self.harness.trial_configuration(),
        }
        return self.save_trial(
            scenario=scenario,
            variant=variant,
            block=trial,
            trial=trial,
            workers=0,
            schedulers=1,
            count=count,
            queue=queue,
            task_type="test.noop",
            payload={},
            submission=submission,
            tasks=tasks,
            attempts=attempts,
            raw=raw,
            correctness_result=correct,
            prom_start=prom_start,
            prom_end=prom_end,
            reconciliation=reconciliation,
            resource_samples=resources.samples,
            metadata=metadata,
            extra={"api_concurrency": concurrency},
        )

    def recovery_trial(self, trial: int) -> dict[str, Any]:
        scenario = "recovery_storm"
        workers = int(self.profile["recovery_workers"])
        count = int(self.profile["recovery_tasks"])
        percentage = int(self.profile["recovery_kill_percentage"])
        variant = f"kill-{percentage}"
        queue = f"tf012b-recovery-k{percentage}-t{trial}"
        print(f"[recovery] kill={percentage}% trial={trial}", flush=True)
        host_before = self.host_state()
        prom_start = prepare_trial(self.harness, workers, 1)
        trial_started = dt.datetime.now(dt.UTC)
        with ResourceSampler(self.harness) as resources:
            submission = self.harness.loadgen(
                operation="submit",
                count=count,
                concurrency=min(200, count),
                task_type="test.sleep",
                payload=json.dumps({"duration_ms": self.profile["recovery_sleep_ms"]}),
                queue=queue,
                max_attempts=2,
                key_mode="unique",
                key_prefix=queue,
            )
            self.harness.wait_for_status("RUNNING", workers, timeout=30)
            containers = worker_containers(self.harness)
            killed = max(1, math.ceil(len(containers) * percentage / 100))
            victims = containers[:killed]
            run_command(["docker", "pause", *victims], timeout=60)
            hostnames = container_hostnames(victims)
            escaped_names = ",".join("'" + name.replace("'", "''") + "'" for name in hostnames)
            captured = self.harness.json_sql(
                "SELECT json_build_object('tasks', coalesce(json_agg(json_build_object("
                "'id', tasks.id, 'lease_expires_at', tasks.lease_expires_at)), '[]'::json)) "
                "FROM tasks JOIN workers ON workers.id=tasks.claimed_by_worker_id "
                f"WHERE tasks.status='RUNNING' AND tasks.queue='{queue}' "
                f"AND workers.name IN ({escaped_names})"
            )
            run_command(["docker", "kill", *victims], timeout=60)
            self.harness.compose(
                "up",
                "-d",
                "--no-deps",
                "--scale",
                f"worker={workers}",
                "worker",
                timeout=180,
            )
            self.harness.current_workers = workers
            self.harness.wait_for_tasks(queue, count, timeout=max(300, count * 2))
        trial_end = dt.datetime.now(dt.UTC)
        prom_end = finish_trial_snapshot(self.harness, workers, 1, trial_end)
        tasks, attempts = capture_rows(self.harness, queue)
        raw = derive_raw(tasks, attempts)
        stranded = len(captured.get("tasks", []))
        correct = trusted_correctness(
            self.harness,
            queue,
            count,
            count + stranded,
            expected_abandoned=stranded,
        )
        reconciliation = build_reconciliation(
            raw,
            prom_start,
            prom_end,
            scenario,
            intentional_worker_churn=True,
        )
        metadata = {
            "run_id": self.run_id,
            "scenario": scenario,
            "variant": variant,
            "classification": "PUBLIC",
            "block": trial,
            "trial": trial,
            "trial_start": trial_started.isoformat(),
            "trial_end": trial_end.isoformat(),
            "prometheus_start_sample_time": prom_start.get("sample_time_max"),
            "prometheus_end_sample_time": prom_end.get("sample_time_max"),
            "host_before": host_before,
            "host_after": self.host_state(),
            "configuration": self.harness.trial_configuration(),
            "intentional_worker_churn": True,
        }
        return self.save_trial(
            scenario=scenario,
            variant=variant,
            block=trial,
            trial=trial,
            workers=workers,
            schedulers=1,
            count=count,
            queue=queue,
            task_type="test.sleep",
            payload={"duration_ms": self.profile["recovery_sleep_ms"]},
            submission=submission,
            tasks=tasks,
            attempts=attempts,
            raw=raw,
            correctness_result=correct,
            prom_start=prom_start,
            prom_end=prom_end,
            reconciliation=reconciliation,
            resource_samples=resources.samples,
            metadata=metadata,
            extra={
                "kill_percentage": percentage,
                "killed_workers": killed,
                "stranded_attempts": stranded,
                "captured_running_before_kill": captured,
            },
        )


def run_scaling(trusted: TrustedRun) -> None:
    profile = trusted.profile
    seed = int(profile["random_seed"])
    workloads = [
        ("noop_scaling", "test.noop", {}, int(profile["noop_tasks"])),
        ("io50_scaling", "test.sleep", {"duration_ms": 50}, int(profile["io_tasks"])),
        (
            "cpu_scaling",
            "test.cpu",
            {"iterations": int(profile["cpu_iterations"])},
            int(profile["cpu_tasks"]),
        ),
    ]
    for block in range(1, int(profile["required_blocks"]) + 1):
        if block > 1:
            reset_started = dt.datetime.now(dt.UTC)
            trusted.harness.reset()
            trusted.harness.start()
            block_warmup = warmup(trusted.harness, int(profile.get("warmup_tasks", 20)))
        else:
            reset_started = dt.datetime.now(dt.UTC)
            block_warmup = {"excluded": True, "source": "initial run warmup"}
        trusted.block_events.append(
            {
                "block": block,
                "fresh_environment": True,
                "reset_started_at": reset_started.isoformat(),
                "ready_at": dt.datetime.now(dt.UTC).isoformat(),
                "warmup": block_warmup,
            }
        )
        for scenario, task_type, payload, count in workloads:
            workers = list(profile["scaling_workers"])
            block_seed = seed + block * 100 + sum(ord(character) for character in scenario)
            random.Random(block_seed).shuffle(workers)
            for order_index, worker_count in enumerate(workers, start=1):
                trusted.processing_trial(
                    scenario=scenario,
                    variant=f"w{worker_count}",
                    block=block,
                    trial=block,
                    workers=int(worker_count),
                    task_type=task_type,
                    payload=payload,
                    count=count,
                    concurrency=min(200, count),
                    timeout=max(300, count * 0.25),
                    order_index=order_index,
                    random_seed=block_seed,
                )
                time.sleep(float(profile.get("cooldown_seconds", 0)))


def run_public_matrix(trusted: TrustedRun) -> None:
    profile = trusted.profile
    trials = int(profile["minimum_public_trials"])
    run_scaling(trusted)
    for concurrency in profile["api_concurrency"]:
        for trial in range(1, trials + 1):
            trusted.api_trial(int(concurrency), trial)
    for rate in profile["arrival_rates"]:
        for trial in range(1, trials + 1):
            count = int(rate * profile["arrival_seconds"])
            item = trusted.processing_trial(
                scenario="arrival_saturation",
                variant=f"r{rate}",
                block=trial,
                trial=trial,
                workers=int(profile["saturation_workers"]),
                task_type="test.noop",
                payload={},
                count=count,
                concurrency=min(200, count),
                rate=float(rate),
                timeout=max(180, int(profile["arrival_seconds"]) * 5),
            )
            processing = item["raw"].get("processing_throughput_per_second") or 0
            item["offered_rate_per_second"] = rate
            item["backpressure"] = {
                "offered_minus_processing_per_second": rate - processing,
                "queue_growth_detected": processing < rate * 0.95,
            }
    for trial in range(1, trials + 1):
        retry_count = int(profile["retry_tasks"])
        trusted.processing_trial(
            scenario="retry_storm",
            variant="fail-once",
            block=trial,
            trial=trial,
            workers=int(profile["retry_workers"]),
            schedulers=1,
            task_type="test.fail_n_then_succeed",
            payload={"failures": 1},
            count=retry_count,
            concurrency=min(200, retry_count),
            max_attempts=2,
            expected_attempts=retry_count * 2,
            timeout=max(300, retry_count),
        )
    for trial in range(1, trials + 1):
        trusted.recovery_trial(trial)


def write_results_csv(path: pathlib.Path, results: list[dict[str, Any]]) -> None:
    fields = [
        "scenario",
        "variant",
        "classification",
        "block",
        "trial",
        "workers",
        "schedulers",
        "count",
        "submission_throughput_per_second",
        "processing_throughput_per_second",
        "end_to_end_throughput_per_second",
        "queue_p95_seconds",
        "execution_p95_seconds",
        "total_p95_seconds",
        "prometheus_valid",
        "correctness_passed",
        "valid",
        "artifact_manifest",
    ]
    rows = []
    for item in results:
        rows.append(
            {
                "scenario": item["scenario"],
                "variant": item["variant"],
                "classification": item["classification"],
                "block": item.get("block"),
                "trial": item.get("trial"),
                "workers": item.get("workers"),
                "schedulers": item.get("schedulers"),
                "count": item.get("count"),
                "submission_throughput_per_second": item.get("submission", {}).get(
                    "requests_per_second"
                ),
                "processing_throughput_per_second": item.get("raw", {}).get(
                    "processing_throughput_per_second"
                ),
                "end_to_end_throughput_per_second": item.get("raw", {}).get(
                    "end_to_end_throughput_per_second"
                ),
                "queue_p95_seconds": item.get("raw", {}).get("queue_p95_seconds"),
                "execution_p95_seconds": item.get("raw", {}).get("execution_p95_seconds"),
                "total_p95_seconds": item.get("raw", {}).get("total_p95_seconds"),
                "prometheus_valid": item.get("prometheus_reconciliation", {}).get(
                    "prometheus_valid"
                ),
                "correctness_passed": item.get("correctness", {}).get("passed"),
                "valid": item.get("valid"),
                "artifact_manifest": item.get("artifacts", {}).get("manifest"),
            }
        )
    write_csv(path, rows, fields)


def generated_artifact_hashes(output_dir: pathlib.Path) -> dict[str, str]:
    paths = [output_dir / "report.md", *sorted((output_dir / "plots").glob("*"))]
    return {
        path.relative_to(output_dir).as_posix(): sha256_file(path)
        for path in paths
        if path.is_file()
    }


def generate_artifacts(results_path: pathlib.Path) -> dict[str, str]:
    output_dir = results_path.parent
    commands = [
        [sys.executable, str(BENCHMARKS / "report.py"), str(results_path)],
        [sys.executable, str(BENCHMARKS / "plot.py"), str(results_path)],
    ]
    for command in commands:
        run_command(command, timeout=120)
    first = generated_artifact_hashes(output_dir)
    for command in commands:
        run_command(command, timeout=120)
    second = generated_artifact_hashes(output_dir)
    if not first or first != second:
        raise BenchmarkError(
            f"report/plot regeneration was not byte-identical: first={first} second={second}"
        )
    return second


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("trust-smoke", "release"), default="trust-smoke")
    parser.add_argument("--project", default="taskforge-tf012-trusted")
    parser.add_argument("--output-dir", type=pathlib.Path)
    parser.add_argument("--development", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-regression", action="store_true")
    parser.add_argument("--keep", action="store_true")
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    publishable = not arguments.development
    if publishable and (arguments.skip_build or arguments.skip_regression):
        raise BenchmarkError(
            "publishable runs may not skip image builds or recorded regression commands"
        )
    contract = run_contract(development=arguments.development)
    source = contract["source"]
    profile = json.loads((BENCHMARKS / "config" / f"{arguments.profile}.json").read_text())
    run_id = new_run_id(source, arguments.profile)
    output_dir = arguments.output_dir or BENCHMARKS / "results" / run_id
    create_run_directory(output_dir)

    harness = Harness(profile, arguments.project, arguments.keep)
    document: dict[str, Any] = {
        "schema_version": 2,
        "tf_ticket": "TF-012B",
        "run_id": run_id,
        "profile": profile,
        "publishable": publishable,
        "publication_status": contract["publication_status"],
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "source": source,
        "harness": harness_identity(),
        "images": {},
        "environment": None,
        "regression": None,
        "warmup": None,
        "artifact_reproducibility": {
            "passed": False,
            "method": "two consecutive generations from saved results.json must be byte-identical",
        },
        "results": [],
        "summaries": [],
        "trust": None,
        "errors": [],
        "completed_at": None,
    }
    results_path = output_dir / "results.json"
    try:
        harness.reset()
        if not arguments.skip_build:
            harness.build()
        document["images"] = image_provenance(harness)
        document["regression"] = (
            {"passed": False, "skipped": True, "commands": []}
            if arguments.skip_regression
            else run_regressions(arguments.profile)
        )
        harness.start()
        document["environment"] = harness.environment()
        document["warmup"] = warmup(harness, int(profile.get("warmup_tasks", 20)))
        environment = document["environment"]
        trial_provenance = {
            "publishable": publishable,
            "publication_status": contract["publication_status"],
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
        run_public_matrix(trusted)
        document["results"] = trusted.results
        document["run_blocks"] = trusted.block_events
        document["summaries"] = aggregate(trusted.results)
        document["trust"] = evaluate_trust(document, output_dir)
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
    except Exception as exc:  # noqa: BLE001 - persist complete failure evidence
        document["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        document["trust"] = document.get("trust") or {
            "overall": {"result": "FAIL"},
            "execution": {"result": "FAIL", "error": str(exc)},
        }
        write_json(results_path, document)
        raise
    finally:
        harness.close()

    write_json(results_path, document)
    write_results_csv(output_dir / "results.csv", document["results"])
    generated = generate_artifacts(results_path)
    document["artifact_reproducibility"] = {
        "passed": True,
        "method": "two consecutive generations from saved results.json were byte-identical",
        "artifacts_compared": sorted(generated),
    }
    document["trust"] = evaluate_trust(document, output_dir)
    write_json(results_path, document)
    final_generated = generate_artifacts(results_path)
    if set(final_generated) != set(generated):
        raise BenchmarkError("final generated artifact set changed after trust evaluation")
    create_manifest(output_dir)
    print(results_path)
    if document["trust"]["overall"]["result"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
