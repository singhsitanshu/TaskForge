"""Pure TF-012 evidence, reconciliation, statistics, and trust-gate helpers."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import pathlib
import statistics
from collections.abc import Iterable
from typing import Any

HARNESS_VERSION = "TF-012D/2"
QUANTILE_METHOD = "linear interpolation at index (n - 1) * q (Hyndman-Fan type 7)"
PUBLIC_SCENARIOS = {
    "noop_scaling",
    "io50_scaling",
    "cpu_scaling",
    "api_submission",
    "api_throughput",
    "arrival_saturation",
    "retry_storm",
    "recovery_storm",
}
CORE_SCALING_SCENARIOS = {"noop_scaling", "io50_scaling", "cpu_scaling"}
API_SUBMISSION_SCENARIOS = {"api_submission", "api_throughput"}

COUNTER_METRICS = {
    "api_requests": "taskforge_api_requests_total",
    "api_submissions": "taskforge_task_submissions_total",
    "claimed": "taskforge_worker_tasks_claimed_total",
    "completed": "taskforge_worker_tasks_completed_total",
    "attempts": "taskforge_task_attempts_total",
    "retries_scheduled": "taskforge_task_retries_scheduled_total",
    "retry_promotions": "taskforge_task_retries_promoted_total",
    "recoveries": "taskforge_task_recoveries_total",
}
HISTOGRAM_METRICS = {
    "queue": "taskforge_task_queue_wait_seconds_bucket",
    "claim": "taskforge_worker_claim_duration_seconds_bucket",
    "execution": "taskforge_task_execution_duration_seconds_bucket",
    "retry_lateness": "taskforge_retry_lateness_seconds_bucket",
    "retry_batch": "taskforge_scheduler_retry_batch_duration_seconds_bucket",
    "recovery": "taskforge_recovery_lag_seconds_bucket",
    "recovery_batch": "taskforge_scheduler_recovery_batch_duration_seconds_bucket",
}
HISTOGRAM_COMPONENT_METRICS = tuple(
    component
    for bucket_metric in HISTOGRAM_METRICS.values()
    for component in (
        bucket_metric,
        bucket_metric.removesuffix("_bucket") + "_sum",
        bucket_metric.removesuffix("_bucket") + "_count",
    )
)
IDENTITY_METRICS = ("process_start_time_seconds",)
PROMETHEUS_METRICS = (
    tuple(COUNTER_METRICS.values()) + HISTOGRAM_COMPONENT_METRICS + IDENTITY_METRICS
)


def percentile(values: Iterable[float], fraction: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def parse_time(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds_between(end: str | None, start: str | None) -> float | None:
    end_time = parse_time(end)
    start_time = parse_time(start)
    if end_time is None or start_time is None:
        return None
    return (end_time - start_time).total_seconds()


def quantiles(prefix: str, values: list[float]) -> dict[str, float | None]:
    return {
        f"{prefix}_p50_seconds": percentile(values, 0.50),
        f"{prefix}_p95_seconds": percentile(values, 0.95),
        f"{prefix}_p99_seconds": percentile(values, 0.99),
    }


def derive_raw(tasks: list[dict[str, str]], attempts: list[dict[str, str]]) -> dict[str, Any]:
    task_statuses: dict[str, int] = {}
    attempt_statuses: dict[str, int] = {}
    for task in tasks:
        task_statuses[task["final_status"]] = task_statuses.get(task["final_status"], 0) + 1
    for attempt in attempts:
        attempt_statuses[attempt["status"]] = attempt_statuses.get(attempt["status"], 0) + 1

    queue_values: list[float] = []
    execution_values: list[float] = []
    total_values: list[float] = []
    retry_lateness: list[float] = []
    retry_delay: list[float] = []
    recovery_lag: list[float] = []
    missing_queue_evidence = 0
    negative = {
        "queue_wait": 0,
        "execution": 0,
        "total_latency": 0,
        "retry_lateness": 0,
        "recovery_lag": 0,
    }

    for attempt in attempts:
        started = attempt.get("attempt_started_at")
        queue_entered = attempt.get("queue_entered_at")
        if started:
            if not queue_entered:
                missing_queue_evidence += 1
            else:
                value = seconds_between(started, queue_entered)
                assert value is not None
                queue_values.append(value)
                negative["queue_wait"] += int(value < 0)
        if started and attempt.get("attempt_finished_at"):
            value = seconds_between(attempt["attempt_finished_at"], started)
            assert value is not None
            execution_values.append(value)
            negative["execution"] += int(value < 0)
        if (
            int(attempt["attempt_number"]) > 1
            and queue_entered
            and attempt.get("scheduled_at_snapshot")
        ):
            value = seconds_between(queue_entered, attempt["scheduled_at_snapshot"])
            assert value is not None
            retry_lateness.append(value)
            negative["retry_lateness"] += int(value < 0)
        if attempt.get("retry_scheduled_at") and attempt.get("attempt_finished_at"):
            value = seconds_between(attempt["retry_scheduled_at"], attempt["attempt_finished_at"])
            assert value is not None
            retry_delay.append(value)
        if attempt.get("recovered_at") and attempt.get("recovered_lease_expires_at"):
            value = seconds_between(attempt["recovered_at"], attempt["recovered_lease_expires_at"])
            assert value is not None
            recovery_lag.append(value)
            negative["recovery_lag"] += int(value < 0)

    completion_times: list[dt.datetime] = []
    creation_times: list[dt.datetime] = []
    for task in tasks:
        created = parse_time(task.get("task_created_at"))
        completed = parse_time(task.get("task_completed_at"))
        if created is not None:
            creation_times.append(created)
        if completed is not None and created is not None:
            value = (completed - created).total_seconds()
            total_values.append(value)
            completion_times.append(completed)
            negative["total_latency"] += int(value < 0)

    started_times = [
        parsed
        for attempt in attempts
        if (parsed := parse_time(attempt.get("attempt_started_at"))) is not None
    ]
    finished_times = [
        parsed
        for attempt in attempts
        if (parsed := parse_time(attempt.get("attempt_finished_at"))) is not None
    ]
    processing_seconds = (
        (max(finished_times) - min(started_times)).total_seconds()
        if started_times and finished_times
        else None
    )
    end_to_end_seconds = (
        (max(completion_times) - min(creation_times)).total_seconds()
        if completion_times and creation_times
        else None
    )
    steady_completions = 0
    steady_seconds: float | None = None
    if completion_times:
        ordered = sorted(completion_times)
        lower = ordered[math.floor((len(ordered) - 1) * 0.10)]
        upper = ordered[math.floor((len(ordered) - 1) * 0.90)]
        steady_completions = sum(lower <= value <= upper for value in ordered)
        steady_seconds = (upper - lower).total_seconds()

    count = len(tasks)
    raw: dict[str, Any] = {
        "task_count": count,
        "attempt_count": len(attempts),
        "status_counts": task_statuses,
        "attempt_status_counts": attempt_statuses,
        "queue_observations": len(queue_values),
        "execution_observations": len(execution_values),
        "total_observations": len(total_values),
        "retry_lateness_observations": len(retry_lateness),
        "recovery_lag_observations": len(recovery_lag),
        "missing_queue_evidence": missing_queue_evidence,
        "negative_durations": negative,
        "processing_seconds": processing_seconds,
        "end_to_end_seconds": end_to_end_seconds,
        "steady_state_completions": steady_completions,
        "steady_state_seconds": steady_seconds,
        "quantile_method": QUANTILE_METHOD,
    }
    raw.update(quantiles("queue", queue_values))
    raw.update(quantiles("execution", execution_values))
    raw.update(quantiles("total", total_values))
    raw.update(quantiles("retry_lateness", retry_lateness))
    raw.update(quantiles("retry_delay", retry_delay))
    raw.update(quantiles("recovery_lag", recovery_lag))
    raw["processing_throughput_per_second"] = (
        count / processing_seconds if processing_seconds and processing_seconds > 0 else None
    )
    raw["end_to_end_throughput_per_second"] = (
        count / end_to_end_seconds if end_to_end_seconds and end_to_end_seconds > 0 else None
    )
    raw["steady_state_throughput_per_second"] = (
        steady_completions / steady_seconds if steady_seconds and steady_seconds > 0 else None
    )
    return raw


def derive_retry_raw(tasks: list[dict[str, str]], attempts: list[dict[str, str]]) -> dict[str, Any]:
    """Extend immutable lifecycle evidence with attempt-2-only queue timing."""
    raw = derive_raw(tasks, attempts)
    attempt2_queue: list[float] = []
    missing_attempt2_queue_evidence = 0
    negative_attempt2_queue = 0
    for attempt in attempts:
        if int(attempt.get("attempt_number") or 0) != 2:
            continue
        started = attempt.get("attempt_started_at")
        queue_entered = attempt.get("queue_entered_at")
        if not started or not queue_entered:
            missing_attempt2_queue_evidence += 1
            continue
        value = seconds_between(started, queue_entered)
        assert value is not None
        attempt2_queue.append(value)
        negative_attempt2_queue += int(value < 0)
    raw["attempt2_queue_observations"] = len(attempt2_queue)
    raw["missing_attempt2_queue_evidence"] = missing_attempt2_queue_evidence
    raw["negative_durations"]["attempt2_queue_wait"] = negative_attempt2_queue
    raw.update(quantiles("attempt2_queue", attempt2_queue))
    return raw


def derive_recovery_raw(
    tasks: list[dict[str, str]],
    attempts: list[dict[str, str]],
    failure_boundary: dict[str, Any],
) -> dict[str, Any]:
    """Extend immutable lifecycle evidence with crash-boundary recovery timing."""
    raw = derive_raw(tasks, attempts)
    kill_time = parse_time(failure_boundary.get("kill_timestamp"))
    completion_times = [
        parsed
        for task in tasks
        if (parsed := parse_time(task.get("task_completed_at"))) is not None
    ]
    kill_to_drain = (
        (max(completion_times) - kill_time).total_seconds()
        if kill_time is not None and completion_times
        else None
    )
    affected = failure_boundary.get("affected_tasks", [])
    raw["affected_task_count"] = len(affected) if isinstance(affected, list) else 0
    raw["kill_to_final_drain_seconds"] = kill_to_drain
    raw["negative_durations"]["kill_to_final_drain"] = int(
        kill_to_drain is not None and kill_to_drain < 0
    )
    return raw


def recovery_history_evidence(
    tasks: list[dict[str, str]],
    attempts: list[dict[str, str]],
    failure_boundary: dict[str, Any],
    expected_tasks: int,
) -> dict[str, Any]:
    """Validate the exact captured ABANDONED-to-success recovery histories."""
    task_ids = {row.get("task_id") for row in tasks}
    attempts_by_task: dict[str | None, list[dict[str, str]]] = {}
    for attempt in attempts:
        attempts_by_task.setdefault(attempt.get("task_id"), []).append(attempt)

    affected_rows = failure_boundary.get("affected_tasks", [])
    affected = affected_rows if isinstance(affected_rows, list) else []
    affected_ids = [row.get("task_id") for row in affected if isinstance(row, dict)]
    affected_set = {value for value in affected_ids if value}
    selected_workers = failure_boundary.get("selected_workers", [])
    selected = selected_workers if isinstance(selected_workers, list) else []
    selected_owner_ids = {
        row.get("worker_id") for row in selected if isinstance(row, dict) and row.get("worker_id")
    }
    affected_owner_ids = {
        row.get("owner_worker_id")
        for row in affected
        if isinstance(row, dict) and row.get("owner_worker_id")
    }

    duplicate_attempt_identities = len(attempts) - len(
        {(row.get("task_id"), row.get("attempt_number")) for row in attempts}
    )
    orphan_attempts = sum(row.get("task_id") not in task_ids for row in attempts)
    abandoned_rows = [row for row in attempts if row.get("status") == "ABANDONED"]
    abandoned_ids = {row.get("task_id") for row in abandoned_rows}
    unaffected_abandoned = len(abandoned_ids - affected_set)
    duplicate_abandonments = len(abandoned_rows) - len(abandoned_ids)

    recovered_successes = 0
    captured_attempt_mismatches = 0
    replacement_failures = 0
    stale_owner_completions = 0
    unexpected_histories = 0
    for boundary_row in affected:
        if not isinstance(boundary_row, dict):
            captured_attempt_mismatches += 1
            continue
        task_id = boundary_row.get("task_id")
        captured_number = str(boundary_row.get("attempt_number") or "")
        captured_id = boundary_row.get("attempt_id")
        rows = sorted(
            attempts_by_task.get(task_id, []),
            key=lambda row: int(row.get("attempt_number") or 0),
        )
        captured = [
            row
            for row in rows
            if row.get("attempt_number") == captured_number and row.get("attempt_id") == captured_id
        ]
        captured_ok = bool(
            len(captured) == 1
            and captured[0].get("status") == "ABANDONED"
            and captured[0].get("worker_id") == boundary_row.get("owner_worker_id")
            and captured[0].get("recovery_action") == "requeued"
            and captured[0].get("recovered_at")
            and captured[0].get("recovered_lease_expires_at")
            == boundary_row.get("lease_expires_at")
        )
        captured_attempt_mismatches += int(not captured_ok)
        stale_owner_completions += int(bool(captured) and captured[0].get("status") == "SUCCEEDED")
        later = [
            row for row in rows if int(row.get("attempt_number") or 0) > int(captured_number or 0)
        ]
        later_success = [row for row in later if row.get("status") == "SUCCEEDED"]
        stale_owner_completions += sum(
            row.get("worker_id") in selected_owner_ids for row in later_success
        )
        exact_history = bool(
            captured_ok
            and len(rows) == 2
            and len(later) == 1
            and len(later_success) == 1
            and int(later[0].get("attempt_number") or 0) == int(captured_number or 0) + 1
        )
        recovered_successes += int(exact_history)
        replacement_failures += int(not later_success)
        unexpected_histories += int(not exact_history)

    first_attempt_successes = 0
    for task_id in task_ids - affected_set:
        rows = attempts_by_task.get(task_id, [])
        exact_first_success = bool(
            len(rows) == 1
            and rows[0].get("attempt_number") == "1"
            and rows[0].get("status") == "SUCCEEDED"
        )
        first_attempt_successes += int(exact_first_success)
        unexpected_histories += int(not exact_first_success)

    unfinished_attempts = sum(
        row.get("status") not in {"SUCCEEDED", "FAILED", "ABANDONED"} for row in attempts
    )
    selected_worker_count = int(failure_boundary.get("expected_killed_workers") or 0)
    liveness = failure_boundary.get("worker_liveness", {})
    liveness_ok = bool(
        isinstance(liveness, dict)
        and int(liveness.get("killed_dead") or 0) == selected_worker_count
        and int(liveness.get("surviving_active") or 0)
        == int(liveness.get("expected_surviving_workers") or 0)
    )
    boundary_shape_ok = bool(
        failure_boundary.get("kill_timestamp")
        and len(selected) == selected_worker_count
        and len(selected_owner_ids) == selected_worker_count
        and affected_set
        and len(affected_ids) == len(affected_set)
        and len({row.get("attempt_id") for row in affected if isinstance(row, dict)})
        == len(affected_set)
        and int(failure_boundary.get("affected_task_count") or 0) == len(affected_set)
        and affected_owner_ids == selected_owner_ids
        and all(
            isinstance(row, dict)
            and row.get("pre_kill_status") == "RUNNING"
            and row.get("task_id")
            and row.get("attempt_id")
            and row.get("attempt_number")
            and row.get("lease_expires_at")
            and row.get("owner_worker_id")
            for row in affected
        )
    )
    expected_attempts = expected_tasks + len(affected_set)
    evidence = {
        "recovery_history_expected": True,
        "expected_affected_tasks": len(affected_set),
        "affected_tasks": len(affected_set),
        "expected_abandoned": len(affected_set),
        "abandoned_attempts": len(abandoned_rows),
        "first_attempt_successes": first_attempt_successes,
        "recovered_replacement_successes": recovered_successes,
        "captured_attempt_mismatches": captured_attempt_mismatches,
        "replacement_success_failures": replacement_failures,
        "unaffected_abandoned_attempts": unaffected_abandoned,
        "duplicate_recovery_abandonments": duplicate_abandonments,
        "duplicate_attempt_identities": duplicate_attempt_identities,
        "orphan_attempts": orphan_attempts,
        "stale_owner_completions": stale_owner_completions,
        "unfinished_attempts": unfinished_attempts,
        "unexpected_recovery_histories": unexpected_histories,
        "expected_total_attempts": expected_attempts,
        "worker_liveness_passed": liveness_ok,
        "failure_boundary_passed": boundary_shape_ok,
        "stale_lease_renewal_evidence_available": False,
    }
    evidence["recovery_history_passed"] = bool(
        len(tasks) == expected_tasks
        and len(attempts) == expected_attempts
        and task_ids == set(attempts_by_task)
        and affected_set <= task_ids
        and len(abandoned_rows) == len(affected_set)
        and abandoned_ids == affected_set
        and first_attempt_successes == expected_tasks - len(affected_set)
        and recovered_successes == len(affected_set)
        and captured_attempt_mismatches == 0
        and replacement_failures == 0
        and unaffected_abandoned == 0
        and duplicate_abandonments == 0
        and duplicate_attempt_identities == 0
        and orphan_attempts == 0
        and stale_owner_completions == 0
        and unfinished_attempts == 0
        and unexpected_histories == 0
        and boundary_shape_ok
        and liveness_ok
    )
    return evidence


def retry_history_evidence(
    tasks: list[dict[str, str]], attempts: list[dict[str, str]], expected_tasks: int
) -> dict[str, Any]:
    """Summarize the exact fail-once-then-succeed durable attempt contract."""
    attempt1 = [row for row in attempts if row.get("attempt_number") == "1"]
    attempt2 = [row for row in attempts if row.get("attempt_number") == "2"]
    attempt_numbers = {row.get("attempt_number") for row in attempts}
    task_ids = {row.get("task_id") for row in tasks}
    attempts_by_task: dict[str | None, list[dict[str, str]]] = {}
    for attempt in attempts:
        attempts_by_task.setdefault(attempt.get("task_id"), []).append(attempt)

    retry_schedules = sum(bool(row.get("retry_scheduled_at")) for row in attempt1)
    retry_promotions = 0
    retry_chain_mismatches = 0
    for task_id in task_ids:
        rows = attempts_by_task.get(task_id, [])
        by_number = {row.get("attempt_number"): row for row in rows}
        first = by_number.get("1")
        second = by_number.get("2")
        exact_pair = len(rows) == 2 and set(by_number) == {"1", "2"}
        exact_statuses = bool(
            first
            and second
            and first.get("status") == "FAILED"
            and second.get("status") == "SUCCEEDED"
        )
        schedule = parse_time(first.get("retry_scheduled_at")) if first else None
        scheduled_snapshot = parse_time(second.get("scheduled_at_snapshot")) if second else None
        queue_entered = parse_time(second.get("queue_entered_at")) if second else None
        exact_promotion = bool(
            schedule
            and scheduled_snapshot
            and queue_entered
            and schedule == scheduled_snapshot
            and queue_entered >= scheduled_snapshot
        )
        retry_promotions += int(exact_promotion)
        retry_chain_mismatches += int(not (exact_pair and exact_statuses and exact_promotion))

    orphan_attempt_tasks = len(set(attempts_by_task) - task_ids)
    duplicate_identities = len(attempts) - len(
        {(row.get("task_id"), row.get("attempt_number")) for row in attempts}
    )
    evidence = {
        "retry_history_expected": True,
        "expected_attempt1_failed": expected_tasks,
        "attempt1_failed": sum(row.get("status") == "FAILED" for row in attempt1),
        "expected_attempt2_succeeded": expected_tasks,
        "attempt2_succeeded": sum(row.get("status") == "SUCCEEDED" for row in attempt2),
        "expected_retry_schedules": expected_tasks,
        "retry_schedules": retry_schedules,
        "expected_retry_promotions": expected_tasks,
        "retry_promotions": retry_promotions,
        "retry_duplicate_identities": duplicate_identities,
        "retry_chain_mismatches": retry_chain_mismatches,
        "retry_orphan_attempt_tasks": orphan_attempt_tasks,
        "retry_unexpected_attempt_numbers": sorted(
            str(value) for value in attempt_numbers - {"1", "2"}
        ),
    }
    evidence["retry_history_passed"] = bool(
        len(tasks) == expected_tasks
        and len(attempts) == expected_tasks * 2
        and len(attempt1) == expected_tasks
        and len(attempt2) == expected_tasks
        and evidence["attempt1_failed"] == expected_tasks
        and evidence["attempt2_succeeded"] == expected_tasks
        and retry_schedules == expected_tasks
        and retry_promotions == expected_tasks
        and duplicate_identities == 0
        and retry_chain_mismatches == 0
        and orphan_attempt_tasks == 0
        and not evidence["retry_unexpected_attempt_numbers"]
    )
    return evidence


def write_csv(path: pathlib.Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: pathlib.Path) -> list[dict[str, str]]:
    with path.open(newline="") as source:
        return list(csv.DictReader(source))


def same_measurement(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, int | float) and isinstance(right, int | float):
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
    return left == right


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: pathlib.Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def create_manifest(directory: pathlib.Path, *, exclude: set[str] | None = None) -> dict[str, Any]:
    excluded = {"manifest.json"} | (exclude or set())
    artifacts = []
    for path in sorted(candidate for candidate in directory.rglob("*") if candidate.is_file()):
        relative = path.relative_to(directory).as_posix()
        if relative in excluded:
            continue
        artifacts.append(
            {"path": relative, "sha256": sha256_file(path), "bytes": path.stat().st_size}
        )
    manifest = {
        "schema_version": 1,
        "algorithm": "sha256",
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "artifacts": artifacts,
    }
    write_json(directory / "manifest.json", manifest)
    return manifest


def verify_manifest(directory: pathlib.Path) -> tuple[bool, list[str]]:
    manifest_path = directory / "manifest.json"
    if not manifest_path.exists():
        return False, ["manifest.json missing"]
    manifest = json.loads(manifest_path.read_text())
    errors = []
    if manifest.get("algorithm") != "sha256":
        errors.append("manifest algorithm is not sha256")
    listed: set[str] = set()
    for item in manifest.get("artifacts", []):
        relative = item.get("path", "")
        parts = pathlib.PurePosixPath(relative).parts
        if not relative or pathlib.PurePosixPath(relative).is_absolute() or ".." in parts:
            errors.append(f"unsafe artifact path: {relative!r}")
            continue
        if relative in listed:
            errors.append(f"{relative} listed more than once")
            continue
        listed.add(relative)
        path = directory / relative
        if not path.exists():
            errors.append(f"{relative} missing")
        elif sha256_file(path) != item.get("sha256"):
            errors.append(f"{relative} hash mismatch")
        elif path.stat().st_size != item.get("bytes"):
            errors.append(f"{relative} size mismatch")
    actual = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*")
        if path.is_file() and path != manifest_path
    }
    for relative in sorted(actual - listed):
        errors.append(f"{relative} is not listed")
    return not errors, errors


def _sample_rows(snapshot: dict[str, Any], metric: str) -> list[dict[str, Any]]:
    rows = snapshot.get("metrics", {}).get(metric, [])
    return rows if isinstance(rows, list) else []


def _series_key(row: dict[str, Any], *, omit: set[str] | None = None) -> str:
    labels = {
        key: value for key, value in row.get("metric", {}).items() if key not in (omit or set())
    }
    return json.dumps(labels, sort_keys=True, separators=(",", ":"))


def _series_values(
    snapshot: dict[str, Any], metric: str, *, labels: dict[str, str] | None = None
) -> dict[str, float]:
    values: dict[str, float] = {}
    for row in _sample_rows(snapshot, metric):
        series_labels = row.get("metric", {})
        if labels and any(series_labels.get(key) != value for key, value in labels.items()):
            continue
        values[_series_key(row)] = float(row["value"][1])
    return values


def counter_delta(
    start: dict[str, Any],
    end: dict[str, Any],
    metric: str,
    *,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    before = _series_values(start, metric, labels=labels)
    after = _series_values(end, metric, labels=labels)
    decreases = []
    missing = []
    total = 0.0
    for key in sorted(set(before) | set(after)):
        start_value = before.get(key, 0.0)
        if key not in after:
            missing.append(key)
            continue
        end_value = after[key]
        if end_value < start_value:
            decreases.append(key)
        else:
            total += end_value - start_value
    return {
        "delta": total,
        "valid": not decreases and not missing,
        "counter_decreases": decreases,
        "missing_end_series": missing,
    }


def histogram_delta(start: dict[str, Any], end: dict[str, Any], metric: str) -> dict[str, Any]:
    def by_series(snapshot: dict[str, Any]) -> dict[str, tuple[float, float]]:
        values: dict[str, tuple[float, float]] = {}
        for row in _sample_rows(snapshot, metric):
            labels = row.get("metric", {})
            le_text = labels.get("le")
            if le_text is None:
                continue
            upper = math.inf if le_text == "+Inf" else float(le_text)
            values[_series_key(row)] = (upper, float(row["value"][1]))
        return values

    before = by_series(start)
    after = by_series(end)
    buckets: dict[float, float] = {}
    decreases = []
    missing = []
    for key in sorted(set(before) | set(after)):
        start_upper, start_value = before.get(key, after.get(key, (math.inf, 0.0)))
        if key not in after:
            missing.append(key)
            continue
        end_upper, end_value = after[key]
        upper = (
            end_upper if math.isfinite(end_upper) or not math.isfinite(start_upper) else start_upper
        )
        if end_value < start_value:
            decreases.append(key)
        else:
            buckets[upper] = buckets.get(upper, 0.0) + end_value - start_value
    ordered = sorted(buckets.items(), key=lambda item: item[0])
    base = metric.removesuffix("_bucket")
    sum_delta = counter_delta(start, end, base + "_sum")
    count_delta = counter_delta(start, end, base + "_count")
    non_monotonic = []
    previous = 0.0
    for upper, count in ordered:
        if count < previous:
            non_monotonic.append("+Inf" if math.isinf(upper) else str(upper))
        previous = count
    bucket_count = ordered[-1][1] if ordered else 0.0
    count_matches = math.isclose(
        bucket_count,
        float(count_delta["delta"]),
        rel_tol=0,
        abs_tol=1e-9,
    )
    valid = bool(
        not decreases
        and not missing
        and not non_monotonic
        and sum_delta["valid"]
        and count_delta["valid"]
        and count_matches
    )
    return {
        "buckets": [
            {"le": "+Inf" if math.isinf(upper) else upper, "count": count}
            for upper, count in ordered
        ],
        "sum": sum_delta,
        "count": count_delta,
        "valid": valid,
        "counter_decreases": decreases,
        "missing_end_series": missing,
        "non_monotonic_buckets": non_monotonic,
        "bucket_count_matches_count": count_matches,
    }


def histogram_quantile(
    delta: dict[str, Any], fraction: float
) -> tuple[float | None, tuple[float, float] | None]:
    buckets = [
        (math.inf if item["le"] == "+Inf" else float(item["le"]), float(item["count"]))
        for item in delta.get("buckets", [])
    ]
    if not buckets:
        return None, None
    total = buckets[-1][1]
    if total <= 0:
        return None, None
    rank = fraction * total
    previous_upper = 0.0
    previous_count = 0.0
    for upper, cumulative in buckets:
        if cumulative >= rank:
            if math.isinf(upper):
                estimate = previous_upper
                return estimate, (previous_upper, math.inf)
            bucket_count = cumulative - previous_count
            if bucket_count <= 0:
                return upper, (previous_upper, upper)
            estimate = previous_upper + (upper - previous_upper) * (
                (rank - previous_count) / bucket_count
            )
            return estimate, (previous_upper, upper)
        previous_upper = upper
        previous_count = cumulative
    return None, None


def target_ids(snapshot: dict[str, Any], job: str | None = None) -> set[str]:
    targets = snapshot.get("targets", [])
    return {
        f"{item.get('job')}|{item.get('instance')}"
        for item in targets
        if job is None or item.get("job") == job
    }


def replica_ids(snapshot: dict[str, Any], service: str | None = None) -> set[str]:
    return {
        f"{item.get('service')}|{item.get('name')}|{item.get('container_id')}"
        for item in snapshot.get("replicas", [])
        if service is None or item.get("service") == service
    }


def boundary_errors(snapshot: dict[str, Any]) -> list[str]:
    errors = []
    boundary_after = snapshot.get("boundary_after")
    sample_time = snapshot.get("sample_time_min")
    if boundary_after is not None:
        boundary_epoch = parse_time(boundary_after)
        if boundary_epoch is None or sample_time is None:
            errors.append("boundary sample timestamp missing")
        elif float(sample_time) <= boundary_epoch.timestamp():
            errors.append("boundary sample is not newer than requested boundary")
    targets = snapshot.get("targets", [])
    for target in targets:
        if target.get("health") != "up" or target.get("last_error"):
            errors.append(f"target not up: {target.get('job')}|{target.get('instance')}")
    if "replicas" in snapshot:
        target_counts: dict[str, int] = {}
        for target in targets:
            target_counts[target.get("job")] = target_counts.get(target.get("job"), 0) + 1
        service_jobs = {
            "api": "taskforge-api",
            "worker": "taskforge-worker",
            "scheduler": "taskforge-scheduler",
        }
        replicas = snapshot.get("replicas", [])
        for service, job in service_jobs.items():
            service_replicas = [item for item in replicas if item.get("service") == service]
            if len(service_replicas) != target_counts.get(job, 0):
                errors.append(
                    f"target/replica count mismatch for {service}: "
                    f"targets={target_counts.get(job, 0)} replicas={len(service_replicas)}"
                )
            for replica in service_replicas:
                if replica.get("state") != "running":
                    errors.append(f"replica not running: {service}|{replica.get('name')}")
    return errors


def process_identity_changes(start: dict[str, Any], end: dict[str, Any]) -> dict[str, list[str]]:
    before = _series_values(start, IDENTITY_METRICS[0])
    after = _series_values(end, IDENTITY_METRICS[0])
    expected_start = {
        target
        for target in target_ids(start)
        if target.startswith(("taskforge-worker|", "taskforge-scheduler|"))
    }
    expected_end = {
        target
        for target in target_ids(end)
        if target.startswith(("taskforge-worker|", "taskforge-scheduler|"))
    }
    observed_start = {
        f"{row.get('metric', {}).get('job')}|{row.get('metric', {}).get('instance')}"
        for row in _sample_rows(start, IDENTITY_METRICS[0])
    }
    observed_end = {
        f"{row.get('metric', {}).get('job')}|{row.get('metric', {}).get('instance')}"
        for row in _sample_rows(end, IDENTITY_METRICS[0])
    }
    return {
        "restarted": sorted(
            key
            for key in set(before) & set(after)
            if not math.isclose(before[key], after[key], rel_tol=0, abs_tol=1e-9)
        ),
        "missing": sorted(set(before) - set(after)),
        "new": sorted(set(after) - set(before)),
        "unobserved_start_targets": sorted(expected_start - observed_start),
        "unobserved_end_targets": sorted(expected_end - observed_end),
    }


def _identity_target(value: str) -> str:
    try:
        labels = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value
    return f"{labels.get('job')}|{labels.get('instance')}"


def allowed_worker_churn_analysis(
    start: dict[str, Any],
    end: dict[str, Any],
    identities: dict[str, list[str]],
    allowed: dict[str, Any] | None,
) -> dict[str, Any]:
    """Separate explicitly killed worker churn from unrelated churn."""
    if not allowed:
        return {
            "configured": False,
            "unexpected_target_churn": target_ids(start) != target_ids(end),
            "unexpected_replica_churn": replica_ids(start) != replica_ids(end),
            "unexpected_process_identity_changes": identities,
            "allowed_worker_churn_observed": False,
            "unobserved_allowed_worker_churn": [],
        }

    allowed_start_targets = set(allowed.get("start_targets", []))
    allowed_end_targets = set(allowed.get("end_targets", []))
    allowed_start_replicas = set(allowed.get("start_replicas", []))
    allowed_end_replicas = set(allowed.get("end_replicas", []))
    start_workers = target_ids(start, "taskforge-worker")
    end_workers = target_ids(end, "taskforge-worker")
    unexpected_worker_targets = bool(
        (start_workers - end_workers) - allowed_start_targets
        or (end_workers - start_workers) - allowed_end_targets
    )
    stable_services_changed = any(
        target_ids(start, job) != target_ids(end, job)
        for job in ("taskforge-api", "taskforge-scheduler")
    )

    unexpected_identities: dict[str, list[str]] = {}
    for kind, values in identities.items():
        permitted = (
            allowed_end_targets
            if kind in {"new", "unobserved_end_targets"}
            else allowed_start_targets
        )
        unexpected_identities[kind] = sorted(
            value for value in values if _identity_target(value) not in permitted
        )

    unexpected_replicas = bool(
        (replica_ids(start) - replica_ids(end)) - allowed_start_replicas
        or (replica_ids(end) - replica_ids(start)) - allowed_end_replicas
    )
    changed_start_targets = {
        _identity_target(value)
        for kind in ("restarted", "missing", "unobserved_start_targets")
        for value in identities.get(kind, [])
    } | (start_workers - end_workers)
    changed_end_targets = {
        _identity_target(value)
        for kind in ("restarted", "new", "unobserved_end_targets")
        for value in identities.get(kind, [])
    } | (end_workers - start_workers)
    removed_replicas = replica_ids(start) - replica_ids(end)
    added_replicas = replica_ids(end) - replica_ids(start)
    unobserved_pairs = []
    for pair in allowed.get("pairs", []):
        start_target = pair.get("start_target")
        end_target = pair.get("end_target")
        start_replica = pair.get("start_replica")
        end_replica = pair.get("end_replica")
        observed = bool(
            start_target in changed_start_targets
            or end_target in changed_end_targets
            or start_replica in removed_replicas
            or end_replica in added_replicas
        )
        if not observed:
            unobserved_pairs.append(str(pair.get("worker_name") or start_target))
    return {
        "configured": True,
        "unexpected_target_churn": stable_services_changed or unexpected_worker_targets,
        "unexpected_replica_churn": unexpected_replicas,
        "unexpected_process_identity_changes": unexpected_identities,
        "allowed_worker_churn_observed": bool(allowed.get("pairs")) and not unobserved_pairs,
        "unobserved_allowed_worker_churn": sorted(unobserved_pairs),
    }


def delta_quantiles(delta: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for percentile, fraction in ((50, 0.50), (95, 0.95), (99, 0.99)):
        estimate, bucket = histogram_quantile(delta, fraction)
        values[f"p{percentile}"] = estimate
        values[f"p{percentile}_bucket"] = bucket
    return values


def build_reconciliation(
    raw: dict[str, Any],
    start: dict[str, Any],
    end: dict[str, Any],
    scenario: str,
    *,
    intentional_worker_churn: bool = False,
    allowed_worker_churn: dict[str, Any] | None = None,
    legacy_execution_percentile: bool = False,
    require_retry_batch: bool = False,
    require_recovery_contract: bool = False,
) -> dict[str, Any]:
    start_targets = target_ids(start)
    end_targets = target_ids(end)
    worker_churn = target_ids(start, "taskforge-worker") != target_ids(end, "taskforge-worker")
    identities = process_identity_changes(start, end)
    start_replica_ids = replica_ids(start)
    end_replica_ids = replica_ids(end)
    replica_churn = start_replica_ids != end_replica_ids
    start_boundary_errors = boundary_errors(start)
    end_boundary_errors = boundary_errors(end)
    churn = allowed_worker_churn_analysis(start, end, identities, allowed_worker_churn)
    stable_target_churn = (
        churn["unexpected_target_churn"]
        or churn["unexpected_replica_churn"]
        or any(churn["unexpected_process_identity_changes"].values())
        or (bool(allowed_worker_churn) and not churn["allowed_worker_churn_observed"])
        or bool(start_boundary_errors)
        or bool(end_boundary_errors)
    )
    counters: dict[str, Any] = {}

    expected_counters: dict[str, int] = {}
    counter_labels: dict[str, dict[str, str]] = {}
    counter_metrics: dict[str, str] = {}
    if scenario in API_SUBMISSION_SCENARIOS:
        expected_counters["api_requests"] = int(raw.get("task_count", 0))
        counter_labels["api_requests"] = {
            "method": "POST",
            "route": "/tasks",
            "status_class": "2xx",
        }
        expected_counters["api_submissions"] = int(raw.get("task_count", 0))
        counter_labels["api_submissions"] = {"outcome": "created"}
    elif scenario == "recovery_storm" and require_recovery_contract:
        recovered = int(raw.get("attempt_status_counts", {}).get("ABANDONED", 0))
        expected_counters["recoveries_requeued"] = recovered
        counter_labels["recoveries_requeued"] = {"outcome": "requeued"}
        counter_metrics["recoveries_requeued"] = COUNTER_METRICS["recoveries"]
        expected_counters["recoveries_exhausted"] = 0
        counter_labels["recoveries_exhausted"] = {"outcome": "failed"}
        counter_metrics["recoveries_exhausted"] = COUNTER_METRICS["recoveries"]
    else:
        expected_counters["claimed"] = int(raw.get("attempt_count", 0))
        expected_counters["completed"] = int(raw.get("attempt_count", 0)) - int(
            raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
        )
        expected_counters["attempts"] = int(raw.get("attempt_count", 0))
    if scenario == "retry_storm":
        expected_counters["retries_scheduled"] = int(
            raw.get("attempt_status_counts", {}).get("FAILED", 0)
        )
        expected_counters["retry_promotions"] = int(raw.get("retry_lateness_observations", 0))
    if scenario == "recovery_storm" and not require_recovery_contract:
        expected_counters["recoveries"] = int(
            raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
        )

    for name, expected in expected_counters.items():
        measured = counter_delta(
            start,
            end,
            counter_metrics[name] if name in counter_metrics else COUNTER_METRICS[name],
            labels=counter_labels.get(name),
        )
        difference = measured["delta"] - expected
        measured.update(
            {
                "raw": expected,
                "prometheus": measured["delta"],
                "difference": difference,
                "tolerance": 0,
                "status": "PASS"
                if measured["valid"] and difference == 0 and not stable_target_churn
                else "FAIL",
            }
        )
        counters[name] = measured

    histograms: dict[str, Any] = {}
    histogram_specs: list[tuple[str, str, str, int]] = []
    if scenario not in API_SUBMISSION_SCENARIOS and not require_recovery_contract:
        histogram_specs.extend(
            [
                ("queue", "queue_p95_seconds", "queue_observations", 95),
            ]
        )
        claim_delta = histogram_delta(start, end, HISTOGRAM_METRICS["claim"])
        claim_quantiles = delta_quantiles(claim_delta)
        claim_estimate = claim_quantiles["p95"]
        claim_bucket = claim_quantiles["p95_bucket"]
        claim_observations = (
            float(claim_delta["buckets"][-1]["count"]) if claim_delta.get("buckets") else 0.0
        )
        expected_claims = int(raw.get("attempt_count", 0))
        histograms["claim"] = {
            **claim_delta,
            "prometheus_quantiles": claim_quantiles,
            "raw": None,
            "prometheus": claim_estimate,
            "raw_expected_observations": expected_claims,
            "prometheus_observations": claim_observations,
            "raw_bucket": claim_bucket,
            "status": "PASS"
            if claim_delta["valid"]
            and not stable_target_churn
            and claim_observations == expected_claims
            else "FAIL",
            "note": "Operational-only histogram; PostgreSQL has no claim-transaction timestamp pair.",
        }
        if legacy_execution_percentile:
            histogram_specs.append(
                ("execution", "execution_p95_seconds", "execution_observations", 95)
            )
        else:
            execution_delta = histogram_delta(start, end, HISTOGRAM_METRICS["execution"])
            execution_quantiles = delta_quantiles(execution_delta)
            execution_observations = (
                float(execution_delta["buckets"][-1]["count"])
                if execution_delta.get("buckets")
                else 0.0
            )
            expected_executions = int(raw.get("execution_observations", 0))
            histograms["execution"] = {
                **execution_delta,
                "prometheus_quantiles": execution_quantiles,
                "raw": None,
                "prometheus": execution_quantiles["p95"],
                "database_attempt_p95_seconds": raw.get("execution_p95_seconds"),
                "raw_expected_observations": expected_executions,
                "prometheus_observations": execution_observations,
                "raw_bucket": None,
                "status": "PASS"
                if execution_delta["valid"]
                and not stable_target_churn
                and execution_observations == expected_executions
                else "FAIL",
                "note": (
                    "Count-and-structure reconciliation only: the worker histogram times "
                    "handler invocation with Go's monotonic clock, while the immutable "
                    "database attempt interval spans claim and completion work using "
                    "PostgreSQL wall-clock timestamps. Their percentiles are not "
                    "semantically comparable."
                ),
            }
    if scenario == "retry_storm":
        histogram_specs.append(
            ("retry_lateness", "retry_lateness_p95_seconds", "retry_lateness_observations", 95)
        )
    if scenario == "recovery_storm":
        histogram_specs.append(
            ("recovery", "recovery_lag_p95_seconds", "recovery_lag_observations", 95)
        )
    for name, raw_key, count_key, quantile in histogram_specs:
        delta = histogram_delta(start, end, HISTOGRAM_METRICS[name])
        quantiles = delta_quantiles(delta)
        estimate = quantiles[f"p{quantile}"]
        bucket = quantiles[f"p{quantile}_bucket"]
        raw_value = raw.get(raw_key)
        observation_count = float(delta["buckets"][-1]["count"]) if delta.get("buckets") else 0.0
        expected_observations = int(raw.get(count_key, 0))
        bucket_match = (
            raw_value is not None
            and bucket is not None
            and raw_value >= bucket[0]
            and (math.isinf(bucket[1]) or raw_value <= bucket[1])
        )
        status = (
            "PASS"
            if delta["valid"]
            and not stable_target_churn
            and observation_count == expected_observations
            and bucket_match
            else "FAIL"
        )
        histograms[name] = {
            **delta,
            "prometheus_quantiles": quantiles,
            "raw": raw_value,
            "prometheus": estimate,
            "absolute_difference": None
            if estimate is None or raw_value is None
            else estimate - raw_value,
            "relative_difference": None
            if estimate is None or raw_value in (None, 0)
            else (estimate - raw_value) / raw_value,
            "raw_expected_observations": expected_observations,
            "prometheus_observations": observation_count,
            "raw_bucket": bucket,
            "status": status,
        }

    if scenario == "recovery_storm" and require_recovery_contract:
        recovery_batch_delta = histogram_delta(start, end, HISTOGRAM_METRICS["recovery_batch"])
        recovery_batch_quantiles = delta_quantiles(recovery_batch_delta)
        recovery_batch_observations = (
            float(recovery_batch_delta["buckets"][-1]["count"])
            if recovery_batch_delta.get("buckets")
            else 0.0
        )
        histograms["recovery_batch"] = {
            **recovery_batch_delta,
            "prometheus_quantiles": recovery_batch_quantiles,
            "raw": None,
            "prometheus": recovery_batch_quantiles["p95"],
            "raw_expected_observations": None,
            "prometheus_observations": recovery_batch_observations,
            "raw_bucket": None,
            "status": "PASS"
            if recovery_batch_delta["valid"]
            and not stable_target_churn
            and recovery_batch_observations > 0
            else "FAIL",
            "note": "Operational scheduler recovery-batch histogram; raw recovery lag remains authoritative.",
        }

    if scenario == "retry_storm" and require_retry_batch:
        retry_batch_delta = histogram_delta(start, end, HISTOGRAM_METRICS["retry_batch"])
        retry_batch_quantiles = delta_quantiles(retry_batch_delta)
        retry_batch_observations = (
            float(retry_batch_delta["buckets"][-1]["count"])
            if retry_batch_delta.get("buckets")
            else 0.0
        )
        histograms["retry_batch"] = {
            **retry_batch_delta,
            "prometheus_quantiles": retry_batch_quantiles,
            "raw": None,
            "prometheus": retry_batch_quantiles["p95"],
            "raw_expected_observations": None,
            "prometheus_observations": retry_batch_observations,
            "raw_bucket": None,
            "status": "PASS"
            if retry_batch_delta["valid"]
            and not stable_target_churn
            and retry_batch_observations > 0
            else "FAIL",
            "note": "Operational scheduler retry-batch histogram; no raw batch count exists.",
        }

    required = [item["status"] for item in counters.values()] + [
        item["status"] for item in histograms.values()
    ]
    status = (
        "PASS" if not stable_target_churn and all(value == "PASS" for value in required) else "FAIL"
    )
    reconciliation = {
        "prometheus_valid": status == "PASS",
        "status": status,
        "intentional_worker_churn": intentional_worker_churn,
        "worker_target_churn": worker_churn,
        "unexpected_target_churn": stable_target_churn,
        "process_identity_changes": identities,
        "replica_churn": replica_churn,
        "start_replicas": sorted(start_replica_ids),
        "end_replicas": sorted(end_replica_ids),
        "start_boundary_errors": start_boundary_errors,
        "end_boundary_errors": end_boundary_errors,
        "start_targets": sorted(start_targets),
        "end_targets": sorted(end_targets),
        "counters": counters,
        "histograms": histograms,
    }
    if allowed_worker_churn is not None:
        reconciliation.update(
            {
                "allowed_worker_churn": allowed_worker_churn,
                "allowed_worker_churn_observed": churn["allowed_worker_churn_observed"],
                "unobserved_allowed_worker_churn": churn["unobserved_allowed_worker_churn"],
                "unexpected_process_identity_changes": churn["unexpected_process_identity_changes"],
                "unexpected_replica_churn": churn["unexpected_replica_churn"],
            }
        )
    return reconciliation


def aggregate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in results:
        if not item.get("valid", False):
            continue
        groups.setdefault((item["scenario"], item["variant"]), []).append(item)
    summaries = []
    for (scenario, variant), items in groups.items():
        throughput_key = (
            "submission_throughput_per_second"
            if scenario in API_SUBMISSION_SCENARIOS
            else "processing_throughput_per_second"
        )
        values = [
            float(value)
            for item in items
            if (
                value := item.get("raw", {}).get(throughput_key)
                or item.get("submission", {}).get("requests_per_second")
            )
            is not None
        ]
        block_values: dict[int, list[float]] = {}
        for item, value in zip(items, values, strict=False):
            block_values.setdefault(int(item.get("block", 0)), []).append(value)
        block_medians = {
            str(block): statistics.median(measured)
            for block, measured in block_values.items()
            if measured
        }
        mean = statistics.fmean(values) if values else None
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0 if values else None
        median_value = statistics.median(values) if values else None
        drift = (
            (max(block_medians.values()) - min(block_medians.values())) / median_value
            if median_value and len(block_medians) > 1
            else 0.0
            if median_value is not None
            else None
        )
        summaries.append(
            {
                "scenario": scenario,
                "variant": variant,
                "classification": items[0].get("classification"),
                "trials": len(items),
                "blocks": len(block_medians),
                "throughput_kind": "submission"
                if scenario in API_SUBMISSION_SCENARIOS
                else "processing",
                "throughput_mean": mean,
                "throughput_median": median_value,
                "throughput_min": min(values) if values else None,
                "throughput_max": max(values) if values else None,
                "throughput_stdev": stdev,
                "throughput_cv": stdev / mean if stdev is not None and mean else None,
                "block_medians": block_medians,
                "between_block_drift": drift,
                "submission_latency_p50_ms_median": statistics.median(
                    measured
                    for item in items
                    if (measured := item.get("submission", {}).get("latency_ms", {}).get("p50"))
                    is not None
                )
                if any(
                    item.get("submission", {}).get("latency_ms", {}).get("p50") is not None
                    for item in items
                )
                else None,
                "submission_latency_p95_ms_median": statistics.median(
                    measured
                    for item in items
                    if (measured := item.get("submission", {}).get("latency_ms", {}).get("p95"))
                    is not None
                )
                if any(
                    item.get("submission", {}).get("latency_ms", {}).get("p95") is not None
                    for item in items
                )
                else None,
                "submission_latency_p99_ms_median": statistics.median(
                    measured
                    for item in items
                    if (measured := item.get("submission", {}).get("latency_ms", {}).get("p99"))
                    is not None
                )
                if any(
                    item.get("submission", {}).get("latency_ms", {}).get("p99") is not None
                    for item in items
                )
                else None,
                "queue_p95_median": statistics.median(
                    value
                    for item in items
                    if (value := item.get("raw", {}).get("queue_p95_seconds")) is not None
                )
                if any(item.get("raw", {}).get("queue_p95_seconds") is not None for item in items)
                else None,
                "claim_p95_prometheus_median": statistics.median(
                    value
                    for item in items
                    if (
                        value := item.get("prometheus_reconciliation", {})
                        .get("histograms", {})
                        .get("claim", {})
                        .get("prometheus")
                    )
                    is not None
                )
                if any(
                    item.get("prometheus_reconciliation", {})
                    .get("histograms", {})
                    .get("claim", {})
                    .get("prometheus")
                    is not None
                    for item in items
                )
                else None,
            }
        )
    baselines = {
        item["scenario"]: item.get("throughput_median")
        for item in summaries
        if item.get("scenario") in CORE_SCALING_SCENARIOS and item.get("variant") == "w1"
    }
    for item in summaries:
        baseline = baselines.get(item.get("scenario"))
        if baseline and str(item.get("variant", "")).startswith("w"):
            workers = int(str(item["variant"])[1:])
            item["speedup_vs_w1"] = item["throughput_median"] / baseline
            item["scaling_efficiency"] = item["speedup_vs_w1"] / workers
    return summaries


REQUIRED_TRIAL_ARTIFACTS = {
    "metadata.json",
    "tasks.csv",
    "attempts.csv",
    "correctness.json",
    "prometheus_start.json",
    "prometheus_end.json",
    "prometheus_reconciliation.json",
    "summary.json",
    "manifest.json",
}
REQUIRED_IMAGE_SERVICES = {"api", "worker", "scheduler", "load_generator"}
REGRESSION_CATEGORIES = {"api_integration", "worker", "scheduler", "benchmark_harness"}


def _trial_name(item: dict[str, Any]) -> str:
    return f"{item.get('scenario')}/{item.get('variant')}/t{item.get('trial')}"


def _trial_directory(
    output_dir: pathlib.Path, item: dict[str, Any]
) -> tuple[pathlib.Path | None, str | None]:
    relative = item.get("artifacts", {}).get("directory")
    if not isinstance(relative, str) or not relative:
        return None, "artifact directory is not recorded"
    root = output_dir.resolve()
    candidate = (output_dir / relative).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None, "artifact directory escapes the run directory"
    return candidate, None


def _read_json(path: pathlib.Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _json_equivalent(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
        right, sort_keys=True, separators=(",", ":")
    )


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _correctness_errors(
    item: dict[str, Any],
    tasks: list[dict[str, str]],
    attempts: list[dict[str, str]],
    failure_boundary: dict[str, Any] | None = None,
) -> list[str]:
    check = item.get("correctness", {})
    errors = []
    expected_tasks = _int_value(check.get("expected_tasks"))
    actual_tasks = _int_value(check.get("actual_tasks"))
    expected_attempts = _int_value(check.get("expected_attempts"))
    actual_attempts = _int_value(check.get("actual_attempts"))
    if check.get("passed") is not True:
        errors.append("correctness result is not passed")
    if expected_tasks is None or actual_tasks != expected_tasks or len(tasks) != expected_tasks:
        errors.append("expected, recorded, and raw task counts do not match")
    if (
        expected_attempts is None
        or actual_attempts != expected_attempts
        or len(attempts) != expected_attempts
    ):
        errors.append("expected, recorded, and raw attempt counts do not match")

    task_ids = [row.get("task_id", "") for row in tasks]
    if not all(task_ids) or len(task_ids) != len(set(task_ids)):
        errors.append("raw tasks contain missing or duplicate task ids")
    attempt_keys = [(row.get("task_id", ""), row.get("attempt_number", "")) for row in attempts]
    if not all(all(key) for key in attempt_keys) or len(attempt_keys) != len(set(attempt_keys)):
        errors.append("raw attempts contain duplicate or incomplete attempt identities")
    if any(task_id not in set(task_ids) for task_id, _ in attempt_keys):
        errors.append("raw attempt references a task outside the trial")
    for task in tasks:
        count = sum(row.get("task_id") == task.get("task_id") for row in attempts)
        if _int_value(task.get("attempt_count")) != count:
            errors.append(f"task {task.get('task_id')} attempt_count does not match raw attempts")

    zero_fields = (
        "duplicate_attempts",
        "attempt_count_mismatches",
        "stranded_leases",
        "unexpected_attempt_states",
        "missing_queue_evidence",
        "negative_queue_waits",
    )
    for field in zero_fields:
        if _int_value(check.get(field)) != 0:
            errors.append(f"{field} is not zero")
    if _int_value(check.get("abandoned_attempts")) != _int_value(check.get("expected_abandoned")):
        errors.append("abandoned attempt count does not match the scenario expectation")

    terminal_expected = check.get("terminal_expected")
    if terminal_expected is True:
        if _int_value(check.get("terminal_tasks")) != expected_tasks:
            errors.append("not every expected task is terminal")
        if _int_value(check.get("succeeded_tasks")) != expected_tasks:
            errors.append("not every expected task succeeded")
        if any(row.get("final_status") != "SUCCEEDED" for row in tasks):
            errors.append("raw tasks include a failed or non-terminal task")
    elif terminal_expected is False:
        if _int_value(check.get("queued_tasks")) != expected_tasks:
            errors.append("submission scenario did not retain every expected queued task")
        if _int_value(check.get("terminal_tasks")) != 0:
            errors.append("submission scenario unexpectedly contains terminal tasks")
    else:
        errors.append("terminal expectation is not recorded")

    if item.get("scenario") == "api_submission":
        submission = item.get("submission", {})
        status_counts = submission.get("status_counts", {}) or {}
        status_2xx = sum(
            int(value) for status, value in status_counts.items() if str(status).startswith("2")
        )
        status_5xx = sum(
            int(value) for status, value in status_counts.items() if str(status).startswith("5")
        )
        transport_errors = sum(
            int(value) for value in (submission.get("error_counts", {}) or {}).values()
        )
        if (
            _int_value(submission.get("request_count")) != expected_tasks
            or _int_value(submission.get("successes")) != expected_tasks
            or _int_value(submission.get("distinct_task_ids")) != expected_tasks
            or status_2xx != expected_tasks
            or status_5xx != 0
            or transport_errors != 0
            or expected_attempts != 0
        ):
            errors.append("API submission HTTP/task-row correctness contract failed")

    if item.get("scenario") == "retry_storm" and check.get("retry_history_expected") is True:
        evidence = retry_history_evidence(tasks, attempts, expected_tasks or 0)
        if evidence.get("retry_history_passed") is not True:
            errors.append("fail-once retry attempt history is invalid")
        for field, value in evidence.items():
            if check.get(field) != value:
                errors.append(f"recorded retry history {field} does not match raw attempts")
    if item.get("scenario") == "recovery_storm" and check.get("recovery_history_expected") is True:
        if not failure_boundary:
            errors.append("failure-boundary evidence is missing")
        else:
            if _int_value(failure_boundary.get("expected_killed_workers")) != _int_value(
                item.get("killed_workers")
            ):
                errors.append("failure-boundary kill count differs from the indexed trial")
            evidence = recovery_history_evidence(
                tasks, attempts, failure_boundary, expected_tasks or 0
            )
            if evidence.get("recovery_history_passed") is not True:
                errors.append("captured crash-recovery attempt history is invalid")
            for field, value in evidence.items():
                if check.get(field) != value:
                    errors.append(f"recorded recovery history {field} does not match raw evidence")
    return errors


def _prometheus_errors(reconciliation: dict[str, Any]) -> list[str]:
    errors = []
    failure_aware = bool(reconciliation.get("allowed_worker_churn"))
    if reconciliation.get("prometheus_valid") is not True:
        errors.append("prometheus_valid is not true")
    if reconciliation.get("status") != "PASS":
        errors.append("reconciliation status is not PASS")
    if reconciliation.get("unexpected_target_churn") is not False:
        errors.append("unexpected target or process churn was detected")
    if failure_aware:
        if reconciliation.get("allowed_worker_churn_observed") is not True:
            errors.append("explicitly allowed killed-worker churn was not observed")
        unexpected_identities = reconciliation.get("unexpected_process_identity_changes", {})
        if not isinstance(unexpected_identities, dict) or any(unexpected_identities.values()):
            errors.append("process churn outside the killed-worker allowlist was detected")
        if reconciliation.get("unexpected_replica_churn") is not False:
            errors.append("replica churn outside the killed-worker allowlist was detected")
    elif reconciliation.get("replica_churn") is not False:
        errors.append("replica churn was detected")
    if reconciliation.get("start_boundary_errors") or reconciliation.get("end_boundary_errors"):
        errors.append("Prometheus boundary errors were recorded")
    if not failure_aware:
        identities = reconciliation.get("process_identity_changes", {})
        if not isinstance(identities, dict) or any(identities.values()):
            errors.append("target process identities changed or were not fully observed")
        if reconciliation.get("start_targets") != reconciliation.get("end_targets"):
            errors.append("start and end target sets differ")
        if reconciliation.get("start_replicas") != reconciliation.get("end_replicas"):
            errors.append("start and end replica sets differ")
    for name, counter in reconciliation.get("counters", {}).items():
        if (
            counter.get("valid") is not True
            or counter.get("status") != "PASS"
            or not same_measurement(counter.get("difference"), 0)
            or not same_measurement(counter.get("raw"), counter.get("prometheus"))
        ):
            errors.append(f"counter {name} does not exactly reconcile")
    for name, histogram in reconciliation.get("histograms", {}).items():
        if (
            histogram.get("valid") is not True
            or histogram.get("status") != "PASS"
            or histogram.get("bucket_count_matches_count") is not True
            or histogram.get("non_monotonic_buckets")
            or histogram.get("sum", {}).get("valid") is not True
            or histogram.get("count", {}).get("valid") is not True
            or (
                histogram.get("raw_expected_observations") is not None
                and not same_measurement(
                    histogram.get("raw_expected_observations"),
                    histogram.get("prometheus_observations"),
                )
            )
        ):
            errors.append(f"histogram {name} delta is invalid")
    return errors


def _regression_category(record: dict[str, Any]) -> str | None:
    command = " ".join(str(value) for value in record.get("command", []))
    inferred = None
    if "integration-tests" in command or "api/tests" in command:
        inferred = "api_integration"
    elif "/worker" in command or "worker/" in command:
        inferred = "worker"
    elif "/scheduler" in command or "scheduler/" in command:
        inferred = "scheduler"
    elif "benchmarks.tests" in command or "benchmarks/loadgen" in command:
        inferred = "benchmark_harness"
    recorded = record.get("category")
    return inferred if recorded in (None, inferred) else None


def evaluate_trust(document: dict[str, Any], output_dir: pathlib.Path) -> dict[str, Any]:
    """Evaluate an existing run directory without executing benchmark work."""
    results = document.get("results", [])
    public = [item for item in results if item.get("classification") == "PUBLIC"]
    profile = document.get("profile", {})
    minimum_trials = int(profile.get("minimum_public_trials", 3))
    required_blocks = int(profile.get("required_blocks", 3))
    harness_version = str(document.get("harness", {}).get("version", ""))
    legacy_execution_percentile = harness_version.startswith("TF-012D/1")

    source_errors = []
    source = document.get("source", {})
    if document.get("publishable") is not True:
        source_errors.append("run is not marked publishable")
    if source.get("clean") is not True:
        source_errors.append("source tree is not clean")
    for field in ("git_commit_sha", "git_tree_hash"):
        if not source.get(field):
            source_errors.append(f"source {field} is missing")
    images = document.get("images", {})
    for service in sorted(REQUIRED_IMAGE_SERVICES):
        if not images.get(service, {}).get("image_id"):
            source_errors.append(f"{service} image identity is missing")
    if not document.get("environment"):
        source_errors.append("environment provenance is missing")
    if not document.get("harness", {}).get("version") or not document.get("harness", {}).get(
        "files"
    ):
        source_errors.append("benchmark harness identity is incomplete")
    if not public:
        source_errors.append("run contains no PUBLIC trial")

    raw_errors = []
    latency_failures = []
    correctness_failures = []
    prometheus_failures = []
    for item in results:
        name = _trial_name(item)
        if item.get("classification") not in {"PUBLIC", "EXPLORATORY"}:
            raw_errors.append(f"{name}: classification must be PUBLIC or EXPLORATORY")
        artifact_dir, directory_error = _trial_directory(output_dir, item)
        if directory_error:
            raw_errors.append(f"{name}: {directory_error}")
            source_errors.append(f"{name}: {directory_error}")
            continue
        assert artifact_dir is not None
        required_trial_artifacts = set(REQUIRED_TRIAL_ARTIFACTS)
        if document.get("tf_ticket") == "TF-012E6" and item.get("scenario") == "recovery_storm":
            required_trial_artifacts.add("failure_boundary.json")
        missing = sorted(
            artifact
            for artifact in required_trial_artifacts
            if not (artifact_dir / artifact).is_file()
        )
        raw_errors.extend(f"{name}: {artifact} missing" for artifact in missing)
        verified, manifest_errors = verify_manifest(artifact_dir)
        if not verified:
            errors = [f"{name}: {error}" for error in manifest_errors]
            raw_errors.extend(errors)
            source_errors.extend(errors)
        if missing:
            continue
        failure_boundary: dict[str, Any] | None = None
        try:
            tasks = read_csv(artifact_dir / "tasks.csv")
            attempts = read_csv(artifact_dir / "attempts.csv")
            metadata = _read_json(artifact_dir / "metadata.json")
            saved_correctness = _read_json(artifact_dir / "correctness.json")
            start = _read_json(artifact_dir / "prometheus_start.json")
            end = _read_json(artifact_dir / "prometheus_end.json")
            saved_reconciliation = _read_json(artifact_dir / "prometheus_reconciliation.json")
            saved_summary = _read_json(artifact_dir / "summary.json")
            if document.get("tf_ticket") == "TF-012E6" and item.get("scenario") == "recovery_storm":
                failure_boundary = _read_json(artifact_dir / "failure_boundary.json")
                recalculated = derive_recovery_raw(tasks, attempts, failure_boundary)
            elif document.get("tf_ticket") == "TF-012E5" and item.get("scenario") == "retry_storm":
                recalculated = derive_retry_raw(tasks, attempts)
            else:
                recalculated = derive_raw(tasks, attempts)
        except (OSError, ValueError, KeyError, AssertionError, json.JSONDecodeError) as exc:
            raw_errors.append(f"{name}: artifact parsing failed: {exc}")
            continue

        if not _json_equivalent(saved_correctness, item.get("correctness")):
            correctness_failures.append(f"{name}: indexed correctness differs from artifact")
        if not _json_equivalent(saved_reconciliation, item.get("prometheus_reconciliation")):
            prometheus_failures.append(f"{name}: indexed reconciliation differs from artifact")
        if not _json_equivalent(saved_summary.get("submission"), item.get("submission")):
            raw_errors.append(f"{name}: indexed submission differs from artifact")
        if failure_boundary is not None and not _json_equivalent(
            failure_boundary, item.get("failure_boundary")
        ):
            raw_errors.append(f"{name}: indexed failure boundary differs from artifact")
        for field in ("scenario", "variant", "classification", "block", "trial"):
            if metadata.get(field) != item.get(field):
                raw_errors.append(f"{name}: metadata {field} identity mismatch")
        for key, value in recalculated.items():
            if not same_measurement(value, item.get("raw", {}).get(key)):
                latency_failures.append(f"{name}: indexed raw {key} mismatch")
            if not same_measurement(value, saved_summary.get("raw", {}).get(key)):
                latency_failures.append(f"{name}: summary raw {key} mismatch")
        if recalculated.get("missing_queue_evidence", 0):
            latency_failures.append(f"{name}: immutable queue-entry evidence is missing")
        for duration, count in recalculated.get("negative_durations", {}).items():
            if count:
                latency_failures.append(f"{name}: negative {duration} duration")

        correctness_failures.extend(
            f"{name}: {error}"
            for error in _correctness_errors(item, tasks, attempts, failure_boundary)
        )
        intentional = bool(metadata.get("intentional_worker_churn", False))
        rebuilt_reconciliation = build_reconciliation(
            recalculated,
            start,
            end,
            str(item.get("scenario")),
            intentional_worker_churn=intentional,
            allowed_worker_churn=(failure_boundary or {}).get("prometheus_allowed_churn"),
            legacy_execution_percentile=legacy_execution_percentile,
            require_retry_batch=document.get("tf_ticket") == "TF-012E5",
            require_recovery_contract=document.get("tf_ticket") == "TF-012E6",
        )
        if not _json_equivalent(rebuilt_reconciliation, saved_reconciliation):
            prometheus_failures.append(
                f"{name}: reconciliation does not reproduce from boundary snapshots"
            )
        prometheus_failures.extend(
            f"{name}: {error}" for error in _prometheus_errors(saved_reconciliation)
        )

        trial_source = item.get("provenance", {}).get("source", {})
        if (
            trial_source.get("git_commit_sha") != source.get("git_commit_sha")
            or trial_source.get("git_tree_hash") != source.get("git_tree_hash")
            or trial_source.get("clean") is not True
        ):
            source_errors.append(f"{name}: trial source provenance does not match the run")
        if item.get("provenance", {}).get("images") != images:
            source_errors.append(f"{name}: trial image provenance does not match the run")
        if not item.get("provenance", {}).get("machine") or not item.get("configuration"):
            source_errors.append(f"{name}: trial machine/configuration provenance is incomplete")
        for field in ("workers", "schedulers", "count", "task_type"):
            if item.get(field) is None:
                source_errors.append(f"{name}: trial {field} configuration is missing")

    if document.get("artifact_reproducibility", {}).get("passed") is not True:
        raw_errors.append("report/plot regeneration was not verified")

    public_groups: dict[tuple[str, str], int] = {}
    for item in public:
        if item.get("valid") is True:
            key = (item["scenario"], item["variant"])
            public_groups[key] = public_groups.get(key, 0) + 1
    repetition_failures = [
        f"{scenario}/{variant}={count}<{minimum_trials} valid PUBLIC trials"
        for (scenario, variant), count in public_groups.items()
        if count < minimum_trials
    ]
    required_scenarios = set(profile.get("required_public_scenarios", PUBLIC_SCENARIOS))
    missing_scenarios = sorted(required_scenarios - {item[0] for item in public_groups})
    repetition_failures.extend(f"{scenario}=missing" for scenario in missing_scenarios)
    required_groups: set[tuple[str, str]] = set()
    for scenario in CORE_SCALING_SCENARIOS:
        if scenario in required_scenarios:
            required_groups.update(
                (scenario, f"w{workers}") for workers in profile.get("scaling_workers", [])
            )
    for api_scenario in sorted(API_SUBMISSION_SCENARIOS & required_scenarios):
        required_groups.update(
            (api_scenario, f"c{concurrency}") for concurrency in profile.get("api_concurrency", [])
        )
    if "arrival_saturation" in required_scenarios:
        required_groups.update(
            ("arrival_saturation", f"r{rate}") for rate in profile.get("arrival_rates", [])
        )
    if "retry_storm" in required_scenarios:
        required_groups.add(("retry_storm", "fail-once"))
    if "recovery_storm" in required_scenarios:
        recovery_variant = (
            f"kill-{profile.get('recovery_kill_workers')}-of-{profile.get('recovery_workers')}"
            if document.get("tf_ticket") == "TF-012E6"
            else f"kill-{profile.get('recovery_kill_percentage')}"
        )
        required_groups.add(("recovery_storm", recovery_variant))
    repetition_failures.extend(
        f"{scenario}/{variant}=missing"
        for scenario, variant in sorted(required_groups - set(public_groups))
    )

    reproducibility_failures = []
    block_events = document.get("run_blocks", [])
    blocks = {event.get("block"): event for event in block_events}
    if len(blocks) != len(block_events):
        reproducibility_failures.append("run block identities are missing or duplicated")
    independent_scenarios = set(CORE_SCALING_SCENARIOS)
    if document.get("tf_ticket") in {"TF-012E4", "TF-012E5", "TF-012E6"}:
        independent_scenarios.update(required_scenarios)
    independent_public = [item for item in public if item.get("scenario") in independent_scenarios]
    for (scenario, variant), _ in public_groups.items():
        if scenario not in independent_scenarios:
            continue
        group = [
            item
            for item in independent_public
            if item.get("scenario") == scenario and item.get("variant") == variant
        ]
        group_blocks = {item.get("block") for item in group}
        if len(group_blocks) < required_blocks:
            reproducibility_failures.append(
                f"{scenario}/{variant} has {len(group_blocks)}<{required_blocks} independent blocks"
            )
        reset_boundaries = {blocks.get(block, {}).get("reset_started_at") for block in group_blocks}
        if None in reset_boundaries or len(reset_boundaries) != len(group_blocks):
            reproducibility_failures.append(
                f"{scenario}/{variant} does not have distinct recorded reset boundaries"
            )
        for item in group:
            event = blocks.get(item.get("block"))
            if not event or event.get("fresh_environment") is not True:
                reproducibility_failures.append(
                    f"{_trial_name(item)}: fresh block reset is not recorded"
                )
            elif not event.get("reset_started_at") or not event.get("ready_at"):
                reproducibility_failures.append(
                    f"{_trial_name(item)}: block reset boundary is incomplete"
                )
            elif event.get("warmup", {}).get("excluded") is not True or not event.get(
                "warmup", {}
            ).get("source"):
                reproducibility_failures.append(
                    f"{_trial_name(item)}: excluded block warmup is not recorded"
                )
            if item.get("order_index") is None:
                reproducibility_failures.append(
                    f"{_trial_name(item)}: execution order is not recorded"
                )
    if not independent_public:
        reproducibility_failures.append("no PUBLIC scoped trial has independent-block evidence")

    regression = document.get("regression", {})
    regression_failures = []
    records = regression.get("commands", [])
    if regression.get("passed") is not True:
        regression_failures.append("regression suite is not marked passed")
    categories: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        category = _regression_category(record)
        if category:
            categories.setdefault(category, []).append(record)
        if record.get("exit_code") != 0:
            regression_failures.append(
                f"recorded regression command failed: {record.get('command')}"
            )
    for category in sorted(REGRESSION_CATEGORIES - set(categories)):
        regression_failures.append(f"{category} regression evidence is missing")
    for category in ("worker", "scheduler"):
        if category in categories and not any(
            "-race" in [str(value) for value in record.get("command", [])]
            for record in categories[category]
        ):
            regression_failures.append(f"{category} Go race regression evidence is missing")

    gates = {
        "source_provenance": {
            "result": "PASS" if not source_errors else "FAIL",
            "errors": source_errors,
        },
        "correctness": {
            "result": "PASS" if results and not correctness_failures else "FAIL",
            "failures": correctness_failures,
        },
        "raw_data": {
            "result": "PASS" if results and not raw_errors else "FAIL",
            "errors": raw_errors,
        },
        "latency": {
            "result": "PASS" if results and not latency_failures else "FAIL",
            "failures": latency_failures,
        },
        "prometheus": {
            "result": "PASS" if results and not prometheus_failures else "FAIL",
            "failures": prometheus_failures,
        },
        "repetition": {
            "result": "PASS" if public_groups and not repetition_failures else "FAIL",
            "failures": repetition_failures,
            "minimum_public_trials": minimum_trials,
        },
        "reproducibility": {
            "result": "PASS" if not reproducibility_failures else "FAIL",
            "failures": reproducibility_failures,
            "required_independent_blocks": required_blocks,
        },
        "regression": {
            "result": "PASS" if not regression_failures else "FAIL",
            "failures": regression_failures,
            "required_categories": sorted(REGRESSION_CATEGORIES),
        },
    }
    gates["overall"] = {
        "result": "PASS" if all(value["result"] == "PASS" for value in gates.values()) else "FAIL"
    }
    return gates


def evaluate_run_directory(run_directory: pathlib.Path) -> dict[str, Any]:
    """Load and evaluate an immutable benchmark run directory."""
    results_path = run_directory / "results.json"
    if not results_path.is_file():
        return {
            "overall": {"result": "FAIL"},
            "input": {"result": "FAIL", "errors": ["results.json missing"]},
        }
    try:
        document = _read_json(results_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "overall": {"result": "FAIL"},
            "input": {"result": "FAIL", "errors": [f"results.json invalid: {exc}"]},
        }
    gates = evaluate_trust(document, run_directory)
    verified, manifest_errors = verify_manifest(run_directory)
    if not verified:
        source_gate = gates["source_provenance"]
        source_gate["result"] = "FAIL"
        source_gate.setdefault("errors", []).extend(
            f"run manifest: {error}" for error in manifest_errors
        )
        gates["overall"]["result"] = "FAIL"
    return gates


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate an existing TF-012 run directory")
    parser.add_argument("run_directory", type=pathlib.Path)
    arguments = parser.parse_args()
    gates = evaluate_run_directory(arguments.run_directory)
    print(json.dumps(gates, indent=2, sort_keys=True))
    return 0 if gates.get("overall", {}).get("result") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
