"""Pure TF-012B evidence, reconciliation, statistics, and trust-gate helpers."""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import math
import pathlib
import statistics
from collections.abc import Iterable
from typing import Any

HARNESS_VERSION = "TF-012B/1"
QUANTILE_METHOD = "linear interpolation at index (n - 1) * q (Hyndman-Fan type 7)"
PUBLIC_SCENARIOS = {
    "noop_scaling",
    "io50_scaling",
    "cpu_scaling",
    "api_throughput",
    "arrival_saturation",
    "retry_storm",
    "recovery_storm",
}
CORE_SCALING_SCENARIOS = {"noop_scaling", "io50_scaling", "cpu_scaling"}

COUNTER_METRICS = {
    "claimed": "taskforge_worker_tasks_claimed_total",
    "completed": "taskforge_worker_tasks_completed_total",
    "retries_scheduled": "taskforge_task_retries_scheduled_total",
    "retry_promotions": "taskforge_task_retries_promoted_total",
    "recoveries": "taskforge_task_recoveries_total",
}
HISTOGRAM_METRICS = {
    "queue": "taskforge_task_queue_wait_seconds_bucket",
    "claim": "taskforge_worker_claim_duration_seconds_bucket",
    "execution": "taskforge_task_execution_duration_seconds_bucket",
    "retry_lateness": "taskforge_retry_lateness_seconds_bucket",
    "recovery": "taskforge_recovery_lag_seconds_bucket",
}
PROMETHEUS_METRICS = tuple(COUNTER_METRICS.values()) + tuple(HISTOGRAM_METRICS.values())


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


def _series_values(snapshot: dict[str, Any], metric: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for row in _sample_rows(snapshot, metric):
        values[_series_key(row)] = float(row["value"][1])
    return values


def counter_delta(start: dict[str, Any], end: dict[str, Any], metric: str) -> dict[str, Any]:
    before = _series_values(start, metric)
    after = _series_values(end, metric)
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
    return {
        "buckets": [
            {"le": "+Inf" if math.isinf(upper) else upper, "count": count}
            for upper, count in ordered
        ],
        "valid": not decreases and not missing,
        "counter_decreases": decreases,
        "missing_end_series": missing,
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


def build_reconciliation(
    raw: dict[str, Any],
    start: dict[str, Any],
    end: dict[str, Any],
    scenario: str,
    *,
    intentional_worker_churn: bool = False,
) -> dict[str, Any]:
    start_targets = target_ids(start)
    end_targets = target_ids(end)
    worker_churn = target_ids(start, "taskforge-worker") != target_ids(end, "taskforge-worker")
    stable_target_churn = (
        target_ids(start, "taskforge-api") != target_ids(end, "taskforge-api")
        or target_ids(start, "taskforge-scheduler") != target_ids(end, "taskforge-scheduler")
        or (worker_churn and not intentional_worker_churn)
    )
    counters: dict[str, Any] = {}

    expected_counters: dict[str, int] = {}
    if scenario != "api_throughput" and not intentional_worker_churn:
        expected_counters["claimed"] = int(raw.get("attempt_count", 0))
        expected_counters["completed"] = int(raw.get("attempt_count", 0)) - int(
            raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
        )
    if scenario == "retry_storm":
        expected_counters["retries_scheduled"] = int(
            raw.get("attempt_status_counts", {}).get("FAILED", 0)
        )
        expected_counters["retry_promotions"] = int(raw.get("retry_lateness_observations", 0))
    if scenario == "recovery_storm":
        expected_counters["recoveries"] = int(
            raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
        )

    for name, expected in expected_counters.items():
        measured = counter_delta(start, end, COUNTER_METRICS[name])
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
    if scenario != "api_throughput" and not intentional_worker_churn:
        histogram_specs.extend(
            [
                ("queue", "queue_p95_seconds", "queue_observations", 95),
                ("execution", "execution_p95_seconds", "execution_observations", 95),
            ]
        )
        claim_delta = histogram_delta(start, end, HISTOGRAM_METRICS["claim"])
        claim_estimate, claim_bucket = histogram_quantile(claim_delta, 0.95)
        claim_observations = (
            float(claim_delta["buckets"][-1]["count"]) if claim_delta.get("buckets") else 0.0
        )
        expected_claims = int(raw.get("attempt_count", 0))
        histograms["claim"] = {
            **claim_delta,
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
        estimate, bucket = histogram_quantile(delta, quantile / 100)
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

    required = [item["status"] for item in counters.values()] + [
        item["status"] for item in histograms.values()
    ]
    status = (
        "PASS" if not stable_target_churn and all(value == "PASS" for value in required) else "FAIL"
    )
    return {
        "prometheus_valid": status == "PASS",
        "status": status,
        "intentional_worker_churn": intentional_worker_churn,
        "worker_target_churn": worker_churn,
        "unexpected_target_churn": stable_target_churn,
        "start_targets": sorted(start_targets),
        "end_targets": sorted(end_targets),
        "counters": counters,
        "histograms": histograms,
    }


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
            if scenario == "api_throughput"
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
                "throughput_kind": "submission" if scenario == "api_throughput" else "processing",
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


def evaluate_trust(document: dict[str, Any], output_dir: pathlib.Path) -> dict[str, Any]:
    results = document.get("results", [])
    public = [item for item in results if item.get("classification") == "PUBLIC"]
    profile = document.get("profile", {})
    minimum_trials = int(profile.get("minimum_public_trials", 3))
    required_blocks = int(profile.get("required_blocks", 3))
    cv_threshold = float(profile.get("trial_cv_threshold", 0.10))
    drift_threshold = float(profile.get("between_block_drift_threshold", 0.10))

    provenance_ok = bool(
        document.get("publishable")
        and document.get("source", {}).get("clean") is True
        and document.get("source", {}).get("git_commit_sha")
        and document.get("source", {}).get("git_tree_hash")
        and document.get("images")
        and all(item.get("image_id") for item in document.get("images", {}).values())
        and document.get("environment")
        and document.get("harness", {}).get("version")
        and document.get("harness", {}).get("files")
        and public
        and all(
            item.get("provenance", {}).get("source", {}).get("git_commit_sha")
            == document.get("source", {}).get("git_commit_sha")
            and item.get("provenance", {}).get("machine")
            and item.get("configuration")
            and item.get("workers") is not None
            and item.get("schedulers") is not None
            and item.get("count") is not None
            and item.get("task_type")
            for item in public
        )
    )
    correctness_ok = bool(public) and all(
        item.get("correctness", {}).get("passed") for item in public
    )
    raw_errors = []
    for item in public:
        artifact_dir = output_dir / item.get("artifacts", {}).get("directory", "")
        verified, errors = verify_manifest(artifact_dir)
        if not verified:
            raw_errors.extend(
                f"{item.get('scenario')}/{item.get('variant')}: {error}" for error in errors
            )
        for name in ("tasks.csv", "attempts.csv", "summary.json"):
            if not (artifact_dir / name).exists():
                raw_errors.append(f"{item.get('scenario')}/{item.get('variant')}: {name} missing")
        if all(
            (artifact_dir / name).exists() for name in ("tasks.csv", "attempts.csv", "summary.json")
        ):
            recalculated = derive_raw(
                read_csv(artifact_dir / "tasks.csv"),
                read_csv(artifact_dir / "attempts.csv"),
            )
            saved_summary = json.loads((artifact_dir / "summary.json").read_text()).get("raw", {})
            for key, value in recalculated.items():
                if not same_measurement(value, item.get("raw", {}).get(key)):
                    raw_errors.append(
                        f"{item.get('scenario')}/{item.get('variant')}: indexed raw {key} mismatch"
                    )
                if not same_measurement(value, saved_summary.get(key)):
                    raw_errors.append(
                        f"{item.get('scenario')}/{item.get('variant')}: summary raw {key} mismatch"
                    )
    raw_ok = (
        bool(public)
        and not raw_errors
        and document.get("artifact_reproducibility", {}).get("passed") is True
    )
    if document.get("artifact_reproducibility", {}).get("passed") is not True:
        raw_errors.append("report/plot regeneration was not verified")
    latency_failures = [
        f"{item['scenario']}/{item['variant']}/t{item.get('trial')}"
        for item in public
        if item.get("raw", {}).get("missing_queue_evidence", 0)
        or any(item.get("raw", {}).get("negative_durations", {}).values())
    ]
    latency_ok = not latency_failures
    prometheus_failures = [
        f"{item['scenario']}/{item['variant']}/t{item.get('trial')}"
        for item in public
        if item.get("prometheus_reconciliation", {}).get("status") != "PASS"
    ]
    prometheus_ok = bool(public) and not prometheus_failures

    public_groups: dict[tuple[str, str], int] = {}
    for item in public:
        public_groups[(item["scenario"], item["variant"])] = (
            public_groups.get((item["scenario"], item["variant"]), 0) + 1
        )
    repetition_failures = [
        f"{scenario}/{variant}={count}<{minimum_trials}"
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
    if "api_throughput" in required_scenarios:
        required_groups.update(
            ("api_throughput", f"c{concurrency}")
            for concurrency in profile.get("api_concurrency", [])
        )
    if "arrival_saturation" in required_scenarios:
        required_groups.update(
            ("arrival_saturation", f"r{rate}") for rate in profile.get("arrival_rates", [])
        )
    if "retry_storm" in required_scenarios:
        required_groups.add(("retry_storm", "fail-once"))
    if "recovery_storm" in required_scenarios:
        required_groups.add(("recovery_storm", f"kill-{profile.get('recovery_kill_percentage')}"))
    repetition_failures.extend(
        f"{scenario}/{variant}=missing"
        for scenario, variant in sorted(required_groups - set(public_groups))
    )
    repetition_ok = bool(public_groups) and not repetition_failures

    summaries = aggregate(results)
    reproducibility_failures = []
    for summary in summaries:
        if summary.get("scenario") not in CORE_SCALING_SCENARIOS:
            continue
        if int(summary.get("blocks", 0)) < required_blocks:
            reproducibility_failures.append(
                f"{summary['scenario']}/{summary['variant']} blocks={summary.get('blocks')}"
            )
        if (summary.get("throughput_cv") or 0) > cv_threshold:
            reproducibility_failures.append(
                f"{summary['scenario']}/{summary['variant']} cv={summary['throughput_cv']:.3f}"
            )
        if (summary.get("between_block_drift") or 0) > drift_threshold:
            reproducibility_failures.append(
                f"{summary['scenario']}/{summary['variant']} drift={summary['between_block_drift']:.3f}"
            )
    reproducibility_ok = (
        bool([item for item in summaries if item.get("scenario") in CORE_SCALING_SCENARIOS])
        and not reproducibility_failures
    )
    regression_ok = document.get("regression", {}).get("passed") is True

    gates = {
        "source_provenance": {"result": "PASS" if provenance_ok else "FAIL"},
        "correctness": {"result": "PASS" if correctness_ok else "FAIL"},
        "raw_data": {
            "result": "PASS" if raw_ok else "FAIL",
            "errors": raw_errors,
        },
        "latency": {
            "result": "PASS" if latency_ok else "FAIL",
            "failures": latency_failures,
        },
        "prometheus": {
            "result": "PASS" if prometheus_ok else "FAIL",
            "failures": prometheus_failures,
        },
        "repetition": {
            "result": "PASS" if repetition_ok else "FAIL",
            "failures": repetition_failures,
        },
        "reproducibility": {
            "result": "PASS" if reproducibility_ok else "FAIL",
            "failures": reproducibility_failures,
            "trial_cv_threshold": cv_threshold,
            "between_block_drift_threshold": drift_threshold,
        },
        "regression": {"result": "PASS" if regression_ok else "FAIL"},
    }
    gates["overall"] = {
        "result": "PASS" if all(value["result"] == "PASS" for value in gates.values()) else "FAIL"
    }
    return gates
