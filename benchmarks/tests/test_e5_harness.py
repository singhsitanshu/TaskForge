"""Focused non-performance tests for the scoped TF-012E5 harness."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.e5 import E5_SPEC, configuration_contract, run_retry_trials
from benchmarks.e5_artifacts import (
    PLOT_NAMES,
    generate,
    plot_specs,
    render_report,
    retry_aggregate,
    retry_rows,
)
from benchmarks.tests.trust_fixture import build_trust_fixture, prometheus_snapshot
from benchmarks.trust import (
    COUNTER_METRICS,
    HISTOGRAM_METRICS,
    build_reconciliation,
    create_manifest,
    derive_retry_raw,
    evaluate_run_directory,
    retry_history_evidence,
    write_csv,
    write_json,
)
from benchmarks.trusted import ATTEMPT_FIELDS, TASK_FIELDS, retry_storm_correctness


def retry_rows_fixture(trial: int = 1) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    task_id = f"retry-task-{trial}"
    tasks = [
        {
            "task_id": task_id,
            "task_created_at": "2026-01-01T00:00:00.000000+00:00",
            "task_completed_at": "2026-01-01T00:00:00.600000+00:00",
            "final_status": "SUCCEEDED",
            "attempt_count": "2",
            "task_type": "test.fail_n_then_succeed",
            "queue": f"retry-{trial}",
        }
    ]
    attempts = [
        {
            "task_id": task_id,
            "attempt_id": f"attempt-{trial}-1",
            "attempt_number": "1",
            "status": "FAILED",
            "worker_id": "worker-1",
            "worker_label": "worker-1",
            "task_created_at": "2026-01-01T00:00:00.000000+00:00",
            "attempt_leased_at": "2026-01-01T00:00:00.100000+00:00",
            "attempt_started_at": "2026-01-01T00:00:00.100000+00:00",
            "attempt_finished_at": "2026-01-01T00:00:00.200000+00:00",
            "queue_entered_at": "2026-01-01T00:00:00.000000+00:00",
            "scheduled_at_snapshot": "2026-01-01T00:00:00.000000+00:00",
            "retry_scheduled_at": "2026-01-01T00:00:00.300000+00:00",
            "recovered_lease_expires_at": "",
            "recovered_at": "",
            "recovery_action": "",
        },
        {
            "task_id": task_id,
            "attempt_id": f"attempt-{trial}-2",
            "attempt_number": "2",
            "status": "SUCCEEDED",
            "worker_id": "worker-2",
            "worker_label": "worker-2",
            "task_created_at": "2026-01-01T00:00:00.000000+00:00",
            "attempt_leased_at": "2026-01-01T00:00:00.400000+00:00",
            "attempt_started_at": "2026-01-01T00:00:00.500000+00:00",
            "attempt_finished_at": "2026-01-01T00:00:00.600000+00:00",
            "queue_entered_at": "2026-01-01T00:00:00.400000+00:00",
            "scheduled_at_snapshot": "2026-01-01T00:00:00.300000+00:00",
            "retry_scheduled_at": "",
            "recovered_lease_expires_at": "",
            "recovered_at": "",
            "recovery_action": "",
        },
    ]
    return tasks, attempts


def synthetic_document() -> dict[str, object]:
    results = []
    for trial in (1, 2, 3):
        results.append(
            {
                "scenario": "retry_storm",
                "variant": "fail-once",
                "classification": "PUBLIC",
                "block": trial,
                "trial": trial,
                "valid": True,
                "raw": {
                    "processing_throughput_per_second": 100.0 + trial,
                    "retry_lateness_p50_seconds": 0.10 + trial / 1000,
                    "retry_lateness_p95_seconds": 0.20 + trial / 1000,
                    "retry_lateness_p99_seconds": 0.30 + trial / 1000,
                    "attempt2_queue_p50_seconds": 0.01 + trial / 1000,
                    "attempt2_queue_p95_seconds": 0.02 + trial / 1000,
                    "attempt2_queue_p99_seconds": 0.03 + trial / 1000,
                    "total_p50_seconds": 0.40 + trial / 1000,
                    "total_p95_seconds": 0.50 + trial / 1000,
                    "total_p99_seconds": 0.60 + trial / 1000,
                },
                "correctness": {
                    "actual_tasks": 1000,
                    "actual_attempts": 2000,
                    "attempt1_failed": 1000,
                    "attempt2_succeeded": 1000,
                    "retry_schedules": 1000,
                    "retry_promotions": 1000,
                    "retry_duplicate_identities": 0,
                    "abandoned_attempts": 0,
                    "stranded_leases": 0,
                },
                "prometheus_reconciliation": {
                    "counters": {
                        name: {
                            "raw": value,
                            "prometheus": value,
                            "difference": 0,
                            "status": "PASS",
                        }
                        for name, value in {
                            "attempts": 2000,
                            "claimed": 2000,
                            "completed": 2000,
                            "retries_scheduled": 1000,
                            "retry_promotions": 1000,
                        }.items()
                    },
                    "histograms": {
                        "retry_batch": {"prometheus_quantiles": {"p95": 0.004 + trial / 10000}}
                    },
                },
            }
        )
    pass_gate = {"result": "PASS"}
    return {
        "run_id": "synthetic-e5",
        "source": {"git_commit_sha": "a" * 40, "git_tree_hash": "b" * 40, "clean": True},
        "environment": {"platform": "synthetic"},
        "profile": configuration_contract(),
        "retry_configuration": configuration_contract()["retry_configuration"],
        "results": results,
        "trust": {
            name: dict(pass_gate)
            for name in (
                "source_provenance",
                "correctness",
                "raw_data",
                "latency",
                "prometheus",
                "repetition",
                "reproducibility",
                "regression",
                "overall",
            )
        },
    }


class FakeHarness:
    def __init__(self) -> None:
        self.reset_calls = 0
        self.start_calls = 0

    def reset(self) -> None:
        self.reset_calls += 1

    def start(self) -> None:
        self.start_calls += 1

    def json_sql(self, _: str) -> dict[str, object]:
        return {
            "expected_tasks": 1,
            "actual_tasks": 1,
            "expected_attempts": 2,
            "actual_attempts": 2,
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
        }


class FakeTrusted:
    def __init__(self) -> None:
        self.profile = configuration_contract()
        self.harness = FakeHarness()
        self.block_events: list[dict[str, object]] = []
        self.calls: list[dict[str, object]] = []

    def processing_trial(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


def set_metric_delta(
    start: dict[str, object], end: dict[str, object], metric: str, delta: int
) -> None:
    start_rows = start["metrics"][metric]  # type: ignore[index]
    end_rows = end["metrics"][metric]  # type: ignore[index]
    for before, after in zip(start_rows, end_rows, strict=True):
        after["value"][1] = str(float(before["value"][1]) + delta)


def build_e5_trust_fixture(root: Path) -> dict[str, object]:
    document = build_trust_fixture(root)
    document["tf_ticket"] = "TF-012E5"
    document["profile"] = {
        "minimum_public_trials": 3,
        "required_blocks": 3,
        "required_public_scenarios": ["retry_storm"],
    }
    for result in document["results"]:
        block = int(result["block"])
        directory = root / result["artifacts"]["directory"]
        tasks, attempts = retry_rows_fixture(block)
        raw = derive_retry_raw(tasks, attempts)
        base_correctness = {
            "expected_tasks": 1,
            "actual_tasks": 1,
            "expected_attempts": 2,
            "actual_attempts": 2,
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
        }
        evidence = retry_history_evidence(tasks, attempts, 1)
        correctness = {**base_correctness, **evidence, "passed": True}
        start = prometheus_snapshot(10)
        end = prometheus_snapshot(10)
        for name, delta in {
            "claimed": 2,
            "completed": 2,
            "attempts": 2,
            "retries_scheduled": 1,
            "retry_promotions": 1,
        }.items():
            set_metric_delta(start, end, COUNTER_METRICS[name], delta)
        for name, delta in {
            "queue": 2,
            "claim": 2,
            "execution": 2,
            "retry_lateness": 1,
            "retry_batch": 1,
        }.items():
            bucket = HISTOGRAM_METRICS[name]
            set_metric_delta(start, end, bucket, delta)
            set_metric_delta(start, end, bucket.removesuffix("_bucket") + "_sum", delta)
            set_metric_delta(start, end, bucket.removesuffix("_bucket") + "_count", delta)
        reconciliation = build_reconciliation(
            raw, start, end, "retry_storm", require_retry_batch=True
        )
        metadata = {
            "scenario": "retry_storm",
            "variant": "fail-once",
            "classification": "PUBLIC",
            "block": block,
            "trial": block,
            "order_index": 1,
            "retry_history_contract": True,
        }
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "correctness.json", correctness)
        write_json(directory / "prometheus_start.json", start)
        write_json(directory / "prometheus_end.json", end)
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        write_json(directory / "summary.json", {"raw": raw})
        create_manifest(directory)
        result.update(
            {
                "scenario": "retry_storm",
                "variant": "fail-once",
                "order_index": 1,
                "workers": 10,
                "schedulers": 3,
                "count": 1,
                "task_type": "test.fail_n_then_succeed",
                "raw": raw,
                "correctness": correctness,
                "prometheus_reconciliation": reconciliation,
                "valid": True,
            }
        )
    return document


class E5HarnessTests(unittest.TestCase):
    def test_configuration_is_exact_and_retry_only(self) -> None:
        profile = configuration_contract()
        self.assertEqual(E5_SPEC.scenario, "retry_storm")
        self.assertEqual(profile["retry_tasks"], 1000)
        self.assertEqual(profile["retry_workers"], 10)
        self.assertEqual(profile["retry_schedulers"], 3)
        self.assertEqual(profile["required_blocks"], 3)
        self.assertEqual(profile["required_public_scenarios"], ["retry_storm"])
        encoded = json.dumps(profile)
        for forbidden in ("cpu_scaling", "api_submission", "recovery_storm", "arrival_saturation"):
            self.assertNotIn(forbidden, encoded)

    def test_retry_trials_are_three_fresh_exact_contracts(self) -> None:
        trusted = FakeTrusted()
        with (
            mock.patch(
                "benchmarks.e5.warmup",
                return_value={"excluded": True, "correctness": {"passed": True}},
            ),
            mock.patch("benchmarks.e5.time.sleep"),
        ):
            run_retry_trials(trusted, E5_SPEC)  # type: ignore[arg-type]
        self.assertEqual(len(trusted.calls), 3)
        self.assertEqual(len(trusted.block_events), 3)
        self.assertEqual(trusted.harness.reset_calls, 3)
        for call in trusted.calls:
            self.assertEqual(call["count"], 1000)
            self.assertEqual(call["workers"], 10)
            self.assertEqual(call["schedulers"], 3)
            self.assertEqual(call["expected_attempts"], 2000)
            self.assertTrue(call["retry_history_contract"])

    def test_exact_retry_history_validator_accepts_fail_once(self) -> None:
        tasks, attempts = retry_rows_fixture()
        check = retry_storm_correctness(FakeHarness(), "queue", 1, tasks, attempts)  # type: ignore[arg-type]
        self.assertTrue(check["passed"])
        self.assertEqual(check["attempt1_failed"], 1)
        self.assertEqual(check["attempt2_succeeded"], 1)

    def test_retry_history_rejects_abandoned_duplicate_and_missing_promotion(self) -> None:
        tasks, attempts = retry_rows_fixture()
        abandoned = copy.deepcopy(attempts)
        abandoned[0]["status"] = "ABANDONED"
        self.assertFalse(retry_history_evidence(tasks, abandoned, 1)["retry_history_passed"])
        duplicate = [*attempts, dict(attempts[1])]
        self.assertFalse(retry_history_evidence(tasks, duplicate, 1)["retry_history_passed"])
        missing = copy.deepcopy(attempts)
        missing[1]["scheduled_at_snapshot"] = ""
        self.assertFalse(retry_history_evidence(tasks, missing, 1)["retry_history_passed"])

    def test_retry_history_rejects_cross_task_and_schedule_mismatch(self) -> None:
        tasks1, attempts1 = retry_rows_fixture(1)
        tasks2, attempts2 = retry_rows_fixture(2)
        attempts2[0]["retry_scheduled_at"] = "2026-01-01T00:00:00.310000+00:00"
        attempts2[1]["scheduled_at_snapshot"] = "2026-01-01T00:00:00.310000+00:00"
        crossed = copy.deepcopy([*attempts1, *attempts2])
        crossed[1]["task_id"] = tasks2[0]["task_id"]
        crossed[3]["task_id"] = tasks1[0]["task_id"]
        self.assertFalse(
            retry_history_evidence([*tasks1, *tasks2], crossed, 2)["retry_history_passed"]
        )

        mismatched_schedule = copy.deepcopy(attempts1)
        mismatched_schedule[1]["scheduled_at_snapshot"] = "2026-01-01T00:00:00.350000+00:00"
        evidence = retry_history_evidence(tasks1, mismatched_schedule, 1)
        self.assertFalse(evidence["retry_history_passed"])
        self.assertEqual(evidence["retry_chain_mismatches"], 1)

    def test_synthetic_aggregation_report_and_four_plots(self) -> None:
        document = synthetic_document()
        rows = retry_rows(document)
        aggregate = retry_aggregate(document)
        self.assertEqual(len(rows), 3)
        self.assertEqual(aggregate["processing_throughput_median"], 102.0)
        report = render_report(document)
        self.assertIn("Retry Lateness", report)
        self.assertIn("Attempt-2 Queue Wait", report)
        self.assertIn("Attempt-History Correctness", report)
        self.assertIn("Per-task chain mismatches", report)
        self.assertEqual(len(plot_specs(document)), 4)

    def test_plot_generation_is_deterministic_and_exactly_four(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            results = Path(temporary) / "results.json"
            results.write_text(json.dumps(synthetic_document()))
            first = {path.name: path.read_bytes() for path in generate(results)}
            second = {path.name: path.read_bytes() for path in generate(results)}
            self.assertEqual(first, second)
            self.assertEqual(
                sorted(PLOT_NAMES),
                sorted(path.name for path in (results.parent / "plots").iterdir()),
            )

    def test_e5_saved_artifact_trust_fixture_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e5_trust_fixture(root)
            write_json(root / "results.json", document)
            create_manifest(root)
            trust = evaluate_run_directory(root)
            self.assertEqual(trust["overall"]["result"], "PASS")


if __name__ == "__main__":
    unittest.main()
