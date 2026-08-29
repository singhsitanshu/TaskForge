"""Focused non-performance tests for the scoped TF-012E6 harness."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.e6 import E6_SPEC, configuration_contract, run_recovery_trials
from benchmarks.e6_artifacts import (
    PLOT_NAMES,
    generate,
    plot_specs,
    recovery_aggregate,
    recovery_rows,
    render_report,
)
from benchmarks.tests.trust_fixture import build_trust_fixture
from benchmarks.trust import (
    COUNTER_METRICS,
    HISTOGRAM_METRICS,
    build_reconciliation,
    create_manifest,
    derive_recovery_raw,
    evaluate_run_directory,
    read_csv,
    recovery_history_evidence,
    write_csv,
    write_json,
)
from benchmarks.trusted import (
    ATTEMPT_FIELDS,
    TASK_FIELDS,
    deterministic_recovery_victims,
)


def recovery_evidence(
    trial: int = 1,
) -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, object]]:
    tasks = []
    attempts = []
    affected = []
    selected_workers = []
    for index in range(1, 11):
        task_id = f"task-{trial}-{index}"
        worker_id = f"worker-{trial}-{index}"
        tasks.append(
            {
                "task_id": task_id,
                "task_created_at": "2026-01-01T00:00:00+00:00",
                "task_completed_at": (
                    "2026-01-01T00:00:02.500000+00:00"
                    if index <= 2
                    else "2026-01-01T00:00:00.600000+00:00"
                ),
                "final_status": "SUCCEEDED",
                "attempt_count": "2" if index <= 2 else "1",
                "task_type": "test.sleep",
                "queue": f"recovery-{trial}",
            }
        )
        first = {
            "task_id": task_id,
            "attempt_id": f"attempt-{trial}-{index}-1",
            "attempt_number": "1",
            "status": "ABANDONED" if index <= 2 else "SUCCEEDED",
            "worker_id": worker_id,
            "worker_label": f"worker-{index}",
            "task_created_at": "2026-01-01T00:00:00+00:00",
            "attempt_leased_at": "2026-01-01T00:00:00.100000+00:00",
            "attempt_started_at": "2026-01-01T00:00:00.100000+00:00",
            "attempt_finished_at": (
                "2026-01-01T00:00:01.100000+00:00"
                if index <= 2
                else "2026-01-01T00:00:00.600000+00:00"
            ),
            "queue_entered_at": "2026-01-01T00:00:00+00:00",
            "scheduled_at_snapshot": "2026-01-01T00:00:00+00:00",
            "retry_scheduled_at": "",
            "recovered_lease_expires_at": ("2026-01-01T00:00:01+00:00" if index <= 2 else ""),
            "recovered_at": "2026-01-01T00:00:01.100000+00:00" if index <= 2 else "",
            "recovery_action": "requeued" if index <= 2 else "",
        }
        attempts.append(first)
        if index <= 2:
            attempts.append(
                {
                    **first,
                    "attempt_id": f"attempt-{trial}-{index}-2",
                    "attempt_number": "2",
                    "status": "SUCCEEDED",
                    "worker_id": f"replacement-{trial}-{index}",
                    "worker_label": f"replacement-{index}",
                    "attempt_leased_at": "2026-01-01T00:00:01.200000+00:00",
                    "attempt_started_at": "2026-01-01T00:00:01.200000+00:00",
                    "attempt_finished_at": "2026-01-01T00:00:02.500000+00:00",
                    "queue_entered_at": "2026-01-01T00:00:01.100000+00:00",
                    "scheduled_at_snapshot": "2026-01-01T00:00:01.100000+00:00",
                    "recovered_lease_expires_at": "",
                    "recovered_at": "",
                    "recovery_action": "",
                }
            )
            affected.append(
                {
                    "task_id": task_id,
                    "attempt_id": first["attempt_id"],
                    "attempt_number": 1,
                    "lease_expires_at": "2026-01-01T00:00:01+00:00",
                    "owner_worker_id": worker_id,
                    "owner_instance_id": f"instance-{trial}-{index}",
                    "owner_name": f"worker-{index}",
                    "pre_kill_status": "RUNNING",
                }
            )
            selected_workers.append(
                {
                    "worker_id": worker_id,
                    "worker_name": f"worker-{index}",
                    "container_name": f"worker-slot-{index}",
                    "container_id": f"old-{index}",
                    "hostname": f"worker-{index}",
                    "start_target": f"taskforge-worker|killed-{index}:8080",
                }
            )
    pairs = [
        {
            "worker_name": f"worker-slot-{index}",
            "start_target": f"taskforge-worker|killed-{index}:8080",
            "end_target": f"taskforge-worker|replacement-{index}:8080",
            "start_replica": f"worker|worker-slot-{index}|old-{index}",
            "end_replica": f"worker|worker-slot-{index}|new-{index}",
        }
        for index in (1, 2)
    ]
    boundary: dict[str, object] = {
        "schema_version": 1,
        "selection_rule": "synthetic deterministic selection",
        "selection_seed": 1000 + trial,
        "expected_workers": 10,
        "expected_killed_workers": 2,
        "selected_workers": selected_workers,
        "surviving_workers": [
            {"worker_id": f"worker-{trial}-{index}", "worker_name": f"worker-{index}"}
            for index in range(3, 11)
        ],
        "affected_tasks": affected,
        "affected_task_count": 2,
        "pre_kill_status": "RUNNING",
        "hard_kill_method": "docker kill",
        "kill_timestamp": "2026-01-01T00:00:00.500000+00:00",
        "kill_completed_at": "2026-01-01T00:00:00.510000+00:00",
        "final_drain_observed_at": "2026-01-01T00:00:02.600000+00:00",
        "worker_liveness": {
            "expected_killed_workers": 2,
            "expected_surviving_workers": 8,
            "killed_dead": 2,
            "surviving_active": 8,
        },
        "prometheus_allowed_churn": {
            "start_targets": [pair["start_target"] for pair in pairs],
            "end_targets": [pair["end_target"] for pair in pairs],
            "start_replicas": [pair["start_replica"] for pair in pairs],
            "end_replicas": [pair["end_replica"] for pair in pairs],
            "pairs": pairs,
        },
    }
    return tasks, attempts, boundary


def _sample(metric: str, value: float, *, job: str, instance: str, **labels: str):
    return {
        "metric": {"__name__": metric, "job": job, "instance": instance, **labels},
        "value": [1_800_000_000, str(value)],
    }


def recovery_snapshots(boundary: dict[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    allowed = boundary["prometheus_allowed_churn"]
    start_workers = [f"survivor-{index}:8080" for index in range(3, 11)] + [
        "killed-1:8080",
        "killed-2:8080",
    ]
    end_workers = [f"survivor-{index}:8080" for index in range(3, 11)] + [
        "replacement-1:8080",
        "replacement-2:8080",
    ]
    schedulers = [f"scheduler-{index}:8080" for index in range(1, 4)]

    def snapshot(workers: list[str], *, end: bool) -> dict[str, object]:
        targets = [
            {"job": "taskforge-api", "instance": "api:8000", "health": "up", "last_error": ""},
            *[
                {
                    "job": "taskforge-worker",
                    "instance": instance,
                    "health": "up",
                    "last_error": "",
                }
                for instance in workers
            ],
            *[
                {
                    "job": "taskforge-scheduler",
                    "instance": instance,
                    "health": "up",
                    "last_error": "",
                }
                for instance in schedulers
            ],
        ]
        replicas = [
            {"service": "api", "name": "api-1", "container_id": "api-id", "state": "running"},
            *[
                {
                    "service": "worker",
                    "name": (f"worker-slot-{index}" if index <= 2 else f"worker-slot-{index}"),
                    "container_id": (
                        f"{'new' if end else 'old'}-{index}" if index <= 2 else f"stable-{index}"
                    ),
                    "state": "running",
                }
                for index in range(1, 11)
            ],
            *[
                {
                    "service": "scheduler",
                    "name": f"scheduler-{index}",
                    "container_id": f"scheduler-id-{index}",
                    "state": "running",
                }
                for index in range(1, 4)
            ],
        ]
        metrics: dict[str, list[dict[str, object]]] = {"process_start_time_seconds": []}
        for instance in workers:
            metrics["process_start_time_seconds"].append(
                _sample(
                    "process_start_time_seconds", 100, job="taskforge-worker", instance=instance
                )
            )
        for instance in schedulers:
            metrics["process_start_time_seconds"].append(
                _sample(
                    "process_start_time_seconds", 200, job="taskforge-scheduler", instance=instance
                )
            )
        return {"targets": targets, "replicas": replicas, "metrics": metrics}

    start = snapshot(start_workers, end=False)
    end = snapshot(end_workers, end=True)
    recovery_metric = COUNTER_METRICS["recoveries"]
    start["metrics"][recovery_metric] = [  # type: ignore[index]
        _sample(
            recovery_metric,
            10,
            job="taskforge-scheduler",
            instance="scheduler-1:8080",
            outcome="requeued",
        )
    ]
    end["metrics"][recovery_metric] = [  # type: ignore[index]
        _sample(
            recovery_metric,
            12,
            job="taskforge-scheduler",
            instance="scheduler-1:8080",
            outcome="requeued",
        )
    ]

    def histogram(metric: str, observations: int, increment_sum: float) -> None:
        base = metric.removesuffix("_bucket")
        start["metrics"][metric] = [  # type: ignore[index]
            _sample(metric, 10, job="taskforge-scheduler", instance="scheduler-1:8080", le="0.1"),
            _sample(metric, 10, job="taskforge-scheduler", instance="scheduler-1:8080", le="+Inf"),
        ]
        end["metrics"][metric] = [  # type: ignore[index]
            _sample(
                metric,
                10 + observations,
                job="taskforge-scheduler",
                instance="scheduler-1:8080",
                le="0.1",
            ),
            _sample(
                metric,
                10 + observations,
                job="taskforge-scheduler",
                instance="scheduler-1:8080",
                le="+Inf",
            ),
        ]
        start["metrics"][base + "_sum"] = [  # type: ignore[index]
            _sample(base + "_sum", 1, job="taskforge-scheduler", instance="scheduler-1:8080")
        ]
        end["metrics"][base + "_sum"] = [  # type: ignore[index]
            _sample(
                base + "_sum",
                1 + increment_sum,
                job="taskforge-scheduler",
                instance="scheduler-1:8080",
            )
        ]
        start["metrics"][base + "_count"] = [  # type: ignore[index]
            _sample(base + "_count", 10, job="taskforge-scheduler", instance="scheduler-1:8080")
        ]
        end["metrics"][base + "_count"] = [  # type: ignore[index]
            _sample(
                base + "_count",
                10 + observations,
                job="taskforge-scheduler",
                instance="scheduler-1:8080",
            )
        ]

    histogram(HISTOGRAM_METRICS["recovery"], 2, 0.2)
    histogram(HISTOGRAM_METRICS["recovery_batch"], 3, 0.03)
    assert isinstance(allowed, dict)
    return start, end


def build_e6_trust_fixture(root: Path) -> dict[str, object]:
    document = build_trust_fixture(root)
    document["tf_ticket"] = "TF-012E6"
    document["profile"] = {
        "minimum_public_trials": 3,
        "required_blocks": 3,
        "required_public_scenarios": ["recovery_storm"],
        "recovery_workers": 10,
        "recovery_kill_workers": 2,
    }
    for result in document["results"]:
        trial = int(result["trial"])
        directory = root / result["artifacts"]["directory"]
        tasks, attempts, boundary = recovery_evidence(trial)
        raw = derive_recovery_raw(tasks, attempts, boundary)
        evidence = recovery_history_evidence(tasks, attempts, boundary, 10)
        correctness = {
            "expected_tasks": 10,
            "actual_tasks": 10,
            "expected_attempts": 12,
            "actual_attempts": 12,
            "terminal_tasks": 10,
            "succeeded_tasks": 10,
            "queued_tasks": 0,
            "duplicate_attempts": 0,
            "attempt_count_mismatches": 0,
            "stranded_leases": 0,
            "unexpected_attempt_states": 0,
            "missing_queue_evidence": 0,
            "negative_queue_waits": 0,
            "terminal_expected": True,
            **evidence,
            "passed": True,
        }
        start, end = recovery_snapshots(boundary)
        reconciliation = build_reconciliation(
            raw,
            start,
            end,
            "recovery_storm",
            intentional_worker_churn=True,
            allowed_worker_churn=boundary["prometheus_allowed_churn"],
            require_recovery_contract=True,
        )
        metadata = {
            "scenario": "recovery_storm",
            "variant": "kill-2-of-10",
            "classification": "PUBLIC",
            "block": trial,
            "trial": trial,
            "order_index": 1,
            "intentional_worker_churn": True,
            "recovery_contract": True,
        }
        submission = {"request_count": 10, "successes": 10}
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "correctness.json", correctness)
        write_json(directory / "prometheus_start.json", start)
        write_json(directory / "prometheus_end.json", end)
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        write_json(directory / "summary.json", {"raw": raw, "submission": submission})
        write_json(directory / "failure_boundary.json", boundary)
        create_manifest(directory)
        result.update(
            {
                "scenario": "recovery_storm",
                "variant": "kill-2-of-10",
                "workers": 10,
                "schedulers": 3,
                "count": 10,
                "task_type": "test.sleep",
                "submission": submission,
                "raw": raw,
                "correctness": correctness,
                "prometheus_reconciliation": reconciliation,
                "failure_boundary": boundary,
                "killed_workers": 2,
                "valid": True,
            }
        )
    return document


class FakeHarness:
    def __init__(self) -> None:
        self.reset_calls = 0
        self.start_calls = 0

    def reset(self) -> None:
        self.reset_calls += 1

    def start(self) -> None:
        self.start_calls += 1

    def trial_configuration(self) -> dict[str, str]:
        return dict(configuration_contract()["timing_configuration"])


class FakeTrusted:
    def __init__(self) -> None:
        self.profile = configuration_contract()
        self.harness = FakeHarness()
        self.block_events: list[dict[str, object]] = []
        self.calls: list[dict[str, object]] = []

    def recovery_crash_trial(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


class E6HarnessTests(unittest.TestCase):
    def test_configuration_is_exact_and_recovery_only(self) -> None:
        profile = configuration_contract()
        self.assertEqual(E6_SPEC.scenario, "recovery_storm")
        self.assertEqual(profile["recovery_tasks"], 1000)
        self.assertEqual(profile["recovery_workers"], 20)
        self.assertEqual(profile["recovery_schedulers"], 3)
        self.assertEqual(profile["recovery_kill_workers"], 10)
        self.assertEqual(profile["recovery_sleep_ms"], 500)
        self.assertEqual(profile["required_blocks"], 3)
        encoded = json.dumps(profile)
        for forbidden in ("cpu_scaling", "api_submission", "retry_storm", "arrival_saturation"):
            self.assertNotIn(forbidden, encoded)

    def test_three_fresh_trials_use_the_fixed_contract(self) -> None:
        trusted = FakeTrusted()
        with (
            mock.patch(
                "benchmarks.e6.warmup",
                return_value={"excluded": True, "correctness": {"passed": True}},
            ),
            mock.patch("benchmarks.e6.time.sleep"),
        ):
            run_recovery_trials(trusted, E6_SPEC)  # type: ignore[arg-type]
        self.assertEqual(trusted.harness.reset_calls, 3)
        self.assertEqual(len(trusted.block_events), 3)
        self.assertEqual(len(trusted.calls), 3)
        for call in trusted.calls:
            self.assertEqual(call["count"], 1000)
            self.assertEqual(call["workers"], 20)
            self.assertEqual(call["schedulers"], 3)
            self.assertEqual(call["killed_workers"], 10)
            self.assertEqual(call["sleep_ms"], 500)

    def test_kill_selection_is_deterministic_and_exact(self) -> None:
        containers = [f"worker-{index}" for index in range(1, 21)]
        first = deterministic_recovery_victims(containers, 10, 1234)
        second = deterministic_recovery_victims(list(reversed(containers)), 10, 1234)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 10)
        self.assertEqual(len(set(first)), 10)

    def test_exact_recovery_history_and_boundary_pass(self) -> None:
        tasks, attempts, boundary = recovery_evidence()
        evidence = recovery_history_evidence(tasks, attempts, boundary, 10)
        self.assertTrue(evidence["recovery_history_passed"])
        self.assertEqual(evidence["first_attempt_successes"], 8)
        self.assertEqual(evidence["recovered_replacement_successes"], 2)

    def test_negative_recovery_histories_are_rejected(self) -> None:
        tasks, attempts, boundary = recovery_evidence()
        missing_abandoned = copy.deepcopy(attempts)
        missing_abandoned[0]["status"] = "SUCCEEDED"
        self.assertFalse(
            recovery_history_evidence(tasks, missing_abandoned, boundary, 10)[
                "recovery_history_passed"
            ]
        )
        missing_replacement = [row for row in attempts if row["attempt_id"] != "attempt-1-1-2"]
        self.assertFalse(
            recovery_history_evidence(tasks, missing_replacement, boundary, 10)[
                "recovery_history_passed"
            ]
        )
        duplicate = [*attempts, dict(attempts[1])]
        self.assertFalse(
            recovery_history_evidence(tasks, duplicate, boundary, 10)["recovery_history_passed"]
        )
        unaffected = copy.deepcopy(attempts)
        unaffected[4]["status"] = "ABANDONED"
        self.assertFalse(
            recovery_history_evidence(tasks, unaffected, boundary, 10)["recovery_history_passed"]
        )
        mismatched_boundary = copy.deepcopy(boundary)
        mismatched_boundary["affected_tasks"][0]["lease_expires_at"] = (  # type: ignore[index]
            "2026-01-01T00:00:00.900000+00:00"
        )
        self.assertFalse(
            recovery_history_evidence(tasks, attempts, mismatched_boundary, 10)[
                "recovery_history_passed"
            ]
        )

    def test_stranded_liveness_and_negative_lag_fixtures_fail(self) -> None:
        tasks, attempts, boundary = recovery_evidence()
        bad_liveness = copy.deepcopy(boundary)
        bad_liveness["worker_liveness"]["killed_dead"] = 1  # type: ignore[index]
        self.assertFalse(
            recovery_history_evidence(tasks, attempts, bad_liveness, 10)["recovery_history_passed"]
        )
        negative = copy.deepcopy(attempts)
        negative[0]["recovered_at"] = "2026-01-01T00:00:00.900000+00:00"
        raw = derive_recovery_raw(tasks, negative, boundary)
        self.assertEqual(raw["negative_durations"]["recovery_lag"], 1)

    def test_saved_negative_stranded_and_negative_lag_fixtures_fail_trust(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e6_trust_fixture(root)
            first = document["results"][0]
            directory = root / first["artifacts"]["directory"]
            correctness = copy.deepcopy(first["correctness"])
            correctness["stranded_leases"] = 1
            correctness["passed"] = False
            first["correctness"] = correctness
            write_json(directory / "correctness.json", correctness)
            create_manifest(directory)
            write_json(root / "results.json", document)
            create_manifest(root)
            self.assertEqual(evaluate_run_directory(root)["overall"]["result"], "FAIL")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e6_trust_fixture(root)
            first = document["results"][0]
            directory = root / first["artifacts"]["directory"]
            attempts = read_csv(directory / "attempts.csv")
            attempts[0]["recovered_at"] = "2026-01-01T00:00:00.900000+00:00"
            write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
            create_manifest(directory)
            write_json(root / "results.json", document)
            create_manifest(root)
            self.assertEqual(evaluate_run_directory(root)["overall"]["result"], "FAIL")

    def test_only_explicit_killed_worker_churn_is_accepted(self) -> None:
        tasks, attempts, boundary = recovery_evidence()
        raw = derive_recovery_raw(tasks, attempts, boundary)
        start, end = recovery_snapshots(boundary)
        accepted = build_reconciliation(
            raw,
            start,
            end,
            "recovery_storm",
            intentional_worker_churn=True,
            allowed_worker_churn=boundary["prometheus_allowed_churn"],
            require_recovery_contract=True,
        )
        self.assertEqual(accepted["status"], "PASS")
        unrelated = copy.deepcopy(end)
        unrelated["metrics"]["process_start_time_seconds"][2]["value"][1] = "300"  # type: ignore[index]
        rejected = build_reconciliation(
            raw,
            start,
            unrelated,
            "recovery_storm",
            intentional_worker_churn=True,
            allowed_worker_churn=boundary["prometheus_allowed_churn"],
            require_recovery_contract=True,
        )
        self.assertEqual(rejected["status"], "FAIL")
        unrelated_scheduler = copy.deepcopy(end)
        unrelated_scheduler["metrics"]["process_start_time_seconds"][10]["value"][1] = (  # type: ignore[index]
            "300"
        )
        rejected_scheduler = build_reconciliation(
            raw,
            start,
            unrelated_scheduler,
            "recovery_storm",
            intentional_worker_churn=True,
            allowed_worker_churn=boundary["prometheus_allowed_churn"],
            require_recovery_contract=True,
        )
        self.assertEqual(rejected_scheduler["status"], "FAIL")

    def test_synthetic_aggregation_report_and_four_plots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e6_trust_fixture(root)
            document["trust"] = {
                name: {"result": "PASS"}
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
            }
            rows = recovery_rows(document)
            aggregate = recovery_aggregate(document)
            self.assertEqual(len(rows), 3)
            self.assertEqual(aggregate["affected_tasks_median"], 2)
            report = render_report(document)
            self.assertIn("Failure Boundary", report)
            self.assertIn("Scheduler Contention", report)
            self.assertEqual(len(plot_specs(document)), 4)

    def test_plot_generation_is_deterministic_and_exactly_four(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results = root / "results.json"
            results.write_text(json.dumps(build_e6_trust_fixture(root)))
            first = {path.name: path.read_bytes() for path in generate(results)}
            second = {path.name: path.read_bytes() for path in generate(results)}
            self.assertEqual(first, second)
            self.assertEqual(
                sorted(PLOT_NAMES), sorted(path.name for path in (root / "plots").iterdir())
            )

    def test_saved_e6_artifact_fixture_and_boundary_manifest_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e6_trust_fixture(root)
            write_json(root / "results.json", document)
            create_manifest(root)
            trust = evaluate_run_directory(root)
            self.assertEqual(trust["overall"]["result"], "PASS")
            manifest = json.loads((root / "trials" / "trial-1" / "manifest.json").read_text())
            self.assertIn("failure_boundary.json", {item["path"] for item in manifest["artifacts"]})


if __name__ == "__main__":
    unittest.main()
