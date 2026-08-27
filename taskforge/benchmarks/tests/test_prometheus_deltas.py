import copy
import unittest

from benchmarks.trust import (
    COUNTER_METRICS,
    HISTOGRAM_METRICS,
    PROMETHEUS_METRICS,
    build_reconciliation,
    counter_delta,
    delta_quantiles,
    histogram_delta,
)


def sample(metric: str, value: float, *, job: str, instance: str, **labels):
    return {
        "metric": {"__name__": metric, "job": job, "instance": instance, **labels},
        "value": [1_800_000_000, str(value)],
    }


def add_counter(snapshot, metric, value, *, job, instance, **labels):
    snapshot["metrics"].setdefault(metric, []).append(
        sample(metric, value, job=job, instance=instance, **labels)
    )


def add_histogram_pair(start, end, metric, observations, *, job, instance):
    base = metric.removesuffix("_bucket")
    start_counts = (("0.01", 900), ("0.05", 990), ("+Inf", 1000))
    increments = (
        ("0.01", observations * 0.50),
        ("0.05", observations * 0.95),
        ("+Inf", observations),
    )
    for (upper, start_value), (_, increment) in zip(
        start_counts, increments, strict=True
    ):
        add_counter(start, metric, start_value, job=job, instance=instance, le=upper)
        add_counter(
            end, metric, start_value + increment, job=job, instance=instance, le=upper
        )
    add_counter(start, base + "_sum", 100, job=job, instance=instance)
    add_counter(
        end, base + "_sum", 100 + observations * 0.03, job=job, instance=instance
    )
    add_counter(start, base + "_count", 1000, job=job, instance=instance)
    add_counter(end, base + "_count", 1000 + observations, job=job, instance=instance)


def snapshots(*, attempts: int, retries: bool = False):
    targets = [
        {
            "job": "taskforge-api",
            "instance": "api:8000",
            "health": "up",
            "last_error": "",
        },
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
        {
            "service": "api",
            "name": "api-1",
            "container_id": "api-id",
            "state": "running",
        },
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
    start = {
        "targets": copy.deepcopy(targets),
        "replicas": copy.deepcopy(replicas),
        "metrics": {},
    }
    end = copy.deepcopy(start)

    for snapshot in (start, end):
        add_counter(
            snapshot,
            "process_start_time_seconds",
            100,
            job="taskforge-worker",
            instance="worker:8080",
        )
        add_counter(
            snapshot,
            "process_start_time_seconds",
            200,
            job="taskforge-scheduler",
            instance="scheduler:8080",
        )

    add_counter(
        start,
        COUNTER_METRICS["claimed"],
        5000,
        job="taskforge-worker",
        instance="worker:8080",
    )
    add_counter(
        end,
        COUNTER_METRICS["claimed"],
        5000 + attempts,
        job="taskforge-worker",
        instance="worker:8080",
    )

    if retries:
        outcomes = (("retryable_failure", 7000), ("success", 8000))
        attempt_outcomes = (("failed", 9000), ("succeeded", 10000))
        per_outcome = attempts // 2
    else:
        outcomes = (("success", 8000),)
        attempt_outcomes = (("succeeded", 10000),)
        per_outcome = attempts
    for outcome, baseline in outcomes:
        add_counter(
            start,
            COUNTER_METRICS["completed"],
            baseline,
            job="taskforge-worker",
            instance="worker:8080",
            outcome=outcome,
        )
        add_counter(
            end,
            COUNTER_METRICS["completed"],
            baseline + per_outcome,
            job="taskforge-worker",
            instance="worker:8080",
            outcome=outcome,
        )
    for outcome, baseline in attempt_outcomes:
        add_counter(
            start,
            COUNTER_METRICS["attempts"],
            baseline,
            job="taskforge-worker",
            instance="worker:8080",
            outcome=outcome,
        )
        add_counter(
            end,
            COUNTER_METRICS["attempts"],
            baseline + per_outcome,
            job="taskforge-worker",
            instance="worker:8080",
            outcome=outcome,
        )

    for name in ("queue", "claim", "execution"):
        add_histogram_pair(
            start,
            end,
            HISTOGRAM_METRICS[name],
            attempts,
            job="taskforge-worker",
            instance="worker:8080",
        )
    if retries:
        add_counter(
            start,
            COUNTER_METRICS["retries_scheduled"],
            3000,
            job="taskforge-worker",
            instance="worker:8080",
        )
        add_counter(
            end,
            COUNTER_METRICS["retries_scheduled"],
            3100,
            job="taskforge-worker",
            instance="worker:8080",
        )
        add_counter(
            start,
            COUNTER_METRICS["retry_promotions"],
            4000,
            job="taskforge-scheduler",
            instance="scheduler:8080",
        )
        add_counter(
            end,
            COUNTER_METRICS["retry_promotions"],
            4100,
            job="taskforge-scheduler",
            instance="scheduler:8080",
        )
        add_histogram_pair(
            start,
            end,
            HISTOGRAM_METRICS["retry_lateness"],
            100,
            job="taskforge-scheduler",
            instance="scheduler:8080",
        )
    return start, end


class PrometheusDeltaTests(unittest.TestCase):
    def test_histogram_components_are_captured(self):
        for bucket in HISTOGRAM_METRICS.values():
            base = bucket.removesuffix("_bucket")
            self.assertIn(bucket, PROMETHEUS_METRICS)
            self.assertIn(base + "_sum", PROMETHEUS_METRICS)
            self.assertIn(base + "_count", PROMETHEUS_METRICS)

    def test_counter_uses_delta_not_cumulative_end_value(self):
        start, end = snapshots(attempts=100)
        measured = counter_delta(start, end, COUNTER_METRICS["completed"])
        self.assertTrue(measured["valid"])
        self.assertEqual(measured["delta"], 100)

    def test_counter_reset_is_explicitly_invalid(self):
        start, end = snapshots(attempts=100)
        end["metrics"][COUNTER_METRICS["claimed"]][0]["value"][1] = "10"
        measured = counter_delta(start, end, COUNTER_METRICS["claimed"])
        self.assertFalse(measured["valid"])
        self.assertTrue(measured["counter_decreases"])

    def test_histogram_quantiles_use_delta_buckets_sum_and_count(self):
        start, end = snapshots(attempts=100)
        measured = histogram_delta(start, end, HISTOGRAM_METRICS["queue"])
        quantiles = delta_quantiles(measured)
        self.assertTrue(measured["valid"])
        self.assertEqual(measured["count"]["delta"], 100)
        self.assertAlmostEqual(measured["sum"]["delta"], 3)
        self.assertEqual([item["count"] for item in measured["buckets"]], [50, 95, 100])
        self.assertAlmostEqual(quantiles["p50"], 0.01)
        self.assertAlmostEqual(quantiles["p95"], 0.05)
        self.assertAlmostEqual(quantiles["p99"], 0.05)

    def test_100_successful_tasks_reconcile(self):
        start, end = snapshots(attempts=100)
        raw = {
            "attempt_count": 100,
            "attempt_status_counts": {"SUCCEEDED": 100},
            "queue_observations": 100,
            "queue_p95_seconds": 0.05,
            "execution_observations": 100,
            "execution_p95_seconds": 0.05,
        }
        reconciliation = build_reconciliation(raw, start, end, "tf012c_success")
        self.assertTrue(reconciliation["prometheus_valid"])
        self.assertEqual(reconciliation["counters"]["completed"]["raw"], 100)
        self.assertEqual(reconciliation["counters"]["completed"]["prometheus"], 100)
        self.assertEqual(reconciliation["counters"]["completed"]["difference"], 0)

    def test_100_retry_once_tasks_reconcile_200_attempts(self):
        start, end = snapshots(attempts=200, retries=True)
        raw = {
            "attempt_count": 200,
            "attempt_status_counts": {"FAILED": 100, "SUCCEEDED": 100},
            "queue_observations": 200,
            "queue_p95_seconds": 0.05,
            "execution_observations": 200,
            "execution_p95_seconds": 0.05,
            "retry_lateness_observations": 100,
            "retry_lateness_p95_seconds": 0.05,
        }
        reconciliation = build_reconciliation(raw, start, end, "retry_storm")
        self.assertTrue(reconciliation["prometheus_valid"])
        self.assertEqual(reconciliation["counters"]["attempts"]["prometheus"], 200)
        self.assertEqual(
            reconciliation["counters"]["retries_scheduled"]["prometheus"], 100
        )
        self.assertEqual(
            reconciliation["counters"]["retry_promotions"]["prometheus"], 100
        )

    def test_worker_restart_cannot_look_valid(self):
        start, end = snapshots(attempts=100)
        end["metrics"]["process_start_time_seconds"][0]["value"][1] = "300"
        end["replicas"][1]["container_id"] = "replacement-worker-id"
        raw = {
            "attempt_count": 100,
            "attempt_status_counts": {"SUCCEEDED": 100},
            "queue_observations": 100,
            "queue_p95_seconds": 0.05,
            "execution_observations": 100,
            "execution_p95_seconds": 0.05,
        }
        reconciliation = build_reconciliation(
            raw, start, end, "tf012c_restart", intentional_worker_churn=True
        )
        self.assertFalse(reconciliation["prometheus_valid"])
        self.assertTrue(reconciliation["unexpected_target_churn"])
        self.assertTrue(reconciliation["replica_churn"])
        self.assertTrue(reconciliation["process_identity_changes"]["restarted"])


if __name__ == "__main__":
    unittest.main()
