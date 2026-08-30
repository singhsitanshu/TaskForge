"""Focused non-performance tests for the scoped TF-012E4 harness."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.e4 import E4_SPEC, EXPECTED_CONCURRENCY, configuration_contract, run_api_blocks
from benchmarks.e4_artifacts import PLOT_NAMES, api_rows, generate, plot_specs, render_report
from benchmarks.tests.trust_fixture import build_trust_fixture, prometheus_snapshot
from benchmarks.trust import (
    build_reconciliation,
    create_manifest,
    derive_raw,
    evaluate_run_directory,
    write_csv,
    write_json,
)
from benchmarks.trusted import ATTEMPT_FIELDS, TASK_FIELDS, api_submission_correctness


def synthetic_document() -> dict[str, object]:
    results = []
    for block in (1, 2, 3):
        for order, concurrency in enumerate(EXPECTED_CONCURRENCY, start=1):
            throughput = float(concurrency * 100 + block)
            submission = {
                "request_count": 2000,
                "successes": 2000,
                "failures": 0,
                "distinct_task_ids": 2000,
                "requests_per_second": throughput,
                "status_counts": {"201": 2000},
                "error_counts": {},
                "latency_ms": {
                    "p50": concurrency / 10 + block,
                    "p95": concurrency / 5 + block,
                    "p99": concurrency / 4 + block,
                },
            }
            results.append(
                {
                    "scenario": "api_submission",
                    "variant": f"c{concurrency}",
                    "classification": "PUBLIC",
                    "block": block,
                    "trial": block,
                    "order_index": order,
                    "random_seed": 1000 + block,
                    "api_concurrency": concurrency,
                    "workers": 0,
                    "valid": True,
                    "submission": submission,
                    "raw": {"submission_throughput_per_second": throughput},
                    "correctness": {
                        "actual_http_requests": 2000,
                        "successful_responses": 2000,
                        "actual_tasks": 2000,
                        "actual_attempts": 0,
                        "queued_tasks": 2000,
                    },
                    "prometheus_reconciliation": {
                        "counters": {
                            "api_requests": {
                                "raw": 2000,
                                "prometheus": 2000,
                                "difference": 0,
                                "status": "PASS",
                            },
                            "api_submissions": {
                                "raw": 2000,
                                "prometheus": 2000,
                                "difference": 0,
                                "status": "PASS",
                            },
                        }
                    },
                }
            )
    pass_gate = {"result": "PASS"}
    return {
        "run_id": "synthetic-e4",
        "source": {"git_commit_sha": "a" * 40, "git_tree_hash": "b" * 40, "clean": True},
        "environment": {"platform": "synthetic"},
        "profile": configuration_contract(),
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
    def __init__(self, check: dict[str, object] | None = None) -> None:
        self.check = check
        self.reset_calls = 0
        self.start_calls = 0

    def reset(self) -> None:
        self.reset_calls += 1

    def start(self) -> None:
        self.start_calls += 1

    def json_sql(self, _: str) -> dict[str, object]:
        return dict(
            self.check
            or {
                "expected_tasks": 2000,
                "actual_tasks": 2000,
                "expected_attempts": 0,
                "actual_attempts": 0,
                "terminal_tasks": 0,
                "succeeded_tasks": 0,
                "queued_tasks": 2000,
                "duplicate_attempts": 0,
                "attempt_count_mismatches": 0,
                "stranded_leases": 0,
                "unexpected_attempt_states": 0,
                "abandoned_attempts": 0,
                "missing_queue_evidence": 0,
                "negative_queue_waits": 0,
            }
        )


class FakeTrusted:
    def __init__(self) -> None:
        self.profile = configuration_contract()
        self.harness = FakeHarness()
        self.block_events: list[dict[str, object]] = []
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def api_trial(self, *args: object, **kwargs: object) -> None:
        self.calls.append((args, kwargs))


def build_e4_trust_fixture(root: Path) -> dict[str, object]:
    document = build_trust_fixture(root)
    document["tf_ticket"] = "TF-012E4"
    document["profile"] = {
        "minimum_public_trials": 3,
        "required_blocks": 3,
        "required_public_scenarios": ["api_submission"],
        "api_concurrency": [1],
    }
    for result in document["results"]:
        block = int(result["block"])
        directory = root / result["artifacts"]["directory"]
        task_id = f"api-task-{block}"
        tasks = [
            {
                "task_id": task_id,
                "task_created_at": "2026-01-01T00:00:00+00:00",
                "task_completed_at": "",
                "final_status": "QUEUED",
                "attempt_count": "0",
                "task_type": "test.noop",
                "queue": f"api-{block}",
            }
        ]
        attempts: list[dict[str, str]] = []
        raw = derive_raw(tasks, attempts)
        raw["submission_throughput_per_second"] = 100.0
        raw["submission_latency_ms"] = {"p50": 1.0, "p95": 2.0, "p99": 3.0}
        submission = {
            "request_count": 1,
            "successes": 1,
            "failures": 0,
            "distinct_task_ids": 1,
            "requests_per_second": 100.0,
            "status_counts": {"201": 1},
            "error_counts": {},
            "latency_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0},
        }
        correctness = {
            "expected_tasks": 1,
            "actual_tasks": 1,
            "expected_attempts": 0,
            "actual_attempts": 0,
            "terminal_tasks": 0,
            "succeeded_tasks": 0,
            "queued_tasks": 1,
            "duplicate_attempts": 0,
            "attempt_count_mismatches": 0,
            "stranded_leases": 0,
            "unexpected_attempt_states": 0,
            "abandoned_attempts": 0,
            "missing_queue_evidence": 0,
            "negative_queue_waits": 0,
            "expected_abandoned": 0,
            "terminal_expected": False,
            "expected_http_requests": 1,
            "actual_http_requests": 1,
            "successful_responses": 1,
            "distinct_response_task_ids": 1,
            "http_2xx": 1,
            "http_4xx": 0,
            "http_5xx": 0,
            "transport_errors": 0,
            "passed": True,
        }
        start = prometheus_snapshot(10)
        end = prometheus_snapshot(11)
        reconciliation = build_reconciliation(raw, start, end, "api_submission")
        metadata = {
            "scenario": "api_submission",
            "variant": "c1",
            "classification": "PUBLIC",
            "block": block,
            "trial": block,
            "order_index": 1,
        }
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        write_json(directory / "metadata.json", metadata)
        write_json(directory / "correctness.json", correctness)
        write_json(directory / "prometheus_start.json", start)
        write_json(directory / "prometheus_end.json", end)
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        write_json(directory / "summary.json", {"raw": raw, "submission": submission})
        create_manifest(directory)
        result.update(
            {
                "scenario": "api_submission",
                "variant": "c1",
                "order_index": 1,
                "workers": 0,
                "count": 1,
                "submission": submission,
                "raw": raw,
                "correctness": correctness,
                "prometheus_reconciliation": reconciliation,
                "api_concurrency": 1,
                "valid": True,
            }
        )
    return document


class E4HarnessTests(unittest.TestCase):
    def test_configuration_is_exact_and_api_only(self) -> None:
        profile = configuration_contract()
        self.assertEqual(E4_SPEC.scenario, "api_submission")
        self.assertEqual(profile["api_concurrency"], EXPECTED_CONCURRENCY)
        self.assertEqual(profile["api_requests"], 2000)
        self.assertEqual(profile["required_blocks"], 3)
        self.assertEqual(profile["api_submission_workers"], 0)
        self.assertEqual(profile["required_public_scenarios"], ["api_submission"])
        encoded = json.dumps(profile)
        for forbidden in ("cpu_scaling", "retry_storm", "recovery_storm", "arrival_saturation"):
            self.assertNotIn(forbidden, encoded)

    def test_api_blocks_are_reset_randomized_keyless_and_exactly_fifteen(self) -> None:
        trusted = FakeTrusted()
        with (
            mock.patch(
                "benchmarks.e4.warmup",
                return_value={"excluded": True, "correctness": {"passed": True}},
            ),
            mock.patch("benchmarks.e4.time.sleep"),
        ):
            run_api_blocks(trusted, E4_SPEC)  # type: ignore[arg-type]
        self.assertEqual(len(trusted.calls), 15)
        self.assertEqual(len(trusted.block_events), 3)
        self.assertEqual(trusted.harness.reset_calls, 3)
        self.assertTrue(all(call[1]["scenario"] == "api_submission" for call in trusted.calls))
        self.assertTrue(all(call[1]["key_mode"] == "none" for call in trusted.calls))
        self.assertEqual(
            {(call[1]["block"], call[0][0]) for call in trusted.calls},
            {(block, concurrency) for block in (1, 2, 3) for concurrency in EXPECTED_CONCURRENCY},
        )

    def test_api_correctness_accepts_queued_tasks_and_zero_attempts(self) -> None:
        submission = {
            "request_count": 2000,
            "successes": 2000,
            "distinct_task_ids": 2000,
            "status_counts": {"201": 2000},
            "error_counts": {},
        }
        check = api_submission_correctness(FakeHarness(), "queue", 2000, submission)  # type: ignore[arg-type]
        self.assertTrue(check["passed"])
        self.assertEqual(check["actual_attempts"], 0)
        self.assertEqual(check["queued_tasks"], 2000)

    def test_api_correctness_rejects_database_row_mismatch(self) -> None:
        mismatch = FakeHarness()
        mismatch.check = mismatch.json_sql("")
        mismatch.check["actual_tasks"] = 1999
        submission = {
            "request_count": 2000,
            "successes": 2000,
            "distinct_task_ids": 2000,
            "status_counts": {"201": 2000},
            "error_counts": {},
        }
        check = api_submission_correctness(mismatch, "queue", 2000, submission)  # type: ignore[arg-type]
        self.assertFalse(check["passed"])

    def test_synthetic_aggregation_report_and_three_plots(self) -> None:
        document = synthetic_document()
        rows = api_rows(document)
        self.assertEqual([row["concurrency"] for row in rows], EXPECTED_CONCURRENCY)
        self.assertEqual(rows[0]["submission_throughput_median"], 102.0)
        report = render_report(document)
        self.assertIn("SUBMISSION THROUGHPUT", report)
        self.assertIn("tasks intentionally remain `QUEUED`", report)
        self.assertIn("it is not processing capacity", report.lower())
        self.assertNotIn("## PROCESSING THROUGHPUT", report)
        self.assertEqual(len(plot_specs(document)), 3)

    def test_plot_generation_is_deterministic_and_exactly_three(self) -> None:
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

    def test_e4_saved_artifact_trust_fixture_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            document = build_e4_trust_fixture(root)
            write_json(root / "results.json", document)
            create_manifest(root)
            trust = evaluate_run_directory(root)
            self.assertEqual(trust["overall"]["result"], "PASS")


if __name__ == "__main__":
    unittest.main()
