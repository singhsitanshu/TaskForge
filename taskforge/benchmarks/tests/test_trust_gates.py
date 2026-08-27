"""Focused synthetic tests for the TF-012D publication trust engine."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.tests.trust_fixture import build_trust_fixture, rehash_trial
from benchmarks.trust import (
    create_manifest,
    derive_raw,
    evaluate_run_directory,
    evaluate_trust,
    read_csv,
    write_csv,
    write_json,
)
from benchmarks.trusted import ATTEMPT_FIELDS, TASK_FIELDS


class TrustGateEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.document = build_trust_fixture(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def evaluate(self) -> dict[str, object]:
        return evaluate_trust(self.document, self.root)

    def assert_gate_fails(self, gate: str) -> None:
        trust = self.evaluate()
        self.assertEqual(trust[gate]["result"], "FAIL")  # type: ignore[index]
        self.assertEqual(trust["overall"]["result"], "FAIL")  # type: ignore[index]

    def test_positive_synthetic_fixture_passes_all_gates(self) -> None:
        write_json(self.root / "results.json", self.document)
        create_manifest(self.root)
        trust = evaluate_run_directory(self.root)
        self.assertEqual(trust["overall"]["result"], "PASS")  # type: ignore[index]
        for name, gate in trust.items():
            self.assertIn(gate["result"], {"PASS", "FAIL"}, name)  # type: ignore[index]
            self.assertEqual(gate["result"], "PASS", name)  # type: ignore[index]

    def test_dirty_or_unpublishable_source_fails(self) -> None:
        self.document["source"]["clean"] = False
        self.assert_gate_fails("source_provenance")
        self.document["source"]["clean"] = True
        self.document["publishable"] = False
        self.assert_gate_fails("source_provenance")

    def test_missing_attempts_file_fails(self) -> None:
        (self.root / "trials" / "trial-1" / "attempts.csv").unlink()
        self.assert_gate_fails("raw_data")

    def test_manifest_hash_mismatch_fails(self) -> None:
        with (self.root / "trials" / "trial-1" / "summary.json").open("a") as output:
            output.write(" ")
        trust = self.evaluate()
        self.assertEqual(trust["source_provenance"]["result"], "FAIL")
        self.assertEqual(trust["raw_data"]["result"], "FAIL")

    def test_negative_queue_duration_fails(self) -> None:
        directory = self.root / "trials" / "trial-1"
        attempts = read_csv(directory / "attempts.csv")
        attempts[0]["queue_entered_at"] = "2026-01-01T00:00:02+00:00"
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        tasks = read_csv(directory / "tasks.csv")
        raw = derive_raw(tasks, attempts)
        self.document["results"][0]["raw"] = raw
        write_json(directory / "summary.json", {"raw": raw})
        rehash_trial(self.root, 1)
        self.assert_gate_fails("latency")

    def test_raw_quantile_mismatch_fails(self) -> None:
        self.document["results"][0]["raw"]["queue_p95_seconds"] = 999
        self.assert_gate_fails("latency")

    def test_prometheus_counter_mismatch_fails(self) -> None:
        directory = self.root / "trials" / "trial-1"
        reconciliation = self.document["results"][0]["prometheus_reconciliation"]
        reconciliation["counters"]["completed"]["prometheus"] = 2
        reconciliation["counters"]["completed"]["difference"] = 1
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        rehash_trial(self.root, 1)
        self.assert_gate_fails("prometheus")

    def test_prometheus_invalid_fails(self) -> None:
        directory = self.root / "trials" / "trial-1"
        reconciliation = self.document["results"][0]["prometheus_reconciliation"]
        reconciliation["prometheus_valid"] = False
        reconciliation["status"] = "FAIL"
        write_json(directory / "prometheus_reconciliation.json", reconciliation)
        rehash_trial(self.root, 1)
        self.assert_gate_fails("prometheus")

    def test_one_public_trial_fails(self) -> None:
        self.document["results"] = self.document["results"][:1]
        self.assert_gate_fails("repetition")

    def test_missing_independent_blocks_fails(self) -> None:
        for result in self.document["results"]:
            result["block"] = 1
        self.assert_gate_fails("reproducibility")

    def test_failed_regression_command_fails(self) -> None:
        self.document["regression"]["passed"] = False
        self.document["regression"]["commands"][0]["exit_code"] = 1
        self.assert_gate_fails("regression")

    def test_duplicate_attempt_identity_fails(self) -> None:
        directory = self.root / "trials" / "trial-1"
        attempts = read_csv(directory / "attempts.csv")
        attempts.append(dict(attempts[0]))
        write_csv(directory / "attempts.csv", attempts, ATTEMPT_FIELDS)
        rehash_trial(self.root, 1)
        self.assert_gate_fails("correctness")

    def test_failed_or_missing_task_fails(self) -> None:
        directory = self.root / "trials" / "trial-1"
        tasks = read_csv(directory / "tasks.csv")
        tasks[0]["final_status"] = "FAILED"
        write_csv(directory / "tasks.csv", tasks, TASK_FIELDS)
        correctness = json.loads((directory / "correctness.json").read_text())
        correctness["passed"] = False
        correctness["succeeded_tasks"] = 0
        self.document["results"][0]["correctness"] = correctness
        write_json(directory / "correctness.json", correctness)
        rehash_trial(self.root, 1)
        self.assert_gate_fails("correctness")


if __name__ == "__main__":
    unittest.main()
