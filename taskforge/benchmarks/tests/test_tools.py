import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.plot import line_plot
from benchmarks.report import render
from benchmarks.run import BenchmarkError, Harness, parse_bytes, percentile
from benchmarks.tests.trust_fixture import build_trust_fixture
from benchmarks.trust import derive_raw, evaluate_trust


class HarnessSafetyTests(unittest.TestCase):
    def test_rejects_non_benchmark_project(self) -> None:
        with self.assertRaises(BenchmarkError):
            Harness({}, "taskforge", keep=False)

    def test_accepts_scoped_benchmark_project(self) -> None:
        harness = Harness({}, "taskforge-tf012-test", keep=True)
        self.assertEqual(harness.project, "taskforge-tf012-test")


class StatisticsTests(unittest.TestCase):
    def test_percentile(self) -> None:
        self.assertEqual(percentile([5, 1, 3, 2, 4], 0.50), 3)
        self.assertIsNone(percentile([], 0.95))

    def test_parse_docker_bytes(self) -> None:
        self.assertEqual(parse_bytes("2MiB"), 2 * 1024 * 1024)
        self.assertEqual(parse_bytes("1GB"), 1_000_000_000)
        self.assertEqual(parse_bytes("not-a-size"), 0)


class ArtifactTests(unittest.TestCase):
    def test_plot_is_svg(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "plot.svg"
            line_plot(output, "Test", "x", "y", [("series", [(1, 2), (2, 4)])])
            self.assertIn("<svg", output.read_text())
            self.assertIn("series", output.read_text())

    def test_smoke_result_cannot_claim_complete_baseline(self) -> None:
        document = {
            "schema_version": 1,
            "tf_ticket": "TF-012",
            "suite": "smoke",
            "profile": {"name": "smoke", "stability_seconds": 10},
            "results": [],
            "environment": {},
            "warmup": {"excluded": True},
            "all_correctness_passed": True,
            "errors": [],
        }
        report = render(document, Path("results.json"))
        self.assertIn("FAIL — TF-012 REQUIRES FIXES", report)
        self.assertEqual(report.count("# "), 24)

    def test_configs_are_valid_json(self) -> None:
        config_directory = Path(__file__).resolve().parents[1] / "config"
        for path in config_directory.glob("*.json"):
            self.assertIn("name", json.loads(path.read_text()))


class TrustGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.document = self.valid_document()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def valid_document(self) -> dict[str, object]:
        return build_trust_fixture(self.root)

    def verdict(self, document: dict[str, object]) -> str:
        return evaluate_trust(document, self.root)["overall"]["result"]

    def test_valid_fixture_passes(self) -> None:
        self.assertEqual(self.verdict(self.document), "PASS")

    def test_trusted_report_has_gated_verdict_and_23_sections(self) -> None:
        self.document["schema_version"] = 2
        self.document["trust"] = evaluate_trust(self.document, self.root)
        report = render(self.document, self.root / "results.json")
        self.assertIn("PASS — BENCHMARKS TRUSTWORTHY", report)
        self.assertEqual(report.count("# "), 23)

    def test_dirty_source_fails(self) -> None:
        self.document["source"]["clean"] = False  # type: ignore[index]
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_missing_raw_artifact_fails(self) -> None:
        (self.root / "trials" / "trial-1" / "tasks.csv").unlink()
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_negative_duration_fails(self) -> None:
        result = self.document["results"][0]  # type: ignore[index]
        result["raw"]["negative_durations"]["queue_wait"] = 1
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_prometheus_mismatch_fails(self) -> None:
        self.document["results"][0]["prometheus_reconciliation"]["status"] = "FAIL"  # type: ignore[index]
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_single_trial_public_result_fails(self) -> None:
        self.document["results"] = self.document["results"][:1]  # type: ignore[index]
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_failed_regression_fails(self) -> None:
        self.document["regression"]["passed"] = False  # type: ignore[index]
        self.assertEqual(self.verdict(self.document), "FAIL")

    def test_retry_queue_entries_are_attempt_specific(self) -> None:
        attempts = [
            {
                "attempt_number": "1",
                "status": "FAILED",
                "attempt_started_at": "2026-01-01T00:00:01+00:00",
                "attempt_finished_at": "2026-01-01T00:00:02+00:00",
                "queue_entered_at": "2026-01-01T00:00:00+00:00",
                "scheduled_at_snapshot": "2026-01-01T00:00:00+00:00",
            },
            {
                "attempt_number": "2",
                "status": "SUCCEEDED",
                "attempt_started_at": "2026-01-01T00:00:04+00:00",
                "attempt_finished_at": "2026-01-01T00:00:05+00:00",
                "queue_entered_at": "2026-01-01T00:00:03+00:00",
                "scheduled_at_snapshot": "2026-01-01T00:00:03+00:00",
            },
        ]
        raw = derive_raw([], attempts)
        self.assertEqual(raw["queue_observations"], 2)
        self.assertEqual(raw["queue_p95_seconds"], 1)
        self.assertFalse(any(raw["negative_durations"].values()))


if __name__ == "__main__":
    unittest.main()
