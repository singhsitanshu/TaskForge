"""Focused non-performance tests for the final trusted TF-012 reporting."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from benchmarks.e1_artifacts import summary_rows
from benchmarks.final_report import (
    DEFAULT_INPUTS,
    correctness_rows,
    load_trusted_input,
    load_trusted_inputs,
    render_report,
    render_summary,
    resume_findings,
    scaling_maps,
    write_reports,
)
from benchmarks.run import BENCHMARKS, BenchmarkError

FAILED_E3 = (
    BENCHMARKS
    / "results/20260828T033607521399Z_618cbfe18d0e_tf-012e3-cpu_c32a5e9d860d/results.json"
)
FINAL_E3_RUN_ID = "20260828T194454461406Z_053d2edf177d_tf-012e3-cpu_34a34bf2ab90"


class FinalReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.documents = load_trusted_inputs()
        cls.report = render_report(cls.documents)
        cls.summary = render_summary(cls.documents)

    def test_all_e1_through_e6_inputs_independently_pass(self) -> None:
        self.assertEqual(set(self.documents), set(DEFAULT_INPUTS))
        for label, document in self.documents.items():
            with self.subTest(label=label):
                self.assertEqual(document["_standalone_trust"]["overall"]["result"], "PASS")
                self.assertEqual(document["trust"]["overall"]["result"], "PASS")

    def test_failed_historical_e3_is_rejected(self) -> None:
        with self.assertRaisesRegex(BenchmarkError, "E3 standalone trust is not PASS"):
            load_trusted_input("E3", FAILED_E3)

    def test_report_uses_only_the_accepted_final_e3(self) -> None:
        self.assertIn(FINAL_E3_RUN_ID, self.report)
        self.assertNotIn("20260828T033607521399Z_618cbfe18d0e", self.report)
        self.assertNotIn("20260828T070013621798Z_e76b95f98246", self.report)

    def test_required_timing_and_submission_semantics_are_explicit(self) -> None:
        flattened = " ".join(self.report.replace("**", "").split())
        self.assertIn("after lease expiration", flattened)
        self.assertIn("not worker kill to recovery", flattened)
        self.assertIn("submission throughput, not worker processing throughput", flattened)
        self.assertIn("Attempt Lifecycle", self.report)
        self.assertIn("Handler", self.report)
        self.assertIn("not the same timing semantic", flattened)

    def test_cross_workload_speedups_match_trusted_artifacts_exactly(self) -> None:
        actual = scaling_maps(self.documents)
        for label, scenario in (
            ("E1", "noop_scaling"),
            ("E2", "io50_scaling"),
            ("E3", "cpu_scaling"),
        ):
            expected = {
                int(row["workers"]): row["speedup"]
                for row in summary_rows(self.documents[label], scenario)
            }
            self.assertEqual(
                {workers: row["speedup"] for workers, row in actual[label].items()}, expected
            )

    def test_correctness_totals_match_accepted_artifacts(self) -> None:
        rows = {row["experiment"]: row for row in correctness_rows(self.documents)}
        self.assertEqual((rows["E1"]["tasks"], rows["E1"]["attempts"]), (60_000, 60_000))
        self.assertEqual((rows["E2"]["tasks"], rows["E2"]["attempts"]), (12_000, 12_000))
        self.assertEqual((rows["E3"]["tasks"], rows["E3"]["attempts"]), (12_000, 12_000))
        self.assertEqual((rows["E4"]["tasks"], rows["E4"]["attempts"]), (30_000, 0))
        self.assertEqual((rows["E5"]["tasks"], rows["E5"]["attempts"]), (3_000, 6_000))
        self.assertEqual((rows["E6"]["tasks"], rows["E6"]["attempts"]), (3_000, 3_030))
        self.assertTrue(
            all(
                row["duplicates"] == row["lost_failed"] == row["stranded"] == 0
                for row in rows.values()
            )
        )

    def test_resume_safe_findings_trace_to_trusted_values(self) -> None:
        findings = resume_findings(self.documents)
        scaling = scaling_maps(self.documents)
        self.assertEqual(len(findings), 5)
        self.assertIn(f"{scaling['E2'][16]['speedup']:.3f}x", findings[0])
        self.assertIn("2143.48 requests/s", findings[1])
        self.assertIn("54.120 ms", findings[2])
        self.assertIn("36.681 ms after lease expiration", findings[3])
        self.assertIn(f"{scaling['E3'][8]['speedup']:.3f}x", findings[4])

    def test_report_and_summary_render_deterministically(self) -> None:
        self.assertEqual(self.report, render_report(self.documents))
        self.assertEqual(self.summary, render_summary(self.documents))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report_path = root / "report.md"
            summary_path = root / "summary.md"
            write_reports(self.documents, report_path, summary_path)
            first = (report_path.read_bytes(), summary_path.read_bytes())
            write_reports(self.documents, report_path, summary_path)
            second = (report_path.read_bytes(), summary_path.read_bytes())
            self.assertEqual(first, second)

    def test_summary_has_no_more_than_five_key_results(self) -> None:
        key_section = self.summary.split("## Recommended Resume Bullet", 1)[0]
        results = [line for line in key_section.splitlines() if line[:1].isdigit()]
        self.assertEqual(len(results), 5)


if __name__ == "__main__":
    unittest.main()
