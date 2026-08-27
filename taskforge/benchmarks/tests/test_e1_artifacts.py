"""Focused non-performance tests for TF-012E1 artifact generation."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.e1_artifacts import PLOT_NAMES, generate, summary_rows


class E1ArtifactTests(unittest.TestCase):
    def document(self) -> dict[str, object]:
        results = []
        for block in (1, 2, 3):
            for order, workers in enumerate((8, 1, 16, 4), start=1):
                throughput = workers * 1000 + block
                raw = {
                    "processing_throughput_per_second": throughput,
                    **{
                        f"{name}_p{quantile}_seconds": workers / 1000 + quantile / 100000
                        for name in ("queue", "execution", "total")
                        for quantile in (50, 95, 99)
                    },
                }
                claim = {
                    "prometheus_quantiles": {
                        f"p{quantile}": workers / 10000 + quantile / 1000000
                        for quantile in (50, 95, 99)
                    }
                }
                results.append(
                    {
                        "scenario": "noop_scaling",
                        "variant": f"w{workers}",
                        "classification": "PUBLIC",
                        "block": block,
                        "trial": block,
                        "order_index": order,
                        "random_seed": 123 + block,
                        "workers": workers,
                        "valid": True,
                        "raw": raw,
                        "correctness": {
                            "actual_tasks": 5000,
                            "actual_attempts": 5000,
                            "duplicate_attempts": 0,
                            "stranded_leases": 0,
                            "succeeded_tasks": 5000,
                        },
                        "prometheus_reconciliation": {
                            "counters": {
                                "completed": {
                                    "raw": 5000,
                                    "prometheus": 5000,
                                    "difference": 0,
                                    "status": "PASS",
                                }
                            },
                            "histograms": {"claim": claim},
                        },
                    }
                )
        return {
            "run_id": "synthetic-e1",
            "profile": {"noop_tasks": 5000},
            "source": {"clean": True},
            "images": {},
            "environment": {},
            "trust": {"overall": {"result": "PASS"}},
            "results": results,
        }

    def test_summary_uses_medians_and_one_value_per_block(self) -> None:
        rows = summary_rows(self.document())
        self.assertEqual([row["workers"] for row in rows], [1, 4, 8, 16])
        self.assertEqual(rows[0]["processing_throughput_median"], 1002)
        self.assertEqual(rows[0]["block_values"], {1: 1001.0, 2: 1002.0, 3: 1003.0})
        self.assertEqual(rows[0]["speedup"], 1)

    def test_generates_only_five_scoped_plots_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results_path = root / "results.json"
            results_path.write_text(json.dumps(self.document()))
            first = generate(results_path)
            first_content = {path.name: path.read_bytes() for path in first}
            second = generate(results_path)
            self.assertEqual(first_content, {path.name: path.read_bytes() for path in second})
            self.assertEqual(
                sorted(path.name for path in (root / "plots").iterdir()), sorted(PLOT_NAMES)
            )
            report = (root / "tf-012e1-noop-scaling.md").read_text()
            self.assertIn("PROCESSING THROUGHPUT", report)
            self.assertIn("Per-trial raw measurements", report)
            self.assertNotIn("I/O Scaling", report)

    def test_invalid_trials_are_excluded_from_summary(self) -> None:
        document = copy.deepcopy(self.document())
        document["results"][0]["valid"] = False
        row = next(item for item in summary_rows(document) if item["workers"] == 8)
        self.assertEqual(set(row["block_values"]), {2, 3})


if __name__ == "__main__":
    unittest.main()
