"""Non-performance contract and artifact tests for TF-012E2A."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.e1_artifacts import summary_rows
from benchmarks.e2 import E2_SPEC, EXPECTED_WORKERS, configuration_contract, load_trusted_e1
from benchmarks.e2_artifacts import PLOT_NAMES, generate, render_report


def synthetic_document() -> dict[str, object]:
    results = []
    for block in (1, 2, 3):
        for order, workers in enumerate((8, 1, 16, 4), start=1):
            throughput = workers * 10 + block
            raw = {
                "processing_throughput_per_second": throughput,
                "negative_durations": {
                    "queue_wait": 0,
                    "execution": 0,
                    "total_latency": 0,
                    "retry_lateness": 0,
                    "recovery_lag": 0,
                },
                **{
                    f"{name}_p{quantile}_seconds": (
                        0.05 + quantile / 1_000_000
                        if name == "execution"
                        else workers / 1000 + quantile / 100_000
                    )
                    for name in ("queue", "execution", "total")
                    for quantile in (50, 95, 99)
                },
            }
            results.append(
                {
                    "scenario": "io50_scaling",
                    "variant": f"w{workers}",
                    "classification": "PUBLIC",
                    "block": block,
                    "trial": block,
                    "order_index": order,
                    "random_seed": 9000 + block,
                    "workers": workers,
                    "valid": True,
                    "raw": raw,
                    "correctness": {
                        "actual_tasks": 1000,
                        "actual_attempts": 1000,
                        "duplicate_attempts": 0,
                        "stranded_leases": 0,
                        "succeeded_tasks": 1000,
                    },
                    "prometheus_reconciliation": {
                        "counters": {
                            "completed": {
                                "raw": 1000,
                                "prometheus": 1000,
                                "difference": 0,
                                "status": "PASS",
                            }
                        },
                        "histograms": {
                            "claim": {
                                "prometheus_quantiles": {
                                    "p50": 0.001,
                                    "p95": 0.002,
                                    "p99": 0.003,
                                }
                            }
                        },
                    },
                }
            )
    return {
        "run_id": "synthetic-e2",
        "profile": {"io_tasks": 1000},
        "source": {"clean": True},
        "environment": {},
        "trust": {"overall": {"result": "PASS"}},
        "e1_comparison": {
            "run_id": "trusted-e1",
            "results_sha256": "abc",
            "speedup": {"1": 1.0, "4": 1.6, "8": 1.7, "16": 1.5},
        },
        "results": results,
    }


class E2ContractTests(unittest.TestCase):
    def test_scenario_selects_only_test_sleep_50_ms(self) -> None:
        self.assertEqual(E2_SPEC.scenario, "io50_scaling")
        self.assertEqual(E2_SPEC.task_type, "test.sleep")
        self.assertEqual(E2_SPEC.payload, {"duration_ms": 50})

    def test_forbidden_scenarios_are_absent(self) -> None:
        contract = configuration_contract()
        self.assertEqual(contract["required_public_scenarios"], ["io50_scaling"])
        encoded = json.dumps(contract)
        for forbidden in (
            "noop_scaling",
            "cpu_scaling",
            "api_throughput",
            "arrival_saturation",
            "retry_storm",
            "recovery_storm",
        ):
            self.assertNotIn(forbidden, encoded)

    def test_configuration_is_exact_and_defaults_to_1000_tasks(self) -> None:
        contract = configuration_contract()
        self.assertEqual(contract["scaling_workers"], EXPECTED_WORKERS)
        self.assertEqual(contract["required_blocks"], 3)
        self.assertEqual(contract["io_tasks"], 1000)
        self.assertEqual(contract["workload"], {"task_type": "test.sleep", "duration_ms": 50})

    def test_synthetic_e2_aggregation(self) -> None:
        rows = summary_rows(synthetic_document(), "io50_scaling")
        self.assertEqual([row["workers"] for row in rows], EXPECTED_WORKERS)
        self.assertEqual(rows[0]["processing_throughput_median"], 12)
        self.assertEqual(rows[0]["block_values"], {1: 11.0, 2: 12.0, 3: 13.0})

    def test_synthetic_e2_report_is_scoped(self) -> None:
        report = render_report(synthetic_document())
        self.assertIn("test.sleep", report)
        self.assertIn("duration_ms=50", report)
        self.assertIn("Trusted E1 Comparison", report)
        self.assertNotIn("CPU Scaling", report)
        self.assertNotIn("API Re-Test", report)

    def test_synthetic_e2_plot_generator_writes_only_five_plots(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results_path = root / "results.json"
            results_path.write_text(json.dumps(synthetic_document()))
            first = generate(results_path, None)
            first_hashes = {path.name: path.read_bytes() for path in first}
            second = generate(results_path, None)
            self.assertEqual(first_hashes, {path.name: path.read_bytes() for path in second})
            self.assertEqual(
                sorted(path.name for path in (root / "plots").iterdir()), sorted(PLOT_NAMES)
            )

    def test_e1_comparison_reads_external_trusted_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            results_path = root / "results.json"
            results = [
                {
                    "scenario": "noop_scaling",
                    "classification": "PUBLIC",
                    "valid": True,
                    "workers": workers,
                    "block": 1,
                    "raw": {"processing_throughput_per_second": workers * 7},
                }
                for workers in EXPECTED_WORKERS
            ]
            results_path.write_text(
                json.dumps(
                    {
                        "tf_ticket": "TF-012E1",
                        "run_id": "external-e1",
                        "source": {"git_commit_sha": "a", "git_tree_hash": "b"},
                        "results": results,
                    }
                )
            )
            with mock.patch(
                "benchmarks.e2.evaluate_run_directory",
                return_value={"overall": {"result": "PASS"}},
            ):
                comparison = load_trusted_e1(results_path)
            self.assertEqual(comparison["run_id"], "external-e1")
            self.assertEqual(comparison["speedup"]["16"], 16)
            self.assertEqual(len(comparison["results_sha256"]), 64)

    def test_dry_run_prints_contract_without_execution(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "benchmarks.e2", "--dry-run"],
            check=True,
            capture_output=True,
            text=True,
        )
        contract = json.loads(result.stdout)
        self.assertEqual(contract["workers"], EXPECTED_WORKERS)
        self.assertEqual(contract["measured_trials"], 12)
        self.assertFalse(contract["will_execute"])


if __name__ == "__main__":
    unittest.main()
