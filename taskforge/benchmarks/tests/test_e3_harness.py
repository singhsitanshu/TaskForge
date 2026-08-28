"""Focused non-performance contract and artifact tests for TF-012E3A."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks.e3 import (
    CPU_ITERATIONS,
    E3_SPEC,
    EXPECTED_WORKERS,
    configuration_contract,
    load_trusted_e1,
    load_trusted_e2,
)
from benchmarks.e3_artifacts import (
    PLOT_NAMES,
    e3_latency_rows,
    generate,
    plot_specs,
    render_report,
    resource_rows,
)
from benchmarks.run import BENCHMARKS, BenchmarkError
from benchmarks.scoped import run_scaling_blocks


def synthetic_document() -> dict[str, object]:
    results = []
    orders = {
        1: (8, 1, 16, 4),
        2: (1, 8, 4, 16),
        3: (4, 16, 8, 1),
    }
    for block, worker_order in orders.items():
        for order, workers in enumerate(worker_order, start=1):
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
                    f"{name}_p{quantile}_seconds": (workers / 1000 + quantile / 1_000_000)
                    for name in ("queue", "total")
                    for quantile in (50, 95, 99)
                },
                "execution_p50_seconds": 0.010,
                "execution_p95_seconds": 0.015,
                "execution_p99_seconds": 0.020,
            }
            counters = {
                name: {"raw": 1000, "prometheus": 1000, "difference": 0, "status": "PASS"}
                for name in ("attempts", "claimed", "completed")
            }
            results.append(
                {
                    "scenario": "cpu_scaling",
                    "variant": f"w{workers}",
                    "classification": "PUBLIC",
                    "block": block,
                    "trial": block,
                    "order_index": order,
                    "random_seed": 9100 + block,
                    "workers": workers,
                    "task_type": "test.cpu",
                    "payload": {"iterations": CPU_ITERATIONS},
                    "valid": True,
                    "raw": raw,
                    "resources": {
                        "worker": {"cpu_percent_mean": workers * 50 + block},
                        "postgres": {"cpu_percent_mean": workers + block},
                    },
                    "correctness": {
                        "actual_tasks": 1000,
                        "actual_attempts": 1000,
                        "duplicate_attempts": 0,
                        "stranded_leases": 0,
                        "succeeded_tasks": 1000,
                    },
                    "prometheus_reconciliation": {
                        "counters": counters,
                        "histograms": {
                            "claim": {
                                "prometheus_quantiles": {
                                    "p50": 0.001,
                                    "p95": 0.002,
                                    "p99": 0.003,
                                }
                            },
                            "execution": {
                                "prometheus_quantiles": {
                                    "p50": 0.035,
                                    "p95": 0.045,
                                    "p99": 0.049,
                                }
                            },
                        },
                    },
                }
            )
    return {
        "run_id": "synthetic-e3",
        "profile": {
            "cpu_tasks": 1000,
            "workload": {"task_type": "test.cpu", "iterations": CPU_ITERATIONS},
        },
        "source": {"clean": True, "git_commit_sha": "commit", "git_tree_hash": "tree"},
        "environment": {"host_logical_cpus": 12},
        "images": {},
        "trust": {"overall": {"result": "PASS"}},
        "e1_comparison": {
            "run_id": "trusted-e1",
            "results_sha256": "e1hash",
            "speedup": {"1": 1.0, "4": 1.6, "8": 1.7, "16": 1.5},
        },
        "e2_comparison": {
            "run_id": "trusted-e2",
            "results_sha256": "e2hash",
            "speedup": {"1": 1.0, "4": 4.0, "8": 8.0, "16": 15.8},
        },
        "results": results,
    }


def external_document(ticket: str, scenario: str) -> dict[str, object]:
    return {
        "tf_ticket": ticket,
        "run_id": f"external-{ticket.lower()}",
        "source": {"git_commit_sha": "commit", "git_tree_hash": "tree"},
        "results": [
            {
                "scenario": scenario,
                "variant": f"w{workers}",
                "classification": "PUBLIC",
                "valid": True,
                "workers": workers,
                "block": block,
                "raw": {"processing_throughput_per_second": workers * 7 + block},
            }
            for workers in EXPECTED_WORKERS
            for block in (1, 2, 3)
        ],
    }


class FakeHarness:
    def reset(self) -> None:
        pass

    def start(self) -> None:
        pass


class FakeTrusted:
    def __init__(self) -> None:
        self.profile = configuration_contract()
        self.harness = FakeHarness()
        self.block_events: list[dict[str, object]] = []
        self.calls: list[dict[str, object]] = []

    def processing_trial(self, **kwargs: object) -> None:
        self.calls.append(kwargs)


class E3ContractTests(unittest.TestCase):
    def test_scenario_selects_only_fixed_test_cpu_workload(self) -> None:
        self.assertEqual(E3_SPEC.scenario, "cpu_scaling")
        self.assertEqual(E3_SPEC.task_type, "test.cpu")
        self.assertEqual(E3_SPEC.payload, {"iterations": CPU_ITERATIONS})
        self.assertEqual(CPU_ITERATIONS, 200000)

    def test_configuration_is_exact_and_has_no_unrelated_scenario(self) -> None:
        contract = configuration_contract()
        self.assertEqual(contract["scaling_workers"], EXPECTED_WORKERS)
        self.assertEqual(contract["required_blocks"], 3)
        self.assertEqual(contract["cpu_tasks"], 1000)
        self.assertEqual(contract["required_public_scenarios"], ["cpu_scaling"])
        encoded = json.dumps(contract)
        for forbidden in (
            "noop_scaling",
            "io50_scaling",
            "api_throughput",
            "arrival_saturation",
            "retry_storm",
            "recovery_storm",
        ):
            self.assertNotIn(forbidden, encoded)

    def test_block_orchestration_uses_fixed_payload_for_exactly_twelve_trials(self) -> None:
        trusted = FakeTrusted()
        with (
            mock.patch(
                "benchmarks.scoped.warmup",
                return_value={"excluded": True, "correctness": {"passed": True}},
            ),
            mock.patch("benchmarks.scoped.time.sleep"),
        ):
            run_scaling_blocks(trusted, E3_SPEC)  # type: ignore[arg-type]
        self.assertEqual(len(trusted.calls), 12)
        self.assertEqual(len(trusted.block_events), 3)
        self.assertEqual(
            {(int(call["block"]), int(call["workers"])) for call in trusted.calls},
            {(block, workers) for block in (1, 2, 3) for workers in EXPECTED_WORKERS},
        )
        self.assertTrue(all(call["task_type"] == "test.cpu" for call in trusted.calls))
        self.assertTrue(
            all(call["payload"] == {"iterations": CPU_ITERATIONS} for call in trusted.calls)
        )

    def test_cpu_resource_aggregation_accepts_existing_sample_summaries(self) -> None:
        rows = resource_rows(synthetic_document())
        self.assertEqual([row["workers"] for row in rows], EXPECTED_WORKERS)
        self.assertEqual(rows[0]["worker_cpu_percent"], 52)
        self.assertEqual(rows[0]["postgres_cpu_percent"], 3)
        self.assertEqual(rows[-1]["worker_cpu_percent"], 802)

    def test_scoped_report_renders_from_synthetic_fixture(self) -> None:
        report = render_report(synthetic_document())
        self.assertIn("TF-012E3 Deterministic CPU Scaling", report)
        self.assertIn("iterations per attempt: `200000`", report)
        self.assertIn("CPU Resource Behavior", report)
        self.assertIn("E1 / E2 / E3 Speedup Comparison", report)
        self.assertIn("Attempt Lifecycle p95", report)
        self.assertIn("Handler p50", report)
        self.assertIn("Handler p95", report)
        self.assertNotIn("| Execution p95 |", report)
        self.assertNotIn("API Re-Test", report)
        self.assertNotIn("Recovery", report)

    def test_e3_latency_model_keeps_attempt_and_handler_sources_distinct(self) -> None:
        rows = e3_latency_rows(synthetic_document())
        self.assertEqual([row["attempt_lifecycle_p95"] for row in rows], [0.015] * 4)
        self.assertEqual([row["handler_p95"] for row in rows], [0.045] * 4)

    def test_plot_04_uses_handler_p95_not_attempt_lifecycle_p95(self) -> None:
        specs = plot_specs(synthetic_document())
        name, title, _, series = specs[3]
        self.assertEqual(name, PLOT_NAMES[3])
        self.assertEqual(title, "CPU Handler-Execution p95 vs Workers")
        self.assertEqual(series[0][0], "Prometheus handler execution")
        self.assertEqual([value for _, value in series[0][1]], [0.045] * 4)
        self.assertNotEqual([value for _, value in series[0][1]], [0.015] * 4)

    def test_throughput_and_speedup_plot_sources_remain_unchanged(self) -> None:
        specs = plot_specs(synthetic_document())
        self.assertEqual(specs[0][1:3], ("CPU processing throughput", "Tasks/second"))
        self.assertEqual(specs[1][1:3], ("CPU speedup", "Speedup vs 1 worker"))
        self.assertEqual(specs[4][1], "Trusted E1 vs E2 vs E3 speedup")
        self.assertEqual([name for name, _ in specs[4][3]], ["E1 no-op", "E2 50 ms", "E3 CPU"])

    def test_retained_trusted_e3_uses_recorded_handler_histogram(self) -> None:
        results_path = (
            BENCHMARKS
            / "results"
            / "20260828T070013621798Z_e76b95f98246_tf-012e3-cpu_57f885b840a0"
            / "results.json"
        )
        if not results_path.is_file():
            self.skipTest("retained trusted E3 result is not available")
        document = json.loads(results_path.read_text())
        row = next(item for item in e3_latency_rows(document) if item["workers"] == 8)
        plotted = dict(plot_specs(document)[3][3][0][1])[8]
        self.assertAlmostEqual(row["attempt_lifecycle_p95"], 0.01469225)
        self.assertAlmostEqual(row["handler_p95"], 0.04621212121212121)
        self.assertAlmostEqual(plotted, row["handler_p95"])
        self.assertNotAlmostEqual(plotted, row["attempt_lifecycle_p95"])

        with tempfile.TemporaryDirectory() as temporary:
            regenerated_results = Path(temporary) / "results.json"
            regenerated_results.write_text(results_path.read_text())
            generate(regenerated_results, None)
            retained_plots = results_path.parent / "plots"
            regenerated_plots = regenerated_results.parent / "plots"
            for index in (0, 1, 2, 4):
                name = PLOT_NAMES[index]
                self.assertEqual(
                    (regenerated_plots / name).read_bytes(),
                    (retained_plots / name).read_bytes(),
                    name,
                )
            new_plot = (regenerated_plots / PLOT_NAMES[3]).read_bytes()
            self.assertIn("CPU Handler-Execution p95 vs Workers", new_plot.decode())
            self.assertIn("Prometheus handler execution", new_plot.decode())
            self.assertNotIn("CPU Attempt-Execution p95 vs Workers", new_plot.decode())

    def test_plot_generator_writes_only_five_deterministic_e3_plots(self) -> None:
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
            self.assertEqual(len(PLOT_NAMES), 5)

    def test_e1_and_e2_comparisons_read_external_trusted_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            e1_path = root / "e1" / "results.json"
            e2_path = root / "e2" / "results.json"
            e1_path.parent.mkdir()
            e2_path.parent.mkdir()
            e1_path.write_text(json.dumps(external_document("TF-012E1", "noop_scaling")))
            e2_path.write_text(json.dumps(external_document("TF-012E2", "io50_scaling")))
            with mock.patch(
                "benchmarks.scoped.evaluate_run_directory",
                return_value={"overall": {"result": "PASS"}},
            ):
                e1 = load_trusted_e1(e1_path)
                e2 = load_trusted_e2(e2_path)
            self.assertEqual(e1["run_id"], "external-tf-012e1")
            self.assertEqual(e2["run_id"], "external-tf-012e2")
            self.assertAlmostEqual(e1["speedup"]["16"], 114 / 9)
            self.assertEqual(len(e1["results_sha256"]), 64)
            self.assertEqual(len(e2["results_sha256"]), 64)

    def test_untrusted_or_invalid_comparison_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "results.json"
            path.write_text(json.dumps(external_document("TF-012E1", "noop_scaling")))
            with mock.patch(
                "benchmarks.scoped.evaluate_run_directory",
                return_value={"overall": {"result": "FAIL"}},
            ):
                with self.assertRaises(BenchmarkError):
                    load_trusted_e1(path)
            with mock.patch(
                "benchmarks.scoped.evaluate_run_directory",
                return_value={"overall": {"result": "PASS"}},
            ):
                with self.assertRaises(BenchmarkError):
                    load_trusted_e2(path)

    def test_dry_run_prints_contract_without_execution(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "benchmarks.e3", "--dry-run"],
            check=True,
            capture_output=True,
            text=True,
        )
        contract = json.loads(result.stdout)
        self.assertEqual(contract["workers"], EXPECTED_WORKERS)
        self.assertEqual(contract["payload"], {"iterations": CPU_ITERATIONS})
        self.assertEqual(contract["measured_trials"], 12)
        self.assertEqual(contract["scenarios"], ["cpu_scaling"])
        self.assertFalse(contract["will_execute"])


if __name__ == "__main__":
    unittest.main()
