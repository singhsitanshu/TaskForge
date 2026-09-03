from __future__ import annotations

import io
import unittest
import urllib.error
from dataclasses import replace
from pathlib import Path
from unittest import mock

from scripts.demo.client import APIClient, ApiError, DemoError, wait_for
from scripts.demo.config import parse_go_duration, resolve_config
from scripts.demo.runtime import Probe, command_probe, require_safe_local_docker
from scripts.demo.scenarios import (
    Container,
    attempt_duration,
    demo_dataset,
    select_owner_container,
    validate_normal,
    validate_recovery,
    validate_retry,
)


def task(status: str) -> dict[str, object]:
    return {"id": "task-1", "status": status}


def attempt(
    number: int, status: str, identifier: str | None = None
) -> dict[str, object]:
    return {
        "id": identifier or f"attempt-{number}",
        "attempt_number": number,
        "status": status,
    }


class ConfigTests(unittest.TestCase):
    def test_url_resolution_uses_environment_ports_and_overrides(self) -> None:
        environment = {
            "API_PORT": "18000",
            "WEB_PORT": "13000",
            "PROMETHEUS_PORT": "19090",
            "GRAFANA_PORT": "13001",
            "DEMO_GRAFANA_URL": "http://localhost:7777/",
            "WORKER_TASK_LEASE_DURATION": "1m30s",
            "SCHEDULER_RECOVERY_INTERVAL": "250ms",
        }
        config = resolve_config(Path("/tmp/project"), environment)
        self.assertEqual(config.api_url, "http://localhost:18000")
        self.assertEqual(config.web_url, "http://localhost:13000")
        self.assertEqual(config.grafana_url, "http://localhost:7777")
        self.assertEqual(config.lease_seconds, 90)
        self.assertEqual(config.recovery_interval_seconds, 0.25)

    def test_go_duration_rejects_unknown_syntax(self) -> None:
        with self.assertRaises(ValueError):
            parse_go_duration("30 seconds")


class StatusTests(unittest.TestCase):
    @mock.patch("scripts.demo.runtime.run_command")
    def test_service_status_parser_requires_expected_output(
        self, run: mock.Mock
    ) -> None:
        run.return_value.returncode = 0
        run.return_value.stdout = "PONG"
        run.return_value.stderr = ""
        config = resolve_config(Path("/tmp/project"), {})
        self.assertEqual(
            command_probe(config, "Redis", ["probe"], "PONG"), Probe("Redis", "READY")
        )
        run.return_value.stdout = "NOT PONG"
        self.assertEqual(
            command_probe(config, "Redis", ["probe"], "PONG").state, "NOT READY"
        )


class HistoryTests(unittest.TestCase):
    def test_normal_history(self) -> None:
        validate_normal(task("SUCCEEDED"), [attempt(1, "SUCCEEDED")])

    def test_retry_history(self) -> None:
        validate_retry(
            task("SUCCEEDED"),
            [attempt(1, "FAILED"), attempt(2, "SUCCEEDED")],
        )

    def test_unexpected_retry_history_is_rejected(self) -> None:
        with self.assertRaisesRegex(DemoError, "expected FAILED -> SUCCEEDED"):
            validate_retry(
                task("SUCCEEDED"),
                [
                    attempt(1, "FAILED"),
                    attempt(2, "FAILED"),
                    attempt(3, "SUCCEEDED"),
                ],
            )

    def test_recovery_history_requires_captured_abandonment_and_later_success(
        self,
    ) -> None:
        validate_recovery(
            task("SUCCEEDED"),
            [attempt(1, "ABANDONED", "captured"), attempt(2, "SUCCEEDED")],
            "captured",
        )
        with self.assertRaisesRegex(DemoError, "not ABANDONED"):
            validate_recovery(
                task("SUCCEEDED"),
                [attempt(1, "FAILED", "captured"), attempt(2, "SUCCEEDED")],
                "captured",
            )

    def test_attempt_duration_uses_persisted_timestamps(self) -> None:
        self.assertEqual(
            attempt_duration(
                {
                    "started_at": "2026-09-03T10:00:00Z",
                    "finished_at": "2026-09-03T10:00:00.500000Z",
                }
            ),
            "0.50s",
        )


class PollingTests(unittest.TestCase):
    def test_timeout_is_bounded_and_reports_last_value(self) -> None:
        clock = iter((0.0, 1.0, 2.0)).__next__
        with self.assertRaisesRegex(DemoError, "last value=pending"):
            wait_for(
                lambda: "pending",
                lambda _: False,
                description="test state",
                timeout=1,
                interval=0,
                clock=clock,
                sleep=lambda _: None,
            )

    def test_recovery_polling_timeout_reports_compact_attempt_history(self) -> None:
        clock = iter((0.0, 1.0)).__next__
        with self.assertRaisesRegex(DemoError, "attempt_number.*RUNNING"):
            wait_for(
                lambda: [
                    {
                        "attempt_number": 1,
                        "status": "RUNNING",
                        "worker_id": "worker-1",
                        "output": {"large": "omitted"},
                    }
                ],
                lambda _: False,
                description="lease recovery",
                timeout=1,
                interval=0,
                clock=clock,
                sleep=lambda _: None,
            )


class ApiTests(unittest.TestCase):
    @mock.patch("urllib.request.urlopen")
    def test_api_error_includes_operation_endpoint_status_and_message(
        self, urlopen: mock.Mock
    ) -> None:
        urlopen.side_effect = urllib.error.HTTPError(
            "http://localhost:8000/tasks",
            422,
            "unprocessable",
            {},
            io.BytesIO(b'{"detail":{"message":"bad payload"}}'),
        )
        with self.assertRaises(ApiError) as raised:
            APIClient("http://localhost:8000").submit({}, "key")
        message = str(raised.exception)
        self.assertIn("submit task", message)
        self.assertIn("HTTP 422", message)
        self.assertIn("bad payload", message)


class DatasetTests(unittest.TestCase):
    def test_dataset_is_bounded_deterministic_and_representative(self) -> None:
        first = demo_dataset()
        self.assertEqual(first, demo_dataset())
        self.assertEqual(len(first), 16)
        self.assertTrue(10 <= len(first) <= 30)
        self.assertEqual(sum(item["expected"] == "FAILED" for item in first), 1)
        self.assertEqual(
            sum(item["task_type"] == "test.fail_n_then_succeed" for item in first), 2
        )
        self.assertEqual({item["priority"] for item in first}, {0, 10, 25, 50, 100})


class RecoverySafetyTests(unittest.TestCase):
    def test_owner_selection_uses_worker_hostname_identity(self) -> None:
        containers = [
            Container("id-a", "host-a", "taskforge-worker-1", "unless-stopped"),
            Container("id-b", "host-b", "taskforge-worker-2", "unless-stopped"),
        ]
        selected = select_owner_container(
            {"name": "host-b", "instance_id": "host-b-random"}, containers
        )
        self.assertEqual(selected.container_id, "id-b")

    def test_ambiguous_or_missing_owner_is_rejected(self) -> None:
        with self.assertRaisesRegex(DemoError, "exactly one"):
            select_owner_container(
                {"name": "unknown", "instance_id": "unknown-random"},
                [Container("id-a", "host-a", "worker", "unless-stopped")],
            )

    def test_nonlocal_recovery_is_refused_before_docker_access(self) -> None:
        config = resolve_config(Path("/tmp/project"), {})
        config = replace(config, api_url="https://taskforge.example.com")
        with mock.patch("scripts.demo.runtime.run_command") as run:
            with self.assertRaisesRegex(DemoError, "not local"):
                require_safe_local_docker(config)
            run.assert_not_called()


class DocumentationContractTests(unittest.TestCase):
    def test_required_files_and_make_targets_exist(self) -> None:
        root = Path(__file__).resolve().parents[3]
        makefile = (root / "Makefile").read_text(encoding="utf-8")
        for target in (
            "demo",
            "demo-data",
            "demo-recovery",
            "demo-status",
            "demo-reset",
        ):
            self.assertIn(f"{target}:", makefile)
        self.assertTrue((root / ".env.example").is_file())
        self.assertTrue((root / "scripts" / "demo" / "cli.py").is_file())
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        for variable in (
            "API_PORT",
            "WEB_PORT",
            "GRAFANA_PORT",
            "PROMETHEUS_PORT",
        ):
            self.assertIn(variable, compose)
            self.assertIn(variable, (root / ".env.example").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
