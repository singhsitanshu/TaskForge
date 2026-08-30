#!/usr/bin/env python3
"""TaskForge TF-012 PostgreSQL-backed benchmark orchestrator.

The load generator is compiled Go. This script owns only safe environment
isolation, scenario orchestration, raw SQL/Prometheus/resource evidence, and
machine-readable result assembly.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import pathlib
import platform
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "benchmarks"
DEFAULT_PROJECT = "taskforge-tf012-benchmark"
TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED")


class BenchmarkError(RuntimeError):
    pass


def run_command(
    arguments: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        arguments,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise BenchmarkError(
            f"command failed ({result.returncode}): {' '.join(arguments)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def host_cpu_description() -> str:
    if sys.platform == "darwin":
        result = run_command(
            ["sysctl", "-n", "machdep.cpu.brand_string"], check=False, timeout=30
        )
        if result.stdout.strip():
            return result.stdout.strip()
    cpuinfo = pathlib.Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(errors="replace").splitlines():
            if line.lower().startswith(("model name", "hardware")) and ":" in line:
                return line.split(":", 1)[1].strip()
    return platform.processor().strip() or platform.machine()


def host_memory_bytes() -> int | None:
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)]


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class ResourceSampler:
    def __init__(self, harness: Harness, interval: float = 0.5) -> None:
        self.harness = harness
        self.interval = interval
        self.samples: list[dict[str, Any]] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self) -> ResourceSampler:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop_event.set()
        self.thread.join(timeout=3)

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            result = run_command(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{json .}}",
                ],
                check=False,
                timeout=10,
            )
            rows: list[dict[str, Any]] = []
            for line in result.stdout.splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = item.get("Name", "")
                if name.startswith(self.harness.project + "-"):
                    rows.append(item)
            self.samples.append(
                {"timestamp": dt.datetime.now(dt.UTC).isoformat(), "containers": rows}
            )
            self.stop_event.wait(self.interval)


class Harness:
    def __init__(self, profile: dict[str, Any], project: str, keep: bool) -> None:
        if not re.fullmatch(r"taskforge-tf012-[a-z0-9-]+", project):
            raise BenchmarkError(
                "benchmark project must match taskforge-tf012-[a-z0-9-]+; "
                "refusing an unsafe Compose target"
            )
        self.profile = profile
        self.project = project
        self.keep = keep
        self.image = f"{project}-loadgen"
        self.env = os.environ.copy()
        self.env.update(
            {
                "COMPOSE_PROJECT_NAME": project,
                "POSTGRES_PORT": "15432",
                "REDIS_PORT": "16379",
                "API_PORT": "18000",
                "WEB_PORT": "13000",
                "PROMETHEUS_PORT": "19090",
                "GRAFANA_PORT": "13001",
                "POLL_INTERVAL": "10ms",
                "WORKER_HEARTBEAT_INTERVAL": "1s",
                "WORKER_STALE_AFTER": "5s",
                "WORKER_DEAD_AFTER": "10s",
                "WORKER_TASK_LEASE_DURATION": "5s",
                "WORKER_TASK_LEASE_RENEW_INTERVAL": "1s",
                "WORKER_TASK_LEASE_RENEW_TIMEOUT": "500ms",
                "TASK_RETRY_BASE_DELAY": "100ms",
                "TASK_RETRY_MAX_DELAY": "100ms",
                "TASK_RETRY_JITTER": "0",
                "SCHEDULER_RECOVERY_INTERVAL": "100ms",
                "SCHEDULER_RECOVERY_BATCH_SIZE": "1000",
                "SCHEDULER_RETRY_PROMOTION_INTERVAL": "100ms",
                "SCHEDULER_RETRY_PROMOTION_BATCH_SIZE": "1000",
                "SCHEDULER_METRICS_INTERVAL": "1s",
            }
        )
        self.api_url = "http://127.0.0.1:18000"
        self.prometheus_url = "http://127.0.0.1:19090"
        self.current_workers = 0
        self.current_schedulers = 0

    def compose(self, *arguments: str, timeout: float | None = None) -> str:
        return run_command(
            ["docker", "compose", "-p", self.project, *arguments],
            env=self.env,
            timeout=timeout,
        ).stdout

    def build(self) -> None:
        self.compose("build", "api", "worker", "scheduler", timeout=900)
        run_command(
            [
                "docker",
                "build",
                "-t",
                self.image,
                str(BENCHMARKS / "loadgen"),
            ],
            timeout=600,
        )

    def reset(self) -> None:
        if not self.project.startswith("taskforge-tf012-"):
            raise BenchmarkError("unsafe project name during reset")
        self.compose("down", "--volumes", "--remove-orphans", timeout=180)

    def start(self) -> None:
        self.compose("up", "-d", "postgres", timeout=180)
        run_command([str(ROOT / "scripts" / "migrate.sh"), "up"], env=self.env, timeout=180)
        self.compose(
            "up",
            "-d",
            "--scale",
            "worker=1",
            "--scale",
            "scheduler=1",
            "api",
            "worker",
            "scheduler",
            "prometheus",
            timeout=240,
        )
        self.current_workers = 1
        self.current_schedulers = 1
        self.wait_http(self.api_url + "/readyz", timeout=90)
        self.wait_http(self.prometheus_url + "/-/ready", timeout=90)

    def close(self) -> None:
        if not self.keep:
            self.reset()

    def reset_prometheus(self) -> None:
        if not self.project.startswith("taskforge-tf012-"):
            raise BenchmarkError("unsafe project name during Prometheus reset")
        self.compose("stop", "prometheus", timeout=60)
        self.compose("rm", "-f", "prometheus", timeout=60)
        run_command(
            ["docker", "volume", "rm", f"{self.project}_prometheus-data"],
            timeout=60,
        )
        self.compose("up", "-d", "--no-deps", "prometheus", timeout=120)
        self.wait_http(self.prometheus_url + "/-/ready", timeout=60)
        time.sleep(6)

    def wait_http(self, url: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if 200 <= response.status < 300:
                        return
            except Exception as exc:  # noqa: BLE001 - retained for final diagnostics
                last_error = str(exc)
            time.sleep(0.25)
        raise BenchmarkError(f"timed out waiting for {url}: {last_error}")

    def scale(self, workers: int | None = None, schedulers: int | None = None) -> None:
        if workers is not None and workers != self.current_workers:
            self.compose(
                "up", "-d", "--no-deps", "--scale", f"worker={workers}", "worker", timeout=180
            )
            self.current_workers = workers
        if schedulers is not None and schedulers != self.current_schedulers:
            self.compose(
                "up",
                "-d",
                "--no-deps",
                "--scale",
                f"scheduler={schedulers}",
                "scheduler",
                timeout=180,
            )
            self.current_schedulers = schedulers
        time.sleep(0.5)

    def recreate_workers(self, workers: int) -> None:
        self.compose(
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "--scale",
            f"worker={workers}",
            "worker",
            timeout=240,
        )
        self.current_workers = workers
        time.sleep(1)

    def recreate_schedulers(self, schedulers: int) -> None:
        self.compose(
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "--scale",
            f"scheduler={schedulers}",
            "scheduler",
            timeout=240,
        )
        self.current_schedulers = schedulers
        time.sleep(1)

    def trial_configuration(self) -> dict[str, str]:
        return {
            key: self.env[key]
            for key in sorted(self.env)
            if key.startswith(("POLL_", "WORKER_", "TASK_RETRY_", "SCHEDULER_"))
        }

    def clear_tasks(self) -> None:
        self.psql("TRUNCATE task_attempts, tasks CASCADE")

    def psql(self, query: str) -> str:
        return self.compose(
            "exec",
            "-T",
            "postgres",
            "psql",
            "--set",
            "ON_ERROR_STOP=1",
            "--tuples-only",
            "--no-align",
            "--username",
            self.env.get("POSTGRES_USER", "taskforge"),
            "--dbname",
            self.env.get("POSTGRES_DB", "taskforge"),
            "--command",
            query,
            timeout=120,
        ).strip()

    def json_sql(self, query: str) -> dict[str, Any]:
        output = self.psql(query)
        if not output:
            return {}
        return json.loads(output.splitlines()[-1])

    def loadgen(self, **options: Any) -> dict[str, Any]:
        command = [
            "docker",
            "run",
            "--rm",
            "--network",
            f"{self.project}_default",
            self.image,
            "-url",
            "http://api:8000",
        ]
        for name, value in options.items():
            if value is None:
                continue
            command.extend(["-" + name.replace("_", "-"), str(value)])
        result = run_command(command, timeout=max(180, int(options.get("count", 1)) * 2))
        return json.loads(result.stdout)

    def wait_for_tasks(self, queue: str, expected: int, timeout: float = 180) -> dict[str, int]:
        deadline = time.monotonic() + timeout
        counts: dict[str, int] = {}
        while time.monotonic() < deadline:
            rows = self.psql(
                "SELECT status::text, count(*) FROM tasks WHERE queue = "
                + sql_literal(queue)
                + " GROUP BY status ORDER BY status"
            )
            counts = {}
            for line in rows.splitlines():
                if "|" in line:
                    status, count = line.split("|", 1)
                    counts[status] = int(count)
            if sum(counts.get(status, 0) for status in TERMINAL) >= expected:
                return counts
            time.sleep(0.05)
        raise BenchmarkError(
            f"tasks did not reach terminal state: queue={queue} expected={expected} counts={counts}"
        )

    def wait_for_status(self, status: str, expected: int, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            count = int(
                self.psql(f"SELECT count(*) FROM tasks WHERE status = {sql_literal(status)}") or 0
            )
            if count >= expected:
                return
            time.sleep(0.05)
        raise BenchmarkError(f"expected {expected} {status} tasks")

    def prometheus(self, queries: dict[str, str]) -> dict[str, Any]:
        captured: dict[str, Any] = {}
        for name, query in queries.items():
            url = self.prometheus_url + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    captured[name] = json.load(response).get("data", {}).get("result", [])
            except Exception as exc:  # noqa: BLE001
                captured[name] = {"error": str(exc)}
        return captured

    def prometheus_window(self, duration_seconds: float) -> dict[str, Any]:
        window = max(10, math.ceil(duration_seconds) + 5)
        suffix = f"[{window}s]"
        queries = {
            "completed_increase": (
                f"sum(increase(taskforge_worker_tasks_completed_total{suffix}))"
            ),
            "claimed_increase": f"sum(increase(taskforge_worker_tasks_claimed_total{suffix}))",
        }
        for quantile in (0.50, 0.95, 0.99):
            label = str(int(quantile * 100))
            for name, metric in (
                ("queue", "taskforge_task_queue_wait_seconds_bucket"),
                ("claim", "taskforge_worker_claim_duration_seconds_bucket"),
                ("execution", "taskforge_task_execution_duration_seconds_bucket"),
                ("recovery", "taskforge_recovery_lag_seconds_bucket"),
                ("retry_lateness", "taskforge_retry_lateness_seconds_bucket"),
            ):
                queries[f"{name}_p{label}"] = (
                    f"histogram_quantile({quantile}, sum by (le) (increase({metric}{suffix})))"
                )
        captured = self.prometheus(queries)
        captured["window_seconds"] = window
        return captured

    def environment(self) -> dict[str, Any]:
        commands = {
            "git_sha": ["git", "rev-parse", "HEAD"],
            "git_status": ["git", "status", "--short"],
            "docker_version": ["docker", "version", "--format", "{{json .}}"],
            "docker_info": ["docker", "info", "--format", "{{json .}}"],
            "go_version": ["docker", "run", "--rm", "golang:1.23-alpine", "go", "version"],
        }
        values: dict[str, Any] = {}
        for name, command in commands.items():
            result = run_command(command, check=False, timeout=30)
            values[name] = result.stdout.strip() or result.stderr.strip()
        values.update(
            {
                "captured_at": dt.datetime.now(dt.UTC).isoformat(),
                "platform": platform.platform(),
                "os": {"system": platform.system(), "release": platform.release()},
                "host_cpu": host_cpu_description(),
                "host_logical_cpus": os.cpu_count(),
                "host_memory_bytes": host_memory_bytes(),
                "python": platform.python_version(),
                "python_version": platform.python_version(),
                "compose_project": self.project,
                "api_replicas": 1,
                "profile": self.profile,
                "configuration": {
                    key: self.env[key]
                    for key in sorted(self.env)
                    if key.startswith(("POLL_", "WORKER_", "TASK_RETRY_", "SCHEDULER_"))
                },
                "postgresql": self.psql(
                    "SELECT json_build_object('version', version(), "
                    "'max_connections', current_setting('max_connections'), "
                    "'shared_buffers', current_setting('shared_buffers'), "
                    "'effective_cache_size', current_setting('effective_cache_size'))"
                ),
            }
        )
        return values


PROMETHEUS_QUERIES = {
    "claimed": "sum(taskforge_worker_tasks_claimed_total)",
    "completed": "sum(taskforge_worker_tasks_completed_total)",
    "retries_scheduled": "sum(taskforge_task_retries_scheduled_total)",
    "recoveries": "sum(taskforge_task_recoveries_total)",
    "retry_promotions": "sum(taskforge_task_retries_promoted_total)",
    "eligible": "sum(taskforge_tasks_eligible_for_claim)",
    "expired_leases": "sum(taskforge_expired_running_leases)",
    "queue_p95": (
        "histogram_quantile(0.95, sum by (le) (taskforge_task_queue_wait_seconds_bucket))"
    ),
    "claim_p95": (
        "histogram_quantile(0.95, sum by (le) (taskforge_worker_claim_duration_seconds_bucket))"
    ),
    "execution_p95": (
        "histogram_quantile(0.95, sum by (le) (taskforge_task_execution_duration_seconds_bucket))"
    ),
    "recovery_lag_p95": (
        "histogram_quantile(0.95, sum by (le) (taskforge_recovery_lag_seconds_bucket))"
    ),
    "retry_lateness_p95": (
        "histogram_quantile(0.95, sum by (le) (taskforge_retry_lateness_seconds_bucket))"
    ),
}


def raw_measurements(harness: Harness, queue: str) -> dict[str, Any]:
    queue_sql = sql_literal(queue)
    return harness.json_sql(
        f"""
        WITH selected_tasks AS (
            SELECT * FROM tasks WHERE queue = {queue_sql}
        ), selected_attempts AS (
            SELECT attempt.* FROM task_attempts AS attempt
            JOIN selected_tasks AS task ON task.id = attempt.task_id
        ), durations AS (
            SELECT
                EXTRACT(EPOCH FROM (attempt.started_at - attempt.queue_entered_at)) AS queue_wait,
                EXTRACT(EPOCH FROM (attempt.finished_at - attempt.started_at)) AS execution,
                EXTRACT(EPOCH FROM (task.completed_at - task.created_at)) AS total_latency,
                EXTRACT(EPOCH FROM (attempt.started_at - attempt.leased_at)) AS claim_to_start
            FROM selected_tasks AS task
            JOIN selected_attempts AS attempt ON attempt.task_id = task.id
            WHERE attempt.started_at IS NOT NULL
        ), timeline AS (
            SELECT min(created_at) AS submitted_first, max(created_at) AS submitted_last,
                   min(completed_at) AS completed_first, max(completed_at) AS completed_last
            FROM selected_tasks
        ), attempt_timeline AS (
            SELECT min(started_at) AS processing_first, max(finished_at) AS processing_last
            FROM selected_attempts
        ), completion_bounds AS (
            SELECT percentile_disc(0.10) WITHIN GROUP (ORDER BY completed_at) AS lower_bound,
                   percentile_disc(0.90) WITHIN GROUP (ORDER BY completed_at) AS upper_bound
            FROM selected_tasks WHERE completed_at IS NOT NULL
        ), retry_lateness AS (
            SELECT EXTRACT(EPOCH FROM (attempt.leased_at - task.scheduled_at)) AS seconds
            FROM selected_attempts AS attempt
            JOIN selected_tasks AS task ON task.id = attempt.task_id
            WHERE attempt.attempt_number > 1
        )
        SELECT json_build_object(
            'task_count', (SELECT count(*) FROM selected_tasks),
            'attempt_count', (SELECT count(*) FROM selected_attempts),
            'status_counts', (SELECT coalesce(json_object_agg(status, count), '{{}}'::json) FROM
                (SELECT status::text AS status, count(*) FROM selected_tasks GROUP BY status) AS statuses),
            'attempt_status_counts', (SELECT coalesce(json_object_agg(status, count), '{{}}'::json) FROM
                (SELECT status::text AS status, count(*) FROM selected_attempts GROUP BY status) AS statuses),
            'queue_p50_seconds', (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY queue_wait) FROM durations),
            'queue_p95_seconds', (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY queue_wait) FROM durations),
            'queue_p99_seconds', (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY queue_wait) FROM durations),
            'execution_p50_seconds', (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY execution) FROM durations),
            'execution_p95_seconds', (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY execution) FROM durations),
            'execution_p99_seconds', (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY execution) FROM durations),
            'total_p50_seconds', (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY total_latency) FROM durations),
            'total_p95_seconds', (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY total_latency) FROM durations),
            'total_p99_seconds', (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY total_latency) FROM durations),
            'processing_seconds', (SELECT EXTRACT(EPOCH FROM (processing_last - processing_first)) FROM attempt_timeline),
            'end_to_end_seconds', (SELECT EXTRACT(EPOCH FROM (completed_last - submitted_first)) FROM timeline),
            'submission_span_seconds', (SELECT EXTRACT(EPOCH FROM (submitted_last - submitted_first)) FROM timeline),
            'steady_state_completions', (SELECT count(*) FROM selected_tasks, completion_bounds
                WHERE completed_at BETWEEN lower_bound AND upper_bound),
            'steady_state_seconds', (SELECT EXTRACT(EPOCH FROM (upper_bound - lower_bound)) FROM completion_bounds)
            ,'retry_lateness_p50_seconds', (SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY seconds) FROM retry_lateness)
            ,'retry_lateness_p95_seconds', (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds) FROM retry_lateness)
            ,'retry_lateness_p99_seconds', (SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY seconds) FROM retry_lateness)
        )
        """
    )


def correctness(
    harness: Harness, queue: str, expected_tasks: int, expected_attempts: int | None
) -> dict[str, Any]:
    global_checks = harness.psql((BENCHMARKS / "sql" / "validate.sql").read_text()).splitlines()[-1]
    scoped = harness.json_sql(
        f"""
        WITH selected AS (SELECT id, status, attempt_count FROM tasks WHERE queue = {sql_literal(queue)})
        SELECT json_build_object(
            'expected_tasks', {expected_tasks},
            'actual_tasks', (SELECT count(*) FROM selected),
            'expected_attempts', {"null" if expected_attempts is None else expected_attempts},
            'actual_attempts', (SELECT count(*) FROM task_attempts WHERE task_id IN (SELECT id FROM selected)),
            'nonterminal_tasks', (SELECT count(*) FROM selected WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELLED')),
            'succeeded_tasks', (SELECT count(*) FROM selected WHERE status = 'SUCCEEDED')
        )
        """
    )
    checks = json.loads(global_checks)
    checks.update(scoped)
    checks["passed"] = (
        all(
            checks[name] == 0
            for name in (
                "duplicate_attempts",
                "invalid_terminal_tasks",
                "invalid_active_tasks",
                "invalid_attempt_counts",
                "invalid_running_attempts",
                "nonterminal_tasks",
            )
        )
        and checks["actual_tasks"] == expected_tasks
        and checks["succeeded_tasks"] == expected_tasks
        and (expected_attempts is None or checks["actual_attempts"] == expected_attempts)
    )
    return checks


def summarize_resources(samples: list[dict[str, Any]]) -> dict[str, Any]:
    by_service: dict[str, dict[str, list[float]]] = {}
    for sample in samples:
        sample_totals: dict[str, dict[str, float]] = {}
        for container in sample["containers"]:
            name = container.get("Name", "")
            parts = name.rsplit("-", 2)
            service = parts[-2] if len(parts) >= 3 else name
            cpu_text = container.get("CPUPerc", "0").rstrip("%")
            try:
                cpu = float(cpu_text)
            except ValueError:
                cpu = 0.0
            memory_text = container.get("MemUsage", "0").split("/")[0].strip()
            memory = parse_bytes(memory_text)
            network_rx, network_tx = parse_io_pair(container.get("NetIO", "0B / 0B"))
            block_read, block_write = parse_io_pair(container.get("BlockIO", "0B / 0B"))
            totals = sample_totals.setdefault(
                service,
                {
                    "cpu_percent": 0,
                    "memory_bytes": 0,
                    "network_rx_bytes": 0,
                    "network_tx_bytes": 0,
                    "block_read_bytes": 0,
                    "block_write_bytes": 0,
                },
            )
            totals["cpu_percent"] += cpu
            totals["memory_bytes"] += memory
            totals["network_rx_bytes"] += network_rx
            totals["network_tx_bytes"] += network_tx
            totals["block_read_bytes"] += block_read
            totals["block_write_bytes"] += block_write
        for service, totals in sample_totals.items():
            values = by_service.setdefault(service, {key: [] for key in totals})
            for key, measured in totals.items():
                values[key].append(measured)
    return {
        service: {
            "cpu_percent_max": max(values["cpu_percent"], default=0),
            "cpu_percent_mean": statistics.fmean(values["cpu_percent"])
            if values["cpu_percent"]
            else 0,
            "memory_bytes_max": max(values["memory_bytes"], default=0),
            "network_rx_bytes_max": max(values["network_rx_bytes"], default=0),
            "network_tx_bytes_max": max(values["network_tx_bytes"], default=0),
            "block_read_bytes_max": max(values["block_read_bytes"], default=0),
            "block_write_bytes_max": max(values["block_write_bytes"], default=0),
        }
        for service, values in by_service.items()
    }


def parse_bytes(value: str) -> float:
    match = re.fullmatch(r"([0-9.]+)([KMGTP]?i?B)", value)
    if not match:
        return 0
    number = float(match.group(1))
    units = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "KiB": 1024,
        "MB": 1000**2,
        "MiB": 1024**2,
        "GB": 1000**3,
        "GiB": 1024**3,
    }
    return number * units.get(match.group(2), 1)


def parse_io_pair(value: str) -> tuple[float, float]:
    parts = [part.strip() for part in value.split("/")]
    if len(parts) != 2:
        return 0, 0
    return parse_bytes(parts[0]), parse_bytes(parts[1])


def enrich_raw(raw: dict[str, Any], count: int) -> dict[str, Any]:
    processing_seconds = raw.get("processing_seconds") or 0
    end_to_end_seconds = raw.get("end_to_end_seconds") or 0
    steady_seconds = raw.get("steady_state_seconds") or 0
    raw["processing_throughput_per_second"] = (
        count / processing_seconds if processing_seconds > 0 else None
    )
    raw["end_to_end_throughput_per_second"] = (
        count / end_to_end_seconds if end_to_end_seconds > 0 else None
    )
    raw["steady_state_throughput_per_second"] = (
        raw.get("steady_state_completions", 0) / steady_seconds if steady_seconds > 0 else None
    )
    return raw


def measured_trial(
    harness: Harness,
    *,
    scenario: str,
    variant: str,
    trial: int,
    workers: int,
    schedulers: int = 1,
    task_type: str = "test.noop",
    payload: str = "{}",
    count: int,
    concurrency: int = 100,
    rate: float = 0,
    max_attempts: int = 1,
    expected_attempts: int | None = None,
    timeout: float = 300,
) -> dict[str, Any]:
    queue = f"tf012-{scenario}-{variant}-t{trial}"[:128]
    harness.clear_tasks()
    harness.scale(workers=workers, schedulers=schedulers)
    before = harness.prometheus(PROMETHEUS_QUERIES)
    measured_started = time.monotonic()
    with ResourceSampler(harness) as resources:
        submitted = harness.loadgen(
            operation="submit",
            count=count,
            concurrency=min(concurrency, count),
            rate=rate,
            task_type=task_type,
            payload=payload,
            queue=queue,
            max_attempts=max_attempts,
            key_mode="unique",
            key_prefix=queue,
        )
        counts = harness.wait_for_tasks(queue, count, timeout=timeout)
    measured_seconds = time.monotonic() - measured_started
    raw = raw_measurements(harness, queue)
    enrich_raw(raw, count)
    expected_attempts = count if expected_attempts is None else expected_attempts
    return {
        "scenario": scenario,
        "variant": variant,
        "trial": trial,
        "workers": workers,
        "schedulers": schedulers,
        "task_type": task_type,
        "payload": json.loads(payload),
        "count": count,
        "arrival_rate": rate,
        "status_counts": counts,
        "submission": submitted,
        "raw": raw,
        "prometheus_before": before,
        "prometheus_after": harness.prometheus(PROMETHEUS_QUERIES),
        "prometheus_window": harness.prometheus_window(measured_seconds),
        "resources": summarize_resources(resources.samples),
        "resource_samples": resources.samples,
        "configuration": harness.trial_configuration(),
        "correctness": correctness(harness, queue, count, expected_attempts),
    }


def run_scaling(harness: Harness, results: list[dict[str, Any]]) -> None:
    profile = harness.profile
    trials = int(profile["trials"])
    workloads = [
        ("noop_scaling", "test.noop", "{}", profile["noop_tasks"], profile["noop_workers"]),
        (
            "database_contention",
            "test.noop",
            "{}",
            profile["noop_tasks"],
            profile["contention_workers"],
        ),
        (
            "io50_scaling",
            "test.sleep",
            '{"duration_ms":50}',
            profile["io_tasks"],
            profile["io_workers"],
        ),
        (
            "cpu_scaling",
            "test.cpu",
            json.dumps({"iterations": profile["cpu_iterations"]}),
            profile["cpu_tasks"],
            profile["cpu_workers"],
        ),
    ]
    for scenario, task_type, payload, count, worker_counts in workloads:
        for workers in worker_counts:
            for trial in range(1, trials + 1):
                print(f"[{scenario}] workers={workers} trial={trial}", flush=True)
                results.append(
                    measured_trial(
                        harness,
                        scenario=scenario,
                        variant=f"w{workers}",
                        trial=trial,
                        workers=workers,
                        task_type=task_type,
                        payload=payload,
                        count=count,
                        concurrency=min(200, count),
                        timeout=max(300, count * 0.2),
                    )
                )
    for duration in (10, 100):
        for trial in range(1, trials + 1):
            print(f"[fixed wait] duration={duration}ms trial={trial}", flush=True)
            results.append(
                measured_trial(
                    harness,
                    scenario="fixed_wait",
                    variant=f"{duration}ms",
                    trial=trial,
                    workers=20,
                    task_type="test.sleep",
                    payload=json.dumps({"duration_ms": duration}),
                    count=profile["io_tasks"],
                    concurrency=min(200, profile["io_tasks"]),
                    timeout=300,
                )
            )


def run_api(harness: Harness, results: list[dict[str, Any]]) -> None:
    profile = harness.profile
    harness.scale(workers=0, schedulers=1)
    for concurrency in profile["api_concurrency"]:
        harness.clear_tasks()
        queue = f"tf012-api-c{concurrency}"
        print(f"[api] concurrency={concurrency}", flush=True)
        with ResourceSampler(harness) as resources:
            submission = harness.loadgen(
                operation="submit",
                count=profile["api_requests"],
                concurrency=concurrency,
                task_type="test.noop",
                payload="{}",
                queue=queue,
                max_attempts=1,
                key_mode="unique",
                key_prefix=queue,
            )
        results.append(
            {
                "scenario": "api_throughput",
                "variant": f"c{concurrency}",
                "trial": 1,
                "workers": 0,
                "schedulers": 1,
                "count": profile["api_requests"],
                "submission": submission,
                "resources": summarize_resources(resources.samples),
                "correctness": {
                    "actual_tasks": int(
                        harness.psql(f"SELECT count(*) FROM tasks WHERE queue={sql_literal(queue)}")
                    ),
                    "expected_tasks": profile["api_requests"],
                    "passed": submission["successes"] == profile["api_requests"],
                },
            }
        )

    for key_mode in ("same", "unique"):
        harness.clear_tasks()
        count = int(profile["contention_requests"])
        queue = f"tf012-idempotency-{key_mode}"
        print(f"[idempotency] mode={key_mode} requests={count}", flush=True)
        submission = harness.loadgen(
            operation="submit",
            count=count,
            concurrency=min(200, count),
            task_type="test.noop",
            payload="{}",
            queue=queue,
            max_attempts=1,
            key_mode=key_mode,
            key_prefix=queue,
        )
        expected = 1 if key_mode == "same" else count
        actual = int(harness.psql(f"SELECT count(*) FROM tasks WHERE queue={sql_literal(queue)}"))
        results.append(
            {
                "scenario": "idempotency_contention",
                "variant": key_mode,
                "trial": 1,
                "workers": 0,
                "schedulers": 1,
                "count": count,
                "submission": submission,
                "correctness": {
                    "expected_tasks": expected,
                    "actual_tasks": actual,
                    "distinct_task_ids": submission["distinct_task_ids"],
                    "passed": actual == expected and submission["failures"] == 0,
                },
            }
        )


def run_saturation(harness: Harness, results: list[dict[str, Any]]) -> None:
    profile = harness.profile
    for rate in profile["arrival_rates"]:
        count = int(rate * profile["arrival_seconds"])
        print(f"[saturation] rate={rate} count={count}", flush=True)
        item = measured_trial(
            harness,
            scenario="arrival_saturation",
            variant=f"r{rate}",
            trial=1,
            workers=20,
            task_type="test.noop",
            payload="{}",
            count=count,
            rate=rate,
            concurrency=min(200, count),
            timeout=max(180, profile["arrival_seconds"] * 5),
        )
        item["offered_rate_per_second"] = rate
        completed_rate = item["raw"].get("processing_throughput_per_second") or 0
        item["backpressure"] = {
            "offered_minus_completed_per_second": rate - completed_rate,
            "queue_growth_detected": completed_rate < rate * 0.95,
        }
        results.append(item)


def run_retry(harness: Harness, results: list[dict[str, Any]]) -> None:
    count = int(harness.profile["retry_tasks"])
    trials = int(harness.profile["trials"])
    for trial in range(1, trials + 1):
        print(f"[retry storm] tasks={count} trial={trial}", flush=True)
        results.append(
            measured_trial(
                harness,
                scenario="retry_storm",
                variant="fail-once",
                trial=trial,
                workers=20,
                schedulers=1,
                task_type="test.fail_n_then_succeed",
                payload='{"failures":1}',
                count=count,
                concurrency=min(200, count),
                max_attempts=2,
                expected_attempts=count * 2,
                timeout=max(300, count),
            )
        )

    for schedulers in harness.profile["scheduler_counts"]:
        for trial in range(1, trials + 1):
            print(f"[scheduler scaling] schedulers={schedulers} trial={trial}", flush=True)
            results.append(
                measured_trial(
                    harness,
                    scenario="scheduler_retry_scaling",
                    variant=f"s{schedulers}",
                    trial=trial,
                    workers=20,
                    schedulers=schedulers,
                    task_type="test.fail_n_then_succeed",
                    payload='{"failures":1}',
                    count=count,
                    concurrency=min(200, count),
                    max_attempts=2,
                    expected_attempts=count * 2,
                    timeout=max(300, count),
                )
            )


def worker_containers(harness: Harness) -> list[str]:
    output = harness.compose("ps", "--format", "json", "worker")
    containers: list[str] = []
    for line in output.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        containers.append(item.get("Name") or item.get("ID"))
    return sorted(container for container in containers if container)


def container_hostnames(containers: list[str]) -> list[str]:
    if not containers:
        return []
    output = run_command(
        ["docker", "inspect", "--format", "{{.Config.Hostname}}", *containers],
        timeout=60,
    ).stdout
    return [line.strip() for line in output.splitlines() if line.strip()]


def run_recovery(harness: Harness, results: list[dict[str, Any]]) -> None:
    profile = harness.profile
    total = int(profile["recovery_tasks"])
    workers = int(profile["recovery_workers"])
    for percentage in profile["recovery_kill_percentages"]:
        for trial in range(1, int(profile["trials"]) + 1):
            queue = f"tf012-recovery-k{percentage}-t{trial}"
            harness.clear_tasks()
            harness.scale(workers=workers, schedulers=1)
            print(f"[recovery] kill={percentage}% workers={workers} trial={trial}", flush=True)
            before = harness.prometheus(PROMETHEUS_QUERIES)
            with ResourceSampler(harness) as resources:
                submitted = harness.loadgen(
                    operation="submit",
                    count=total,
                    concurrency=min(200, total),
                    task_type="test.sleep",
                    payload='{"duration_ms":500}',
                    queue=queue,
                    max_attempts=2,
                    key_mode="unique",
                    key_prefix=queue,
                )
                harness.wait_for_status("RUNNING", workers, timeout=30)
                containers = worker_containers(harness)
                killed = max(1, math.ceil(len(containers) * percentage / 100))
                victims = containers[:killed]
                run_command(["docker", "pause", *victims], timeout=60)
                hostnames = container_hostnames(victims)
                hostname_list = ",".join(sql_literal(hostname) for hostname in hostnames)
                captured = harness.json_sql(
                    "SELECT json_build_object('tasks', coalesce(json_agg(json_build_object("
                    "'id', tasks.id, 'lease_expires_at', tasks.lease_expires_at)), '[]'::json)) "
                    "FROM tasks JOIN workers ON workers.id=tasks.claimed_by_worker_id "
                    "WHERE tasks.status='RUNNING' AND tasks.queue="
                    + sql_literal(queue)
                    + f" AND workers.name IN ({hostname_list})"
                )
                run_command(["docker", "kill", *victims], timeout=60)
                harness.compose(
                    "up", "-d", "--no-deps", "--scale", f"worker={workers}", "worker", timeout=180
                )
                harness.current_workers = workers
                counts = harness.wait_for_tasks(queue, total, timeout=max(300, total))
            raw = raw_measurements(harness, queue)
            enrich_raw(raw, total)
            attempts = int(raw["attempt_count"])
            stranded = len(captured.get("tasks", []))
            expected_attempts = total + stranded
            validation = correctness(harness, queue, total, expected_attempts)
            validation["expected_abandoned"] = stranded
            validation["actual_abandoned"] = int(
                raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
            )
            validation["passed"] = (
                validation["passed"] and validation["actual_abandoned"] == stranded
            )
            results.append(
                {
                    "scenario": "recovery_storm",
                    "variant": f"kill-{percentage}",
                    "trial": trial,
                    "workers": workers,
                    "schedulers": 1,
                    "count": total,
                    "kill_percentage": percentage,
                    "killed_workers": killed,
                    "stranded_attempts": stranded,
                    "captured_running_before_kill": captured,
                    "status_counts": counts,
                    "submission": submitted,
                    "raw": raw,
                    "actual_attempts": attempts,
                    "prometheus_before": before,
                    "prometheus_after": harness.prometheus(PROMETHEUS_QUERIES),
                    "resources": summarize_resources(resources.samples),
                    "resource_samples": resources.samples,
                    "correctness": validation,
                }
            )


def run_large_queue(harness: Harness, results: list[dict[str, Any]]) -> None:
    count = int(harness.profile["large_queue_tasks"])
    queue = "tf012-large-queue"
    harness.clear_tasks()
    harness.scale(workers=0, schedulers=1)
    print(f"[large queue] enqueue={count}", flush=True)
    submission = harness.loadgen(
        operation="submit",
        count=count,
        concurrency=200,
        task_type="test.noop",
        payload="{}",
        queue=queue,
        max_attempts=1,
        key_mode="unique",
        key_prefix=queue,
    )
    queued = int(
        harness.psql(
            f"SELECT count(*) FROM tasks WHERE queue={sql_literal(queue)} AND status='QUEUED'"
        )
    )
    claim_plan = json.loads(
        harness.psql("""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id FROM tasks
        WHERE status = 'QUEUED'
          AND scheduled_at <= clock_timestamp()
          AND attempt_count < max_attempts
        ORDER BY priority DESC, created_at ASC, id ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """)
    )
    harness.scale(workers=20)
    harness.wait_for_tasks(queue, count, timeout=max(600, count))
    results.append(
        {
            "scenario": "large_queue",
            "variant": str(count),
            "trial": 1,
            "workers": 20,
            "schedulers": 1,
            "count": count,
            "queued_before_drain": queued,
            "submission": submission,
            "raw": enrich_raw(raw_measurements(harness, queue), count),
            "claim_query_plan": claim_plan,
            "database_size_bytes": int(harness.psql("SELECT pg_database_size(current_database())")),
            "correctness": correctness(harness, queue, count, count),
        }
    )


def run_stability(harness: Harness, results: list[dict[str, Any]]) -> None:
    seconds = int(harness.profile["stability_seconds"])
    rate = min(100, max(harness.profile["arrival_rates"]))
    count = seconds * rate
    print(f"[stability] seconds={seconds} rate={rate}", flush=True)
    harness.scale(workers=20, schedulers=1)
    harness.reset_prometheus()
    item = measured_trial(
        harness,
        scenario="stability",
        variant=f"{seconds}s",
        trial=1,
        workers=20,
        task_type="test.noop",
        payload="{}",
        count=count,
        concurrency=200,
        rate=rate,
        timeout=max(300, seconds * 3),
    )
    item["configured_duration_seconds"] = seconds
    results.append(item)


def run_sensitivity(harness: Harness, results: list[dict[str, Any]]) -> None:
    profile = harness.profile
    count = min(500, int(profile["noop_tasks"]))
    original = harness.env.copy()

    for interval in profile["poll_intervals"]:
        harness.env["POLL_INTERVAL"] = interval
        harness.recreate_workers(4)
        print(f"[poll sensitivity] interval={interval}", flush=True)
        item = measured_trial(
            harness,
            scenario="poll_sensitivity",
            variant=interval,
            trial=1,
            workers=4,
            task_type="test.noop",
            payload="{}",
            count=count,
            concurrency=min(100, count),
        )
        item["poll_interval"] = interval
        results.append(item)

    for lease_duration, renew_interval in profile["lease_timings"]:
        harness.env["WORKER_TASK_LEASE_DURATION"] = lease_duration
        harness.env["WORKER_TASK_LEASE_RENEW_INTERVAL"] = renew_interval
        harness.recreate_workers(20)
        print(f"[lease sensitivity] lease={lease_duration} renew={renew_interval}", flush=True)
        item = measured_trial(
            harness,
            scenario="lease_sensitivity",
            variant=f"lease-{lease_duration}-renew-{renew_interval}",
            trial=1,
            workers=20,
            task_type="test.sleep",
            payload='{"duration_ms":1500}',
            count=min(200, int(profile["io_tasks"])),
            concurrency=100,
            timeout=300,
        )
        item["lease_duration"] = lease_duration
        item["renew_interval"] = renew_interval
        results.append(item)

    retry_count = min(500, int(profile["retry_tasks"]))
    for interval in profile["retry_promotion_intervals"]:
        harness.env["SCHEDULER_RETRY_PROMOTION_INTERVAL"] = interval
        harness.recreate_schedulers(1)
        print(f"[retry scan sensitivity] interval={interval}", flush=True)
        item = measured_trial(
            harness,
            scenario="retry_interval_sensitivity",
            variant=interval,
            trial=1,
            workers=20,
            schedulers=1,
            task_type="test.fail_n_then_succeed",
            payload='{"failures":1}',
            count=retry_count,
            concurrency=100,
            max_attempts=2,
            expected_attempts=retry_count * 2,
            timeout=300,
        )
        item["retry_promotion_interval"] = interval
        results.append(item)

    recovery_count = min(100, int(profile["recovery_tasks"]))
    recovery_workers = min(10, int(profile["recovery_workers"]))
    for interval in profile["recovery_intervals"]:
        harness.env["SCHEDULER_RECOVERY_INTERVAL"] = interval
        harness.recreate_schedulers(1)
        harness.clear_tasks()
        harness.scale(workers=recovery_workers)
        queue = f"tf012-recovery-interval-{interval}"
        print(f"[recovery scan sensitivity] interval={interval}", flush=True)
        before = harness.prometheus(PROMETHEUS_QUERIES)
        submitted = harness.loadgen(
            operation="submit",
            count=recovery_count,
            concurrency=min(100, recovery_count),
            task_type="test.sleep",
            payload='{"duration_ms":500}',
            queue=queue,
            max_attempts=2,
            key_mode="unique",
            key_prefix=queue,
        )
        harness.wait_for_status("RUNNING", recovery_workers, timeout=30)
        victim = worker_containers(harness)[0]
        run_command(["docker", "pause", victim], timeout=60)
        victim_hostname = container_hostnames([victim])[0]
        stranded = int(
            harness.psql(
                "SELECT count(*) FROM tasks JOIN workers ON workers.id=tasks.claimed_by_worker_id "
                "WHERE tasks.status='RUNNING' AND tasks.queue="
                + sql_literal(queue)
                + " AND workers.name="
                + sql_literal(victim_hostname)
            )
        )
        run_command(["docker", "kill", victim], timeout=60)
        harness.compose(
            "up",
            "-d",
            "--no-deps",
            "--scale",
            f"worker={recovery_workers}",
            "worker",
            timeout=180,
        )
        harness.current_workers = recovery_workers
        harness.wait_for_tasks(queue, recovery_count, timeout=300)
        raw = raw_measurements(harness, queue)
        enrich_raw(raw, recovery_count)
        validation = correctness(harness, queue, recovery_count, recovery_count + stranded)
        validation["expected_abandoned"] = stranded
        validation["actual_abandoned"] = int(
            raw.get("attempt_status_counts", {}).get("ABANDONED", 0)
        )
        validation["passed"] = validation["passed"] and validation["actual_abandoned"] == stranded
        results.append(
            {
                "scenario": "recovery_interval_sensitivity",
                "variant": interval,
                "trial": 1,
                "workers": recovery_workers,
                "schedulers": 1,
                "count": recovery_count,
                "recovery_interval": interval,
                "submission": submitted,
                "raw": raw,
                "prometheus_before": before,
                "prometheus_after": harness.prometheus(PROMETHEUS_QUERIES),
                "configuration": harness.trial_configuration(),
                "correctness": validation,
            }
        )

    harness.env.clear()
    harness.env.update(original)
    harness.recreate_workers(20)
    harness.recreate_schedulers(1)


def warmup(harness: Harness) -> dict[str, Any]:
    print("[warmup] 100 noop tasks (excluded from measurements)", flush=True)
    harness.clear_tasks()
    harness.scale(workers=4, schedulers=1)
    queue = "tf012-warmup-excluded"
    submitted = harness.loadgen(
        operation="submit",
        count=100,
        concurrency=25,
        task_type="test.noop",
        payload="{}",
        queue=queue,
        max_attempts=1,
        key_mode="unique",
        key_prefix=queue,
    )
    harness.wait_for_tasks(queue, 100, timeout=120)
    evidence = {"excluded": True, "submission": submitted, "raw": raw_measurements(harness, queue)}
    harness.clear_tasks()
    return evidence


def aggregate(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in results:
        groups.setdefault((item["scenario"], item["variant"]), []).append(item)
    summaries: list[dict[str, Any]] = []
    for (scenario, variant), items in groups.items():
        values = [
            item.get("raw", {}).get("processing_throughput_per_second")
            or item.get("submission", {}).get("requests_per_second")
            for item in items
        ]
        values = [float(value) for value in values if value is not None]
        mean = statistics.fmean(values) if values else None
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0 if values else None
        summaries.append(
            {
                "scenario": scenario,
                "variant": variant,
                "trials": len(items),
                "throughput_mean": mean,
                "throughput_median": statistics.median(values) if values else None,
                "throughput_min": min(values) if values else None,
                "throughput_max": max(values) if values else None,
                "throughput_stdev": stdev,
                "throughput_cv": stdev / mean if stdev is not None and mean else None,
                "correctness_passed": all(
                    item.get("correctness", {}).get("passed", False) for item in items
                ),
            }
        )
    return summaries


def write_csv(path: pathlib.Path, results: list[dict[str, Any]]) -> None:
    fields = [
        "scenario",
        "variant",
        "trial",
        "workers",
        "schedulers",
        "count",
        "arrival_rate",
        "submission_rps",
        "submission_p50_ms",
        "submission_p95_ms",
        "submission_p99_ms",
        "processing_rps",
        "end_to_end_rps",
        "queue_p50_s",
        "queue_p95_s",
        "queue_p99_s",
        "execution_p50_s",
        "execution_p95_s",
        "execution_p99_s",
        "total_p95_s",
        "correctness_passed",
    ]
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for item in results:
            raw = item.get("raw", {})
            submission = item.get("submission", {})
            latency = submission.get("latency_ms", {})
            writer.writerow(
                {
                    "scenario": item["scenario"],
                    "variant": item["variant"],
                    "trial": item.get("trial"),
                    "workers": item.get("workers"),
                    "schedulers": item.get("schedulers"),
                    "count": item.get("count"),
                    "arrival_rate": item.get("arrival_rate"),
                    "submission_rps": submission.get("requests_per_second"),
                    "submission_p50_ms": latency.get("p50"),
                    "submission_p95_ms": latency.get("p95"),
                    "submission_p99_ms": latency.get("p99"),
                    "processing_rps": raw.get("processing_throughput_per_second"),
                    "end_to_end_rps": raw.get("end_to_end_throughput_per_second"),
                    "queue_p50_s": raw.get("queue_p50_seconds"),
                    "queue_p95_s": raw.get("queue_p95_seconds"),
                    "queue_p99_s": raw.get("queue_p99_seconds"),
                    "execution_p50_s": raw.get("execution_p50_seconds"),
                    "execution_p95_s": raw.get("execution_p95_seconds"),
                    "execution_p99_s": raw.get("execution_p99_seconds"),
                    "total_p95_s": raw.get("total_p95_seconds"),
                    "correctness_passed": item.get("correctness", {}).get("passed"),
                }
            )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "suite",
        choices=(
            "reset",
            "smoke",
            "scaling",
            "api",
            "saturation",
            "retry",
            "recovery",
            "sensitivity",
            "large",
            "stability",
            "all",
        ),
    )
    parser.add_argument("--profile", choices=("ci", "smoke", "baseline"), default="smoke")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument(
        "--keep", action="store_true", help="leave the isolated benchmark Compose project running"
    )
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--output-dir", type=pathlib.Path)
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    profile = json.loads((BENCHMARKS / "config" / f"{arguments.profile}.json").read_text())
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = arguments.output_dir or (BENCHMARKS / "results" / timestamp)
    output_dir.mkdir(parents=True, exist_ok=True)
    harness = Harness(profile, arguments.project, arguments.keep)
    if arguments.suite == "reset":
        harness.reset()
        print(f"reset isolated Compose project {harness.project}")
        return 0
    document: dict[str, Any] = {
        "schema_version": 1,
        "tf_ticket": "TF-012",
        "suite": arguments.suite,
        "profile": profile,
        "started_at": dt.datetime.now(dt.UTC).isoformat(),
        "warmup": None,
        "environment": None,
        "results": [],
        "summaries": [],
        "errors": [],
        "completed_at": None,
    }
    json_path = output_dir / "results.json"
    try:
        harness.reset()
        if not arguments.skip_build:
            harness.build()
        harness.start()
        document["environment"] = harness.environment()
        document["warmup"] = warmup(harness)
        selected = (
            {arguments.suite}
            if arguments.suite != "all"
            else {
                "scaling",
                "api",
                "saturation",
                "retry",
                "recovery",
                "sensitivity",
                "large",
                "stability",
            }
        )
        if arguments.suite == "smoke":
            selected = {"scaling", "api", "retry", "recovery"}
        if "scaling" in selected:
            run_scaling(harness, document["results"])
        if "api" in selected:
            run_api(harness, document["results"])
        if "saturation" in selected:
            run_saturation(harness, document["results"])
        if "retry" in selected:
            run_retry(harness, document["results"])
        if "recovery" in selected:
            run_recovery(harness, document["results"])
        if "sensitivity" in selected:
            run_sensitivity(harness, document["results"])
        if "large" in selected:
            run_large_queue(harness, document["results"])
        if "stability" in selected:
            run_stability(harness, document["results"])
        document["summaries"] = aggregate(document["results"])
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        document["all_correctness_passed"] = all(
            item.get("correctness", {}).get("passed", False) for item in document["results"]
        )
    except Exception as exc:  # noqa: BLE001 - persist partial evidence before failing
        document["errors"].append({"type": type(exc).__name__, "message": str(exc)})
        document["completed_at"] = dt.datetime.now(dt.UTC).isoformat()
        json_path.write_text(json.dumps(document, indent=2) + "\n")
        raise
    finally:
        harness.close()
    json_path.write_text(json.dumps(document, indent=2) + "\n")
    write_csv(output_dir / "results.csv", document["results"])
    print(json_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
