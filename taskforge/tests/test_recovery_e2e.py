import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from app.config import HeartbeatSettings
from app.main import create_app
from fastapi.testclient import TestClient
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
UP_SQL = "\n".join(path.read_text() for path in sorted(MIGRATIONS.glob("*.up.sql")))
DOWN_SQL = "\n".join(
    path.read_text() for path in sorted(MIGRATIONS.glob("*.down.sql"), reverse=True)
)
HEARTBEAT_SETTINGS = HeartbeatSettings(
    interval=timedelta(milliseconds=100),
    stale_after=timedelta(milliseconds=300),
    dead_after=timedelta(milliseconds=700),
)


@pytest.fixture
def recovery_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for recovery E2E tests")

    schema_name = f"recovery_e2e_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
            connection.execute(UP_SQL)
            application = create_app(
                database_url,
                database_connection_kwargs={"options": f"-csearch_path={schema_name}"},
                heartbeat_settings=HEARTBEAT_SETTINGS,
            )
            with TestClient(application) as client:
                yield schema_name, client
        finally:
            connection.execute(DOWN_SQL)
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )


def schema_connection(schema_name: str) -> psycopg.Connection:
    return psycopg.connect(
        os.environ["TEST_DATABASE_URL"],
        autocommit=True,
        options=f"-csearch_path={schema_name}",
    )


def start_scheduler(schema_name: str) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "HTTP_ADDR": "127.0.0.1:0",
            "SCHEDULER_RECOVERY_INTERVAL": "25ms",
            "SCHEDULER_RECOVERY_BATCH_SIZE": "100",
            "SCHEDULER_RECOVERY_DB_TIMEOUT": "1s",
        }
    )
    return subprocess.Popen(
        ["taskforge-scheduler"],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def start_worker(schema_name: str, instance_id: str) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "WORKER_ID": instance_id,
            "WORKER_NAME": instance_id,
            "POLL_INTERVAL": "20ms",
            "HTTP_ADDR": "127.0.0.1:0",
            "WORKER_HEARTBEAT_INTERVAL": "100ms",
            "WORKER_STALE_AFTER": "300ms",
            "WORKER_DEAD_AFTER": "700ms",
            "WORKER_HEARTBEAT_TIMEOUT": "50ms",
            "WORKER_TASK_LEASE_DURATION": "400ms",
            "WORKER_TASK_LEASE_RENEW_INTERVAL": "100ms",
            "WORKER_TASK_LEASE_RENEW_TIMEOUT": "50ms",
        }
    )
    return subprocess.Popen(
        ["taskforge-worker"],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for_worker(
    schema_name: str, instance_id: str, process: subprocess.Popen
) -> str:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert_running(process, "worker")
        with schema_connection(schema_name) as connection:
            row = connection.execute(
                "SELECT id::text FROM workers WHERE instance_id = %s",
                (instance_id,),
            ).fetchone()
        if row:
            return row[0]
        time.sleep(0.02)
    pytest.fail(f"worker {instance_id} did not register")


def wait_for_task(
    client: TestClient,
    task_id: str,
    expected_status: str,
    process: subprocess.Popen,
    timeout: float = 6,
) -> dict:
    deadline = time.monotonic() + timeout
    last_task = None
    while time.monotonic() < deadline:
        assert_running(process, "background process")
        last_task = client.get(f"/tasks/{task_id}").json()
        if last_task["status"] == expected_status:
            return last_task
        time.sleep(0.02)
    pytest.fail(f"task did not reach {expected_status}; last state={last_task}")


def wait_for_abandonment(
    schema_name: str,
    task_id: str,
    scheduler: subprocess.Popen,
) -> tuple:
    deadline = time.monotonic() + 5
    last_state = None
    while time.monotonic() < deadline:
        assert_running(scheduler, "scheduler")
        with schema_connection(schema_name) as connection:
            last_state = connection.execute(
                """
                SELECT
                    task.status::text,
                    task.attempt_count,
                    task.claimed_by_worker_id,
                    task.lease_expires_at,
                    attempt.status::text,
                    attempt.error,
                    attempt.finished_at
                FROM tasks AS task
                JOIN task_attempts AS attempt ON attempt.task_id = task.id
                WHERE task.id = %s AND attempt.attempt_number = 1
                """,
                (task_id,),
            ).fetchone()
        if last_state and last_state[0] == "QUEUED":
            return last_state
        time.sleep(0.02)
    pytest.fail(f"scheduler did not recover task; last state={last_state}")


def assert_running(process: subprocess.Popen, label: str) -> None:
    if process.poll() is not None:
        pytest.fail(f"{label} exited unexpectedly:\n{read_output(process)}")


def read_output(process: subprocess.Popen) -> str:
    if process.stdout is None:
        return ""
    return process.stdout.read()


def stop_process(process: subprocess.Popen) -> str:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    return read_output(process)


def test_crash_recovery_reclaim_and_success(
    recovery_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = recovery_environment
    scheduler = start_scheduler(schema_name)
    worker_a_instance = f"recovery-worker-a-{uuid.uuid4().hex}"
    worker_a = start_worker(schema_name, worker_a_instance)
    worker_b = None
    scheduler_output = ""
    worker_b_output = ""
    try:
        worker_a_id = wait_for_worker(schema_name, worker_a_instance, worker_a)
        response = client.post(
            "/tasks",
            json={
                "task_type": "test.sleep",
                "payload": {"duration_ms": 1000},
                "max_attempts": 3,
            },
        )
        assert response.status_code == 201
        task_id = response.json()["id"]
        running = wait_for_task(client, task_id, "RUNNING", worker_a)
        initial_lease = running["lease_expires_at"]

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            renewed_lease = client.get(f"/tasks/{task_id}").json()["lease_expires_at"]
            if renewed_lease > initial_lease:
                break
            time.sleep(0.02)
        else:
            pytest.fail("worker A did not renew attempt 1 lease before crash")

        worker_a.kill()
        worker_a.wait(timeout=5)
        abandoned = wait_for_abandonment(schema_name, task_id, scheduler)
        assert abandoned[0:6] == (
            "QUEUED",
            1,
            None,
            None,
            "ABANDONED",
            "lease_expired",
        )
        assert abandoned[6] is not None

        worker_b_instance = f"recovery-worker-b-{uuid.uuid4().hex}"
        worker_b = start_worker(schema_name, worker_b_instance)
        worker_b_id = wait_for_worker(schema_name, worker_b_instance, worker_b)
        succeeded = wait_for_task(client, task_id, "SUCCEEDED", worker_b, timeout=8)
        assert succeeded["attempt_count"] == 2
        assert succeeded["result"] == {"slept_ms": 1000}
        assert succeeded["claimed_by_worker_id"] is None
        assert succeeded["lease_expires_at"] is None

        history = client.get(f"/tasks/{task_id}/attempts").json()["items"]
        assert [(item["attempt_number"], item["status"]) for item in history] == [
            (1, "ABANDONED"),
            (2, "SUCCEEDED"),
        ]
        assert history[0]["worker_id"] == worker_a_id
        assert history[0]["error"] == "lease_expired"
        assert history[1]["worker_id"] == worker_b_id
        assert history[1]["error"] is None
        print(
            "CRASH_RECOVERY task=SUCCEEDED attempt_count=2 "
            "attempt_1=ABANDONED attempt_2=SUCCEEDED"
        )
    finally:
        if worker_a.poll() is None:
            worker_a.kill()
            worker_a.wait(timeout=5)
        if worker_b is not None:
            worker_b_output = stop_process(worker_b)
        scheduler_output = stop_process(scheduler)
        assert "task_recovered" in scheduler_output, scheduler_output
        if worker_b is not None:
            assert worker_b.returncode == 0, worker_b_output
