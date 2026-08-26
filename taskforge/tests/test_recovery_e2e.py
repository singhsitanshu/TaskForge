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
            "SCHEDULER_RETRY_PROMOTION_INTERVAL": "20ms",
            "SCHEDULER_RETRY_PROMOTION_BATCH_SIZE": "100",
            "SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT": "1s",
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
            "TASK_RETRY_BASE_DELAY": "100ms",
            "TASK_RETRY_MAX_DELAY": "400ms",
            "TASK_RETRY_JITTER": "0",
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
    idempotency_key = f"crash-recovery-{uuid.uuid4().hex}"
    submission = {
        "task_type": "test.sleep",
        "payload": {"duration_ms": 1000},
        "max_attempts": 3,
    }
    try:
        worker_a_id = wait_for_worker(schema_name, worker_a_instance, worker_a)
        response = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert response.status_code == 201
        task_id = response.json()["id"]
        running = wait_for_task(client, task_id, "RUNNING", worker_a)
        running_replay = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert running_replay.status_code == 200
        assert running_replay.json()["id"] == task_id
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

        queued_replay = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert queued_replay.status_code == 200
        assert queued_replay.json()["id"] == task_id
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
        final_replay = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert final_replay.status_code == 200
        assert final_replay.json()["id"] == task_id
        assert final_replay.json()["status"] == "SUCCEEDED"
        assert client.get(f"/tasks/{task_id}/attempts").json()["items"] == history
        with schema_connection(schema_name) as connection:
            keyed_task_count = connection.execute(
                "SELECT count(*) FROM tasks WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()[0]
        assert keyed_task_count == 1
        print(
            "CRASH_RECOVERY keyed_tasks=1 task=SUCCEEDED attempt_count=2 "
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


def test_retry_backoff_then_success(
    recovery_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = recovery_environment
    scheduler = start_scheduler(schema_name)
    worker_instance = f"retry-success-{uuid.uuid4().hex}"
    worker = None
    idempotency_key = f"retry-success-{uuid.uuid4().hex}"
    submission = {
        "task_type": "test.fail_n_then_succeed",
        "payload": {"failures": 2},
        "max_attempts": 3,
    }
    try:
        response = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert response.status_code == 201
        task_id = response.json()["id"]
        queued_replay = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert queued_replay.status_code == 200
        assert queued_replay.json()["id"] == task_id
        assert queued_replay.json()["status"] == "QUEUED"

        worker = start_worker(schema_name, worker_instance)
        wait_for_worker(schema_name, worker_instance, worker)
        retrying_attempts: set[int] = set()
        deadline = time.monotonic() + 8
        succeeded = None
        while time.monotonic() < deadline:
            assert_running(worker, "retry worker")
            task = client.get(f"/tasks/{task_id}").json()
            if task["status"] == "RETRYING":
                replay = client.post(
                    "/tasks",
                    headers={"Idempotency-Key": idempotency_key},
                    json=submission,
                )
                assert replay.status_code == 200
                assert replay.json()["id"] == task_id
                retrying_attempts.add(task["attempt_count"])
            if task["status"] == "SUCCEEDED":
                succeeded = task
                break
            time.sleep(0.01)
        if succeeded is None:
            pytest.fail(f"keyed retry task did not succeed; last task={task}")
        assert retrying_attempts == {1, 2}
        assert succeeded["attempt_count"] == 3
        assert succeeded["result"] == {"succeeded_on_attempt": 3}

        final_replay = client.post(
            "/tasks",
            headers={"Idempotency-Key": idempotency_key},
            json=submission,
        )
        assert final_replay.status_code == 200
        assert final_replay.json()["id"] == task_id
        assert final_replay.json()["status"] == "SUCCEEDED"

        with schema_connection(schema_name) as connection:
            attempts = connection.execute(
                """
                SELECT attempt_number, status::text, leased_at, finished_at, error
                FROM task_attempts
                WHERE task_id = %s
                ORDER BY attempt_number
                """,
                (task_id,),
            ).fetchall()
            keyed_task_count = connection.execute(
                "SELECT count(*) FROM tasks WHERE idempotency_key = %s",
                (idempotency_key,),
            ).fetchone()[0]
        assert keyed_task_count == 1
        assert [(row[0], row[1]) for row in attempts] == [
            (1, "FAILED"),
            (2, "FAILED"),
            (3, "SUCCEEDED"),
        ]
        replayed_history = client.get(f"/tasks/{task_id}/attempts").json()["items"]
        assert [item["status"] for item in replayed_history] == [
            "FAILED",
            "FAILED",
            "SUCCEEDED",
        ]
        first_wait = (attempts[1][2] - attempts[0][3]).total_seconds()
        second_wait = (attempts[2][2] - attempts[1][3]).total_seconds()
        assert first_wait >= 0.09
        assert second_wait >= 0.18
        assert attempts[0][4] and "attempt 1" in attempts[0][4]
        assert attempts[1][4] and "attempt 2" in attempts[1][4]
        print(
            "RETRY_SUCCESS keyed_tasks=1 delays_ms="
            f"{first_wait * 1000:.1f},{second_wait * 1000:.1f} "
            "history=FAILED,FAILED,SUCCEEDED"
        )
    finally:
        worker_output = stop_process(worker) if worker is not None else ""
        scheduler_output = stop_process(scheduler)
        if worker is not None:
            assert worker.returncode == 0, worker_output
        assert "task_retry_scheduled" in worker_output, worker_output
        assert "task_retry_promoted" in scheduler_output, scheduler_output


def test_retry_exhaustion_has_no_attempt_four(
    recovery_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = recovery_environment
    scheduler = start_scheduler(schema_name)
    worker_instance = f"retry-exhaust-{uuid.uuid4().hex}"
    worker = start_worker(schema_name, worker_instance)
    try:
        wait_for_worker(schema_name, worker_instance, worker)
        submitted = client.post(
            "/tasks",
            json={"task_type": "test.fail_retryable", "max_attempts": 3},
        ).json()
        failed = wait_for_task(client, submitted["id"], "FAILED", worker, timeout=8)
        assert failed["attempt_count"] == 3
        time.sleep(0.25)
        history = client.get(f"/tasks/{submitted['id']}/attempts").json()["items"]
        assert [(item["attempt_number"], item["status"]) for item in history] == [
            (1, "FAILED"),
            (2, "FAILED"),
            (3, "FAILED"),
        ]
    finally:
        worker_output = stop_process(worker)
        stop_process(scheduler)
        assert worker.returncode == 0, worker_output
        assert "task_retry_exhausted" in worker_output, worker_output


def test_mixed_retry_and_abandoned_history(
    recovery_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = recovery_environment
    scheduler = start_scheduler(schema_name)
    worker_instance = f"retry-mixed-{uuid.uuid4().hex}"
    worker = start_worker(schema_name, worker_instance)
    try:
        wait_for_worker(schema_name, worker_instance, worker)
        submitted = client.post(
            "/tasks",
            json={"task_type": "test.mixed_failure", "max_attempts": 4},
        ).json()
        task_id = submitted["id"]
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            task = client.get(f"/tasks/{task_id}").json()
            if task["status"] == "RUNNING" and task["attempt_count"] == 2:
                break
            assert_running(worker, "mixed-failure worker")
            time.sleep(0.02)
        else:
            pytest.fail(f"attempt 2 did not start; last task={task}")

        with schema_connection(schema_name) as connection:
            connection.execute(
                """
                UPDATE tasks
                SET lease_expires_at = clock_timestamp() - interval '1 millisecond'
                WHERE id = %s AND attempt_count = 2
                """,
                (task_id,),
            )
        succeeded = wait_for_task(client, task_id, "SUCCEEDED", worker, timeout=10)
        assert succeeded["attempt_count"] == 4
        history = client.get(f"/tasks/{task_id}/attempts").json()["items"]
        assert [(item["attempt_number"], item["status"]) for item in history] == [
            (1, "FAILED"),
            (2, "ABANDONED"),
            (3, "FAILED"),
            (4, "SUCCEEDED"),
        ]
        assert history[1]["error"] == "lease_expired"

        exhausted = client.post(
            "/tasks",
            json={"task_type": "test.mixed_failure", "max_attempts": 3},
        ).json()
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            exhausted_task = client.get(f"/tasks/{exhausted['id']}").json()
            if (
                exhausted_task["status"] == "RUNNING"
                and exhausted_task["attempt_count"] == 2
            ):
                break
            time.sleep(0.02)
        else:
            pytest.fail(f"mixed exhaustion attempt 2 did not start: {exhausted_task}")
        with schema_connection(schema_name) as connection:
            connection.execute(
                """
                UPDATE tasks
                SET lease_expires_at = clock_timestamp() - interval '1 millisecond'
                WHERE id = %s AND attempt_count = 2
                """,
                (exhausted["id"],),
            )
        final_failed = wait_for_task(
            client, exhausted["id"], "FAILED", worker, timeout=10
        )
        assert final_failed["attempt_count"] == 3
        exhausted_history = client.get(f"/tasks/{exhausted['id']}/attempts").json()[
            "items"
        ]
        assert [item["status"] for item in exhausted_history] == [
            "FAILED",
            "ABANDONED",
            "FAILED",
        ]
        print(
            "MIXED_HISTORY success=FAILED,ABANDONED,FAILED,SUCCEEDED "
            "exhausted=FAILED,ABANDONED,FAILED"
        )
    finally:
        worker_output = stop_process(worker)
        scheduler_output = stop_process(scheduler)
        assert worker.returncode == 0, worker_output
        assert "task_retry_scheduled" in worker_output, worker_output
        assert "task_recovered" in scheduler_output, scheduler_output
