import os
import re
import socket
import subprocess
import time
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from app.main import create_app
from fastapi.testclient import TestClient
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
UP_SQL = "\n".join(path.read_text() for path in sorted(MIGRATIONS.glob("*.up.sql")))
DOWN_SQL = "\n".join(
    path.read_text() for path in sorted(MIGRATIONS.glob("*.down.sql"), reverse=True)
)


@pytest.fixture
def worker_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for worker end-to-end tests")

    schema_name = f"worker_e2e_test_{uuid.uuid4().hex}"
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
                database_connection_kwargs={
                    "options": f"-csearch_path={schema_name}",
                },
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


def wait_for_worker_registration(
    schema_name: str,
    worker_name: str,
    process: subprocess.Popen,
) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"worker exited during registration:\n{read_output(process)}")
        with schema_connection(schema_name) as connection:
            registered = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM workers WHERE name = %s)",
                (worker_name,),
            ).fetchone()[0]
        if registered:
            return
        time.sleep(0.05)
    pytest.fail("worker did not register within five seconds")


def wait_for_status(
    client: TestClient,
    task_id: str,
    expected_status: str,
    process: subprocess.Popen,
) -> dict:
    deadline = time.monotonic() + 5
    last_task = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"worker exited while polling:\n{read_output(process)}")
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200
        last_task = response.json()
        if last_task["status"] == expected_status:
            return last_task
        time.sleep(0.05)
    pytest.fail(f"task did not reach {expected_status}; last state: {last_task}")


def read_output(process: subprocess.Popen) -> str:
    if process.stdout is None:
        return ""
    return process.stdout.read()


def stop_worker(process: subprocess.Popen) -> str:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    return read_output(process)


def unused_tcp_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def test_api_submission_worker_execution_and_poll_again(
    worker_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = worker_environment
    worker_name = f"e2e-worker-{uuid.uuid4().hex}"
    metrics_port = unused_tcp_port()
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "WORKER_ID": worker_name,
            "WORKER_NAME": worker_name,
            "POLL_INTERVAL": "20ms",
            "HTTP_ADDR": f"127.0.0.1:{metrics_port}",
        }
    )
    process = subprocess.Popen(
        ["taskforge-worker"],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        wait_for_worker_registration(schema_name, worker_name, process)

        first_response = client.post(
            "/tasks",
            json={
                "task_type": "test.echo",
                "payload": {"message": "first"},
            },
        )
        assert first_response.status_code == 201
        assert first_response.json()["status"] == "QUEUED"

        first_task = wait_for_status(
            client,
            first_response.json()["id"],
            "SUCCEEDED",
            process,
        )
        assert first_task["result"] == {"echo": {"message": "first"}}

        second_response = client.post(
            "/tasks",
            json={
                "task_type": "test.echo",
                "payload": {"message": "polled-again"},
            },
        )
        assert second_response.status_code == 201
        second_task = wait_for_status(
            client,
            second_response.json()["id"],
            "SUCCEEDED",
            process,
        )
        assert second_task["result"] == {"echo": {"message": "polled-again"}}

        failure_response = client.post(
            "/tasks",
            json={"task_type": "test.fail"},
        )
        failed_task = wait_for_status(
            client,
            failure_response.json()["id"],
            "FAILED",
            process,
        )
        assert "requested failure" in failed_task["last_error"]

        with schema_connection(schema_name) as connection:
            attempts = connection.execute(
                """
                SELECT ta.status::text
                FROM task_attempts AS ta
                JOIN tasks AS t ON t.id = ta.task_id
                WHERE t.id IN (%s, %s)
                ORDER BY t.created_at
                """,
                (first_response.json()["id"], second_response.json()["id"]),
            ).fetchall()
        assert attempts == [
            ("SUCCEEDED",),
            ("SUCCEEDED",),
        ]

        with urllib.request.urlopen(
            f"http://127.0.0.1:{metrics_port}/readyz", timeout=2
        ) as response:
            assert response.status == 200
        with urllib.request.urlopen(
            f"http://127.0.0.1:{metrics_port}/metrics", timeout=2
        ) as response:
            assert "text/plain" in response.headers["Content-Type"]
            metrics_text = response.read().decode()
        assert "taskforge_worker_tasks_claimed_total 3" in metrics_text
        assert 'taskforge_worker_tasks_completed_total{outcome="success"} 2' in metrics_text
        assert 'taskforge_worker_tasks_completed_total{outcome="terminal_failure"} 1' in metrics_text
        assert "taskforge_task_queue_wait_seconds_count 3" in metrics_text
        assert first_response.json()["id"] not in metrics_text
    finally:
        output = stop_worker(process)
        assert process.returncode == 0, output


def test_worker_claims_by_priority_age_and_id(
    worker_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = worker_environment
    same_created_at = "2026-01-01T00:00:00+00:00"
    ordered_tasks = [
        (
            uuid.UUID("00000000-0000-0000-0000-000000000100"),
            "priority-100",
            100,
            "2026-01-03T00:00:00+00:00",
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000075"),
            "priority-75-older",
            75,
            "2026-01-01T00:00:00+00:00",
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000076"),
            "priority-75-newer",
            75,
            "2026-01-02T00:00:00+00:00",
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000001"),
            "priority-60-id-1",
            60,
            same_created_at,
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000002"),
            "priority-60-id-2",
            60,
            same_created_at,
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000050"),
            "priority-50",
            50,
            "2026-01-01T00:00:00+00:00",
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000010"),
            "priority-1",
            1,
            "2026-01-01T00:00:00+00:00",
        ),
    ]
    excluded_tasks = [
        (uuid.UUID("00000000-0000-0000-0000-000000000200"), "cancelled", "CANCELLED"),
        (uuid.UUID("00000000-0000-0000-0000-000000000201"), "succeeded", "SUCCEEDED"),
    ]

    with schema_connection(schema_name) as connection:
        for task_id, label, priority, created_at in ordered_tasks:
            connection.execute(
                """
                INSERT INTO tasks (id, task_type, payload, priority, created_at)
                VALUES (%s, 'test.echo', %s::jsonb, %s, %s::timestamptz)
                """,
                (task_id, f'{{"label": "{label}"}}', priority, created_at),
            )
        for task_id, label, task_status in excluded_tasks:
            connection.execute(
                """
                INSERT INTO tasks (
                    id,
                    task_type,
                    payload,
                    status,
                    priority,
                    completed_at
                )
                VALUES (%s, 'test.echo', %s::jsonb, %s, 32767, clock_timestamp())
                """,
                (task_id, f'{{"label": "{label}"}}', task_status),
            )

    worker_name = f"priority-worker-{uuid.uuid4().hex}"
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "WORKER_ID": worker_name,
            "WORKER_NAME": "priority-test-worker",
            "POLL_INTERVAL": "20ms",
            "HTTP_ADDR": "127.0.0.1:0",
        }
    )
    process = subprocess.Popen(
        ["taskforge-worker"],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output = ""
    try:
        wait_for_worker_registration(schema_name, "priority-test-worker", process)
        for task_id, _, _, _ in ordered_tasks:
            wait_for_status(client, str(task_id), "SUCCEEDED", process)
    finally:
        output = stop_worker(process)
        assert process.returncode == 0, output

    executed_ids = re.findall(
        r"event=task_claimed worker_instance_id=\S+ task_id=([0-9a-f-]+)",
        output,
    )
    assert executed_ids == [str(task_id) for task_id, _, _, _ in ordered_tasks]

    with schema_connection(schema_name) as connection:
        excluded_attempts = connection.execute(
            """
            SELECT count(*)
            FROM task_attempts
            WHERE task_id = ANY(%s::uuid[])
            """,
            ([str(task_id) for task_id, _, _ in excluded_tasks],),
        ).fetchone()[0]
    assert excluded_attempts == 0
