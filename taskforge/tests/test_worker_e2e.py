import os
import subprocess
import time
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
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        try:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
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
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name))
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


def test_api_submission_worker_execution_and_poll_again(
    worker_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = worker_environment
    worker_name = f"e2e-worker-{uuid.uuid4().hex}"
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "PGOPTIONS": f"-csearch_path={schema_name}",
            "WORKER_NAME": worker_name,
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
                SELECT ta.status::text, ta.lease_token, ta.lease_expires_at
                FROM task_attempts AS ta
                JOIN tasks AS t ON t.id = ta.task_id
                WHERE t.id IN (%s, %s)
                ORDER BY t.created_at
                """,
                (first_response.json()["id"], second_response.json()["id"]),
            ).fetchall()
        assert attempts == [
            ("SUCCEEDED", None, None),
            ("SUCCEEDED", None, None),
        ]
    finally:
        output = stop_worker(process)
        assert process.returncode == 0, output
