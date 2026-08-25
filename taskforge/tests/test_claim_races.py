import json
import os
import subprocess
import threading
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
RACE_ITERATIONS = 50


@pytest.fixture(scope="module")
def cancel_race_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for cancellation race tests")

    schema_name = f"cancel_race_{uuid.uuid4().hex}"
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


def test_cancel_vs_claim_race_has_only_serialized_outcomes(
    cancel_race_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = cancel_race_environment
    cancel_wins = 0
    claim_wins = 0

    for iteration in range(RACE_ITERATIONS):
        response = client.post(
            "/tasks",
            json={"task_type": "test.echo", "payload": {"iteration": iteration}},
        )
        assert response.status_code == 201
        task_id = response.json()["id"]
        instance_id = f"cancel-race-{iteration}-{uuid.uuid4().hex}"
        environment = os.environ.copy()
        environment.update(
            {
                "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
                "PGOPTIONS": f"-csearch_path={schema_name}",
                "WORKER_ID": instance_id,
                "WORKER_NAME": "TF-005 cancel contender",
            }
        )
        process = subprocess.Popen(
            ["taskforge-claim-harness"],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        assert process.stdin is not None
        ready = json.loads(process.stdout.readline())
        assert ready["event"] == "ready", ready

        release = threading.Barrier(2)
        cancellation: list = []

        def cancel() -> None:
            release.wait()
            cancellation.append(client.post(f"/tasks/{task_id}/cancel"))

        cancel_thread = threading.Thread(target=cancel)
        cancel_thread.start()
        release.wait()
        process.stdin.write("\n")
        process.stdin.flush()
        cancel_thread.join(timeout=5)
        assert not cancel_thread.is_alive()
        assert len(cancellation) == 1

        claim_result = json.loads(process.stdout.readline())
        return_code = process.wait(timeout=5)
        stderr = process.stderr.read() if process.stderr is not None else ""
        assert return_code == 0, (claim_result, stderr)
        assert claim_result["event"] == "claim_result"

        with schema_connection(schema_name) as connection:
            task_state = connection.execute(
                """
                SELECT status::text, attempt_count, claimed_by_worker_id::text
                FROM tasks
                WHERE id = %s
                """,
                (task_id,),
            ).fetchone()
            attempts = connection.execute(
                """
                SELECT status::text, attempt_number, worker_id::text, finished_at
                FROM task_attempts
                WHERE task_id = %s
                """,
                (task_id,),
            ).fetchall()

        if claim_result.get("claimed"):
            claim_wins += 1
            assert cancellation[0].status_code == 409
            assert task_state == ("RUNNING", 1, ready["worker_id"])
            assert attempts == [("RUNNING", 1, ready["worker_id"], None)]
            assert claim_result["task_id"] == task_id
            assert claim_result["attempt_number"] == 1
        else:
            cancel_wins += 1
            assert cancellation[0].status_code == 200
            assert task_state == ("CANCELLED", 0, None)
            assert attempts == []

        with schema_connection(schema_name) as connection:
            connection.execute("DELETE FROM tasks WHERE id = %s", (task_id,))

    assert cancel_wins + claim_wins == RACE_ITERATIONS
    print(
        "CANCEL_RACE "
        f"iterations={RACE_ITERATIONS} cancel_wins={cancel_wins} "
        f"claim_wins={claim_wins} invalid_states=0"
    )
