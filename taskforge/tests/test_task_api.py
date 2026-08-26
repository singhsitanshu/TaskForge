import os
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


@pytest.fixture(scope="module")
def database_schema() -> Iterator[str]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for task API integration tests")

    schema_name = f"task_api_test_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
            connection.execute(UP_SQL)
            yield schema_name
        finally:
            connection.execute(DOWN_SQL)
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )


@pytest.fixture(scope="module")
def api_client(database_schema: str) -> Iterator[TestClient]:
    database_url = os.environ["TEST_DATABASE_URL"]
    application = create_app(
        database_url,
        database_connection_kwargs={
            "options": f"-csearch_path={database_schema}",
        },
    )
    with TestClient(application) as client:
        yield client


def database_connection(database_schema: str) -> psycopg.Connection:
    return psycopg.connect(
        os.environ["TEST_DATABASE_URL"],
        autocommit=True,
        options=f"-csearch_path={database_schema}",
    )


def test_submit_get_list_and_cancel(
    api_client: TestClient,
    database_schema: str,
) -> None:
    response = api_client.post(
        "/tasks",
        json={
            "task_type": "email.send",
            "queue": "emails",
            "payload": {"to": "person@example.com"},
            "priority": 10,
            "max_attempts": 2,
            "idempotency_key": "email-request-1",
        },
    )

    assert response.status_code == 201
    submitted = response.json()
    task_id = submitted["id"]
    assert submitted["status"] == "QUEUED"
    assert submitted["attempt_count"] == 0
    assert submitted["payload"] == {"to": "person@example.com"}

    with database_connection(database_schema) as connection:
        persisted_status = connection.execute(
            "SELECT status FROM tasks WHERE id = %s",
            (task_id,),
        ).fetchone()[0]
    assert persisted_status == "QUEUED"

    get_response = api_client.get(f"/tasks/{task_id}")
    assert get_response.status_code == 200
    assert get_response.json() == submitted

    list_response = api_client.get(
        "/tasks",
        params={"status": "QUEUED", "queue": "emails", "limit": 10, "offset": 0},
    )
    assert list_response.status_code == 200
    listing = list_response.json()
    assert listing["limit"] == 10
    assert listing["offset"] == 0
    assert task_id in {item["id"] for item in listing["items"]}

    with database_connection(database_schema) as connection:
        attempts_before = connection.execute(
            "SELECT count(*) FROM task_attempts WHERE task_id = %s",
            (task_id,),
        ).fetchone()[0]

    cancel_response = api_client.post(f"/tasks/{task_id}/cancel")
    assert cancel_response.status_code == 200
    cancelled = cancel_response.json()
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["completed_at"] is not None

    repeated_cancel = api_client.post(f"/tasks/{task_id}/cancel")
    assert repeated_cancel.status_code == 200
    assert repeated_cancel.json() == cancelled

    with database_connection(database_schema) as connection:
        attempts_after = connection.execute(
            "SELECT count(*) FROM task_attempts WHERE task_id = %s",
            (task_id,),
        ).fetchone()[0]
    assert attempts_before == attempts_after == 0

    metrics_response = api_client.get("/metrics")
    assert metrics_response.status_code == 200
    assert "text/plain" in metrics_response.headers["content-type"]
    metrics_text = metrics_response.text
    assert 'taskforge_task_submissions_total{outcome="created"}' in metrics_text
    assert 'taskforge_task_cancellations_total{outcome="cancelled"}' in metrics_text
    assert 'route="/tasks/{task_id}"' in metrics_text
    assert task_id not in metrics_text


def test_deprecated_body_idempotency_key_replays_globally(
    api_client: TestClient,
) -> None:
    payload = {
        "task_type": "report.generate",
        "queue": "reports",
        "idempotency_key": f"report-request-{uuid.uuid4().hex}",
    }
    first = api_client.post("/tasks", json=payload)
    assert first.status_code == 201

    replay = api_client.post("/tasks", json=payload)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]

    payload["queue"] = "priority-reports"
    conflict = api_client.post("/tasks", json=payload)
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "IDEMPOTENCY_KEY_REUSE"


@pytest.mark.parametrize("terminal_status", ["FAILED", "SUCCEEDED"])
def test_cancel_terminal_task_returns_conflict(
    api_client: TestClient,
    database_schema: str,
    terminal_status: str,
) -> None:
    submitted = api_client.post(
        "/tasks",
        json={"task_type": f"already.{terminal_status.lower()}"},
    ).json()

    with database_connection(database_schema) as connection:
        connection.execute(
            """
            UPDATE tasks
            SET status = %s, completed_at = now()
            WHERE id = %s
            """,
            (terminal_status, submitted["id"]),
        )

    response = api_client.post(f"/tasks/{submitted['id']}/cancel")
    assert response.status_code == 409
    assert terminal_status in response.json()["detail"]


def test_cancel_running_task_returns_conflict_and_preserves_attempt(
    api_client: TestClient,
    database_schema: str,
) -> None:
    submitted = api_client.post(
        "/tasks",
        json={"task_type": "currently.running"},
    ).json()

    with database_connection(database_schema) as connection:
        worker_id = connection.execute(
            """
            INSERT INTO workers (instance_id, name)
            VALUES (%s, %s)
            RETURNING id
            """,
            (f"api-test-{uuid.uuid4()}", "api-test-running-worker"),
        ).fetchone()[0]
        connection.execute(
            """
            UPDATE tasks
            SET
                status = 'RUNNING',
                claimed_by_worker_id = %s,
                attempt_count = 1,
                lease_expires_at = clock_timestamp() + interval '1 minute'
            WHERE id = %s
            """,
            (worker_id, submitted["id"]),
        )
        attempt_id = connection.execute(
            """
            INSERT INTO task_attempts (
                task_id,
                worker_id,
                attempt_number,
                status,
                started_at
            )
            VALUES (%s, %s, 1, 'RUNNING', clock_timestamp())
            RETURNING id
            """,
            (submitted["id"], worker_id),
        ).fetchone()[0]

    response = api_client.post(f"/tasks/{submitted['id']}/cancel")
    assert response.status_code == 409
    assert "RUNNING" in response.json()["detail"]

    with database_connection(database_schema) as connection:
        task_status = connection.execute(
            "SELECT status::text FROM tasks WHERE id = %s",
            (submitted["id"],),
        ).fetchone()[0]
        attempt = connection.execute(
            "SELECT status::text, finished_at FROM task_attempts WHERE id = %s",
            (attempt_id,),
        ).fetchone()

    assert task_status == "RUNNING"
    assert attempt == ("RUNNING", None)


def test_cancel_retrying_task_prevents_future_promotion(
    api_client: TestClient,
    database_schema: str,
) -> None:
    submitted = api_client.post(
        "/tasks",
        json={"task_type": "waiting.retry", "max_attempts": 3},
    ).json()
    with database_connection(database_schema) as connection:
        worker_id = connection.execute(
            """
            INSERT INTO workers (instance_id, name)
            VALUES (%s, 'retry cancellation worker')
            RETURNING id
            """,
            (f"retry-cancel-{uuid.uuid4().hex}",),
        ).fetchone()[0]
        connection.execute(
            """
            UPDATE tasks
            SET
                status = 'RETRYING',
                attempt_count = 1,
                scheduled_at = clock_timestamp() + interval '1 hour',
                last_error = 'temporary failure'
            WHERE id = %s
            """,
            (submitted["id"],),
        )
        connection.execute(
            """
            INSERT INTO task_attempts (
                task_id, worker_id, attempt_number, status,
                started_at, finished_at, error
            )
            VALUES (
                %s, %s, 1, 'FAILED',
                clock_timestamp(), clock_timestamp(), 'temporary failure'
            )
            """,
            (submitted["id"], worker_id),
        )

    response = api_client.post(f"/tasks/{submitted['id']}/cancel")
    assert response.status_code == 200
    cancelled = response.json()
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["attempt_count"] == 1
    assert cancelled["completed_at"] is not None
    assert cancelled["scheduled_at"] <= cancelled["completed_at"]

    with database_connection(database_schema) as connection:
        attempt = connection.execute(
            """
            SELECT status::text, error
            FROM task_attempts
            WHERE task_id = %s
            """,
            (submitted["id"],),
        ).fetchone()
    assert attempt == ("FAILED", "temporary failure")


def test_unknown_tasks_return_not_found(api_client: TestClient) -> None:
    task_id = uuid.uuid4()
    assert api_client.get(f"/tasks/{task_id}").status_code == 404
    assert api_client.get(f"/tasks/{task_id}/attempts").status_code == 404
    assert api_client.post(f"/tasks/{task_id}/cancel").status_code == 404


def test_attempt_history_is_ordered_and_exposes_abandonment(
    api_client: TestClient,
    database_schema: str,
) -> None:
    submitted = api_client.post(
        "/tasks",
        json={"task_type": "history.demo", "max_attempts": 3},
    ).json()
    with database_connection(database_schema) as connection:
        worker_a = connection.execute(
            """
            INSERT INTO workers (instance_id, name)
            VALUES (%s, 'history worker A')
            RETURNING id
            """,
            (f"history-a-{uuid.uuid4().hex}",),
        ).fetchone()[0]
        worker_b = connection.execute(
            """
            INSERT INTO workers (instance_id, name)
            VALUES (%s, 'history worker B')
            RETURNING id
            """,
            (f"history-b-{uuid.uuid4().hex}",),
        ).fetchone()[0]
        connection.execute(
            """
            UPDATE tasks
            SET
                status = 'SUCCEEDED',
                attempt_count = 2,
                completed_at = clock_timestamp(),
                result = '{"ok": true}'::jsonb
            WHERE id = %s
            """,
            (submitted["id"],),
        )
        connection.execute(
            """
            INSERT INTO task_attempts (
                task_id, worker_id, attempt_number, status,
                started_at, finished_at, error
            )
            VALUES
                (%s, %s, 2, 'SUCCEEDED', clock_timestamp(), clock_timestamp(), NULL),
                (%s, %s, 1, 'ABANDONED', clock_timestamp(), clock_timestamp(), 'lease_expired')
            """,
            (submitted["id"], worker_b, submitted["id"], worker_a),
        )

    response = api_client.get(f"/tasks/{submitted['id']}/attempts")
    assert response.status_code == 200
    attempts = response.json()["items"]
    assert [attempt["attempt_number"] for attempt in attempts] == [1, 2]
    assert [attempt["status"] for attempt in attempts] == ["ABANDONED", "SUCCEEDED"]
    assert attempts[0]["worker_id"] == str(worker_a)
    assert attempts[0]["error"] == "lease_expired"
    assert attempts[0]["finished_at"] is not None
    assert attempts[1]["worker_id"] == str(worker_b)
    assert "lease_token" not in attempts[0]


@pytest.mark.parametrize(
    "request_body",
    [
        {},
        {"task_type": "   "},
        {"task_type": "valid", "payload": []},
        {"task_type": "valid", "max_attempts": 0},
    ],
)
def test_submission_validation(
    api_client: TestClient,
    request_body: dict,
) -> None:
    assert api_client.post("/tasks", json=request_body).status_code == 422


def test_list_validation(api_client: TestClient) -> None:
    assert api_client.get("/tasks", params={"status": "UNKNOWN"}).status_code == 422
    assert api_client.get("/tasks", params={"limit": 101}).status_code == 422
