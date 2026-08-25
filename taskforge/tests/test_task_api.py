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
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        try:
            connection.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
            connection.execute(UP_SQL)
            yield schema_name
        finally:
            connection.execute(DOWN_SQL)
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name))
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

    cancel_response = api_client.post(f"/tasks/{task_id}/cancel")
    assert cancel_response.status_code == 200
    cancelled = cancel_response.json()
    assert cancelled["status"] == "CANCELLED"
    assert cancelled["completed_at"] is not None

    repeated_cancel = api_client.post(f"/tasks/{task_id}/cancel")
    assert repeated_cancel.status_code == 200
    assert repeated_cancel.json() == cancelled


def test_idempotency_key_conflict_is_scoped_to_queue(api_client: TestClient) -> None:
    payload = {
        "task_type": "report.generate",
        "queue": "reports",
        "idempotency_key": "report-request-1",
    }
    assert api_client.post("/tasks", json=payload).status_code == 201

    conflict = api_client.post("/tasks", json=payload)
    assert conflict.status_code == 409

    payload["queue"] = "priority-reports"
    assert api_client.post("/tasks", json=payload).status_code == 201


def test_cancel_terminal_task_returns_conflict(
    api_client: TestClient,
    database_schema: str,
) -> None:
    submitted = api_client.post(
        "/tasks",
        json={"task_type": "already.finished"},
    ).json()

    with database_connection(database_schema) as connection:
        connection.execute(
            """
            UPDATE tasks
            SET status = 'SUCCEEDED', completed_at = now()
            WHERE id = %s
            """,
            (submitted["id"],),
        )

    response = api_client.post(f"/tasks/{submitted['id']}/cancel")
    assert response.status_code == 409
    assert "SUCCEEDED" in response.json()["detail"]


def test_unknown_tasks_return_not_found(api_client: TestClient) -> None:
    task_id = uuid.uuid4()
    assert api_client.get(f"/tasks/{task_id}").status_code == 404
    assert api_client.post(f"/tasks/{task_id}/cancel").status_code == 404


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
