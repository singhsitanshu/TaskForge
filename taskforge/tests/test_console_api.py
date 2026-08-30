import os
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
SETTINGS = HeartbeatSettings(
    interval=timedelta(milliseconds=100),
    stale_after=timedelta(milliseconds=300),
    dead_after=timedelta(milliseconds=700),
)


@pytest.fixture
def console_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for console API tests")

    schema_name = f"console_api_test_{uuid.uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
            connection.execute(UP_SQL)
            application = create_app(
                database_url,
                database_connection_kwargs={"options": f"-csearch_path={schema_name}"},
                heartbeat_settings=SETTINGS,
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


def test_overview_returns_real_counts_liveness_and_exceptional_history(
    console_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = console_environment
    database_url = os.environ["TEST_DATABASE_URL"]
    with psycopg.connect(
        database_url,
        autocommit=True,
        options=f"-csearch_path={schema_name}",
    ) as connection:
        worker_ids = []
        for name, age in (
            ("active", "100 milliseconds"),
            ("stale", "500 milliseconds"),
            ("dead", "900 milliseconds"),
        ):
            worker_ids.append(
                connection.execute(
                    """
                    INSERT INTO workers (instance_id, name, last_seen_at, created_at)
                    VALUES (
                        %s,
                        %s,
                        clock_timestamp() - %s::interval,
                        clock_timestamp() - %s::interval - interval '1 second'
                    )
                    RETURNING id
                    """,
                    (f"console-{name}-{uuid.uuid4().hex}", name, age, age),
                ).fetchone()[0]
            )

        queued_id = connection.execute(
            "INSERT INTO tasks (task_type) VALUES ('test.echo') RETURNING id"
        ).fetchone()[0]
        retrying_id = connection.execute(
            """
            INSERT INTO tasks (
                task_type, status, attempt_count, max_attempts, scheduled_at, last_error
            )
            VALUES (
                'test.fail_n_then_succeed',
                'RETRYING',
                1,
                3,
                clock_timestamp() + interval '1 second',
                'retryable failure'
            )
            RETURNING id
            """
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO task_attempts (
                task_id,
                worker_id,
                attempt_number,
                status,
                started_at,
                finished_at,
                error,
                retry_scheduled_at
            )
            VALUES (
                %s,
                %s,
                1,
                'FAILED',
                clock_timestamp(),
                clock_timestamp(),
                'retryable failure',
                clock_timestamp() + interval '1 second'
            )
            """,
            (retrying_id, worker_ids[0]),
        )

    response = client.get("/overview", params={"recent_limit": 5})
    assert response.status_code == 200
    overview = response.json()
    assert overview["task_counts"] == {
        "QUEUED": 1,
        "LEASED": 0,
        "RUNNING": 0,
        "RETRYING": 1,
        "SUCCEEDED": 0,
        "FAILED": 0,
        "CANCELLED": 0,
    }
    assert overview["worker_counts"] == {"ACTIVE": 1, "STALE": 1, "DEAD": 1}
    assert {item["id"] for item in overview["recent_tasks"]} == {
        str(queued_id),
        str(retrying_id),
    }
    assert len(overview["recent_exceptions"]) == 1
    exception = overview["recent_exceptions"][0]
    assert exception["task_id"] == str(retrying_id)
    assert exception["status"] == "FAILED"
    assert exception["retry_scheduled_at"] is not None
    assert overview["observed_at"] is not None


def test_overview_is_bounded_and_validates_limit(
    console_environment: tuple[str, TestClient],
) -> None:
    _, client = console_environment
    assert client.get("/overview", params={"recent_limit": 0}).status_code == 422
    assert client.get("/overview", params={"recent_limit": 26}).status_code == 422
