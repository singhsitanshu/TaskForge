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
def worker_api_environment() -> Iterator[tuple[str, TestClient]]:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for worker API tests")

    schema_name = f"worker_api_test_{uuid.uuid4().hex}"
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


def test_worker_list_and_detail_derive_liveness_from_database_time(
    worker_api_environment: tuple[str, TestClient],
) -> None:
    schema_name, client = worker_api_environment
    database_url = os.environ["TEST_DATABASE_URL"]
    workers = [
        (f"active-{uuid.uuid4()}", "active", "100 milliseconds"),
        (f"stale-{uuid.uuid4()}", "stale", "500 milliseconds"),
        (f"dead-{uuid.uuid4()}", "dead", "900 milliseconds"),
    ]
    with psycopg.connect(
        database_url,
        autocommit=True,
        options=f"-csearch_path={schema_name}",
    ) as connection:
        ids = {}
        for instance_id, name, age in workers:
            ids[name] = str(
                connection.execute(
                    """
                    INSERT INTO workers (
                        instance_id,
                        name,
                        last_seen_at,
                        created_at,
                        metadata
                    )
                    VALUES (
                        %s,
                        %s,
                        clock_timestamp() - %s::interval,
                        clock_timestamp() - %s::interval - interval '1 second',
                        %s::jsonb
                    )
                    RETURNING id
                    """,
                    (instance_id, name, age, age, f'{{"group": "{name}"}}'),
                ).fetchone()[0]
            )

    response = client.get("/workers")
    assert response.status_code == 200
    listing = response.json()
    assert listing["limit"] == 50
    assert listing["offset"] == 0
    by_name = {item["name"]: item for item in listing["items"]}
    assert by_name["active"]["liveness"] == "ACTIVE"
    assert by_name["stale"]["liveness"] == "STALE"
    assert by_name["dead"]["liveness"] == "DEAD"
    assert by_name["active"]["metadata"] == {"group": "active"}
    assert by_name["active"]["heartbeat_age_seconds"] < 0.3

    detail = client.get(f"/workers/{ids['stale']}")
    assert detail.status_code == 200
    assert detail.json()["liveness"] == "STALE"
    assert detail.json()["id"] == ids["stale"]
    assert client.get(f"/workers/{uuid.uuid4()}").status_code == 404
    assert client.get("/workers", params={"limit": 101}).status_code == 422
