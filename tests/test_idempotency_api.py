import os
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
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
        pytest.skip("TEST_DATABASE_URL is required for idempotency integration tests")

    schema_name = f"idempotency_test_{uuid.uuid4().hex}"
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
    application = create_app(
        os.environ["TEST_DATABASE_URL"],
        database_connection_kwargs={"options": f"-csearch_path={database_schema}"},
    )
    with TestClient(application) as client:
        yield client


def database_connection(database_schema: str) -> psycopg.Connection:
    return psycopg.connect(
        os.environ["TEST_DATABASE_URL"],
        autocommit=True,
        options=f"-csearch_path={database_schema}",
    )


def keyed_headers(key: str) -> dict[str, str]:
    return {"Idempotency-Key": key}


def request_body(**overrides: object) -> dict:
    body = {
        "task_type": "test.echo",
        "queue": "default",
        "payload": {"nested": {"a": 1, "b": 2}, "text": "café"},
        "priority": 10,
        "max_attempts": 3,
    }
    body.update(overrides)
    return body


def test_first_submission_replay_and_response_loss(
    api_client: TestClient,
    database_schema: str,
) -> None:
    key = f"response-loss-{uuid.uuid4().hex}"
    body = request_body()
    first = api_client.post("/tasks", headers=keyed_headers(key), json=body)
    assert first.status_code == 201

    replay = api_client.post(
        "/tasks",
        headers=keyed_headers(key),
        json={**body, "payload": {"text": "café", "nested": {"b": 2, "a": 1}}},
    )
    assert replay.status_code == 200
    assert replay.json() == first.json()

    with database_connection(database_schema) as connection:
        row = connection.execute(
            """
            SELECT count(*), min(length(request_fingerprint))
            FROM tasks
            WHERE idempotency_key = %s
            """,
            (key,),
        ).fetchone()
    assert row == (1, 64)


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("payload", {"different": True}),
        ("task_type", "test.other"),
        ("priority", 11),
        ("max_attempts", 4),
        ("queue", "other"),
    ],
)
def test_same_key_different_semantics_returns_stable_conflict(
    api_client: TestClient,
    changed_field: str,
    changed_value: object,
) -> None:
    key = f"semantic-conflict-{changed_field}-{uuid.uuid4().hex}"
    assert (
        api_client.post(
            "/tasks", headers=keyed_headers(key), json=request_body()
        ).status_code
        == 201
    )
    conflict = api_client.post(
        "/tasks",
        headers=keyed_headers(key),
        json=request_body(**{changed_field: changed_value}),
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == {
        "code": "IDEMPOTENCY_KEY_REUSE",
        "message": "idempotency key was already used for another submission",
    }


@pytest.mark.parametrize(
    "key",
    ["", "   ", "contains whitespace", "unsafe@character", "x" * 256],
)
def test_key_validation(api_client: TestClient, key: str) -> None:
    response = api_client.post(
        "/tasks",
        headers=keyed_headers(key),
        json=request_body(),
    )
    assert response.status_code == 422


def test_non_ascii_deprecated_body_key_is_rejected(api_client: TestClient) -> None:
    response = api_client.post(
        "/tasks",
        json=request_body(idempotency_key="é"),
    )
    assert response.status_code == 422


def test_header_and_deprecated_body_key_must_match(api_client: TestClient) -> None:
    response = api_client.post(
        "/tasks",
        headers=keyed_headers("header-key"),
        json=request_body(idempotency_key="body-key"),
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "IDEMPOTENCY_KEY_MISMATCH"


def test_keys_are_case_sensitive(api_client: TestClient) -> None:
    key = f"CaseSensitive-{uuid.uuid4().hex}"
    upper = api_client.post("/tasks", headers=keyed_headers(key), json=request_body())
    lower = api_client.post(
        "/tasks", headers=keyed_headers(key.lower()), json=request_body()
    )
    assert upper.status_code == lower.status_code == 201
    assert upper.json()["id"] != lower.json()["id"]


@pytest.mark.parametrize(
    "lifecycle_status",
    ["QUEUED", "RUNNING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELLED"],
)
def test_replay_returns_same_task_in_every_lifecycle_state(
    api_client: TestClient,
    database_schema: str,
    lifecycle_status: str,
) -> None:
    key = f"lifecycle-{lifecycle_status.lower()}-{uuid.uuid4().hex}"
    body = request_body()
    created = api_client.post("/tasks", headers=keyed_headers(key), json=body).json()

    with database_connection(database_schema) as connection:
        if lifecycle_status == "RUNNING":
            worker_id = connection.execute(
                """
                INSERT INTO workers (instance_id, name)
                VALUES (%s, %s)
                RETURNING id
                """,
                (f"instance-{uuid.uuid4().hex}", f"worker-{uuid.uuid4().hex}"),
            ).fetchone()[0]
            connection.execute(
                """
                UPDATE tasks
                SET status = %s,
                    claimed_by_worker_id = %s,
                    lease_expires_at = clock_timestamp() + interval '1 minute',
                    attempt_count = 1
                WHERE id = %s
                """,
                (lifecycle_status, worker_id, created["id"]),
            )
        elif lifecycle_status == "RETRYING":
            connection.execute(
                """
                UPDATE tasks
                SET status = 'RETRYING', attempt_count = 1,
                    scheduled_at = clock_timestamp() + interval '1 minute'
                WHERE id = %s
                """,
                (created["id"],),
            )
        elif lifecycle_status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            connection.execute(
                """
                UPDATE tasks
                SET status = %s, completed_at = clock_timestamp()
                WHERE id = %s
                """,
                (lifecycle_status, created["id"]),
            )

    replay = api_client.post("/tasks", headers=keyed_headers(key), json=body)
    assert replay.status_code == 200
    assert replay.json()["id"] == created["id"]
    assert replay.json()["status"] == lifecycle_status


def test_cancelled_replay_does_not_create_replacement(api_client: TestClient) -> None:
    key = f"cancelled-{uuid.uuid4().hex}"
    body = request_body()
    task = api_client.post("/tasks", headers=keyed_headers(key), json=body).json()
    assert api_client.post(f"/tasks/{task['id']}/cancel").status_code == 200
    replay = api_client.post("/tasks", headers=keyed_headers(key), json=body)
    assert replay.status_code == 200
    assert replay.json()["id"] == task["id"]
    assert replay.json()["status"] == "CANCELLED"


def test_keyless_identical_submissions_remain_distinct(api_client: TestClient) -> None:
    responses = [api_client.post("/tasks", json=request_body()) for _ in range(20)]
    assert all(response.status_code == 201 for response in responses)
    assert len({response.json()["id"] for response in responses}) == 20


def concurrent_posts(
    api_client: TestClient,
    requests: list[tuple[dict[str, str], dict]],
) -> list:
    barrier = threading.Barrier(len(requests))

    def submit(item: tuple[dict[str, str], dict]):
        barrier.wait(timeout=10)
        headers, body = item
        return api_client.post("/tasks", headers=headers, json=body)

    with ThreadPoolExecutor(max_workers=len(requests)) as executor:
        return list(executor.map(submit, requests))


@pytest.mark.parametrize("round_number", range(3))
def test_100_way_identical_contention(
    api_client: TestClient,
    database_schema: str,
    round_number: int,
) -> None:
    key = f"contention-{round_number}-{uuid.uuid4().hex}"
    requests = [(keyed_headers(key), request_body()) for _ in range(100)]
    responses = concurrent_posts(api_client, requests)
    statuses = [response.status_code for response in responses]
    ids = {response.json()["id"] for response in responses}

    assert statuses.count(201) == 1
    assert statuses.count(200) == 99
    assert set(statuses) == {200, 201}
    assert len(ids) == 1
    with database_connection(database_schema) as connection:
        task_count = connection.execute(
            "SELECT count(*) FROM tasks WHERE idempotency_key = %s", (key,)
        ).fetchone()[0]
    assert task_count == 1
    print(
        f"IDEMPOTENCY_CONTENTION round={round_number + 1} requests=100 "
        "tasks=1 distinct_ids=1 created=1 replayed=99 errors=0"
    )


def test_mixed_fingerprint_contention(
    api_client: TestClient,
    database_schema: str,
) -> None:
    key = f"mixed-contention-{uuid.uuid4().hex}"
    body_a = request_body(payload={"winner": "A"})
    body_b = request_body(payload={"winner": "B"})
    requests = [
        *((keyed_headers(key), body_a) for _ in range(50)),
        *((keyed_headers(key), body_b) for _ in range(50)),
    ]
    responses = concurrent_posts(api_client, requests)
    statuses = [response.status_code for response in responses]

    assert statuses.count(201) == 1
    assert statuses.count(200) == 49
    assert statuses.count(409) == 50
    assert set(statuses) == {200, 201, 409}
    successful_ids = {
        response.json()["id"]
        for response in responses
        if response.status_code in {200, 201}
    }
    assert len(successful_ids) == 1
    with database_connection(database_schema) as connection:
        row = connection.execute(
            """
            SELECT count(*), min(payload->>'winner'), min(request_fingerprint)
            FROM tasks
            WHERE idempotency_key = %s
            """,
            (key,),
        ).fetchone()
    assert row[0] == 1
    assert row[1] in {"A", "B"}
    assert len(row[2]) == 64
    print(
        f"IDEMPOTENCY_MIXED requests_a=50 requests_b=50 winner={row[1]} "
        "tasks=1 successful=50 conflicts=50 unexpected=0"
    )


def test_transaction_rollback_leaves_no_ghost_reservation(
    api_client: TestClient,
    database_schema: str,
) -> None:
    key = f"rollback-{uuid.uuid4().hex}"
    with database_connection(database_schema) as connection:
        connection.execute("CREATE SEQUENCE idempotency_fail_once")
        connection.execute(
            """
            CREATE FUNCTION fail_first_task_commit() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                IF nextval('idempotency_fail_once') = 1 THEN
                    RAISE EXCEPTION 'injected post-insert commit failure';
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        connection.execute(
            """
            CREATE CONSTRAINT TRIGGER fail_first_task_commit_trigger
            AFTER INSERT ON tasks
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION fail_first_task_commit()
            """
        )

    with pytest.raises(
        psycopg.errors.RaiseException,
        match="injected post-insert commit failure",
    ):
        api_client.post("/tasks", headers=keyed_headers(key), json=request_body())

    retry = api_client.post("/tasks", headers=keyed_headers(key), json=request_body())
    assert retry.status_code == 201
    with database_connection(database_schema) as connection:
        count = connection.execute(
            "SELECT count(*) FROM tasks WHERE idempotency_key = %s", (key,)
        ).fetchone()[0]
        connection.execute("DROP TRIGGER fail_first_task_commit_trigger ON tasks")
        connection.execute("DROP FUNCTION fail_first_task_commit()")
        connection.execute("DROP SEQUENCE idempotency_fail_once")
    assert count == 1


def test_idempotency_lookup_uses_partial_unique_index(
    database_schema: str,
) -> None:
    with database_connection(database_schema) as connection:
        connection.execute(
            """
            INSERT INTO tasks (task_type, payload)
            SELECT 'query-plan-noop', jsonb_build_object('sequence', value)
            FROM generate_series(1, 20000) AS value
            """
        )
        connection.execute("ANALYZE tasks")
        plan = "\n".join(
            row[0]
            for row in connection.execute(
                """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT id
                FROM tasks
                WHERE idempotency_key = 'query-plan-target'
                """
            )
        )
    assert "tasks_idempotency_key_idx" in plan
    print(f"IDEMPOTENCY_QUERY_PLAN\n{plan}")
