import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import errors, sql

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"
UP_SQL = "\n".join(path.read_text() for path in sorted(MIGRATIONS.glob("*.up.sql")))
DOWN_SQL = "\n".join(
    path.read_text() for path in sorted(MIGRATIONS.glob("*.down.sql"), reverse=True)
)
DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="TEST_DATABASE_URL is required for PostgreSQL migration tests",
)

EXPECTED_STATUSES = [
    "QUEUED",
    "LEASED",
    "RUNNING",
    "RETRYING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "ABANDONED",
]


def fetch_scalar(connection: psycopg.Connection, query: str, parameters=()):
    return connection.execute(query, parameters).fetchone()[0]


def test_migration_applies_constraints_indexes_and_rolls_back() -> None:
    assert DATABASE_URL is not None
    schema_name = f"migration_test_{uuid.uuid4().hex}"

    with psycopg.connect(DATABASE_URL, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )
        try:
            connection.execute(
                sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name))
            )
            connection.execute(UP_SQL)

            tables = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name IN (
                          'schema_migrations',
                          'tasks',
                          'workers',
                          'task_attempts'
                      )
                    """,
                    (schema_name,),
                )
            }
            assert tables == {"schema_migrations", "tasks", "workers", "task_attempts"}

            applied_versions = [
                row[0]
                for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            ]
            assert applied_versions == [
                "000001_tasks_workers_attempts",
                "000002_first_worker_claim",
                "000003_pre_tf005_remediation",
                "000004_task_leases",
                "000005_expired_lease_recovery",
                "000006_task_retries",
                "000007_submission_idempotency",
                "000008_observability_queue_time",
            ]

            statuses = [
                row[0]
                for row in connection.execute(
                    """
                    SELECT enumlabel
                    FROM pg_enum
                    WHERE enumtypid = 'task_status'::regtype
                    ORDER BY enumsortorder
                    """
                )
            ]
            assert statuses == EXPECTED_STATUSES

            indexes = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = %s
                    """,
                    (schema_name,),
                )
            }
            assert {
                "tasks_idempotency_key_idx",
                "tasks_dispatch_idx",
                "tasks_running_lease_idx",
                "tasks_status_updated_idx",
                "tasks_claimed_worker_idx",
                "tasks_claim_priority_idx",
                "workers_available_idx",
                "workers_name_idx",
                "task_attempts_finished_idx",
                "tasks_retry_due_idx",
            } <= indexes

            worker_id = fetch_scalar(
                connection,
                """
                INSERT INTO workers (instance_id, name)
                VALUES (%s, %s)
                RETURNING id
                """,
                ("migration-test-instance", "migration-test-worker"),
            )

            with pytest.raises(errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO workers (instance_id, name)
                    VALUES (%s, %s)
                    """,
                    ("migration-test-instance", "another-display-name"),
                )
            task_id = fetch_scalar(
                connection,
                """
                INSERT INTO tasks (queue, task_type, payload)
                VALUES ('default', 'test.noop', '{"input": "value"}')
                RETURNING id
                """,
            )

            connection.execute(
                """
                INSERT INTO tasks (
                    queue, task_type, idempotency_key, request_fingerprint
                )
                VALUES ('critical', 'test.noop', 'same-request', repeat('a', 64))
                """
            )
            with pytest.raises(errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (
                        queue, task_type, idempotency_key, request_fingerprint
                    )
                    VALUES ('another-queue', 'test.noop', 'same-request', repeat('a', 64))
                    """
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (task_type, idempotency_key)
                    VALUES ('missing-fingerprint', 'missing-fingerprint')
                    """
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (task_type, request_fingerprint)
                    VALUES ('missing-key', repeat('b', 64))
                    """
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (
                        task_type,
                        status,
                        claimed_by_worker_id,
                        lease_expires_at
                    )
                    VALUES ('invalid-lease', 'QUEUED', %s, now() + interval '1 minute')
                    """,
                    (worker_id,),
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (task_type, status, claimed_by_worker_id)
                    VALUES ('missing-running-lease', 'RUNNING', %s)
                    """,
                    (worker_id,),
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO tasks (task_type, status)
                    VALUES ('invalid-terminal', 'SUCCEEDED')
                    """,
                )

            connection.execute(
                """
                INSERT INTO task_attempts (
                    task_id,
                    worker_id,
                    attempt_number,
                    status
                )
                VALUES (%s, %s, 1, 'LEASED')
                """,
                (task_id, worker_id),
            )

            abandoned_task_id = fetch_scalar(
                connection,
                """
                INSERT INTO tasks (task_type)
                VALUES ('abandoned-attempt-constraint')
                RETURNING id
                """,
            )
            connection.execute(
                """
                INSERT INTO task_attempts (
                    task_id,
                    worker_id,
                    attempt_number,
                    status,
                    started_at,
                    finished_at,
                    error
                )
                VALUES (
                    %s, %s, 1, 'ABANDONED',
                    clock_timestamp(), clock_timestamp(), 'lease_expired'
                )
                """,
                (abandoned_task_id, worker_id),
            )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE tasks
                    SET status = 'ABANDONED'
                    WHERE id = %s
                    """,
                    (abandoned_task_id,),
                )

            with pytest.raises(errors.UniqueViolation):
                connection.execute(
                    """
                    INSERT INTO task_attempts (
                        task_id,
                        worker_id,
                        attempt_number,
                        status
                    )
                    VALUES (%s, %s, 1, 'LEASED')
                    """,
                    (task_id, worker_id),
                )

            with pytest.raises(errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO task_attempts (
                        task_id,
                        worker_id,
                        attempt_number,
                        status
                    )
                    VALUES (%s, %s, 2, 'QUEUED')
                    """,
                    (task_id, worker_id),
                )

            with pytest.raises(errors.ForeignKeyViolation):
                connection.execute(
                    """
                    INSERT INTO task_attempts (
                        task_id,
                        worker_id,
                        attempt_number,
                        status
                    )
                    VALUES (%s, %s, 2, 'LEASED')
                    """,
                    (task_id, uuid.uuid4()),
                )
            connection.execute(DOWN_SQL)

            assert fetch_scalar(connection, "SELECT to_regclass('tasks')") is None
            assert fetch_scalar(connection, "SELECT to_regclass('workers')") is None
            assert (
                fetch_scalar(connection, "SELECT to_regclass('task_attempts')") is None
            )
            assert (
                fetch_scalar(connection, "SELECT to_regclass('schema_migrations')")
                is None
            )
            assert fetch_scalar(connection, "SELECT to_regtype('task_status')") is None
            assert (
                fetch_scalar(connection, "SELECT to_regprocedure('set_updated_at()')")
                is None
            )
        finally:
            # The happy path already exercises the complete rollback. On failure,
            # discard any open migration transaction and let the temporary schema
            # provide deterministic cleanup.
            connection.rollback()
            connection.execute("SET search_path TO public")
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )
