from collections.abc import Sequence
from typing import Any, Protocol
from uuid import UUID

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from app.domain import (
    ExceptionalAttempt,
    NewTask,
    SubmitTaskResult,
    Task,
    TaskAttempt,
    TaskAttemptStatus,
    TaskStatus,
    WorkerRecord,
)
from app.liveness import WorkerLiveness

_TASK_COLUMNS = """
    id,
    queue,
    task_type,
    payload,
    status,
    priority,
    max_attempts,
    attempt_count,
    scheduled_at,
    queued_at,
    claimed_by_worker_id,
    lease_expires_at,
    completed_at,
    result,
    last_error,
    idempotency_key,
    request_fingerprint,
    created_at,
    updated_at
"""

_TASK_ATTEMPT_COLUMNS = """
    id,
    task_id,
    worker_id,
    attempt_number,
    status,
    leased_at,
    queue_entered_at,
    scheduled_at_snapshot,
    started_at,
    finished_at,
    retry_scheduled_at,
    recovered_lease_expires_at,
    recovered_at,
    recovery_action,
    output,
    error,
    created_at,
    updated_at
"""


class TaskRepository(Protocol):
    async def submit(
        self,
        new_task: NewTask,
        request_fingerprint: str | None,
    ) -> SubmitTaskResult: ...

    async def get(self, task_id: UUID) -> Task | None: ...

    async def list(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        task_type: str | None,
        limit: int,
        offset: int,
    ) -> Sequence[Task]: ...

    async def count(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        task_type: str | None,
    ) -> int: ...

    async def count_by_status(self) -> dict[TaskStatus, int]: ...

    async def list_recent_exceptions(self, *, limit: int) -> Sequence[ExceptionalAttempt]: ...

    async def cancel_active(self, task_id: UUID) -> Task | None: ...

    async def list_attempts(self, task_id: UUID) -> Sequence[TaskAttempt]: ...


class WorkerRepository(Protocol):
    async def get(self, worker_id: UUID) -> WorkerRecord | None: ...

    async def list(self, *, limit: int, offset: int) -> Sequence[WorkerRecord]: ...

    async def count(self) -> int: ...

    async def count_by_liveness(
        self,
        *,
        stale_after_seconds: float,
        dead_after_seconds: float,
    ) -> dict[WorkerLiveness, int]: ...


class PostgresTaskRepository:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def submit(
        self,
        new_task: NewTask,
        request_fingerprint: str | None,
    ) -> SubmitTaskResult:
        query = f"""
            INSERT INTO tasks (
                queue,
                task_type,
                payload,
                status,
                priority,
                max_attempts,
                scheduled_at,
                idempotency_key,
                request_fingerprint
            )
            VALUES (%s, %s, %s, 'QUEUED', %s, %s, COALESCE(%s, now()), %s, %s)
            ON CONFLICT (idempotency_key)
                WHERE idempotency_key IS NOT NULL
                DO NOTHING
            RETURNING {_TASK_COLUMNS}
        """
        parameters = (
            new_task.queue,
            new_task.task_type,
            Jsonb(new_task.payload),
            new_task.priority,
            new_task.max_attempts,
            new_task.scheduled_at,
            new_task.idempotency_key,
            request_fingerprint,
        )

        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, parameters)
            row = await cursor.fetchone()
            if row is not None:
                return SubmitTaskResult(task=_task_from_row(row), created=True)

            assert new_task.idempotency_key is not None
            cursor = await connection.execute(
                f"SELECT {_TASK_COLUMNS} FROM tasks WHERE idempotency_key = %s",
                (new_task.idempotency_key,),
            )
            existing = await cursor.fetchone()
            if existing is None:
                raise RuntimeError("idempotency conflict winner was not visible")
            return SubmitTaskResult(task=_task_from_row(existing), created=False)

    async def get(self, task_id: UUID) -> Task | None:
        query = f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = %s"
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (task_id,))
            row = await cursor.fetchone()
        return _task_from_row(row) if row else None

    async def list(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        task_type: str | None,
        limit: int,
        offset: int,
    ) -> Sequence[Task]:
        conditions: list[str] = []
        parameters: list[object] = []

        if status is not None:
            conditions.append("status = %s")
            parameters.append(status.value)
        if queue is not None:
            conditions.append("queue = %s")
            parameters.append(queue)
        if task_type is not None:
            conditions.append("task_type = %s")
            parameters.append(task_type)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT {_TASK_COLUMNS}
            FROM tasks
            {where_clause}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
        """
        parameters.extend((limit, offset))

        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, parameters)
            rows = await cursor.fetchall()
        return [_task_from_row(row) for row in rows]

    async def count(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        task_type: str | None,
    ) -> int:
        conditions: list[str] = []
        parameters: list[object] = []
        if status is not None:
            conditions.append("status = %s")
            parameters.append(status.value)
        if queue is not None:
            conditions.append("queue = %s")
            parameters.append(queue)
        if task_type is not None:
            conditions.append("task_type = %s")
            parameters.append(task_type)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                f"SELECT count(*) AS total FROM tasks {where_clause}",
                parameters,
            )
            row = await cursor.fetchone()
        assert row is not None
        return int(row["total"])

    async def count_by_status(self) -> dict[TaskStatus, int]:
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT status::text AS status, count(*) AS total
                FROM tasks
                GROUP BY status
                """
            )
            rows = await cursor.fetchall()
        counts = {task_status: 0 for task_status in TaskStatus}
        counts.update({TaskStatus(row["status"]): int(row["total"]) for row in rows})
        return counts

    async def list_recent_exceptions(self, *, limit: int) -> Sequence[ExceptionalAttempt]:
        query = """
            SELECT
                ta.task_id,
                t.task_type,
                ta.attempt_number,
                ta.status,
                ta.worker_id,
                ta.error,
                ta.retry_scheduled_at,
                ta.recovered_at,
                ta.recovery_action,
                COALESCE(ta.recovered_at, ta.finished_at, ta.updated_at) AS occurred_at
            FROM task_attempts AS ta
            JOIN tasks AS t ON t.id = ta.task_id
            WHERE ta.status IN ('FAILED', 'ABANDONED')
            ORDER BY COALESCE(ta.recovered_at, ta.finished_at, ta.updated_at) DESC, ta.id DESC
            LIMIT %s
        """
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (limit,))
            rows = await cursor.fetchall()
        return [
            ExceptionalAttempt(
                task_id=row["task_id"],
                task_type=row["task_type"],
                attempt_number=row["attempt_number"],
                status=TaskAttemptStatus(row["status"]),
                worker_id=row["worker_id"],
                error=row["error"],
                retry_scheduled_at=row["retry_scheduled_at"],
                recovered_at=row["recovered_at"],
                recovery_action=row["recovery_action"],
                occurred_at=row["occurred_at"],
            )
            for row in rows
        ]

    async def cancel_active(self, task_id: UUID) -> Task | None:
        query = f"""
            UPDATE tasks
            SET
                status = 'CANCELLED',
                completed_at = GREATEST(statement_timestamp(), created_at),
                scheduled_at = GREATEST(statement_timestamp(), created_at)
            WHERE id = %s
              AND status IN ('QUEUED', 'RETRYING')
            RETURNING {_TASK_COLUMNS}
        """
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (task_id,))
            row = await cursor.fetchone()
        return _task_from_row(row) if row else None

    async def list_attempts(self, task_id: UUID) -> Sequence[TaskAttempt]:
        query = f"""
            SELECT {_TASK_ATTEMPT_COLUMNS}
            FROM task_attempts
            WHERE task_id = %s
            ORDER BY attempt_number ASC
        """
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (task_id,))
            rows = await cursor.fetchall()
        return [_task_attempt_from_row(row) for row in rows]


def _task_from_row(row: dict[str, Any]) -> Task:
    return Task(
        id=row["id"],
        queue=row["queue"],
        task_type=row["task_type"],
        payload=row["payload"],
        status=TaskStatus(row["status"]),
        priority=row["priority"],
        max_attempts=row["max_attempts"],
        attempt_count=row["attempt_count"],
        scheduled_at=row["scheduled_at"],
        queued_at=row["queued_at"],
        claimed_by_worker_id=row["claimed_by_worker_id"],
        lease_expires_at=row["lease_expires_at"],
        completed_at=row["completed_at"],
        result=row["result"],
        last_error=row["last_error"],
        idempotency_key=row["idempotency_key"],
        request_fingerprint=row["request_fingerprint"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _task_attempt_from_row(row: dict[str, Any]) -> TaskAttempt:
    return TaskAttempt(
        id=row["id"],
        task_id=row["task_id"],
        worker_id=row["worker_id"],
        attempt_number=row["attempt_number"],
        status=TaskAttemptStatus(row["status"]),
        leased_at=row["leased_at"],
        queue_entered_at=row["queue_entered_at"],
        scheduled_at_snapshot=row["scheduled_at_snapshot"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        retry_scheduled_at=row["retry_scheduled_at"],
        recovered_lease_expires_at=row["recovered_lease_expires_at"],
        recovered_at=row["recovered_at"],
        recovery_action=row["recovery_action"],
        output=row["output"],
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


_WORKER_COLUMNS = """
    id,
    instance_id,
    name,
    enabled,
    metadata,
    last_seen_at AS last_heartbeat,
    created_at AS registered_at,
    updated_at,
    statement_timestamp() AS observed_at
"""


class PostgresWorkerRepository:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def get(self, worker_id: UUID) -> WorkerRecord | None:
        query = f"SELECT {_WORKER_COLUMNS} FROM workers WHERE id = %s"
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (worker_id,))
            row = await cursor.fetchone()
        return _worker_from_row(row) if row else None

    async def list(self, *, limit: int, offset: int) -> Sequence[WorkerRecord]:
        query = f"""
            SELECT {_WORKER_COLUMNS}
            FROM workers
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
        """
        async with self._pool.connection() as connection:
            cursor = await connection.execute(query, (limit, offset))
            rows = await cursor.fetchall()
        return [_worker_from_row(row) for row in rows]

    async def count(self) -> int:
        async with self._pool.connection() as connection:
            cursor = await connection.execute("SELECT count(*) AS total FROM workers")
            row = await cursor.fetchone()
        assert row is not None
        return int(row["total"])

    async def count_by_liveness(
        self,
        *,
        stale_after_seconds: float,
        dead_after_seconds: float,
    ) -> dict[WorkerLiveness, int]:
        query = """
            SELECT
                count(*) FILTER (
                    WHERE last_seen_at IS NOT NULL
                      AND statement_timestamp() - last_seen_at <= %s * interval '1 second'
                ) AS active,
                count(*) FILTER (
                    WHERE last_seen_at IS NOT NULL
                      AND statement_timestamp() - last_seen_at > %s * interval '1 second'
                      AND statement_timestamp() - last_seen_at <= %s * interval '1 second'
                ) AS stale,
                count(*) FILTER (
                    WHERE last_seen_at IS NULL
                       OR statement_timestamp() - last_seen_at > %s * interval '1 second'
                ) AS dead
            FROM workers
        """
        async with self._pool.connection() as connection:
            cursor = await connection.execute(
                query,
                (
                    stale_after_seconds,
                    stale_after_seconds,
                    dead_after_seconds,
                    dead_after_seconds,
                ),
            )
            row = await cursor.fetchone()
        assert row is not None
        return {
            WorkerLiveness.ACTIVE: int(row["active"]),
            WorkerLiveness.STALE: int(row["stale"]),
            WorkerLiveness.DEAD: int(row["dead"]),
        }


def _worker_from_row(row: dict[str, Any]) -> WorkerRecord:
    return WorkerRecord(
        id=row["id"],
        instance_id=row["instance_id"],
        name=row["name"],
        enabled=row["enabled"],
        metadata=row["metadata"],
        last_heartbeat=row["last_heartbeat"],
        registered_at=row["registered_at"],
        updated_at=row["updated_at"],
        observed_at=row["observed_at"],
    )
