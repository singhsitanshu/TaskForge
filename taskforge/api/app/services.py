import logging
from collections.abc import Sequence
from typing import Protocol
from uuid import UUID

from app.config import HeartbeatSettings
from app.domain import (
    NewTask,
    SubmitTaskResult,
    Task,
    TaskAttempt,
    TaskStatus,
    Worker,
    WorkerRecord,
)
from app.idempotency import idempotency_key_hash, submission_fingerprint
from app.liveness import classify_worker_liveness
from app.repositories import TaskRepository, WorkerRepository

logger = logging.getLogger(__name__)


class TaskMetrics(Protocol):
    def record_submission(self, outcome: str) -> None: ...

    def record_cancellation(self, outcome: str) -> None: ...

    def observe_task_total_latency(self, seconds: float) -> None: ...


class NoopTaskMetrics:
    def record_submission(self, outcome: str) -> None:
        pass

    def record_cancellation(self, outcome: str) -> None:
        pass

    def observe_task_total_latency(self, seconds: float) -> None:
        pass


class TaskNotFoundError(Exception):
    pass


class WorkerNotFoundError(Exception):
    pass


class TaskConflictError(Exception):
    def __init__(self, status: TaskStatus) -> None:
        self.status = status
        super().__init__(f"task in {status} state cannot be cancelled")


class IdempotencyKeyReuseError(Exception):
    pass


class TaskService:
    def __init__(
        self,
        repository: TaskRepository,
        metrics: TaskMetrics | None = None,
    ) -> None:
        self._repository = repository
        self._metrics = metrics or NoopTaskMetrics()

    async def submit(self, new_task: NewTask) -> SubmitTaskResult:
        fingerprint = (
            submission_fingerprint(new_task) if new_task.idempotency_key is not None else None
        )
        result = await self._repository.submit(new_task, fingerprint)
        if result.task.request_fingerprint != fingerprint:
            assert new_task.idempotency_key is not None
            logger.info(
                "task submission idempotency conflict",
                extra={
                    "event": "task_submission_idempotency_conflict",
                    "task_id": str(result.task.id),
                    "idempotency_key_hash": idempotency_key_hash(new_task.idempotency_key),
                    "request_fingerprint": fingerprint[:16],
                },
            )
            self._metrics.record_submission("idempotency_conflict")
            raise IdempotencyKeyReuseError

        if new_task.idempotency_key is not None:
            logger.info(
                "task submission created" if result.created else "task submission replayed",
                extra={
                    "event": (
                        "task_submission_created" if result.created else "task_submission_replayed"
                    ),
                    "task_id": str(result.task.id),
                    "idempotency_key_hash": idempotency_key_hash(new_task.idempotency_key),
                    "request_fingerprint": fingerprint[:16],
                },
            )
        self._metrics.record_submission("created" if result.created else "replayed")
        return result

    async def get(self, task_id: UUID) -> Task:
        task = await self._repository.get(task_id)
        if task is None:
            raise TaskNotFoundError
        return task

    async def list(
        self,
        *,
        status: TaskStatus | None,
        queue: str | None,
        limit: int,
        offset: int,
    ) -> Sequence[Task]:
        return await self._repository.list(
            status=status,
            queue=queue,
            limit=limit,
            offset=offset,
        )

    async def cancel(self, task_id: UUID) -> Task:
        cancelled = await self._repository.cancel_active(task_id)
        if cancelled is not None:
            self._metrics.record_cancellation("cancelled")
            if cancelled.completed_at is not None:
                self._metrics.observe_task_total_latency(
                    (cancelled.completed_at - cancelled.created_at).total_seconds()
                )
            return cancelled

        task = await self._repository.get(task_id)
        if task is None:
            self._metrics.record_cancellation("not_found")
            raise TaskNotFoundError
        if task.status is TaskStatus.CANCELLED:
            self._metrics.record_cancellation("already_cancelled")
            return task
        self._metrics.record_cancellation("conflict")
        raise TaskConflictError(task.status)

    async def list_attempts(self, task_id: UUID) -> Sequence[TaskAttempt]:
        if await self._repository.get(task_id) is None:
            raise TaskNotFoundError
        return await self._repository.list_attempts(task_id)


class WorkerService:
    def __init__(
        self,
        repository: WorkerRepository,
        heartbeat_settings: HeartbeatSettings,
    ) -> None:
        self._repository = repository
        self._heartbeat_settings = heartbeat_settings

    async def get(self, worker_id: UUID) -> Worker:
        worker = await self._repository.get(worker_id)
        if worker is None:
            raise WorkerNotFoundError
        return self._with_liveness(worker)

    async def list(self, *, limit: int, offset: int) -> Sequence[Worker]:
        workers = await self._repository.list(limit=limit, offset=offset)
        return [self._with_liveness(worker) for worker in workers]

    def _with_liveness(self, worker: WorkerRecord) -> Worker:
        liveness, heartbeat_age_seconds = classify_worker_liveness(
            last_heartbeat=worker.last_heartbeat,
            observed_at=worker.observed_at,
            settings=self._heartbeat_settings,
        )
        return Worker(
            id=worker.id,
            instance_id=worker.instance_id,
            name=worker.name,
            enabled=worker.enabled,
            metadata=worker.metadata,
            last_heartbeat=worker.last_heartbeat,
            registered_at=worker.registered_at,
            updated_at=worker.updated_at,
            liveness=liveness,
            heartbeat_age_seconds=heartbeat_age_seconds,
        )


__all__ = [
    "IdempotencyKeyReuseError",
    "TaskConflictError",
    "TaskNotFoundError",
    "TaskService",
    "WorkerNotFoundError",
    "WorkerService",
]
