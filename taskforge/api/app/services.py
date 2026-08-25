from collections.abc import Sequence
from uuid import UUID

from app.config import HeartbeatSettings
from app.domain import NewTask, Task, TaskAttempt, TaskStatus, Worker, WorkerRecord
from app.liveness import classify_worker_liveness
from app.repositories import DuplicateTaskError, TaskRepository, WorkerRepository


class TaskNotFoundError(Exception):
    pass


class WorkerNotFoundError(Exception):
    pass


class TaskConflictError(Exception):
    def __init__(self, status: TaskStatus) -> None:
        self.status = status
        super().__init__(f"task in {status} state cannot be cancelled")


class TaskService:
    def __init__(self, repository: TaskRepository) -> None:
        self._repository = repository

    async def submit(self, new_task: NewTask) -> Task:
        return await self._repository.create(new_task)

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
            return cancelled

        task = await self._repository.get(task_id)
        if task is None:
            raise TaskNotFoundError
        if task.status is TaskStatus.CANCELLED:
            return task
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
    "DuplicateTaskError",
    "TaskConflictError",
    "TaskNotFoundError",
    "TaskService",
    "WorkerNotFoundError",
    "WorkerService",
]
