import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from app.database import create_pool
from app.repositories import PostgresTaskRepository
from app.routes import router as tasks_router
from app.services import TaskService


def create_app(
    database_url: str | None = None,
    database_connection_kwargs: dict[str, Any] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        resolved_database_url = database_url or os.getenv("DATABASE_URL")
        if not resolved_database_url:
            raise RuntimeError("DATABASE_URL is required")

        pool = create_pool(resolved_database_url, database_connection_kwargs)
        await pool.open(wait=True)
        application.state.task_service = TaskService(PostgresTaskRepository(pool))
        try:
            yield
        finally:
            await pool.close()

    application = FastAPI(
        title="TaskForge API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(tasks_router)

    @application.get("/healthz", tags=["operations"])
    async def healthcheck() -> dict[str, str]:
        return {"service": "api", "status": "ok"}

    return application


app = create_app()
