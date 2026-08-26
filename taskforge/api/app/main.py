import asyncio
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST

from app.config import HeartbeatSettings
from app.database import create_pool
from app.metrics import ApiMetrics
from app.repositories import PostgresTaskRepository, PostgresWorkerRepository
from app.routes import router as tasks_router
from app.services import TaskService, WorkerService
from app.worker_routes import router as workers_router


def create_app(
    database_url: str | None = None,
    database_connection_kwargs: dict[str, Any] | None = None,
    heartbeat_settings: HeartbeatSettings | None = None,
) -> FastAPI:
    metrics = ApiMetrics()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        resolved_database_url = database_url or os.getenv("DATABASE_URL")
        if not resolved_database_url:
            raise RuntimeError("DATABASE_URL is required")

        resolved_heartbeat_settings = heartbeat_settings or HeartbeatSettings.from_env()
        pool = create_pool(resolved_database_url, database_connection_kwargs)
        await pool.open(wait=True)
        application.state.pool = pool
        application.state.task_service = TaskService(
            PostgresTaskRepository(pool),
            metrics,
        )
        application.state.worker_service = WorkerService(
            PostgresWorkerRepository(pool),
            resolved_heartbeat_settings,
        )
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
    application.include_router(workers_router)

    @application.middleware("http")
    async def observe_http_requests(request: Request, call_next) -> Response:
        if request.url.path == "/metrics":
            return await call_next(request)
        started_at = time.perf_counter()
        response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
        try:
            response = await call_next(request)
            response_status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            metrics.observe_request(
                method=request.method,
                route=route_path,
                status_code=response_status,
                started_at=started_at,
            )

    @application.get("/healthz", tags=["operations"])
    async def healthcheck() -> dict[str, str]:
        return {"service": "api", "status": "ok"}

    @application.get("/readyz", tags=["operations"])
    async def readiness() -> Response:
        try:
            async with asyncio.timeout(1.0):
                async with application.state.pool.connection(timeout=1.0) as connection:
                    await connection.execute("SELECT 1")
        except Exception:
            return Response(
                content='{"service":"api","status":"not_ready"}',
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                media_type="application/json",
            )
        return Response(
            content='{"service":"api","status":"ready"}',
            media_type="application/json",
        )

    @application.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(
            content=metrics.render(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return application


app = create_app()
