from fastapi import FastAPI

app = FastAPI(title="TaskForge API", version="0.1.0")


@app.get("/healthz", tags=["operations"])
async def healthcheck() -> dict[str, str]:
    """Report process liveness without exercising task functionality."""
    return {"service": "api", "status": "ok"}

