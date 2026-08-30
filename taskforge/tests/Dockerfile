FROM golang:1.23-alpine AS worker-build

WORKDIR /src
COPY worker/go.mod worker/go.sum ./
RUN go mod download
COPY worker/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-worker .
RUN CGO_ENABLED=0 GOOS=linux go build -tags tf005test -trimpath -ldflags="-s -w" -o /out/taskforge-claim-harness ./cmd/claim-harness

FROM golang:1.23-alpine AS scheduler-build

WORKDIR /src
COPY scheduler/go.mod scheduler/go.sum ./
RUN go mod download
COPY scheduler/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-scheduler .

FROM python:3.13-slim

WORKDIR /workspace

COPY api/requirements.txt ./api/requirements.txt
COPY tests/requirements.txt ./tests/requirements.txt
RUN pip install --no-cache-dir \
    --requirement api/requirements.txt \
    --requirement tests/requirements.txt

COPY api/app ./api/app
COPY api/tests ./api/tests
COPY worker/internal/metrics ./worker/internal/metrics
COPY scheduler/internal/metrics ./scheduler/internal/metrics
COPY monitoring ./monitoring
COPY --from=worker-build /out/taskforge-worker /usr/local/bin/taskforge-worker
COPY --from=worker-build /out/taskforge-claim-harness /usr/local/bin/taskforge-claim-harness
COPY --from=scheduler-build /out/taskforge-scheduler /usr/local/bin/taskforge-scheduler
COPY migrations ./migrations
COPY tests/test_migrations.py ./tests/test_migrations.py
COPY tests/test_task_api.py ./tests/test_task_api.py
COPY tests/test_console_api.py ./tests/test_console_api.py
COPY tests/test_idempotency_api.py ./tests/test_idempotency_api.py
COPY tests/test_worker_e2e.py ./tests/test_worker_e2e.py
COPY tests/test_claim_races.py ./tests/test_claim_races.py
COPY tests/test_worker_api.py ./tests/test_worker_api.py
COPY tests/test_worker_heartbeat.py ./tests/test_worker_heartbeat.py
COPY tests/test_recovery_e2e.py ./tests/test_recovery_e2e.py
COPY tests/test_monitoring_config.py ./tests/test_monitoring_config.py

ENV PYTHONPATH=/workspace/api

CMD ["pytest", "-q", "api/tests", "tests/test_migrations.py", "tests/test_task_api.py", "tests/test_idempotency_api.py", "tests/test_worker_e2e.py", "tests/test_claim_races.py", "tests/test_worker_api.py", "tests/test_worker_heartbeat.py", "tests/test_recovery_e2e.py", "tests/test_monitoring_config.py"]
