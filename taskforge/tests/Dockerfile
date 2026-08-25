FROM golang:1.23-alpine AS worker-build

WORKDIR /src
COPY worker/go.mod worker/go.sum ./
RUN go mod download
COPY worker/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge-worker .
RUN CGO_ENABLED=0 GOOS=linux go build -tags tf005test -trimpath -ldflags="-s -w" -o /out/taskforge-claim-harness ./cmd/claim-harness

FROM python:3.13-slim

WORKDIR /workspace

COPY api/requirements.txt ./api/requirements.txt
COPY tests/requirements.txt ./tests/requirements.txt
RUN pip install --no-cache-dir \
    --requirement api/requirements.txt \
    --requirement tests/requirements.txt

COPY api/app ./api/app
COPY --from=worker-build /out/taskforge-worker /usr/local/bin/taskforge-worker
COPY --from=worker-build /out/taskforge-claim-harness /usr/local/bin/taskforge-claim-harness
COPY migrations ./migrations
COPY tests/test_migrations.py ./tests/test_migrations.py
COPY tests/test_task_api.py ./tests/test_task_api.py
COPY tests/test_worker_e2e.py ./tests/test_worker_e2e.py
COPY tests/test_claim_races.py ./tests/test_claim_races.py
COPY tests/test_worker_api.py ./tests/test_worker_api.py
COPY tests/test_worker_heartbeat.py ./tests/test_worker_heartbeat.py

ENV PYTHONPATH=/workspace/api

CMD ["pytest", "-q", "tests/test_migrations.py", "tests/test_task_api.py", "tests/test_worker_e2e.py", "tests/test_claim_races.py", "tests/test_worker_api.py", "tests/test_worker_heartbeat.py"]
