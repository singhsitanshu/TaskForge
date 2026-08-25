FROM golang:1.23-bookworm

WORKDIR /workspace

COPY worker ./worker
COPY migrations ./migrations

WORKDIR /workspace/worker

CMD ["go", "test", "-race", "-v", "-count=1", "./internal/repository"]
