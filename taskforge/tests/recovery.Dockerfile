FROM golang:1.23-bookworm

WORKDIR /workspace

COPY scheduler ./scheduler
COPY migrations ./migrations

WORKDIR /workspace/scheduler

CMD ["go", "test", "-race", "-v", "-count=1", "./..."]
