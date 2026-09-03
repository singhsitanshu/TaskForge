# TaskForge Local Demo

The demo tooling exercises the normal TaskForge path: API submission, PostgreSQL durability,
worker execution, scheduler retry/recovery, attempt persistence, the Web Console, and the existing
Prometheus/Grafana stack. It does not insert demo rows directly or use the benchmark harness.

## Start

Requirements: Docker with Compose, Make, and Python 3.11 or newer.

```bash
cp .env.example .env
make up
make demo
```

`make up` starts PostgreSQL, applies all pending migrations, starts the complete Compose stack, and
waits for readiness. No first-run database command is required beyond `make up`.

The `.env.example` browser URLs are:

- Web Console: <http://localhost:3000>
- API: <http://localhost:8000>
- API documentation: <http://localhost:8000/docs>
- Grafana: <http://localhost:3001>
- Prometheus: <http://localhost:9090>

Grafana is provisioned with its Prometheus data source and TaskForge dashboard. The local example
credentials are `admin` / `taskforge`; override them in `.env`. Run `make demo-status` for actual
resolved URLs when using custom ports.

## Commands

### `make demo`

Checks the API/database path, scheduler, and active worker count before submitting work. It creates:

1. a 500 ms `test.sleep` task that must succeed in exactly one attempt;
2. a `test.fail_n_then_succeed` task that must produce exactly `FAILED → SUCCEEDED`.

The command polls with explicit timeouts, validates the durable API history, and prints direct Web
Console links. `make demo DEMO_JSON=1` provides a small machine-readable summary.

### `make demo-data`

Creates 16 tasks: ten no-op successes with varied priority, three varied-duration successes, two
fail-once retry successes, and one intentional terminal failure. This is bounded demonstration data,
not a performance workload. Each run has a unique run ID and unique `tf014-data-*` idempotency keys,
so repeated runs create a new deterministic logical dataset.

An empty Web Console is intentional on a fresh database. `make demo-data` immediately populates its
overview, task list, task details, attempt histories, and worker views with real state.

### `make demo-recovery`

This opt-in local command:

1. refuses non-loopback API URLs and remote Docker endpoints;
2. submits one 15-second sleep task and waits for `RUNNING`;
3. resolves the attempt's worker UUID through `/workers`;
4. maps the worker hostname to exactly one Compose worker container;
5. hard-kills that exact container, restores it, and waits for the real lease to expire;
6. verifies the captured attempt is `ABANDONED` and a later attempt is `SUCCEEDED`.

The wait is derived from the configured lease and scheduler scan intervals. It does not alter either
setting. The output distinguishes the recorded lease expiration from completion of final recovery.
The worker restart policy is restored and the worker container is started immediately after failure
injection. A separate opt-in smoke entry point is available as `make test-demo-recovery`.

### Status and reset

```bash
make demo-status
make demo-reset
```

Status checks the evidence each service actually exposes and uses `UNKNOWN` if the worker API cannot
be queried. Demo tasks are durable by design. TaskForge has no deletion endpoint, so selective cleanup
is not invented for this ticket. `make demo-reset` prints the guarded complete-development reset:

```bash
make dev-reset CONFIRM=1
make up
```

The reset removes all local TaskForge Compose volumes, not only task rows.

## Stop

```bash
make down
```

## Troubleshooting

### No active workers

Run `make demo-status` and `docker compose ps`. Then inspect `docker compose logs worker`. `make up`
reconciles and waits for the complete local stack.

### API unavailable

The default readiness URL is <http://localhost:8000/readyz>. Check the configured `API_PORT`, then run
`docker compose logs api postgres` and `make up`.

### Grafana has no data

Run `make demo` or `make demo-data`, verify Prometheus is ready, and inspect Prometheus targets at
<http://localhost:9090/targets>. Select a recent Grafana time range; scraping occurs every five seconds.

### Recovery does not complete

The command prints the task ID, owner, and lease expiration before failure injection. Open its direct
task link, then run:

```bash
make demo-status
docker compose logs scheduler worker
```

Do not edit PostgreSQL task state manually. The normal local lease is 30 seconds and the scheduler
scan interval is 5 seconds, so the intentional wait after worker death is expected.
