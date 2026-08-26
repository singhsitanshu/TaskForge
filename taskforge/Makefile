.PHONY: build up down logs health readiness metrics lint format format-check test migrate-up migrate-down test-integration test-claims test-recovery test-retries test-idempotency test-api test-migrations test-worker test-heartbeats test-leases test-observability

build:
	docker compose build

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

health:
	@curl --fail --silent http://localhost:$${API_PORT:-8000}/healthz
	@docker compose exec -T scheduler wget --quiet --tries=1 --spider http://127.0.0.1:8080/healthz
	@docker compose exec -T worker wget --quiet --tries=1 --spider http://127.0.0.1:8080/healthz
	@curl --fail --silent http://localhost:$${WEB_PORT:-3000}/healthz
	@printf '\nAll HTTP services are healthy.\n'

readiness:
	@curl --fail --silent http://localhost:$${API_PORT:-8000}/readyz
	@docker compose exec -T scheduler wget --quiet --tries=1 --spider http://127.0.0.1:8080/readyz
	@docker compose exec -T worker wget --quiet --tries=1 --spider http://127.0.0.1:8080/readyz
	@printf '\nTaskForge processing services are ready.\n'

metrics:
	@curl --fail --silent http://localhost:$${API_PORT:-8000}/metrics >/dev/null
	@curl --fail --silent http://localhost:$${PROMETHEUS_PORT:-9090}/-/ready >/dev/null
	@curl --fail --silent http://localhost:$${GRAFANA_PORT:-3001}/api/health >/dev/null
	@printf 'API metrics, Prometheus, and Grafana are reachable.\n'

lint:
	cd api && python -m ruff check .
	cd scheduler && go vet ./...
	cd worker && go vet ./...
	cd web && npm run lint

format:
	cd api && python -m ruff check --fix . && python -m ruff format .
	cd scheduler && gofmt -w $$(find . -type f -name '*.go')
	cd worker && gofmt -w $$(find . -type f -name '*.go')
	cd web && npm run format

format-check:
	cd api && python -m ruff format --check .
	@files=$$(find scheduler worker -type f -name '*.go' -exec gofmt -l {} +); test -z "$$files" || (printf '%s\n' "$$files" && exit 1)
	cd web && npm run format:check

test:
	cd api && python -m pytest
	cd scheduler && go test ./...
	cd worker && go test ./...
	cd web && npm run build
	$(MAKE) test-integration
	$(MAKE) test-claims
	$(MAKE) test-recovery

migrate-up:
	@./scripts/migrate.sh up

migrate-down:
	@./scripts/migrate.sh down

test-integration:
	docker compose --profile test run --rm --build integration-tests

test-claims:
	docker compose --profile test run --rm --build claim-tests

test-recovery:
	docker compose --profile test run --rm --build recovery-tests

test-retries:
	docker compose --profile test run --rm --build claim-tests go test -race -v -count=1 ./internal/repository -run Retry
	docker compose --profile test run --rm --build recovery-tests go test -race -v -count=1 ./internal/repository -run RetryPromotion
	docker compose --profile test run --rm --build integration-tests pytest -q -s tests/test_recovery_e2e.py

test-idempotency:
	docker compose --profile test run --rm --build integration-tests pytest -q -s tests/test_idempotency_api.py

test-api:
	docker compose --profile test run --rm --build integration-tests pytest -q api/tests tests/test_task_api.py tests/test_idempotency_api.py

test-migrations:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_migrations.py

test-worker:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_worker_e2e.py

test-heartbeats:
	docker compose --profile test run --rm --build integration-tests pytest -q -s tests/test_worker_api.py tests/test_worker_heartbeat.py

test-leases:
	docker compose --profile test run --rm --build claim-tests
	docker compose --profile test run --rm --build integration-tests pytest -q -s tests/test_worker_heartbeat.py

test-observability:
	docker compose --profile test run --rm --build integration-tests pytest -q api/tests/test_metrics.py tests/test_monitoring_config.py tests/test_task_api.py tests/test_worker_e2e.py
	docker compose --profile test run --rm --build recovery-tests go test -race -v -count=1 ./internal/metrics ./internal/service ./internal/repository -run 'Metrics|Collector|Snapshot|Recovery|RetryPromotion'
