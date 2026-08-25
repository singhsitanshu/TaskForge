.PHONY: build up down logs health lint format format-check test migrate-up migrate-down test-integration test-claims test-api test-migrations test-worker test-heartbeats

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
	@curl --fail --silent http://localhost:$${SCHEDULER_PORT:-8081}/healthz
	@docker compose exec -T worker wget --quiet --tries=1 --spider http://127.0.0.1:8080/healthz
	@curl --fail --silent http://localhost:$${WEB_PORT:-3000}/healthz
	@printf '\nAll HTTP services are healthy.\n'

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

migrate-up:
	@./scripts/migrate.sh up

migrate-down:
	@./scripts/migrate.sh down

test-integration:
	docker compose --profile test run --rm --build integration-tests

test-claims:
	docker compose --profile test run --rm --build claim-tests

test-api:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_task_api.py

test-migrations:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_migrations.py

test-worker:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_worker_e2e.py

test-heartbeats:
	docker compose --profile test run --rm --build integration-tests pytest -q -s tests/test_worker_api.py tests/test_worker_heartbeat.py
