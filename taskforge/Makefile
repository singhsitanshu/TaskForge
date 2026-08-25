.PHONY: build up down logs health lint format format-check test migrate-up migrate-down test-integration test-api test-migrations

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
	@curl --fail --silent http://localhost:$${WORKER_PORT:-8082}/healthz
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

migrate-up:
	docker compose up -d postgres
	docker compose exec -T postgres psql --set ON_ERROR_STOP=1 --username "$${POSTGRES_USER:-taskforge}" --dbname "$${POSTGRES_DB:-taskforge}" < migrations/000001_tasks_workers_attempts.up.sql

migrate-down:
	docker compose up -d postgres
	docker compose exec -T postgres psql --set ON_ERROR_STOP=1 --username "$${POSTGRES_USER:-taskforge}" --dbname "$${POSTGRES_DB:-taskforge}" < migrations/000001_tasks_workers_attempts.down.sql

test-integration:
	docker compose --profile test run --rm --build integration-tests

test-api:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_task_api.py

test-migrations:
	docker compose --profile test run --rm --build integration-tests pytest -q tests/test_migrations.py
