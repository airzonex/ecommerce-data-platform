VENV ?= .venv
VENV_BIN := $(CURDIR)/$(VENV)/bin

export PATH := $(VENV_BIN):$(PATH)

.PHONY: \
	up \
	down \
	restart \
	logs \
	ps \
	config \
	check-venv \
	check-oltp \
	check-triggers \
	check-cdc \
	check-ingestion \
	test \
	test-unit \
	test-integration \
	run-ingestion \
	prepare-test-oltp-db \
	check-db \
	register-debezium-connector \
	check-dwh-e2e \
	start-ingestion-background \
	stop-ingestion-background \
	check-idempotency \
	dbt-build

check-venv:
	@test -x "$(VENV_BIN)/python" || \
		(echo "ERROR: virtual environment not found at $(VENV)" >&2; exit 1)

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d

logs:
	docker compose logs -f

ps:
	docker compose ps

config:
	docker compose config

prepare-test-oltp-db:
	./scripts/prepare_test_oltp_db.sh

check-oltp:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce_test \
		-v ON_ERROR_STOP=1 \
		-f /sql/checks/oltp_check.sql

check-triggers:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce_test \
		-v ON_ERROR_STOP=1 \
		-f /sql/checks/triggers_check.sql

check-db:
	$(MAKE) prepare-test-oltp-db
	$(MAKE) check-oltp
	$(MAKE) check-triggers

check-cdc: check-venv
	./scripts/check-cdc-streaming.sh

check-ingestion:
	./scripts/check-ingestion.sh

test-unit: check-venv
	pytest tests/unit

test-integration: check-venv
	pytest tests/integration

test: test-unit test-integration

run-ingestion: check-venv
	ecommerce-cdc

register-debezium-connector: check-venv
	./scripts/register-debezium-connector.sh

check-dwh-e2e: check-venv
	./scripts/check-e2e-cdc.sh

start-ingestion-background: check-venv
	./scripts/start-ingestion-background.sh

stop-ingestion-background:
	./scripts/stop-ingestion-background.sh

check-idempotency: check-venv
	./scripts/check-cdc-idempotency.sh

dbt-build: check-venv
	cd ecommerce_dwh && dbt build