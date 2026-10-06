.PHONY: \
	up \
	down \
	restart \
	logs \
	ps \
	config \
	check \
	check-oltp \
	check-triggers \
	check-cdc \
	test \
	test-unit \
	test-integration \
	run-ingestion

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

check-oltp:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce \
		-f /sql/checks/oltp_check.sql

check-triggers:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce \
		-f /sql/checks/triggers_check.sql

check-cdc:
	./scripts/check-cdc-streaming.sh

check: check-oltp check-triggers check-cdc

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration

test: test-unit test-integration

run-ingestion:
	ecommerce-cdc
