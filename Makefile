.PHONY: \
	up \
	down \
	restart \
	logs \
	ps \
	config \
	check-oltp \
	check-triggers \
	check-cdc \
	check-ingestion \
	test \
	test-unit \
	test-integration \
	run-ingestion \
	prepare-test-oltp-db \
	check-db

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

check-cdc:
	./scripts/check-cdc-streaming.sh

check-ingestion:
	./scripts/check-ingestion.sh

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration

test: test-unit test-integration

run-ingestion:
	ecommerce-cdc
