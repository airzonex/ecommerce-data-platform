.PHONY: up down restart logs ps config check check_oltp check_triggers

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

check_oltp:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce \
		-f /sql/checks/oltp_check.sql

check_triggers:
	docker compose exec -T postgres-oltp \
		psql -U ecommerce -d ecommerce \
		-f /sql/checks/triggers_check.sql

check: check_oltp check_triggers
