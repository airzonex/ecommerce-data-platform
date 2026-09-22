#!/usr/bin/bash

set -euo pipefail

if [[ ! -f .env ]]; then
	echo "ERROR: .env file not found"
	exit 1
fi

set -a
source .env
set +a

python3 - <<'PY' | curl \
	--fail-with-body \
	--silent \
	--show-error \
	-X POST \
	-H 'Content-Type: application/json' \
	--data-binary @- \
	http://localhost:8083/connectors
import json
import os

config = {
	"name": "ecommerce-postgres-connector",
	"config": {
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",

		"database.hostname": "postgres-oltp",
		"database.port": "5432",
		"database.user": os.environ["DEBEZIUM_USER"],
		"database.password": os.environ["DEBEZIUM_PASS"],
		"database.dbname": os.environ["POSTGRES_OLTP_DB"],

		"topic.prefix": "ecommerce",

		"plugin.name": "pgoutput",

		"slot.name": "ecommerce_slot",
		"publication.name": "dbz_publication",
		"publication.autocreate.mode": "disabled",

		"snapshot.mode": "initial",

		"table.include.list": (
			"public.customers,"
			"public.products,"
			"public.orders,"
			"public.order_items,"
			"public.payments"
		)
	}
}

print(json.dumps(config))
PY
