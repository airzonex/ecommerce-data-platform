#!/usr/bin/bash

set -euo pipefail

if [[ ! -f .env ]]; then
	echo "ERROR: .env file not found"
	exit 1
fi

set -a
source .env
set +a

CONNECTOR_NAME="ecommerce-postgres-connector"
CONNECT_URL="http://localhost:${KAFKA_CONNECT_HOST_PORT}"

RESPONSE_FILE="$(mktemp)"

cleanup() {
	rm -f "${RESPONSE_FILE}"
}

trap cleanup EXIT


# =============================================================================
# Check Kafka Connect
# =============================================================================


echo "Waiting for Kafka Connect..."

KAFKA_CONNECT_READY=0

for _ in $(seq 1 30); do
	if curl \
		--fail \
		--silent \
		--show-error \
		"${CONNECT_URL}/" \
		>/dev/null 2>&1
	then
		KAFKA_CONNECT_READY=1
		break
	fi

	sleep 1
done

if (( KAFKA_CONNECT_READY == 1 )); then
	echo "Kafka Connect is available"
else
	echo "ERROR: Kafka Connect is not available at ${CONNECT_URL}" >&2
	exit 1
fi


# =============================================================================
# Check if connector exists
# =============================================================================


connector_exists() {
	local http_code
	local rc=0

	http_code="$(
		curl \
			--silent \
			--show-error \
			--output /dev/null \
			--write-out '%{http_code}' \
			"${CONNECT_URL}/connectors/${CONNECTOR_NAME}"
	)" || rc=$?

	if (( rc != 0 )); then
		echo "ERROR: failed to check Debezium connector" >&2
		return 2
	fi

	case "${http_code}" in
		200)
			return 0
			;;
		404)
			return 1
			;;
		*)
			echo \
				"ERROR: unexpected HTTP status while checking connector: ${http_code}" \
				>&2
			return 2
			;;
	esac
}


# =============================================================================
# Wait for connector to become running
# =============================================================================


wait_for_connector() {
	local status=""
	local attempts="${1:-30}"

	echo "Waiting for Debezium connector to become RUNNING..."

	for _ in $(seq 1 "${attempts}"); do
		status="$(
			curl \
				--fail \
				--silent \
				--show-error \
				"${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" \
				2>/dev/null
		)" || {
			sleep 1
			continue
		}

		if python - "${status}" <<'PY'
import json
import sys

status = json.loads(sys.argv[1])

connector_state = status["connector"]["state"]
tasks = status["tasks"]
task_states = [task["state"] for task in tasks]

if (
	connector_state == "RUNNING"
	and tasks
	and all(state == "RUNNING" for state in task_states)
):
	raise SystemExit(0)

raise SystemExit(1)
PY
		then
			echo "Debezium connector is RUNNING"

			python - "${status}" <<'PY'
import json
import sys

status = json.loads(sys.argv[1])

connector_state = status["connector"]["state"]
task_states = [task["state"] for task in status["tasks"]]

print(f"  connector:  {connector_state}")
print(f"  tasks:      {', '.join(task_states)}")
PY

			return 0
		fi

		sleep 1
	done

	echo "ERROR: Debezium connector did not become RUNNING" >&2

	curl \
		--silent \
		--show-error \
		"${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" \
		>&2 || true
	
	echo >&2
	return 1
}


# =============================================================================
# Skip registration if connector already exists
# =============================================================================


if connector_exists; then
	echo "Debezium connector '${CONNECTOR_NAME}' is already registered"
	wait_for_connector
	exit 0
else
	rc=$?

	if (( rc != 1 )); then
		exit "${rc}"
	fi
fi


# =============================================================================
# Build connector configuration
# =============================================================================


CONNECTOR_CONFIG="$(
	python - "${CONNECTOR_NAME}" <<'PY'
import json
import os
import sys

config = {
	"name": sys.argv[1],
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
		),

		"decimal.handling.mode": "string",
	},
}

print(json.dumps(config))
PY
)"


# =============================================================================
# Register connector
# =============================================================================


echo "Registering Debezium connector '${CONNECTOR_NAME}'..."

HTTP_CODE=""
CURL_RC=0

HTTP_CODE="$(
	curl \
		--silent \
		--show-error \
		--output "${RESPONSE_FILE}" \
		--write-out '%{http_code}' \
		-X POST \
		-H 'Content-Type: application/json' \
		--data-binary "${CONNECTOR_CONFIG}" \
		"${CONNECT_URL}/connectors"
)" || CURL_RC=$?


# =============================================================================
# Handle uncertain result
# =============================================================================


if (( CURL_RC != 0 )); then
	echo "WARNING: connector registration request failed; checking actual state..." >&2

	if connector_exists; then
		echo "Connector exists despite the failed client request; registration succeeded"
	else
		echo "ERROR: failed to register Debezium connector" >&2
		exit "${CURL_RC}"
	fi

elif [[ "${HTTP_CODE}" == "409" ]]; then
	# Another request may have created the connector between our existence check and POST
	if connector_exists; then
		echo "Debezium connector '${CONNECTOR_NAME}' is already registered"
	else
		echo "ERROR: Kafka Connect returned HTTP 409" >&2
		cat "${RESPONSE_FILE}" >&2
		echo >&2
		exit 1
	fi

elif [[ ! "${HTTP_CODE}" =~ ^2[0-9][0-9]$ ]]; then
	echo "ERROR: connector registration failed with HTTP ${HTTP_CODE}" >&2
	cat "${RESPONSE_FILE}" >&2
	echo >&2
	exit 1

else
	echo "Debezium connector registered"
fi


# =============================================================================
# Verify connector
# =============================================================================


wait_for_connector