#!/usr/bin/bash

set -euo pipefail

CONNECTOR_NAME="ecommerce-postgres-connector"
TOPIC="ecommerce.public.products"
CONSUMER_GROUP="cdc-check-$(date +%s%N)"

OUTPUT_FILE="$(mktemp)"
CONSUMER_ERROR_FILE="$(mktemp)"

PRODUCT_ID=""

cleanup() {
    if [[ -n "${PRODUCT_ID}" ]]; then
        docker compose exec -T postgres-oltp \
            psql \
            -X \
            -U "${POSTGRES_OLTP_USER}" \
            -d "${POSTGRES_OLTP_DB}" \
            -v ON_ERROR_STOP=1 \
            -c "DELETE FROM products WHERE id = ${PRODUCT_ID};" \
            >/dev/null 2>&1 || true
    fi

    rm -f "${OUTPUT_FILE}" "${CONSUMER_ERROR_FILE}"
}

trap cleanup EXIT


# =============================================================================
# Load environment
# =============================================================================

if [[ ! -f .env ]]; then
    echo "ERROR: .env file not found"
    exit 1
fi

set -a
source .env
set +a


# =============================================================================
# Check connector
# =============================================================================

echo "Checking Debezium connector..."

CONNECTOR_STATUS="$(
    curl \
        --fail \
        --silent \
        "http://localhost:8083/connectors/${CONNECTOR_NAME}/status"
)"

python3 - "${CONNECTOR_STATUS}" <<'PY'
import json
import sys

status = json.loads(sys.argv[1])

connector_state = status["connector"]["state"]
task_states = [task["state"] for task in status["tasks"]]

if connector_state != "RUNNING":
    raise SystemExit(
        f"ERROR: connector state is {connector_state}"
    )

if any(state != "RUNNING" for state in task_states):
    raise SystemExit(
        f"ERROR: task states are {task_states}"
    )

print(" connector:  RUNNING")
print(" tasks:      RUNNING")
PY


# =============================================================================
# Check Kafka topic
# =============================================================================

echo "Checking Kafka topic..."

docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --topic "${TOPIC}" \
    --describe \
    >/dev/null

echo "  topic:  ${TOPIC}"


# =============================================================================
# Start Kafka consumer
# =============================================================================

echo "Starting Kafka consumer..."

(
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" \
        --group "${CONSUMER_GROUP}" \
        --consumer-property auto.offset.reset=latest \
        --property print.key=true \
        --property print.value=true \
        --max-messages 4 \
        --timeout-ms 15000 \
        >"${OUTPUT_FILE}" \
        2>"${CONSUMER_ERROR_FILE}"
) &

CONSUMER_PID=$!

# Give the consumer time to join the group and receive the latest offset
sleep 2


# =============================================================================
# INSERT
# =============================================================================

PRODUCT_NAME="CDC Check Product $(date +%s%N)"

echo "Executing INSERT..."

PRODUCT_ID="$(
    docker compose exec -T postgres-oltp \
        psql \
        -X \
        -A \
        -t \
        -q \
        -U "${POSTGRES_OLTP_USER}" \
        -d "${POSTGRES_OLTP_DB}" \
        -v ON_ERROR_STOP=1 \
        -c "
            INSERT INTO products (
                name,
                category,
                price    
            )
            VALUES (
                '${PRODUCT_NAME}',
                'CDC Check',
                999.99
            )
            RETURNING id;
        "
)"

PRODUCT_ID="$(echo "${PRODUCT_ID}" | xargs)"

if [[ -z "${PRODUCT_ID}" ]]; then
    echo "ERROR: failed to obtain inserted product id"
    exit 1
fi

echo "  product id: ${PRODUCT_ID}"


# =============================================================================
# UPDATE
# =============================================================================

echo "Executing UPDATE..."

docker compose exec -T postgres-oltp \
    psql \
    -X \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${POSTGRES_OLTP_DB}" \
    -v ON_ERROR_STOP=1 \
    -c "
        UPDATE products
        SET price = 1099.99
        WHERE id = ${PRODUCT_ID};
    " \
    >/dev/null


# =============================================================================
# DELETE
# =============================================================================

echo "Executing DELETE..."

docker compose exec -T postgres-oltp \
    psql \
    -X \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${POSTGRES_OLTP_DB}" \
    -v ON_ERROR_STOP=1 \
    -c "
        DELETE FROM products
        WHERE id = ${PRODUCT_ID};
    " \
    >/dev/null


# =============================================================================
# Wait for consumer
# =============================================================================

set +e
wait "${CONSUMER_PID}"
CONSUMER_RC=$?
set -e

if [[ ${CONSUMER_RC} -ne 0 && ${CONSUMER_RC} -ne 124 ]]; then
    echo "ERROR: Kafka consumer failed"
    cat "${CONSUMER_ERROR_FILE}"
    exit 1
fi


# =============================================================================
# Validate events
# =============================================================================

python3 - "${OUTPUT_FILE}" "${PRODUCT_ID}" <<'PY'
import json
import sys

output_file = sys.argv[1]
expected_id = int(sys.argv[2])

events = []

with open(output_file, "r", encoding="utf-8") as file:
    for line in file:
        line = line.strip()

        if not line:
            continue

        try:
            key_raw, value_raw = line.split("\t", 1)
            key = json.loads(key_raw)

            actual_id = key["payload"]["id"]

            if actual_id != expected_id:
                continue

            value_raw = value_raw.strip()

            if value_raw == "null":
                events.append(
                    {
                        "type": "tombstone",
                        "id": actual_id
                    }
                )
                continue
        
            value = json.loads(value_raw)
            payload = value["payload"]
            source = payload["source"]

            if source["table"] != "products":
                continue

            events.append(
                {
                    "type": "event",
                    "id": actual_id,
                    "op": payload["op"],
                    "snapshot": source["snapshot"]
                }
            )
        
        except (ValueError, KeyError, json.JSONDecodeError):
            continue

expected = [
    ("event", "c"),
    ("event", "u"),
    ("event", "d"),
    ("tombstone", None)
]

actual = [
    (event["type"], event.get("op"))
    for event in events
]

if actual != expected:
    print("CDC CHECK FAILED")
    print(f"Expected: {expected}")
    print(f"Actual: {actual}")

    print("\nCaptured events:")
    for event in events:
        print(event)
    
    raise SystemExit(1)

for event in events:
    if event["type"] == "event" and event["snapshot"] != "false":
        raise SystemExit(
            f"CDC CHECK FAILED: {event['op']} is a snapshot event"
        )

print()
print("CDC STREAMING CHECK PASSED")
print(" INSERT      ->  op=c")
print(" UPDATE      ->  op=u")
print(" DELETE      ->  op=d")
print(" TOMBSTONE   ->  value=null")
PY