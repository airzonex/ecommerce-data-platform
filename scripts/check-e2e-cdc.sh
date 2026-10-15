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
KAFKA_GROUP_ID="ecommerce-dwh-ingestion"
KAFKA_TOPIC_PRODUCTS="ecommerce.public.products"
KAFKA_TOPIC_CUSTOMERS="ecommerce.public.customers"
KAFKA_TOPIC_ORDERS="ecommerce.public.orders"
KAFKA_TOPIC_ORDER_ITEMS="ecommerce.public.order_items"
KAFKA_TOPIC_PAYMENTS="ecommerce.public.payments"


# =============================================================================
# Warning prompt
# =============================================================================


if [[ "${FORCE_E2E_CHECK:-0}" != "1" ]]; then
    echo "WARNING: This E2E check modifies the main OLTP database."
    echo "It will insert, update, and delete test data."
    echo "Debezium will publish generated CDC events to Kafka."
    echo "These events will propagate to DWH and dbt models will be rebuilt."
    echo

    read -r -p "Type 'yes' to continue: " answer

    if [[ "${answer}" != "yes" ]]; then
        echo "E2E check cancelled."
        exit 0
    fi
fi


# =============================================================================
# Check Debezium connector
# =============================================================================


check_debezium_connector() {
    local connector_status=""

    echo
    echo "Checking Kafka connect..."

    if ! curl \
        --fail \
        --silent \
        --show-error \
        "${CONNECT_URL}/" \
        >/dev/null
    then
        echo "ERROR: Kafka Connect is not available" >&2
        echo "Run: make up" >&2
        return 1
    fi

    echo "Kafka Connect is OK"

    echo
    echo "Checking Debezium connector..."

    if ! connector_status="$(
        curl \
        --fail \
        --silent \
        --show-error \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
    )"; then
        echo "ERROR: Debezium connector '${CONNECTOR_NAME}' is not registered" >&2
        echo "Run: make register-debezium-connector" >&2
        return 1
    fi

    if ! python3 - "${connector_status}" <<'PY'
import json
import sys

status = json.loads(sys.argv[1])

connector_state = status["connector"]["state"]
tasks = status["tasks"]
task_states = [task["state"] for task in tasks]

if connector_state != "RUNNING":
    print(
        f"ERROR: Debezium connector state is {connector_state}",
        file=sys.stderr,
    )
    raise SystemExit(1)

if not tasks:
    print(
        "ERROR: Debezium connector has no tasks",
        file=sys.stderr,
    )
    raise SystemExit(1)

if any(state != "RUNNING" for state in task_states):
    print(
        f"ERROR: Debezium task states are {task_states}",
        file=sys.stderr,
    )
    raise SystemExit(1)

print("Debezium connector is OK")
print(f"  connector:    {connector_state}")
print(f"  tasks:        {', '.join(task_states)}")
PY
    then
        return 1
    fi
}


# =============================================================================
# Check DWH ingestion consumer
# =============================================================================


check_ingestion_consumer() {
    local group_status=""
    local group_status_rc=0

    echo
    echo "Checking CDC ingestion consumer..."

    set +e

    group_status="$(
        docker compose exec -T kafka \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:29092 \
            --describe \
            --group "${KAFKA_GROUP_ID}" \
            2>&1
    )"

    group_status_rc=$?

    set -e

    if (( group_status_rc != 0 )); then
        if grep -qiE \
            "GroupIdNotFoundException|group .* not found|does not exist" \
            <<< "${group_status}"
        then
            echo "ERROR: CDC ingestion consumer group does not exist" >&2
            echo "Run: make run-ingestion" >&2
            return 1
        fi

        echo "ERROR: failed to get Kafka consumer group status" >&2
        echo "${group_status}" >&2
        return 1
    fi

    if grep -qiE \
        "has no active members|does not exist|GroupIdNotFoundException|group .* not found" \
        <<< "${group_status}"
    then
        echo "ERROR: CDC ingestion consumer is not running" >&2
        echo "Run: make run-ingestion" >&2
        return 1
    fi

    echo "CDC ingestion consumer is OK"
}


# =============================================================================
# Preflight checks
# =============================================================================


check_debezium_connector || exit 1
check_ingestion_consumer || exit 1


# =============================================================================
# Initial OLTP setup
# =============================================================================


echo
echo "Initial OLTP setup..."

RUN_ID="e2e_$(date +%s)_$$"
CUSTOMER_EMAIL="${RUN_ID}@gmail.com"

IDS="$(
    docker compose exec -T postgres-oltp \
        psql -XqAt \
        -F '|' \
        -U "${POSTGRES_OLTP_USER}" \
        -d "${POSTGRES_OLTP_DB}" \
        -v ON_ERROR_STOP=1 \
        -v customer_email="${CUSTOMER_EMAIL}" \
        -f /sql/e2e/oltp_setup.sql
)"

IFS='|' read -r CUSTOMER_ID PRODUCT_ID ORDER_ID ORDER_ITEM_ID <<< "${IDS}"

if [[ -z "${CUSTOMER_ID}" ]]; then
    echo "ERROR: failed to obtain inserted customer id"
    exit 1
fi

if [[ -z "${PRODUCT_ID}" ]]; then
    echo "ERROR: failed to obtain inserted product id"
    exit 1
fi

if [[ -z "${ORDER_ID}" ]]; then
    echo "ERROR: failed to obtain inserted order id"
    exit 1
fi

if [[ -z "${ORDER_ITEM_ID}" ]]; then
    echo "ERROR: failed to obtain inserted order item id"
    exit 1
fi

echo "  customer id:    ${CUSTOMER_ID}"
echo "  product id:     ${PRODUCT_ID}"
echo "  order id:       ${ORDER_ID}"
echo "  order item id:  ${ORDER_ITEM_ID}"


# =============================================================================
# Wait for DWH function
# =============================================================================


wait_for_dwh() {
    local query="$1"
    local attempts="${2:-30}"
    local actual=""
    
    echo
    echo "Waiting for events in DWH..."

    for _ in $(seq 1 "$attempts"); do
        if ! actual="$(
            docker compose exec -T postgres-dwh \
                psql -XqAt \
                -U "${POSTGRES_DWH_USER}" \
                -d "${POSTGRES_DWH_DB}" \
                -v ON_ERROR_STOP=1 \
                -c "${query}"
        )"; then
            echo "ERROR: failed to query DWH" >&2
            return 1
        fi

        if [[ "${actual}" == "t" ]]; then
            echo "CDC events were found in DWH"
            return 0
        fi

        sleep 1
    done

    echo
    echo "ERROR: not all events found in raw.cdc_events" >&2
    return 1
}


# =============================================================================
# Wait for DWH
# =============================================================================


QUERY="
    SELECT 
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_PRODUCTS}'
                AND op = 'c'
                AND event_key ->> 'id' = '${PRODUCT_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_CUSTOMERS}'
                AND op = 'c'
                AND event_key ->> 'id' = '${CUSTOMER_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_ORDERS}'
                AND op = 'c'
                AND event_key ->> 'id' = '${ORDER_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_ORDER_ITEMS}'
                AND op = 'c'
                AND event_key ->> 'id' = '${ORDER_ITEM_ID}');
"

wait_for_dwh "${QUERY}" || exit 1


# =============================================================================
# Build dbt models function
# =============================================================================


build_models() {
    local output
    local rc=0

    echo
    echo "Building dbt models..."

    output="$( { cd ecommerce_dwh && dbt build; } 2>&1 )" || rc=$?

    if (( rc != 0 )); then
        echo "ERROR: failed to build models (exit code ${rc})" >&2
        echo "${output}" >&2
        return 1
    fi

    echo "Dbt models were built"
}


# =============================================================================
# Build models
# =============================================================================


build_models || exit 1


# =============================================================================
# Initial DWH check
# =============================================================================


echo
echo "Initial DWH check..."

docker compose exec -T postgres-dwh \
    psql -XqAt \
    -U "${POSTGRES_DWH_USER}" \
    -d "${POSTGRES_DWH_DB}" \
    -v ON_ERROR_STOP=1 \
    -v customer_id="${CUSTOMER_ID}" \
    -v product_id="${PRODUCT_ID}" \
    -v order_id="${ORDER_ID}" \
    -v order_item_id="${ORDER_ITEM_ID}" \
    -f /sql/e2e/dwh_initial_check.sql

echo "Initial DWH check passed"


# =============================================================================
# Update customer and product + insert payment to OLTP
# =============================================================================


echo
echo "Update customers, products and insert payment..."

PAYMENT_ID=$(
    docker compose exec -T postgres-oltp \
        psql -XqAt \
        -U "${POSTGRES_OLTP_USER}" \
        -d "${POSTGRES_OLTP_DB}" \
        -v ON_ERROR_STOP=1 \
        -v customer_id="${CUSTOMER_ID}" \
        -v product_id="${PRODUCT_ID}" \
        -v order_id="${ORDER_ID}" \
        -f /sql/e2e/oltp_first_changes.sql
)

if [[ -z "${PAYMENT_ID}" ]]; then
    echo "ERROR: failed to obtain inserted payment id"
    exit 1
fi

echo "Updates and insert are done"


# =============================================================================
# Wait for DWH
# =============================================================================


QUERY="
    SELECT 
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_PRODUCTS}'
                AND op = 'u'
                AND event_key ->> 'id' = '${PRODUCT_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_CUSTOMERS}'
                AND op = 'u'
                AND event_key ->> 'id' = '${CUSTOMER_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_PAYMENTS}'
                AND op = 'c'
                AND event_key ->> 'id' = '${PAYMENT_ID}');
"

wait_for_dwh "${QUERY}" || exit 1


# =============================================================================
# Build models
# =============================================================================


build_models || exit 1


# =============================================================================
# After first changes DWH check
# =============================================================================


echo
echo "Check DWH after first changes..."

docker compose exec -T postgres-dwh \
    psql -XqAt \
    -U "${POSTGRES_DWH_USER}" \
    -d "${POSTGRES_DWH_DB}" \
    -v ON_ERROR_STOP=1 \
    -v customer_id="${CUSTOMER_ID}" \
    -v product_id="${PRODUCT_ID}" \
    -v order_id="${ORDER_ID}" \
    -v payment_id="${PAYMENT_ID}" \
    -v order_item_id="${ORDER_ITEM_ID}" \
    -f /sql/e2e/dwh_after_first_changes_check.sql

echo "After first changes DWH check passed"


# =============================================================================
# Change status in orders and payments in OLTP
# =============================================================================


echo
echo "Update status of order and payment..."

docker compose exec -T postgres-oltp \
    psql -XqAt \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${POSTGRES_OLTP_DB}" \
    -v ON_ERROR_STOP=1 \
    -v order_id="${ORDER_ID}" \
    -v payment_id="${PAYMENT_ID}" \
    -f /sql/e2e/oltp_second_changes.sql

echo "Updates are done"


# =============================================================================
# Wait for DWH
# =============================================================================


QUERY="
    SELECT 
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_PAYMENTS}'
                AND op = 'u'
                AND event_key ->> 'id' = '${PAYMENT_ID}')
        AND
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_ORDERS}'
                AND op = 'u'
                AND event_key ->> 'id' = '${ORDER_ID}');
"

wait_for_dwh "${QUERY}" || exit 1


# =============================================================================
# Build models
# =============================================================================


build_models || exit 1


# =============================================================================
# After second changes DWH check
# =============================================================================


echo
echo "Check DWH after second changes..."

docker compose exec -T postgres-dwh \
    psql -XqAt \
    -U "${POSTGRES_DWH_USER}" \
    -d "${POSTGRES_DWH_DB}" \
    -v ON_ERROR_STOP=1 \
    -v order_id="${ORDER_ID}" \
    -v payment_id="${PAYMENT_ID}" \
    -f /sql/e2e/dwh_after_second_changes_check.sql

echo "After second changes DWH check passed"


# =============================================================================
# Delete payment in OLTP
# =============================================================================


echo
echo "Delete payment..."

docker compose exec -T postgres-oltp \
    psql -XqAt \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${POSTGRES_OLTP_DB}" \
    -v ON_ERROR_STOP=1 \
    -v payment_id="${PAYMENT_ID}" \
    -f /sql/e2e/oltp_third_changes.sql

echo "Delete is done"


# =============================================================================
# Wait for DWH
# =============================================================================


QUERY="
    SELECT
        EXISTS (
            SELECT 1
            FROM raw.cdc_events
            WHERE kafka_topic = '${KAFKA_TOPIC_PAYMENTS}'
                AND op = 'd'
                AND event_key ->> 'id' = '${PAYMENT_ID}');
"

wait_for_dwh "${QUERY}" || exit 1


# =============================================================================
# Build models
# =============================================================================


build_models || exit 1


# =============================================================================
# After third changes DWH check
# =============================================================================


echo
echo "Check DWH after third changes..."

docker compose exec -T postgres-dwh \
    psql -XqAt \
    -U "${POSTGRES_DWH_USER}" \
    -d "${POSTGRES_DWH_DB}" \
    -v ON_ERROR_STOP=1 \
    -v payment_id="${PAYMENT_ID}" \
    -v order_id="${ORDER_ID}" \
    -f /sql/e2e/dwh_after_third_changes_check.sql

echo "After third changes DWH check passed"

echo
echo "ALL CHECKS PASSED"