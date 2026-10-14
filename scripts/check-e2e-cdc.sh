#!/usr/bin/bash

set -euo pipefail

if [[ ! -f .env ]]; then
    echo "ERROR: .env file not found"
    exit 1
fi

set -a
source .env
set +a

KAFKA_GROUP_ID="ecommerce-dwh-ingestion"
KAFKA_TOPIC_PRODUCTS="ecommerce.public.products"
KAFKA_TOPIC_CUSTOMERS="ecommerce.public.customers"
KAFKA_TOPIC_ORDERS="ecommerce.public.orders"
KAFKA_TOPIC_ORDER_ITEMS="ecommerce.public.order_items"
KAFKA_TOPIC_PAYMENTS="ecommerce.public.payments"


# =============================================================================
# Check consumer
# =============================================================================


echo
echo "Checking CDC ingestion consumer..."

set +e

GROUP_STATUS="$(
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:29092 \
        --describe \
        --group "${KAFKA_GROUP_ID}" \
        2>&1
)"

GROUP_STATUS_RC=$?

set -e

if [[ ${GROUP_STATUS_RC} -ne 0 ]]; then
    echo "ERROR: failed to get Kafka consumer group status"
    echo "${GROUP_STATUS}"
    exit 1
fi

if grep -qE "has no active members|does not exist" <<< "${GROUP_STATUS}"; then
    echo "ERROR: CDC ingestion consumer is not running"
    echo "Run: make run-ingestion"
    exit 1
fi

echo "CDC ingestion consumer is OK"


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
    local expected="$2"
    local attempts="${3:-30}"
    local actual=""
    
    echo
    echo "Waiting for events in DWH..."

    for _ in $(seq 1 "$attempts"); do
        actual="$(
            docker compose exec -T postgres-dwh \
                psql -XqAt \
                -U "${POSTGRES_DWH_USER}" \
                -d "${POSTGRES_DWH_DB}" \
                -v ON_ERROR_STOP=1 \
                -c "${query}"
        )"

        if [[ "${actual}" == "${expected}" ]]; then
            echo "CDC events were found in DWH"
            return 0
        fi

        sleep 1
    done

    echo
    echo "ERROR: expected ${expected} events, got '${actual}'" >&2
    return 1
}


# =============================================================================
# Wait for DWH
# =============================================================================


QUERY="
    SELECT COUNT(*)
    FROM (
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_PRODUCTS}'
            AND op = 'c'
            AND event_key ->> 'id' = '${PRODUCT_ID}'
        UNION ALL
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_CUSTOMERS}'
            AND op = 'c'
            AND event_key ->> 'id' = '${CUSTOMER_ID}'
        UNION ALL
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_ORDERS}'
            AND op = 'c'
            AND event_key ->> 'id' = '${ORDER_ID}'
        UNION ALL
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_ORDER_ITEMS}'
            AND op = 'c'
            AND event_key ->> 'id' = '${ORDER_ITEM_ID}'
    );
"

wait_for_dwh "${QUERY}" 4 || exit 1


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
    SELECT COUNT(*)
    FROM (
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_PRODUCTS}'
            AND op = 'u'
            AND event_key ->> 'id' = '${PRODUCT_ID}'
        UNION ALL
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_CUSTOMERS}'
            AND op = 'u'
            AND event_key ->> 'id' = '${CUSTOMER_ID}'
        UNION ALL
        SELECT 1
        FROM raw.cdc_events
        WHERE kafka_topic = '${KAFKA_TOPIC_PAYMENTS}'
            AND op = 'c'
            AND event_key ->> 'id' = '${PAYMENT_ID}'
    );
"

wait_for_dwh "${QUERY}" 3 || exit 1


# =============================================================================
# Build models
# =============================================================================


build_models || exit 1


# =============================================================================
# After changes DWH check
# =============================================================================


echo
echo "After changes DWH check..."

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

echo "After changes DWH check passed"
