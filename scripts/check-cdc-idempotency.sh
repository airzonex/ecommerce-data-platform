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

INGESTION_STARTED=0

cleanup() {
    if (( INGESTION_STARTED == 1 )); then
        echo
        echo "Stopping ingestion..."
        make stop-ingestion-background || true
    fi
}

trap cleanup EXIT


# =============================================================================
# Warning prompt
# =============================================================================


if [[ "${FORCE_IDMP_CHECK:-0}" != "1" ]]; then
    echo "WARNING: This idempotency check modifies the main OLTP database."
    echo "It will insert test data."
    echo "Debezium will publish generated CDC events to Kafka."
    echo "These events will propagate to DWH and dbt models will be rebuilt."
    echo

    read -r -p "Type 'yes' to continue: " answer

    if [[ "${answer}" != "yes" ]]; then
        echo "Idempotency check cancelled."
        exit 0
    fi
fi


# =============================================================================
# Preflight ingestion consumer check
# =============================================================================


echo
echo "Checking ingestion consumer group..."

GROUP_STATUS="$(
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:29092 \
        --describe \
        --group "${KAFKA_GROUP_ID}" \
        2>&1 || true
)"

if ! grep -qiE \
    "has no active members|GroupIdNotFoundException|group .* not found" \
    <<< "${GROUP_STATUS}"
then
    echo "ERROR: ingestion consumer group has active members" >&2
    echo "Stop the running ingestion consumer before starting this check." >&2
    exit 1
fi


# =============================================================================
# Start ingestion
# =============================================================================

echo
echo "Starting ingestion..."
make start-ingestion-background
INGESTION_STARTED=1

# =============================================================================
# Insert unique product
# =============================================================================

echo
echo "Insert unique product to OLTP..."

PRODUCT="idempotency_$(date +%s)_$$"

PRODUCT_ID=$(
    docker compose exec -T postgres-oltp \
        psql -XqAt \
        -U "${POSTGRES_OLTP_USER}" \
        -d "${POSTGRES_OLTP_DB}" \
        -v ON_ERROR_STOP=1 \
        -c "
            INSERT INTO products(
                name,
                category,
                price
            )
            VALUES (
                '${PRODUCT}',
                '${PRODUCT}',
                999.99
            )
            RETURNING id;
        "
)

if [[ -z "${PRODUCT_ID}" ]]; then
    echo "ERROR: failed to obtain inserted product id"
    exit 1
fi

echo "  product id: ${PRODUCT_ID}"
echo "Product inserted"


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
                AND event_key ->> 'id' = '${PRODUCT_ID}');
"

wait_for_dwh "${QUERY}" || exit 1


# =============================================================================
# Get event
# =============================================================================


echo
echo "Retrieving event..."

EVENT="$(
    docker compose exec -T postgres-dwh \
        psql -XqAt \
        -F '|' \
        -U "${POSTGRES_DWH_USER}" \
        -d "${POSTGRES_DWH_DB}" \
        -v ON_ERROR_STOP=1 \
        -v product_id="${PRODUCT_ID}" \
        -v kafka_topic="${KAFKA_TOPIC_PRODUCTS}" \
        -f /sql/checks/idempotency/dwh_get_event.sql
)"

IFS='|' read -r RAW_ID EVENT_TOPIC EVENT_PARTITION EVENT_OFFSET <<< "${EVENT}"

if [[ -z "${RAW_ID}" ]]; then
    echo "ERROR: failed to obtain raw id"
    exit 1
fi

if [[ -z "${EVENT_TOPIC}" ]]; then
    echo "ERROR: failed to obtain event topic"
    exit 1
fi

if [[ -z "${EVENT_PARTITION}" ]]; then
    echo "ERROR: failed to obtain event partition"
    exit 1
fi

if [[ -z "${EVENT_OFFSET}" ]]; then
    echo "ERROR: failed to obtain event offset"
    exit 1
fi

if [[ ! "${EVENT_OFFSET}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: invalid current offset: ${EVENT_OFFSET}" >&2
    exit 1
fi

echo "  raw id:             ${RAW_ID}"
echo "  event topic:        ${EVENT_TOPIC}"
echo "  event partition:    ${EVENT_PARTITION}"
echo "  event offset:       ${EVENT_OFFSET}"
echo "Event retrieved"


# =============================================================================
# Get current kafka offset function
# =============================================================================


get_current_offset() {
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:29092 \
        --describe \
        --group "${KAFKA_GROUP_ID}" \
    | awk \
        -v topic="${EVENT_TOPIC}" \
        -v partition="${EVENT_PARTITION}" \
        '$2 == topic && $3 == partition { print $4 }'
}


# =============================================================================
# Wait for kafka offset to commit
# =============================================================================


echo
echo "Waiting for kafka offset to commit..."

IS_CHANGED=0

for _ in $(seq 1 30); do
    CURRENT_OFFSET="$(get_current_offset)"

    if [[ ! "${CURRENT_OFFSET}" =~ ^[0-9]+$ ]]; then
        sleep 1
        continue
    fi

    if (( CURRENT_OFFSET > EVENT_OFFSET )); then
        echo "  current offset: ${CURRENT_OFFSET}"
        echo "Consumer group offset has changed"
        IS_CHANGED=1
        break
    fi

    sleep 1
done

if (( IS_CHANGED != 1 )); then
    echo "ERROR: Wait timeout while waiting"
    exit 1
fi


# =============================================================================
# Stop ingestion
# =============================================================================

echo
echo "Stopping ingestion..."
make stop-ingestion-background
INGESTION_STARTED=0

# =============================================================================
# Wait for consumer group to become inactive
# =============================================================================


echo
echo "Waiting for consumer group to become inactive..."

IS_INACTIVE=0

for _ in $(seq 1 30); do
    GROUP_STATUS="$(
        docker compose exec -T kafka \
            /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:29092 \
            --describe \
            --group "${KAFKA_GROUP_ID}" \
            2>&1 || true
    )"

    if grep -qi "has no active members" <<< "${GROUP_STATUS}"; then
        echo "Consumer group is inactive"
        IS_INACTIVE=1
        break
    fi

    if grep -qiE "GroupIdNotFoundException|group .* not found" <<< "${GROUP_STATUS}"; then
        echo "ERROR: consumer group does not exist"
        exit 1
    fi

    sleep 1
done

if (( IS_INACTIVE != 1 )); then
    echo "ERROR: Wait timeout while waiting"
    exit 1
fi


# =============================================================================
# Resetting group offset
# =============================================================================


echo
echo "Resetting group offset to offset ${EVENT_OFFSET}..."

docker compose exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:29092 \
    --group "${KAFKA_GROUP_ID}" \
    --topic "${EVENT_TOPIC}:${EVENT_PARTITION}" \
    --reset-offsets \
    --to-offset "${EVENT_OFFSET}" \
    --execute

CURRENT_OFFSET="$(get_current_offset)"

if [[ ! "${CURRENT_OFFSET}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: invalid current offset: ${CURRENT_OFFSET}" >&2
    exit 1
fi

if (( CURRENT_OFFSET != EVENT_OFFSET )); then
    echo "  current offset: ${CURRENT_OFFSET}"
    echo "ERROR: offset reset failed"
    exit 1
fi

echo "  current offset: ${CURRENT_OFFSET}"
echo "Group offset reset"


# =============================================================================
# Starting ingestion
# =============================================================================


echo
echo "Starting ingestion..."

make start-ingestion-background
INGESTION_STARTED=1


# =============================================================================
# Wait for consumer group offset to change
# =============================================================================


echo
echo "Waiting for consumer group offset to change..."

IS_CHANGED=0

for _ in $(seq 1 30); do
    CURRENT_OFFSET="$(get_current_offset)"

    if [[ ! "${CURRENT_OFFSET}" =~ ^[0-9]+$ ]]; then
        sleep 1
        continue
    fi

    if (( CURRENT_OFFSET > EVENT_OFFSET )); then
        echo "  current offset: ${CURRENT_OFFSET}"
        echo "Consumer group offset has changed"
        IS_CHANGED=1
        break
    fi

    sleep 1
done

if (( IS_CHANGED != 1 )); then
    echo "ERROR: Wait timeout while waiting"
    exit 1
fi


# =============================================================================
# Get event again
# =============================================================================


echo
echo "Checking if event does not change..."

EVENT="$(
    docker compose exec -T postgres-dwh \
        psql -XqAt \
        -F '|' \
        -U "${POSTGRES_DWH_USER}" \
        -d "${POSTGRES_DWH_DB}" \
        -v ON_ERROR_STOP=1 \
        -v product_id="${PRODUCT_ID}" \
        -v kafka_topic="${KAFKA_TOPIC_PRODUCTS}" \
        -f /sql/checks/idempotency/dwh_get_event.sql
)"

IFS='|' read -r NEW_RAW_ID NEW_EVENT_TOPIC \
    NEW_EVENT_PARTITION NEW_EVENT_OFFSET <<< "${EVENT}"

if [[ -z "${NEW_RAW_ID}" ]]; then
    echo "ERROR: failed to obtain raw id"
    exit 1
fi

if [[ -z "${NEW_EVENT_TOPIC}" ]]; then
    echo "ERROR: failed to obtain event topic"
    exit 1
fi

if [[ -z "${NEW_EVENT_PARTITION}" ]]; then
    echo "ERROR: failed to obtain event partition"
    exit 1
fi

if [[ -z "${NEW_EVENT_OFFSET}" ]]; then
    echo "ERROR: failed to obtain event offset"
    exit 1
fi

if [[ "${NEW_RAW_ID}" != "${RAW_ID}" ]] ||
   [[ "${NEW_EVENT_TOPIC}" != "${EVENT_TOPIC}" ]] ||
   [[ "${NEW_EVENT_PARTITION}" != "${EVENT_PARTITION}" ]] ||
   [[ "${NEW_EVENT_OFFSET}" != "${EVENT_OFFSET}" ]]; then
    echo "CHECK FAILED: CDC event changed after replay"
    exit 1
fi

echo "  raw id:             ${NEW_RAW_ID}"
echo "  event topic:        ${NEW_EVENT_TOPIC}"
echo "  event partition:    ${NEW_EVENT_PARTITION}"
echo "  event offset:       ${NEW_EVENT_OFFSET}"
echo "Check passed"


# =============================================================================
# Building models
# =============================================================================


echo
echo "Building dbt models..."

RC=0
OUTPUT="$( { cd ecommerce_dwh && dbt build; } 2>&1 )" || RC=$?

if (( RC != 0 )); then
    echo "ERROR: failed to build models (exit code ${RC})" >&2
    echo "${OUTPUT}" >&2
    exit 1
fi

echo "Dbt models were built"


# =============================================================================
# Checking dim_products
# =============================================================================


echo
echo "Checking dim_products..."

docker compose exec -T postgres-dwh \
    psql -XqAt \
    -U "${POSTGRES_DWH_USER}" \
    -d "${POSTGRES_DWH_DB}" \
    -v ON_ERROR_STOP=1 \
    -v product_id="${PRODUCT_ID}" \
    -f /sql/checks/idempotency/dwh_mart_check.sql

echo "Check passed"

echo
echo "IDEMPOTENCY CHECK PASSED"