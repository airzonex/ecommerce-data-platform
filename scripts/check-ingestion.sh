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
KAFKA_TOPIC="ecommerce.public.products"

PRODUCT_NAME="INGESTION_E2E_$(date +%s)_$$"

PRODUCT_ID=""


cleanup() {
    if [[ -n "${PRODUCT_ID}" ]]; then
        docker compose exec -T postgres-oltp \
            psql \
            -U "${POSTGRES_OLTP_USER}" \
            -d "${POSTGRES_OLTP_DB}" \
            -q \
            -c "
                DELETE FROM products
                WHERE id = ${PRODUCT_ID};
            " \
            >/dev/null || true
    fi
}

trap cleanup EXIT


# =============================================================================
# Warning prompt
# =============================================================================


if [[ "${FORCE_INGESTION_CHECK:-0}" != "1" ]]; then
    echo "WARNING: This check modifies main OLTP database."
    echo "Debezium will publish generated CDC events to Kafka."
    echo "These events are expected to propagate to DWH."
    echo

    read -r -p "Type 'yes' to continue: " answer

    if [[ "${answer}" != "yes" ]]; then
        echo "Ingestion check cancelled."
        exit 0
    fi
fi


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


# =============================================================================
# INSERT to OLTP
# =============================================================================


echo "Creating test product..."

PRODUCT_ID="$(
    docker compose exec -T postgres-oltp \
        psql \
        -U "${POSTGRES_OLTP_USER}" \
        -d "${POSTGRES_OLTP_DB}" \
        -qAt \
        -c "
            INSERT INTO products(
                name,
                category,
                price    
            )
            VALUES (
                '${PRODUCT_NAME}',
                'CDC Test',
                1.00
            )
            RETURNING id;
        "
)"

echo "  product id: ${PRODUCT_ID}"


# =============================================================================
# Check DWH
# =============================================================================


echo "Waiting for event in DWH..."

for _ in $(seq 1 30); do
    EVENT_EXISTS="$(
        docker compose exec -T postgres-dwh \
            psql \
            -U "${POSTGRES_DWH_USER}" \
            -d "${POSTGRES_DWH_DB}" \
            -qAt \
            -c "
                SELECT EXISTS (
                    SELECT 1
                    FROM raw.cdc_events
                    WHERE kafka_topic = '${KAFKA_TOPIC}'
                      AND op = 'c'
                      AND event_key ->> 'id' = '${PRODUCT_ID}'
                      AND event_payload #>> '{payload,after,name}' = '${PRODUCT_NAME}'
                );
            "
    )"

    if [[ "${EVENT_EXISTS}" == "t" ]]; then
        echo
        echo "INGESTION CHECK PASSED"
        echo "  product id: ${PRODUCT_ID}"
        echo "  topic:      ${KAFKA_TOPIC}"
        echo "  operation:  c"
        exit 0
    fi

    sleep 1
done

echo
echo "ERROR: CDC event was not found in DWH"
exit 1