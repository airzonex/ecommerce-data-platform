#!/usr/bin/bash

set -euo pipefail

if [[ ! -f .env ]]; then
    echo "ERROR: .env file not found"
    exit 1
fi

set -a
source .env
set +a

TEST_DB="ecommerce_test"


echo "Recreating ${TEST_DB}..."

docker compose exec -T postgres-oltp \
    dropdb \
    -U "${POSTGRES_OLTP_USER}" \
    --if-exists \
    --force \
    "${TEST_DB}"

docker compose exec -T postgres-oltp \
    createdb \
    -U "${POSTGRES_OLTP_USER}" \
    -O "${POSTGRES_OLTP_USER}" \
    "${TEST_DB}"


echo "Applying OLTP schema..."

docker compose exec -T postgres-oltp \
    psql \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${TEST_DB}" \
    -v ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/01-schema.sql


echo "Loading seed data..."

docker compose exec -T postgres-oltp \
    psql \
    -U "${POSTGRES_OLTP_USER}" \
    -d "${TEST_DB}" \
    -v ON_ERROR_STOP=1 \
    -f /docker-entrypoint-initdb.d/02-seed.sql


echo "Test database is ready: ${TEST_DB}"