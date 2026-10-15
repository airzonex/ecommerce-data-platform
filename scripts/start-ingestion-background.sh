#!/usr/bin/bash

set -euo pipefail

PID_FILE=".ingestion.pid"
LOG_FILE=".ingestion.log"

if [[ -f "${PID_FILE}" ]]; then
    PID="$(cat "${PID_FILE}")"

    if kill -0 "${PID}" 2>/dev/null; then
        echo "ERROR: ingestion consumer is already running (PID ${PID})"
        exit 1
    fi

    rm -f "${PID_FILE}"
fi

ecommerce-cdc >>"${LOG_FILE}" 2>&1 &
PID=$!

sleep 1

if ! kill -0 "${PID}" 2>/dev/null; then
    echo "ERROR: ingestion consumer failed to start" >&2
    exit 1
fi

echo "${PID}" > "${PID_FILE}"

echo "Ingestion consumer started (PID ${PID})"