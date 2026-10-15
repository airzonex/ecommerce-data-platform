#!/usr/bin/bash

set -euo pipefail

PID_FILE=".ingestion.pid"

if [[ -f "${PID_FILE}" ]]; then
    PID="$(cat "${PID_FILE}")"

    if kill -0 "${PID}" 2>/dev/null; then
        echo "ERROR: ingestion consumer is already running (PID ${PID})"
        exit 1
    fi

    rm -f "${PID_FILE}"
fi

python -m ecommerce.consumer &
PID=$!

echo "${PID}" > "${PID_FILE}"

echo "Ingestion consumer started (PID ${PID})"