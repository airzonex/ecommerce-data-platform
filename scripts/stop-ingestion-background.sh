#!/usr/bin/bash

PID_FILE=".ingestion.pid"

if [[ ! -f "${PID_FILE}" ]]; then
    echo "Ingestion consumer is not running"
    exit 0
fi

PID="$(cat "${PID_FILE}")"

if kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}"
    wait "${PID}" 2>/dev/null || true
fi

rm -f "${PID_FILE}"

echo "Ingestion consumer stopped"