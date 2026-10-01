CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cdc_events (
    id					BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    kafka_topic			TEXT NOT NULL,
    kafka_partition		INTEGER NOT NULL,
    kafka_offset		BIGINT NOT NULL,

    event_key			JSONB,
    event_payload		JSONB,

    op					CHAR(1),
    is_tombstone		BOOLEAN NOT NULL DEFAULT FALSE,

    source_db			TEXT,
    source_schema		TEXT,
    source_table		TEXT,

    source_lsn			BIGINT,
    source_tx_id		BIGINT,

	source_ts			TIMESTAMPTZ,
    processed_ts		TIMESTAMPTZ,

	ingested_at			TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

COMMENT ON COLUMN raw.cdc_events.source_ts IS 'when change happened in PostgreSQL';
COMMENT ON COLUMN raw.cdc_events.processed_ts IS 'when Debezium formed/processed event';
COMMENT ON COLUMN raw.cdc_events.ingested_at IS 'when Python consumer loaded event to DWH';

