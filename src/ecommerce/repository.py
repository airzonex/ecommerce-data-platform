from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from ecommerce.parser import CdcEvent


class CdcEventRepository:
    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def insert_event(self, event: CdcEvent) -> bool:
        event_key = (
            Jsonb(event.event_key)
            if event.event_key is not None
            else None
        )
        event_payload = (
            Jsonb(event.event_payload)
            if event.event_payload is not None
            else None
        )

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw.cdc_events (
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    event_key,
                    event_payload,
                    op,
                    is_tombstone,
                    source_db,
                    source_schema,
                    source_table,
                    source_lsn,
                    source_tx_id,
                    source_ts,
                    processed_ts
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                DO NOTHING
                RETURNING id
                """,
                (
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset,
                    event_key,
                    event_payload,
                    event.op,
                    event.is_tombstone,
                    event.source_db,
                    event.source_schema,
                    event.source_table,
                    event.source_lsn,
                    event.source_tx_id,
                    event.source_ts,
                    event.processed_ts
                )
            )

            return cursor.fetchone() is not None