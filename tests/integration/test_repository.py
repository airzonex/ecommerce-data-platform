
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from psycopg import Connection

from ecommerce.parser import CdcEvent
from ecommerce.repository import CdcEventRepository

SOURCE_TS = datetime(2026, 9, 30, 5, 10, 54, 949070, tzinfo=UTC)
PROCESSED_TS = datetime(2026, 9, 30, 5, 10, 55, 401238, tzinfo=UTC)


def _make_event(
    *,
    offset: int = 100
) -> CdcEvent:
    return CdcEvent(
        kafka_topic=f'test.repository.{uuid4()}',
        kafka_partition=0,
        kafka_offset=offset,
        event_key={'id': 42},
        event_payload={
            'schema': {
                'type': 'struct'
            },
            'payload': {
                'before': None,
                'after': {
                    'id': 42,
                    'name': 'Test product'
                },
                'source': {
                    'db': 'ecommerce',
                    'schema': 'public',
                    'table': 'products',
                    'lsn': 28182856,
                    "txId": 800,
                    "ts_us": 1790745054949070,
                },
                'op': 'c',
                'ts_us': 1790745055401238
            }
        },
        op='c',
        is_tombstone=False,
        source_db='ecommerce',
        source_schema='public',
        source_table='products',
        source_lsn=28182856,
        source_tx_id=800,
        source_ts=SOURCE_TS,
        processed_ts=PROCESSED_TS,
    )

def _make_tombstone(
    *,
    offset: int = 101
) -> CdcEvent:
    return CdcEvent(
        kafka_topic=f"test.repository.{uuid4()}",
        kafka_partition=0,
        kafka_offset=offset,
        event_key={"id": 42},
        event_payload=None,
        op=None,
        is_tombstone=True,
        source_db=None,
        source_schema=None,
        source_table=None,
        source_lsn=None,
        source_tx_id=None,
        source_ts=None,
        processed_ts=None,
    )


@pytest.mark.parametrize(
    'event_factory',
    [
        pytest.param(_make_event, id='cdc-event'),
        pytest.param(_make_tombstone, id='tombstone')
    ]
)
def test_insert_event(
    dwh_connection: Connection[Any],
    event_factory: Callable[[], CdcEvent]
) -> None:
    repo = CdcEventRepository(dwh_connection)
    event = event_factory()

    inserted = repo.insert_event(event)

    assert inserted is True

    with dwh_connection.cursor() as cur:
        cur.execute(
            """
            SELECT
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
                processed_ts,
                ingested_at
            FROM raw.cdc_events
            WHERE kafka_topic = %s
              AND kafka_partition = %s
              AND kafka_offset = %s
            """,
            (
                event.kafka_topic,
                event.kafka_partition,
                event.kafka_offset
            )
        )

        row = cur.fetchone()

    assert row is not None

    (
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
        processed_ts,
        ingested_at
    ) = row

    assert event_key == event.event_key
    assert event_payload == event.event_payload
    assert op == event.op
    assert is_tombstone == event.is_tombstone
    assert source_db == event.source_db
    assert source_schema == event.source_schema
    assert source_table == event.source_table
    assert source_lsn == event.source_lsn
    assert source_tx_id == event.source_tx_id
    assert source_ts == event.source_ts
    assert processed_ts == event.processed_ts
    assert ingested_at is not None


def test_duplicate_event_is_not_inserted(
    dwh_connection: Connection[Any]
) -> None:
    repo = CdcEventRepository(dwh_connection)
    event = _make_event()

    first_insert = repo.insert_event(event)
    second_insert = repo.insert_event(event)

    assert first_insert is True
    assert second_insert is False

    with dwh_connection.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM raw.cdc_events
            WHERE kafka_topic = %s
              AND kafka_partition = %s
              AND kafka_offset = %s
            """,
            (
                event.kafka_topic,
                event.kafka_partition,
                event.kafka_offset
            )
        )

        row = cur.fetchone()

    assert row is not None
    assert row[0] == 1