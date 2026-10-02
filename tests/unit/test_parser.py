import json
from datetime import UTC, datetime
from typing import Any

import pytest

from ecommerce.parser import CdcEvent, parse_event, parse_key


class FakeMessage:
    """Minimal Kafka message mock for unit tests."""

    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = (
            json.dumps(key).encode('utf-8')
            if key is not None
            else None
        )
        self._value = (
            json.dumps(value).encode('utf-8')
            if value is not None
            else None
        )

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value


def _make_debezium_key() -> dict[str, Any]:
    return {
        'schema': {
            'type': 'struct',
            'fields': [
                {
                    'type': 'int64',
                    'optional': False,
                    'field': 'id'
                }
            ]
        },
        'payload': {
            'id': 42
        }
    }


def _make_debezium_event(
    op: str
) -> dict[str, Any]:
    return {
        'schema': {
            'type': 'struct',
            'name': 'ecommerce.public.products.Envelope',
        },
        'payload': {
            'before': (
                {
                    'id': 42,
                    'name': 'Old product',
                    'category': 'Test',
                    'price': '100.00'
                }
                if op in {'u', 'd'}
                else None
            ),
            'after': (
                {
                    'id': 42,
                    'name': 'CDC Test Product',
                    'category': 'CDC Check',
                    'price': '1099.99'
                }
                if op in {'c', 'u'}
                else None
            ),
            'source': {
                'version': '3.5.2.Final',
                'connector': 'postgresql',
                'name': 'ecommerce',
                'ts_ms': 1790745054949,
                "ts_us": 1790745054949070,
                "ts_ns": 1790745054949070000,
                'db': 'ecommerce',
                'schema': 'public',
                'table': 'products',
                'txId': 800,
                'lsn': 28182856
            },
            'transaction': None,
            'op': op,
            'ts_ms': 1790745055401,
            "ts_us": 1790745055401238,
            "ts_ns": 1790745055401238000,
        }
    }


def test_parse_key_happy_path() -> None:
    raw_key = json.dumps(_make_debezium_key()).encode('utf-8')
    result = parse_key(raw_key)

    assert result == {'id': 42}

def test_parse_key_none() -> None:
    assert parse_key(None) is None

@pytest.mark.parametrize(
    ('operation', 'expected_before', 'expected_after'),
    [
        ('c', None, {'id': 42}),
        ('u', {'id': 42}, {'id': 42}),
        ('d', {'id': 42}, None)
    ]
)
def test_parse_event(
    operation: str,
    expected_before: dict[str, Any] | None,
    expected_after: dict[str, Any] | None
) -> None:
    event_key = _make_debezium_key()
    event_value = _make_debezium_event(operation)

    message = FakeMessage(
        topic='ecommerce.public.products',
        partition=0,
        offset=123,
        key=event_key,
        value=event_value
    )

    result = parse_event(message)

    expected_source_ts = datetime(
        2026,
        9,
        30,
        5,
        10,
        54,
        949070,
        tzinfo=UTC
    )

    expected_processed_ts = datetime(
        2026,
        9,
        30,
        5,
        10,
        55,
        401238,
        tzinfo=UTC
    )

    assert result == CdcEvent(
        kafka_topic='ecommerce.public.products',
        kafka_partition=0,
        kafka_offset=123,
        event_key={'id': 42},
        event_payload=event_value,
        op=operation,
        is_tombstone=False,
        source_db='ecommerce',
        source_schema='public',
        source_table='products',
        source_lsn=28182856,
        source_tx_id=800,
        source_ts=expected_source_ts,
        processed_ts=expected_processed_ts
    )

    assert result.event_payload is not None
    payload = result.event_payload['payload']

    if expected_before is None:
        assert payload['before'] is None
    else:
        assert payload['before']['id'] == expected_before['id']

    if expected_after is None:
        assert payload['after'] is None
    else:
        assert payload['after']['id'] == expected_after['id']


def test_parse_event_tombstone() -> None:
    message = FakeMessage(
        topic='ecommerce.public.products',
        partition=0,
        offset=123,
        key=_make_debezium_key(),
        value=None
    )

    result = parse_event(message)

    assert result == CdcEvent(
        kafka_topic='ecommerce.public.products',
        kafka_partition=0,
        kafka_offset=123,
        event_key={'id': 42},
        event_payload=None,
        op=None,
        is_tombstone=True,
        source_db=None,
        source_schema=None,
        source_table=None,
        source_lsn=None,
        source_tx_id=None,
        source_ts=None,
        processed_ts=None
    )