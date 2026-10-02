import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from confluent_kafka import Message

Operation = Literal['c', 'u', 'd', 'r']


@dataclass(frozen=True)
class CdcEvent:
    # Kafka metadata
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int

    # Kafka record kay payload
    event_key: dict[str, Any] | None

    # Full Debezium event payload
    # None for tombstone
    event_payload: dict[str, Any] | None

    # Debezium operation
    # None for tombstone
    op: Operation | None

    # Kafka tombstone marker
    is_tombstone: bool

    # Debezium source metadata
    source_db: str | None
    source_schema: str | None
    source_table: str | None
    source_lsn: int | None
    source_tx_id: int | None
    source_ts: datetime | None

    # Time when Debezium produced the event
    processed_ts: datetime | None


class CdcParseError(ValueError):
    """Raises when a Kafka record cannot be parsed as a Debezium event."""


def _decode_json(value: bytes | str) -> dict[str, Any]:
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as e:
        raise CdcParseError('Kafka record is not valid json') from e

    if not isinstance(decoded, dict):
        raise CdcParseError('Kafka JSON value must be an object')

    return decoded


def _timestamp_from_microseconds(
    value: Any,
    field_name: str
) -> datetime:
    if not isinstance(value, int):
        raise CdcParseError(
            f'{field_name} must be an integer containing Unix microseconds'
        )

    return datetime.fromtimestamp(
        value / 1_000_000,
        tz=UTC
    )


def parse_key(
    raw_key: bytes | str | None
) -> dict[str, Any] | None:
    """
    Parse Kafka record key.

    Debezium + Kafka Connect JSON key has this structure:

        {
            "schema": {...},
            "payload": {
                "id": 59
            }
        }

    We keep only the semantic key payload:

        {"id": 59}

    Returns None when the Kafka key is null.
    """
    if raw_key is None:
        return None

    key_document = _decode_json(raw_key)

    if 'payload' not in key_document:
        raise CdcParseError(
            'Debezium key does not contain payload'
        )

    payload = key_document['payload']

    if not isinstance(payload, dict):
        raise CdcParseError(
            'Debezium key payload must be an object'
        )

    return payload


def _get_message_metadata(
    message: Message
) -> tuple[str, int, int]:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()

    if topic is None:
        raise CdcParseError('Kafka message has no topic')

    if partition is None:
        raise CdcParseError('Kafka message has no partition')

    if offset is None:
        raise CdcParseError('Kafka message has no offset')

    return topic, partition, offset


def parse_event(message: Message) -> CdcEvent:
    """
    Parse a Kafka message containing a Debezium PostgreSQL event.

    Handles both:
        - regular Debezium events
        - Kafka tombstones

    The full Kafka value is preserved in event_payload
    """
    topic, partition, offset = _get_message_metadata(message)

    raw_key = message.key()
    raw_value = message.value()

    event_key = parse_key(raw_key)

    # -------------------------------------------------------
    # Tombstone
    # -------------------------------------------------------

    if raw_value is None:
        return CdcEvent(
            kafka_topic=topic,
            kafka_partition=partition,
            kafka_offset=offset,
            event_key=event_key,
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

    # -------------------------------------------------------
    # Regular Debezium event
    # -------------------------------------------------------

    event_document = _decode_json(raw_value)

    if 'payload' not in event_document:
        raise CdcParseError('Debezium event does not contain "payload"')

    payload = event_document['payload']

    if not isinstance(payload, dict):
        raise CdcParseError('Debezium event payload must be an object')

    op = payload.get('op')

    if op not in {'c', 'u', 'd', 'r'}:
        raise CdcParseError(f'Unsupported Debezium operation: {op!r}')

    source = payload.get('source')

    if not isinstance(source, dict):
        raise CdcParseError('Debezium event does not contain valid "source" metadata')

    source_db = source.get('db')
    source_schema = source.get('schema')
    source_table = source.get('table')
    source_lsn = source.get('lsn')
    source_tx_id = source.get('txId')

    source_ts = None

    if source.get('ts_us') is not None:
        source_ts = _timestamp_from_microseconds(
            source['ts_us'],
            'source.ts_us'
        )

    processed_ts = None

    if payload.get('ts_us') is not None:
        processed_ts = _timestamp_from_microseconds(
            payload['ts_us'],
            'payload.ts_us'
        )

    return CdcEvent(
        kafka_topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
        event_key=event_key,
        event_payload=event_document,
        op=op,
        is_tombstone=False,
        source_db=source_db,
        source_schema=source_schema,
        source_table=source_table,
        source_lsn=source_lsn,
        source_tx_id=source_tx_id,
        source_ts=source_ts,
        processed_ts=processed_ts
    )