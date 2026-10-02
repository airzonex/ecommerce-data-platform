from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import Message
from psycopg import Connection

from ecommerce.consumer import CdcConsumer
from ecommerce.parser import CdcEvent
from ecommerce.repository import CdcEventRepository


def _make_event() -> CdcEvent:
    return CdcEvent(
        kafka_topic='ecommerce.public.products',
        kafka_partition=0,
        kafka_offset=100,
        event_key={'id': 42},
        event_payload={
            'payload': {
                'op': 'c',
            }
        },
        op='c',
        is_tombstone=False,
        source_db='ecommerce',
        source_schema='public',
        source_table='products',
        source_lsn=28182856,
        source_tx_id=800,
        source_ts=datetime(2026, 9, 30, 5, 10, 54, 949070, tzinfo=UTC),
        processed_ts=datetime(2026, 9, 30, 5, 10, 55, 401238, tzinfo=UTC),
    )


def _make_consumer() -> tuple[
    CdcConsumer,
    MagicMock,
    MagicMock,
    MagicMock
]:
    kafka_consumer = MagicMock(spec=KafkaConsumer)
    dwh_connection = MagicMock(spec=Connection)
    repository = MagicMock(spec=CdcEventRepository)

    consumer = CdcConsumer(
        kafka_consumer=kafka_consumer,
        dwh_connection=dwh_connection,
        repository=repository,
        topics=('ecommerce.public.products',)
    )

    return (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    )


@patch('ecommerce.consumer.parse_event')
def test_process_message(
    parse_event_mock: MagicMock
) -> None:
    (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    ) = _make_consumer()

    message = MagicMock(spec=Message)
    message.error.return_value = None

    event = _make_event()
    parse_event_mock.return_value = event
    repository.insert_event.return_value = True

    consumer.process_message(message)

    parse_event_mock.assert_called_once_with(message)
    repository.insert_event.assert_called_once_with(event)

    dwh_connection.commit.assert_called_once_with()
    dwh_connection.rollback.assert_not_called()

    kafka_consumer.commit.assert_called_once_with(
        message=message,
        asynchronous=False
    )


@patch('ecommerce.consumer.parse_event')
def test_process_message_commits_offset_for_duplicate(
    parse_event_mock: MagicMock
) -> None:
    (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    ) = _make_consumer()

    message = MagicMock(spec=Message)
    message.error.return_value = None

    event = _make_event()
    parse_event_mock.return_value = event

    # Repository found the same
    # topic/partition/offset already in DWH
    repository.insert_event.return_value = False

    consumer.process_message(message)

    repository.insert_event.assert_called_once_with(event)

    dwh_connection.commit.assert_called_once_with()
    dwh_connection.rollback.assert_not_called()

    kafka_consumer.commit.assert_called_once_with(
        message=message,
        asynchronous=False
    )


@patch('ecommerce.consumer.parse_event')
def test_process_message_rolls_back_on_repository_error(
    parse_event_mock: MagicMock
) -> None:
    (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    ) = _make_consumer()

    message = MagicMock(spec=Message)
    message.error.return_value = None

    event = _make_event()
    parse_event_mock.return_value = event

    repository.insert_event.side_effect = RuntimeError(
        'database error'
    )

    with pytest.raises(
        RuntimeError,
        match='database error'
    ):
        consumer.process_message(message)

    dwh_connection.commit.assert_not_called()
    dwh_connection.rollback.assert_called_once_with()

    kafka_consumer.commit.assert_not_called()


@patch('ecommerce.consumer.parse_event')
def test_process_message_rolls_back_when_db_commit_fails(
    parse_event_mock: MagicMock
) -> None:
    (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    ) = _make_consumer()

    message = MagicMock(spec=Message)
    message.error.return_value = None

    event = _make_event()
    parse_event_mock.return_value = event
    repository.insert_event.return_value = True

    dwh_connection.commit.side_effect = RuntimeError(
        'commit failed'
    )

    with pytest.raises(
        RuntimeError,
        match='commit failed'
    ):
        consumer.process_message(message)

    repository.insert_event.assert_called_once_with(event)

    dwh_connection.commit.assert_called_once_with()
    dwh_connection.rollback.assert_called_once_with()
    
    kafka_consumer.commit.assert_not_called()


@patch('ecommerce.consumer.parse_event')
def test_process_message_raises_when_commit_and_rollback_fail(
    parse_event_mock: MagicMock
) -> None:
    (
        consumer,
        kafka_consumer,
        dwh_connection,
        repository
    ) = _make_consumer()

    message = MagicMock(spec=Message)
    message.error.return_value = None

    event = _make_event()
    parse_event_mock.return_value = event
    repository.insert_event.return_value = True

    dwh_connection.commit.side_effect = RuntimeError(
        'commit failed'
    )
    dwh_connection.rollback.side_effect = RuntimeError(
        'rollback failed'
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        consumer.process_message(message)

    assert len(exc_info.value.exceptions) == 2

    kafka_consumer.commit.assert_not_called()
