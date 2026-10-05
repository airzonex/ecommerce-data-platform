
import logging
from typing import Any

import psycopg
from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import KafkaException, Message
from psycopg import Connection

from ecommerce.config import AppConfig
from ecommerce.parser import parse_event
from ecommerce.repository import CdcEventRepository

logger = logging.getLogger(__name__)

class CdcConsumer:
    
    def __init__(
        self,
        kafka_consumer: KafkaConsumer,
        dwh_connection: Connection[Any],
        repository: CdcEventRepository,
        topics: tuple[str, ...]
    ) -> None:
        self._kafka_consumer = kafka_consumer
        self._dwh_connection = dwh_connection
        self._repository = repository
        self._topics = topics
        self._stop_requested = False

    def run(self) -> None:
        logger.info('consumer started')
        self._kafka_consumer.subscribe(list(self._topics))
        logger.info(
            'subscribed to topics: %s',
            self._topics
        )

        try:
            while not self._stop_requested:
                message = self._kafka_consumer.poll(timeout=1.0)

                if self._stop_requested:
                    break

                if message is None:
                    continue

                self.process_message(message)
        finally:
            self.close()

    def process_message(self, message: Message) -> None:
        error = message.error()

        if error is not None:
            logger.error(
                'Kafka message error: %s',
                error
            )
            raise KafkaException(error)

        event = parse_event(message)

        try:
            inserted = self._repository.insert_event(event)
            if inserted:
                logger.info(
                    'message processed: topic=%s partition=%s offset=%s',
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset
                )
            else:
                logger.info(
                    'duplicate skipped: topic=%s partition=%s offset=%s',
                    event.kafka_topic,
                    event.kafka_partition,
                    event.kafka_offset
                )
            self._dwh_connection.commit()
        except Exception as db_error:
            try:
                self._dwh_connection.rollback()
            except Exception as rollback_error:
                raise ExceptionGroup(
                    'Database operation and rollback both failed',
                    [
                        db_error,
                        rollback_error
                    ]
                ) from None
            logger.exception('database operation failed')
            raise

        self._kafka_consumer.commit(
            message=message,
            asynchronous=False
        )

    def close(self) -> None:
        self._kafka_consumer.close()
        self._dwh_connection.close()
        logger.info('consumer stopped')

    def request_stop(self) -> None:
        self._stop_requested = True


def create_cdc_consumer(config: AppConfig) -> CdcConsumer:
    kafka_consumer = KafkaConsumer(
        {
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'group.id': config.kafka.group_id,
            'enable.auto.commit': False,
            'enable.auto.offset.store': False,
            'auto.offset.reset': 'earliest'
        }
    )

    try:
        dwh_connection: Connection[Any] = psycopg.connect(
            dbname=config.dwh.db,
            user=config.dwh.user,
            password=config.dwh.password,
            host=config.dwh.host,
            port=config.dwh.port
        )
    except Exception:
        kafka_consumer.close()
        raise

    repository = CdcEventRepository(dwh_connection)

    return CdcConsumer(
        kafka_consumer=kafka_consumer,
        dwh_connection=dwh_connection,
        repository=repository,
        topics=config.kafka.topics
    )