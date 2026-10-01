from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


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