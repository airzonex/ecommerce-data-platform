# E-commerce Data Platform

A production-oriented data engineering project that demonstrates a CDC-based data platform for an e-commerce system.

The project implements the data flow from an operational PostgreSQL database through Debezium and Kafka into a PostgreSQL data warehouse, with dbt transformations and analytical marts planned as the next stage.

```text
PostgreSQL OLTP
       │
       │ WAL / logical replication
       ▼
    Debezium
       │
       ▼
      Kafka
       │
       ▼
Python CDC ingestion
       │
       ▼
PostgreSQL DWH
 raw.cdc_events
       │
       ▼
      dbt
       │
       ▼
staging / intermediate / marts
```

A near-real-time analytical branch using ClickHouse may be added later:

```text
Kafka
  ├──────────────► PostgreSQL DWH ──► dbt ──► marts / BI
  │
  └──────────────► ClickHouse ──► near-real-time analytics
```

## Project Goals

The project is designed to demonstrate practical Data Engineering patterns:

- relational OLTP data modeling
- Change Data Capture with Debezium
- Kafka-based event streaming
- append-only raw CDC storage
- idempotent data ingestion
- at-least-once delivery handling
- replay-safe processing
- source metadata preservation
- PostgreSQL DWH ingestion
- dbt-based transformations
- analytical data modeling
- handling inserts, updates, deletes, and tombstones
- out-of-order and late-arriving event handling
- data quality and observability
- near-real-time analytical serving

## Current Status

### Implemented

#### OLTP

The operational PostgreSQL database contains:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`

The schema includes:

- primary and foreign keys
- business constraints
- deterministic seed data
- `updated_at` triggers
- PostgreSQL logical replication configuration

#### CDC

PostgreSQL changes are captured with Debezium using logical replication and `pgoutput`.

Debezium streams changes from the OLTP tables into Kafka topics:

```text
ecommerce.public.customers
ecommerce.public.products
ecommerce.public.orders
ecommerce.public.order_items
ecommerce.public.payments
```
The pipeline supports Debezium operations:

```text
r  snapshot read
c  create
u  update
d  delete
```

Kafka tombstone records are also handled.

Kafka data is persisted in a Docker volume so topics, records, consumer offsets, and Kafka Connect metadata survive container restarts.

#### DWH raw layer

CDC events are ingested into:

```text
raw.cdc_events
```

The raw table stores the original Kafka/Debezium event together with technical metadata such as:

- Kafka topic
- Kafka partition
- Kafka offset
- event key
- event payload
- CDC operation
- tombstone flag
- source database
- source schema
- source table
- source LSN
- source transaction ID
- source timestamp
- Debezium processing timestamp
- DWH ingestion timestamp

The combination

```text
(kafka_topic, kafka_partition, kafka_offset)
```

is unique and provides idempotency for Kafka event ingestion.

## CDC Ingestion

The Python ingestion application consumes Debezium events from Kafka, parses them into a common CDC event model, and writes them into the DWH.

The processing sequence is:

```text
Kafka poll
    │
    ▼
parse Debezium event
    │
    ▼
INSERT raw.cdc_events
    │
    ▼
PostgreSQL COMMIT
    │
    ▼
Kafka offset COMMIT
```

Kafka automatic commits are disabled.

This gives the ingestion process at-least-once delivery semantics with an idempotent sink.

### Failure scenarios

If the database operation fails:

```text
DB INSERT
   ✕
ROLLBACK
   │
Kafka offset is not committed
```

The event will be read again.

If the database commit succeeds but the process fails before the Kafka offset is committed:

```text
DB COMMIT
   │
   ▼
process failure
   │
   ▼
Kafka replays event
   │
   ▼
UNIQUE(topic, partition, offset)
   │
   ▼
duplicate skipped
```

The replay is therefore safe.

## Graceful Shutdown

The ingestion process handles SIGINT and SIGTERM.

When a shutdown signal is received:

- the consumer stops polling for new messages
- the currently processed event is allowed to finish
- the PostgreSQL transaction is completed
- the Kafka offset is committed when appropriate
- Kafka and database connections are closed cleanly

## Running the Project

### 1. Configure environment

Create .env from the example configuration and provide the required PostgreSQL and Kafka settings.

### 2. Start infrastructure

```bash
make up
```

Check running containers:

```bash
make ps
```

### 3. Register Debezium connector

```bash
./scripts/register-debezium-connector.sh
```

Verify that the connector and its task are in the RUNNING state.

### 4. Start CDC ingestion

```bash
make run-ingestion
```

The application subscribes to all configured Debezium topics and starts ingesting CDC events into raw.cdc_events.

## Tests and Checks

### Python tests

Run all unit and integration tests:

```bash
make test
```

Run them separately:

```bash
make test-unit
make test-integration
```

Unit tests cover components such as:

- Debezium event parsing
- configuration
- CDC consumer behavior

Integration tests verify interaction with PostgreSQL DWH, including:

- inserting CDC events
- JSONB persistence
- tombstone persistence
- duplicate event handling

### OLTP database checks

```bash
make check-db
```

The command recreates an isolated test database and runs:

```text
prepare-test-oltp-db
        │
        ├──► check-oltp
        │
        └──► check-triggers
```

OLTP schema and trigger checks run against an isolated `ecommerce_test` database so they do not generate events in the main CDC pipeline.

### CDC streaming checks

```bash
make check-cdc
```

This verifies:

```text
PostgreSQL OLTP
       │
       ▼
    Debezium
       │
       ▼
      Kafka
```

The check performs an `INSERT`, `UPDATE`, and `DELETE` and verifies that Kafka receives:

```text
INSERT      → op=c
UPDATE      → op=u
DELETE      → op=d
TOMBSTONE   → value=null
```

> **Warning**
>
> This check modifies the main OLTP database and generates real CDC events.
> If the DWH ingestion consumer is running, these events may also propagate into the DWH.

For this reason, the script requires explicit confirmation before execution.

### End-to-end ingestion check

Start the ingestion process first:

```bash
make run-ingestion
```

Then, in another terminal:

```bash
make check-ingestion
```

The check verifies the complete ingestion path:

```text
PostgreSQL OLTP
       │
       ▼
    Debezium
       │
       ▼
      Kafka
       │
       ▼
Python CDC ingestion
       │
       ▼
PostgreSQL DWH
```

It creates a uniquely identifiable product in the OLTP database and waits until the corresponding `op=c` event appears in `raw.cdc_events`.

> **Warning**
>
> The end-to-end check intentionally generates CDC events in the main pipeline.
> The test product is removed from the OLTP database during cleanup, but its CDC history remains in the append-only raw DWH layer.

For this reason, the script requires explicit confirmation before execution.

## Development Commands

```bash
make up
make down
make restart
make logs
make ps
make config
```

Database checks:

```bash
make prepare-test-oltp-db
make check-db
make check-oltp
make check-triggers
```

CDC checks:

```bash
make check-cdc
make check-ingestion
```

Tests:

```bash
make test
make test-unit
make test-integration
```

CDC ingestion:

```bash
make run-ingestion
```

## Technology Stack

- Python
- PostgreSQL 17
- Apache Kafka
- Debezium
- Kafka Connect
- psycopg
- confluent-kafka
- Docker
- Docker Compose
- pytest
- mypy
- Ruff

Planned for the analytical layer:

- dbt Core
- ClickHouse

## Next Stage

The next stage of the project is to transform the append-only CDC log into analytical data models.

Planned flow:

```text
raw.cdc_events
       │
       ▼
dbt staging models
       │
       ▼
current-state / intermediate models
       │
       ▼
analytical marts
```

The transformation layer will address:

- reconstruction of current entity state from CDC events
- snapshot (`r`) and streaming (`c`, `u`, `d`) events
- hard deletes
- tombstones
- event ordering
- source LSN comparison
- duplicate and replay handling
- late-arriving events
- incremental processing
- customer and order analytics

Later stages may include:

- ClickHouse for near-real-time analytics
- pipeline monitoring
- data quality checks
- anomaly detection
- additional observability