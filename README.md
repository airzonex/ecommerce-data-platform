# E-commerce Data Platform

A production-oriented Data Engineering project that implements an end-to-end CDC pipeline for an e-commerce system.

The platform captures changes from an operational PostgreSQL database with Debezium, streams them through Kafka, ingests them into an append-only DWH raw layer, and transforms CDC events with dbt into analytical dimensions and fact tables.

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
       ├── staging
       ├── intermediate
       └── marts
```

The project focuses not only on moving data, but also on reliability properties expected from CDC-based pipelines:

- at-least-once processing
- idempotent ingestion
- Kafka replay safety
- source ordering with LSN metadata
- hard-delete handling
- SCD Type 2 dimensions
- graceful consumer shutdown
- automated end-to-end validation

---

## Project Goals

The project demonstrates practical Data Engineering patterns using a modern data stack:

- relational OLTP modeling
- Change Data Capture with Debezium
- Kafka-based event streaming
- append-only raw CDC storage
- Python-based ingestion
- at-least-once delivery semantics
- idempotent event processing
- replay-safe ingestion
- source metadata preservation
- CDC event ordering
- dbt staging and transformation layers
- current-state reconstruction
- SCD Type 2 dimensions
- analytical fact modeling
- hard-delete propagation
- automated data quality tests
- end-to-end pipeline validation

---

## Architecture

### OLTP

The source system is PostgreSQL 17 and contains five operational tables:

```text
customers
products
orders
order_items
payments
```

The schema includes:

- primary and foreign keys
- business constraints
- deterministic seed data
- `created_at` / `updated_at` timestamps
- `updated_at` triggers
- logical replication configuration
- `REPLICA IDENTITY FULL` for CDC tables

PostgreSQL logical replication is enabled so Debezium can capture row-level changes from the WAL.

---

### Change Data Capture

Debezium reads PostgreSQL changes using logical replication and publishes them to Kafka.

The main topics are:

```text
ecommerce.public.customers
ecommerce.public.products
ecommerce.public.orders
ecommerce.public.order_items
ecommerce.public.payments
```

The pipeline handles standard Debezium operations:

```text
r  snapshot read
c  create
u  update
d  delete
```

Kafka tombstone records are also supported.

The source metadata preserved with each event includes information such as:

- source LSN
- source transaction ID
- source timestamp
- database
- schema
- table

This metadata is later used by the transformation layer to reconstruct entity state and resolve event ordering.

---

## CDC Ingestion

A Python consumer reads Debezium records from Kafka and stores them in the PostgreSQL DWH.

All events are normalized into a common CDC event model before being persisted.

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

Kafka automatic offset commits are disabled.

The consumer commits a Kafka offset only after the corresponding PostgreSQL transaction has been successfully committed.

This provides **at-least-once processing with an idempotent sink**.

---

## Raw CDC Layer

All source events are stored in:

```text
raw.cdc_events
```

The table is append-only and stores both the original event data and technical metadata.

Examples of stored fields include:

```text
kafka_topic
kafka_partition
kafka_offset
event_key
event_payload
op
is_tombstone

source_db
source_schema
source_table
source_lsn
source_tx_id
source_ts
source_snapshot

processed_ts
```

Kafka identity is protected by the unique constraint:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

The same Kafka record therefore cannot create multiple raw rows.

---

## At-Least-Once Delivery and Idempotency

A failure can occur after the DWH commit but before the Kafka offset commit:

```text
Kafka event
    │
    ▼
DWH INSERT
    │
    ▼
DWH COMMIT
    │
    X process failure
    │
    ▼
Kafka offset was not committed
    │
    ▼
event is delivered again
```

On replay, the consumer attempts to insert the same event again.

The unique Kafka identity:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

prevents a second row from being created.

The duplicate is skipped and the Kafka offset can still advance.

```text
same Kafka record
       │
       ▼
INSERT raw.cdc_events
       │
       ▼
UNIQUE constraint conflict
       │
       ▼
duplicate skipped
       │
       ▼
Kafka offset committed
```

This behavior is verified by an automated replay/idempotency check.

---

## Graceful Shutdown

The ingestion process handles:

```text
SIGINT
SIGTERM
```

When a shutdown signal is received, the consumer stops polling for new messages and closes its resources cleanly.

The currently processed event is allowed to complete so that the database and Kafka offsets remain consistent.

The project supports both foreground and background execution:

```bash
make run-ingestion
```

and:

```bash
make start-ingestion-background
make stop-ingestion-background
```

The background lifecycle commands are primarily used by automated replay tests.

---

## dbt Transformation Layer

CDC events from `raw.cdc_events` are transformed with dbt.

The transformation flow is:

```text
raw.cdc_events
       │
       ▼
staging
       │
       ▼
canonical CDC events
       │
       ▼
current entity state
       │
       ▼
dimensions / facts
```

The dbt project uses three main layers:

```text
stg
int
marts
```

### Staging

The staging layer provides structured access to raw CDC records.

It keeps the ingestion layer separate from business-oriented transformations.

### Intermediate event models

CDC events are converted into canonical entity-specific models.

Examples include:

```text
int_products_events
int_customers_events
int_orders_events
int_order_items_events
int_payments_events
```

For snapshot, create, and update operations, models use the `after` state.

For delete operations, models reconstruct the deleted entity from the `before` state.

### Current-state models

The latest known state is reconstructed for every source entity:

```text
int_products_current
int_customers_current
int_orders_current
int_order_items_current
int_payments_current
```

The models preserve deleted entities using an `is_deleted` flag instead of silently removing their history.

CDC ordering is based on source metadata such as PostgreSQL LSN rather than relying only on ingestion time.

---

## Analytical Model

The analytical layer contains dimensions and fact tables.

### Dimensions

```text
dim_customers
dim_products
dim_date
```

`dim_customers` and `dim_products` are implemented as **SCD Type 2 dimensions**.

Historical versions contain temporal attributes such as:

```text
valid_from
valid_to
is_current
```

This makes it possible to resolve the dimension state that was valid at the time of a business event.

### Facts

```text
fct_orders
fct_order_items
fct_payments
```

The main grains are:

```text
fct_orders       one row per order in its latest known state

fct_order_items  one row per order item in its latest known state

fct_payments     one row per payment in its latest known state
```

Facts reference analytical dimension surrogate keys where appropriate.

The models also propagate source deletions through `is_deleted`.

---

## Handling CDC Ordering

CDC pipelines cannot safely assume that the row inserted last into the DWH represents the newest source state.

The transformation layer therefore preserves and uses source ordering metadata:

```text
source_lsn
source_tx_id
source_ts
```

The current-state models select the latest source event according to CDC ordering rather than relying only on DWH ingestion order.

This provides protection against duplicate delivery and delayed processing of already captured events.

---

## Handling Deletes

Hard deletes from PostgreSQL are captured as Debezium delete events.

The raw layer preserves those events permanently.

The intermediate layer reconstructs the deleted entity from the Debezium `before` state.

Downstream models represent the deletion explicitly:

```text
is_deleted = true
```

This allows analytical models to preserve historical information while still reflecting the latest source state.

Kafka tombstones are stored in the raw layer but are not treated as additional business state changes.

---

## Data Quality

dbt models include tests for properties such as:

- `not_null`
- `unique`
- accepted values
- dimension relationships
- surrogate key integrity

Build all dbt models and run the associated data tests with:

```bash
make dbt-build
```

The command uses dbt from the project virtual environment, so manual activation of `.venv` is not required.

---

## Running the Project

### Prerequisites

The project expects:

- Docker
- Docker Compose
- Python 3.12
- GNU Make

Python and dbt commands are executed from the project virtual environment.

Create the environment before running Python-dependent commands:

```bash
python3.12 -m venv .venv
```

Install the project and development dependencies:

```bash
.venv/bin/python -m pip install -e ".[dev]"
```

The Makefile uses the project virtual environment, so manual:

```bash
source .venv/bin/activate
```

is not required for normal Makefile commands.

---

### 1. Configure environment

Create the local configuration:

```bash
cp .env.example .env
```

Adjust values in `.env` if necessary.

The file contains configuration for:

- OLTP PostgreSQL
- DWH PostgreSQL
- Kafka
- Kafka Connect
- Debezium

---

### 2. Start infrastructure

```bash
make up
```

Check container status:

```bash
make ps
```

The environment includes:

```text
PostgreSQL OLTP
PostgreSQL DWH
Kafka
Kafka Connect + Debezium
```

---

### 3. Register the Debezium connector

```bash
make register-debezium-connector
```

The connector captures changes from all configured OLTP tables.

---

### 4. Start CDC ingestion

Run the consumer in the foreground:

```bash
make run-ingestion
```

The consumer subscribes to the Debezium topics and starts writing CDC events into:

```text
raw.cdc_events
```

Keep the consumer running and use another terminal for the following commands.

---

### 5. Build the analytical layer

Build all dbt models and run their tests:

```bash
make dbt-build
```

The Makefile runs dbt from the project virtual environment, so manual virtual environment activation is not required.

---

## Tests and Validation

The project contains checks at several levels.

### Python unit tests

```bash
make test-unit
```

The unit tests cover components such as:

- CDC event parsing
- configuration
- consumer behavior
- tombstones
- Debezium create/update/delete events

### Python integration tests

```bash
make test-integration
```

Integration tests validate PostgreSQL DWH interaction, including:

- CDC event insertion
- JSONB persistence
- tombstone persistence
- duplicate event handling
- idempotent repository behavior

---

### OLTP checks

```bash
make check-db
```

These checks verify the operational database schema and timestamp triggers.

The OLTP database checks use an isolated test database so they do not create CDC events in the main pipeline.

---

### CDC streaming check

```bash
make check-cdc
```

This check validates:

```text
PostgreSQL
    │
    ▼
 Debezium
    │
    ▼
  Kafka
```

It generates source changes and verifies that Kafka receives the expected CDC records:

```text
INSERT    → c
UPDATE    → u
DELETE    → d
TOMBSTONE → null value
```

> **Warning**
>
> This check modifies the main OLTP database and generates real CDC events.
> If the DWH ingestion consumer is running, these events may also propagate into the DWH.

---

### End-to-End CDC → DWH → dbt Check

Start the CDC consumer and keep it running:

```bash
make run-ingestion
```

Then, in another terminal, run:

```bash
make check-dwh-e2e
```

The check exercises the complete analytical path:

```text
OLTP
 │
 ▼
Debezium
 │
 ▼
Kafka
 │
 ▼
Python ingestion
 │
 ▼
raw.cdc_events
 │
 ▼
dbt
 │
 ▼
dimensions / facts
```

The scenario performs real source changes including:

- inserts
- updates
- status changes
- hard deletes

The script waits for the corresponding CDC records, rebuilds dbt models, and verifies the resulting analytical state after each stage.

It also validates temporal behavior such as SCD Type 2 history.

> **Warning**
>
> The E2E check intentionally modifies the main OLTP database and creates permanent CDC history in the append-only raw layer.

The script requires explicit confirmation before running.

---

### CDC Replay / Idempotency Check

```bash
make check-idempotency
```

This check explicitly verifies that replaying a Kafka record does not create duplicate data.

The scenario is:

```text
create OLTP record
       │
       ▼
CDC event reaches DWH
       │
       ▼
Kafka offset committed
       │
       ▼
stop consumer
       │
       ▼
reset group offset back to event
       │
       ▼
start consumer
       │
       ▼
same Kafka event replayed
       │
       ▼
raw row remains unchanged
       │
       ▼
Kafka offset advances again
       │
       ▼
dbt models remain correct
```

The check verifies both sides of idempotency:

1. the same Kafka record is actually consumed again;
2. the replay does not create another `raw.cdc_events` row.

It then rebuilds the dbt models and verifies that the analytical result is still correct.

The test manages its own background ingestion consumer. Stop any running foreground or background ingestion consumer before executing the check.

> **Warning**
>
> The idempotency check modifies the main OLTP database and resets a Kafka consumer group offset intentionally.

---

## Main Development Commands

Infrastructure:

```bash
make up
make down
make restart
make logs
make ps
make config
```

CDC:

```bash
make register-debezium-connector
make run-ingestion
make start-ingestion-background
make stop-ingestion-background
```

dbt:

```bash
make dbt-build
```

Tests:

```bash
make test
make test-unit
make test-integration
```

OLTP database checks:

```bash
make check-db
```

Individual OLTP database checks, after preparing the isolated test database:

```bash
make prepare-test-oltp-db
make check-oltp
make check-triggers
```

Pipeline checks:

```bash
make check-cdc
make check-dwh-e2e
make check-idempotency
```

---

## Technology Stack

### Data infrastructure

- PostgreSQL 17
- Apache Kafka
- Kafka Connect
- Debezium
- dbt Core

### Application

- Python
- psycopg
- confluent-kafka

### Development and testing

- Docker
- Docker Compose
- pytest
- mypy
- Ruff
- GNU Make

---

## Reliability Properties

The project intentionally demonstrates several reliability patterns that are important in real CDC systems.

| Concern | Implementation |
|---|---|
| Kafka delivery | At-least-once |
| Offset management | Manual commit |
| Offset commit point | After successful DWH commit |
| Raw ingestion idempotency | Unique `(kafka_topic, kafka_partition, kafka_offset)` |
| Replay handling | Duplicate raw event is skipped |
| Event ordering | PostgreSQL source LSN |
| Hard deletes | Preserved as deleted entity state |
| Historical dimensions | SCD Type 2 |
| Consumer shutdown | SIGINT / SIGTERM |
| Replay validation | Automated Kafka offset reset test |
| Pipeline validation | Automated CDC-to-dbt E2E scenario |

---

## Project Status

The core MVP is complete.

Implemented:

```text
OLTP schema
    ✓

PostgreSQL logical replication
    ✓

Debezium CDC
    ✓

Kafka
    ✓

Python CDC ingestion
    ✓

append-only raw DWH
    ✓

at-least-once processing
    ✓

idempotent ingestion
    ✓

Kafka replay validation
    ✓

dbt staging layer
    ✓

CDC event normalization
    ✓

current-state reconstruction
    ✓

SCD Type 2 dimensions
    ✓

fact tables
    ✓

hard-delete handling
    ✓

dbt tests
    ✓

end-to-end validation
    ✓
```

---

## Possible Future Extensions

The current project intentionally focuses on the CDC-to-DWH analytical path.

Possible extensions include:

- ClickHouse for near-real-time analytical serving
- monitoring and operational dashboards
- pipeline metrics
- alerting
- dead-letter / quarantine processing
- schema evolution scenarios
- backfill workflows
- larger-scale stream processing
- orchestration of transformation jobs

A possible real-time branch could look like:

```text
                    ┌──► PostgreSQL DWH ──► dbt ──► analytics
Kafka ──────────────┤
                    └──► ClickHouse ──► near-real-time analytics
```

These are intentionally left outside the current MVP so that the repository remains focused on CDC ingestion, reliability, and analytical data modeling.