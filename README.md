# E-commerce Data Platform

A production-oriented data platform for an e-commerce system.

The project demonstrates a CDC-based data architecture:

```text
PostgreSQL OLTP
       │
       │ CDC
       ▼
     Kafka
       │
       ├──────────────► DWH ──► dbt ──► Data Marts
       │
       └──────────────► ClickHouse
```

## Project Goals

The project is designed to demonstrate:

- PostgreSQL OLTP modeling
- Change Data Capture (CDC)
- Kafka-based event streaming
- Data Warehouse ingestion
- dbt transformations
- Analytical data marts
- Real-time analytical serving
- Data quality and reliability patterns

## Current Status

### Stage 1 - OLTP database

Currently implemented:

- PostgreSQL
- Docker Compose
- Project structure

Planned:

- Debezium CDC
- Kafka
- DWH
- dbt
- Customer and order marts
- ClickHouse
- Data quality and observability

## Architecture

The architecture will evolve incrementally from the initial OLTP system toward a CDC-based data platform.
