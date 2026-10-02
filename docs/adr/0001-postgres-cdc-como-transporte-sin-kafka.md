# PostgreSQL CDC as transport, no Kafka

We migrate the tax engine from Kafka Streams (topics `taxes-calculations`/`merchants`/`certificates` + MongoDB sink) to Flink reading the source PostgreSQL WAL directly via CDC and writing idempotent upserts to the target PostgreSQL. The PoC contains no Kafka anywhere: the operational source of truth is the `tax_calculations` table (INSERT-only) and the queryable materialized view is the target table. Direct CDC was chosen over Kafka because the natural producer is already a transactional database, it removes two operational pieces (broker + Connect), and it concentrates the PoC's learning on Flink itself.

## Considered Options

- **Debezium → Kafka → Flink**: the production reference architecture (replay, multiple consumers), but it adds a broker and connectors the PoC does not need.
- **Direct Flink CDC** (chosen): `flink-sql-connector-postgres-cdc` over logical replication; initial snapshot plus change streaming in a single connector.

## Consequences

- Full reproducibility depends on WAL/snapshot retention, not topic retention: a complete replay requires a re-snapshot plus state retention.
- Kafka remains the natural path if other consumers ever need the same changes; reintroducing it later does not break this design (the CDC connector is replaced by a Kafka source without touching the SQL logic).
