# poc-flink-taxes

Apache Flink 1.20 proof of concept for learning Change Data Capture (CDC) with PostgreSQL.

## Architecture

```text
PostgreSQL source
  WAL / logical replication
        |
        v
 PostgreSQL CDC
        |
        v
 Apache Flink 1.20
 JobManager + TaskManager
        |
        v
 JDBC / upsert
        |
        v
PostgreSQL target
```

The initial goal is to observe how an initial snapshot and subsequent changes (INSERT, UPDATE, and DELETE) propagate from the source PostgreSQL database to a materialized table in the target PostgreSQL database.

## Services

- `dev`: Dev Container with Java 17 and Gradle.
- `jobmanager`: Flink JobManager.
- `taskmanager`: Flink TaskManager with 4 slots.
- `postgres-source`: PostgreSQL configured for logical replication.
- `postgres-target`: PostgreSQL database that materializes the result.
- Flink Web UI: http://localhost:8081
- Source PostgreSQL: localhost:5432
- Target PostgreSQL: localhost:5433

## Getting started

1. Open the repository in VS Code.
2. Run **Dev Containers: Reopen in Container**.
3. Docker Compose starts the Flink cluster and both PostgreSQL databases.
4. Verify the services:

```bash
docker compose ps
```

5. Open the Flink Web UI at http://localhost:8081.

## Databases

Source:

```bash
psql postgresql://flink:flink@postgres-source:5432/taxes
```

Target:

```bash
psql postgresql://flink:flink@postgres-target:5432/taxes
```

The initial source table is `customers` and the target table is `active_customers`.

## CDC exercise

Insert a customer:

```sql
INSERT INTO customers (name, status, balance)
VALUES ('Maxi', 'ACTIVE', 1000.00);
```

Update the balance:

```sql
UPDATE customers
SET balance = 2500.00
WHERE name = 'Maxi';
```

Change the customer status:

```sql
UPDATE customers
SET status = 'INACTIVE'
WHERE name = 'Maxi';
```

The Flink pipeline will be added in the next step to observe the changelog and materialize only ACTIVE customers.

## PostgreSQL CDC

`postgres-source` starts with:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

This allows the Flink PostgreSQL CDC connector to consume the WAL through PostgreSQL logical replication.

## Next steps

1. Add the PostgreSQL CDC connector to the Flink runtime.
2. Create the Flink SQL `customers_source` table.
3. Create the JDBC sink for `active_customers`.
4. Run `INSERT INTO ... SELECT ... WHERE status = 'ACTIVE'`.
5. Test the initial snapshot and subsequent INSERT, UPDATE, and DELETE operations.
6. Enable and observe checkpoints and recovery.
