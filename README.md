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

The goal is to use the PostgreSQL CDC connector to consume changes from the `tax_calculations` and `merchants` tables in the source database, consolidate tax withholdings into certificates with Flink, and materialize them in the target PostgreSQL database.

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

### Seed data

The source database is created with the domain model (`tax_calculations`, `merchants`) and seeded on first startup: five merchants plus a backfill of tax calculations covering both tax families (`RET_*` withholdings and `PER_*` perceptions) at multiple rates.

The target database is created with the `certificate_items` table, which stays empty until the Flink pipeline runs.

Initialization scripts run only on an empty data volume. To re-create and re-seed both databases from scratch:

```bash
docker compose down -v
docker compose up -d
```

### Verify the seed data

```bash
psql postgresql://flink:flink@postgres-source:5432/taxes -c 'SELECT count(*) FROM merchants;'  # expect 5

psql postgresql://flink:flink@postgres-source:5432/taxes -c "SELECT tax_id, count(*), sum(base_tax) AS total_base, sum(tax_amount) AS total_withheld FROM tax_calculations GROUP BY tax_id ORDER BY tax_id;"

psql postgresql://flink:flink@postgres-target:5432/taxes -c 'SELECT count(*) FROM certificate_items;'  # expect 0 until the pipeline runs
```

## Build and deploy

Build the fat JAR (from the host, no devcontainer needed; Gradle comes from the `dev` image):

```bash
docker compose run --rm dev bash -lc 'cd /workspace && gradle --no-daemon shadowJar'
```

The bare form without `bash -lc 'cd /workspace'` does not work: the one-off container starts in `/`, not in the mounted repo. Inside the devcontainer (VS Code) you can run `gradle shadowJar` directly.

Submit the job to the running cluster:

```bash
docker compose exec jobmanager flink run -c com.maxipalacios.taxes.TaxJob /opt/flink/usrlib/poc-flink-taxes-0.1.0-all.jar
```

`build/libs` is mounted into the JobManager at `/opt/flink/usrlib`, so no manual copy is needed. If you run `gradle clean` while the cluster is up, recreate the services so the mount picks up the new build directory (`docker compose up -d --force-recreate jobmanager taskmanager`). If the `flink-checkpoints` volume was just created, make it writable by the Flink user once:

```bash
docker compose exec jobmanager chown flink:flink /opt/flink/checkpoints
```

Then watch the job in the Flink Web UI at http://localhost:8081 (job graph, logs, and the "Checkpoints" tab, which should show completed checkpoints every 10 seconds).

## PostgreSQL CDC

`postgres-source` starts with:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

This allows the Flink PostgreSQL CDC connector to consume the WAL through PostgreSQL logical replication.

## Next steps

1. Create the Flink SQL source tables for `tax_calculations` and `merchants`, backed by the PostgreSQL CDC connector.
2. Consolidate tax withholdings into certificates with an aggregation over tumbling certification periods.
3. Upsert the resulting certificates into the target database via the JDBC connector.
4. Enable and observe checkpoints and recovery.
