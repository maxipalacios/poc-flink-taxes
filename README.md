# poc-flink-taxes

PoC de Apache Flink 1.20 para aprender Change Data Capture (CDC) con PostgreSQL.

## Arquitectura

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

El objetivo inicial es observar cómo un snapshot inicial y los cambios posteriores (INSERT, UPDATE y DELETE) se propagan desde PostgreSQL source hacia una tabla materializada en PostgreSQL target.

## Servicios

- `dev`: Dev Container con Java 17 y Gradle.
- `jobmanager`: Flink JobManager.
- `taskmanager`: Flink TaskManager con 4 slots.
- `postgres-source`: PostgreSQL configurado para logical replication.
- `postgres-target`: PostgreSQL que materializa el resultado.
- Flink Web UI: http://localhost:8081
- Source PostgreSQL: localhost:5432
- Target PostgreSQL: localhost:5433

## Primer arranque

1. Abrir el repositorio en VS Code.
2. Ejecutar **Dev Containers: Reopen in Container**.
3. Docker Compose inicia el cluster y ambas bases.
4. Verificar servicios:

```bash
docker compose ps
```

5. Abrir la UI de Flink en http://localhost:8081.

## Bases de datos

Source:

```bash
psql postgresql://flink:flink@postgres-source:5432/taxes
```

Target:

```bash
psql postgresql://flink:flink@postgres-target:5432/taxes
```

La tabla source inicial es `customers` y la tabla target es `active_customers`.

## Ejercicio CDC

Insertar:

```sql
INSERT INTO customers (name, status, balance)
VALUES ('Maxi', 'ACTIVE', 1000.00);
```

Actualizar:

```sql
UPDATE customers
SET balance = 2500.00
WHERE name = 'Maxi';
```

Cambiar el estado:

```sql
UPDATE customers
SET status = 'INACTIVE'
WHERE name = 'Maxi';
```

La pipeline Flink se agregará como siguiente paso para observar el changelog y materializar únicamente clientes ACTIVE.

## PostgreSQL CDC

`postgres-source` arranca con:

```text
wal_level=logical
max_wal_senders=10
max_replication_slots=10
```

Esto permite que el conector PostgreSQL CDC de Flink consuma el WAL mediante logical replication.

## Próximos pasos

1. Agregar PostgreSQL CDC connector al runtime Flink.
2. Crear la tabla Flink SQL `customers_source`.
3. Crear el JDBC sink `active_customers`.
4. Ejecutar `INSERT INTO ... SELECT ... WHERE status = 'ACTIVE'`.
5. Probar snapshot inicial, INSERT, UPDATE y DELETE.
6. Activar y observar checkpoints y recuperación.
