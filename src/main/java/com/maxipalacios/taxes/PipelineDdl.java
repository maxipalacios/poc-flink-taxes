package com.maxipalacios.taxes;

/**
 * Builds the CREATE TABLE DDL of the certificate consolidation pipeline's
 * table environment: the two CDC source tables and the JDBC sink. Extracted
 * from {@link TaxJob} (divergent change): TaxJob orchestrates the pipeline,
 * this class owns the connector DDL that turns a validated
 * {@link PipelineConfig} into Flink table declarations.
 *
 * <p>Every interpolated value — host, port, username, password, database
 * name, slot names — comes from fields that {@link DbConnection} and
 * {@link PipelineConfig} validated at construction, so the single-quoted DDL
 * literals below cannot be broken out of (no quote, backslash, semicolon or
 * newline survives validation).
 */
final class PipelineDdl {

    private PipelineDdl() {
    }

    // Column types mirror the physical schema of the source `tax_calculations`
    // (docker/postgres/source/init.sql); Postgres timestamptz maps to
    // TIMESTAMP_LTZ. created_at carries precision 3, the maximum Flink allows
    // on a watermark's time field (a time field of precision 6 is rejected by
    // schema validation); source instants are second-granular, so millis
    // precision loses nothing. NOT ENFORCED because Flink never rechecks the
    // key. The 5-second bounded-out-of-orderness watermark gates when
    // enriched rows are emitted (see TaxJob's consolidation INSERT) and
    // drives the late-arrival drop in that pipeline's WHERE (issue #7).
    //
    // The source is declared append-only (no PRIMARY KEY clause):
    // `tax_calculations` is INSERT-only by domain definition (GLOSSARY, "Tax
    // Calculation": every calculation is new, an existing one is never
    // corrected), so declaring the source append-only is semantically exact.
    // It is also load-bearing: with the updating (PK-declared) changelog,
    // Flink 1.20.5 silently drops every row a downstream group aggregation
    // receives from the event-time temporal join (verified in the e2e run:
    // the join emits, the aggregate receives, nothing materializes), while
    // with the append-only declaration the same plan materializes correctly.
    //
    // Publication autocreation is DISABLED: the publications are pre-created
    // by the source init script (docker/postgres/source/init.sql, also
    // mounted into the e2e source containers), because PostgreSQL 17 grants
    // publication creation only to superusers (FOR ALL TABLES) or table
    // owners (FOR TABLE) — and the least-privilege flink_cdc role is
    // deliberately neither. With 'filtered', Debezium would emit exactly such
    // a CREATE PUBLICATION and the job would fail at startup.
    static String createCdcSourceDdl(PipelineConfig cfg) {
        return """
                CREATE TABLE tax_calculations_cdc (
                    id             BIGINT,
                    transaction_id STRING,
                    cuit           STRING,
                    tax_id         STRING,
                    tax_rate       DECIMAL(5, 2),
                    base_tax       DECIMAL(18, 2),
                    tax_amount     DECIMAL(18, 2),
                    tax_status     STRING,
                    exclusion_rate DECIMAL(5, 2),
                    created_at     TIMESTAMP_LTZ(3),
                    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'postgres-cdc',
                    'hostname' = '%s',
                    'port' = '%s',
                    'username' = '%s',
                    'password' = '%s',
                    'database-name' = '%s',
                    'schema-name' = 'public',
                    'table-name' = 'tax_calculations',
                    'slot.name' = '%s',
                    'decoding.plugin.name' = 'pgoutput',
                    'scan.incremental.snapshot.enabled' = 'true',
                    'debezium.publication.autocreate.mode' = 'disabled',
                    'debezium.publication.name' = 'dbz_publication'
                )
                """.formatted(
                        cfg.source().host(),
                        cfg.source().port(),
                        cfg.source().username(),
                        // Safe to interpolate: DbConnection rejected single quotes,
                        // backslashes, semicolons and newlines, so the value stays
                        // inside this single-quoted literal.
                        cfg.source().password(),
                        cfg.source().databaseName(),
                        cfg.replicationSlotName());
    }

    // DDL for the merchants CDC source (issue #6). `merchants` is a mutable
    // changelog table (docker/postgres/source/init.sql): inserts and updates
    // stream as a versioned changelog keyed by CUIT, which is exactly what the
    // Flink temporal-join docs call a "versioned table" — a PK on the join key
    // plus a watermarked time attribute. The version time is Debezium's
    // `source.ts_ms`, exposed by the postgres-cdc connector as the `op_ts`
    // metadata column (present in flink-sql-connector-postgres-cdc 3.6.0's
    // PostgreSQLReadableMetadata, TIMESTAMP_LTZ(3)) and declared VIRTUAL
    // because it is not a physical column of the table. Snapshot records are
    // stamped with the moment the snapshot read them, so a merchant row
    // exists as a version only from the job's first snapshot on: calculations
    // created before that enrich to NULL (as-of finds no version yet), and
    // anything created after it sees the snapshot version — a calculation
    // created before a later merchant UPDATE keeps the snapshot version.
    // The 5-second bounded-out-of-orderness watermark mirrors the
    // tax_calculations source so the combined watermark advances evenly. A
    // distinct publication name keeps this source's publication isolated from
    // the tax_calculations source's `dbz_publication`: both CDC sources run
    // against the same database, and each publication must list only its own
    // table. Like the tax_calculations publication, this one is pre-created
    // by the source init script (see createCdcSourceDdl) — PostgreSQL gates
    // publication creation on superuser or table ownership, which the
    // flink_cdc role deliberately lacks — so autocreation is disabled. NOT
    // ENFORCED because Flink never rechecks the key.
    //
    // Incremental snapshot mode is deliberately DISABLED here: it only makes
    // snapshot chunks flow after their checkpoint commit, which made the tiny
    // merchants dimension's delivery race with the tax source's snapshot and
    // intermittently starve the whole pipeline (observed in the e2e runs).
    // The table is tiny master data; the legacy consistent snapshot starts
    // streaming immediately. NOTE for future maintainers: a merchant UPDATE
    // requires the table to carry REPLICA IDENTITY FULL in the source
    // database (set in docker/postgres/source/init.sql), otherwise the CDC
    // message has no before image and the source fails (observed in the e2e
    // runs).
    static String createMerchantsCdcDdl(PipelineConfig cfg) {
        return """
                CREATE TABLE merchants_cdc (
                    cuit          STRING,
                    name          STRING,
                    establishment STRING,
                    op_ts         TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL,
                    WATERMARK FOR op_ts AS op_ts - INTERVAL '5' SECOND,
                    PRIMARY KEY (cuit) NOT ENFORCED
                ) WITH (
                    'connector' = 'postgres-cdc',
                    'hostname' = '%s',
                    'port' = '%s',
                    'username' = '%s',
                    'password' = '%s',
                    'database-name' = '%s',
                    'schema-name' = 'public',
                    'table-name' = 'merchants',
                    'slot.name' = '%s',
                    'decoding.plugin.name' = 'pgoutput',
                    'scan.incremental.snapshot.enabled' = 'false',
                    'debezium.publication.autocreate.mode' = 'disabled',
                    'debezium.publication.name' = 'flink_tax_merchants_publication'
                )
                """.formatted(
                        cfg.source().host(),
                        cfg.source().port(),
                        cfg.source().username(),
                        // Safe to interpolate: DbConnection rejected single quotes,
                        // backslashes, semicolons and newlines, so the value stays
                        // inside this single-quoted literal.
                        cfg.source().password(),
                        cfg.source().databaseName(),
                        cfg.merchantsReplicationSlotName());
    }

    // Certificate sink DDL: window_start/window_end are declared TIMESTAMP(6)
    // and cast in the INSERT (TaxJob's consolidation query) because the JDBC
    // sink's Postgres dialect converter (flink-connector-jdbc 3.3.0) has no
    // converter for TIMESTAMP_LTZ. The INSERT therefore writes plain
    // TIMESTAMP values, which the JDBC/pgjdbc stack interprets in the JVM
    // default time zone — UTC in every runtime of this project (compose
    // containers and test host) — so the consolidation INSERT produces each
    // period boundary as the UTC wall clock (session wall clock minus the
    // zone's offset interval), and the boundary instants land exactly in the
    // physical timestamptz columns (docker/postgres/target/init.sql). No
    // stringtype=unspecified: the sink has no uuid column whose types Postgres
    // would need to infer from the target columns. The declared key matches
    // the physical PRIMARY KEY, so the JDBC sink upserts and replays converge
    // idempotently.
    static String createCertificateSinkDdl(PipelineConfig cfg) {
        return """
                CREATE TABLE certificate_items (
                    cuit             STRING,
                    tax_id           STRING,
                    window_start     TIMESTAMP(6),
                    tax_rate         DECIMAL(5, 2),
                    establishment    STRING,
                    merchant_name    STRING,
                    window_end       TIMESTAMP(6),
                    total_base_tax   DECIMAL(18, 2),
                    total_tax_amount DECIMAL(18, 2),
                    PRIMARY KEY (cuit, tax_id, window_start, tax_rate) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = 'jdbc:postgresql://%s:%s/%s',
                    'table-name' = 'certificate_items',
                    'username' = '%s',
                    'password' = '%s'
                )
                """.formatted(
                        cfg.target().host(),
                        cfg.target().port(),
                        cfg.target().databaseName(),
                        cfg.target().username(),
                        // Safe to interpolate: DbConnection rejected single quotes,
                        // backslashes, semicolons and newlines, so the value stays
                        // inside this single-quoted literal.
                        cfg.target().password());
    }
}
