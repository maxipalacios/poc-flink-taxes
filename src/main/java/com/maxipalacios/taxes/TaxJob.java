package com.maxipalacios.taxes;

import java.time.Duration;
import java.time.ZoneId;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

/**
 * The Flink job of the tax engine PoC. {@link #main} runs the certificate
 * consolidation pipeline (issues #5 and #6): it reads the CDC of the source
 * {@code tax_calculations} table, consolidates the {@code RET_*} withholdings
 * into fixed 60-second certification periods, enriches every rate line with
 * the merchant master data as of each calculation's {@code created_at}
 * (event-time temporal join against the CDC of {@code merchants}), and
 * incrementally upserts the growing certificate rate lines into
 * {@code certificate_items} in the target database.
 *
 * <p>The mirror pipeline ({@link #mirrorTaxCalculations}, issue #4) remains a
 * debugging artifact used to verify the CDC pipeline end to end; it is not a
 * domain table, later issues do not consume it, and it is only exercised by
 * its e2e test.
 */
public final class TaxJob {

    private TaxJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // CDC incremental snapshots commit read chunks on checkpoints, so
        // checkpointing must be enabled before the table environment exists.
        env.enableCheckpointing(10_000);

        consolidateCertificates(PipelineConfig.fromEnv(), env).await();
    }

    // Fail fast: without checkpointing the CDC snapshot never commits a
    // chunk and the pipeline's output silently stays empty.
    private static void requireCheckpointing(StreamExecutionEnvironment env, String pipeline) {
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "Checkpointing must be enabled on env before building the " + pipeline
                            + " pipeline: CDC snapshot chunks commit on checkpoints.");
        }
    }

    /**
     * Builds and submits the certificate consolidation pipeline. Public for
     * the e2e tests, which run it on the local mini-cluster; rejects
     * {@code env} without checkpointing (see {@link #main}). The returned
     * streaming job runs until cancelled; {@link TableResult#await()} blocks
     * for it.
     */
    public static TableResult consolidateCertificates(PipelineConfig cfg, StreamExecutionEnvironment env) {
        requireCheckpointing(env, "certificate consolidation");

        StreamTableEnvironment tables = StreamTableEnvironment.create(env);

        // Pinned to UTC so the TIMESTAMP_LTZ -> TIMESTAMP cast in the
        // consolidation INSERT is deterministic: the Postgres sessions run
        // UTC, so instants round-trip exactly. 60-second periods align
        // identically in UTC and Buenos Aires (whole-hour offset), so UTC
        // minute floors are correct here; issue #7 revisits the zone.
        tables.getConfig().setLocalTimeZone(ZoneId.of("UTC"));

        // The event-time temporal join in the consolidation INSERT buffers
        // every calculation until the combined watermark of BOTH inputs
        // passes the calculation's created_at. The merchants dimension is
        // quiet in streaming (merchant changes are rare) and its watermark
        // freezes at its last record's op_ts minus 5 seconds — the snapshot
        // read time — which sits below every later streaming calculation, so
        // without help it would hold every buffered calculation back
        // forever. Flink's default table.exec.source.idle-timeout is 0
        // (disabled); after 2 seconds without records the merchants input is
        // marked idle and stops gating the combined watermark while no
        // merchant changes stream in, and a real merchant change re-activates
        // it instantly. The value is short ON PURPOSE: both inputs idle on
        // this same timeout, and once BOTH are idle the combined watermark
        // never recomputes, so if the tax input idled before the merchants
        // one, every buffered row would freeze below its emission watermark
        // (observed as a total stall in the e2e runs with a 10-second
        // timeout). 2 seconds lets the merchants input — whose tiny snapshot
        // finishes first — reliably go idle first.
        tables.getConfig().set(
                ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofSeconds(2));

        tables.executeSql(createCdcSourceDdl(cfg, false));
        tables.executeSql(createMerchantsCdcDdl(cfg));
        tables.executeSql(createCertificateSinkDdl(cfg));

        return tables.executeSql(createConsolidationInsertSql());
    }

    /**
     * Builds and submits the mirror pipeline. Public for the e2e tests, which
     * run it on the local mini-cluster; rejects {@code env} without
     * checkpointing (see {@link #main}). The returned streaming job runs until
     * cancelled; {@link TableResult#await()} blocks for it.
     */
    public static TableResult mirrorTaxCalculations(PipelineConfig cfg, StreamExecutionEnvironment env) {
        requireCheckpointing(env, "mirror");

        StreamTableEnvironment tables = StreamTableEnvironment.create(env);

        tables.executeSql(createCdcSourceDdl(cfg, true));
        tables.executeSql(createMirrorSinkDdl(cfg));

        return tables.executeSql("""
                INSERT INTO tax_calculations_mirror
                SELECT id, transaction_id, cuit, tax_id, tax_rate, base_tax, tax_amount,
                       tax_status, exclusion_rate, CAST(created_at AS TIMESTAMP(6))
                FROM tax_calculations_cdc
                """);
    }

    // Column types mirror the physical schema of the source `tax_calculations`
    // (docker/postgres/source/init.sql); Postgres timestamptz maps to
    // TIMESTAMP_LTZ. created_at carries precision 3, the maximum Flink allows
    // on a watermark's time field (a time field of precision 6 is rejected by
    // schema validation); source instants are second-granular, so millis
    // precision loses nothing. NOT ENFORCED because Flink never rechecks the
    // key. Both pipelines share this DDL: the 5-second bounded-out-of-orderness
    // watermark is inert for the mirror, but for consolidation it now gates
    // when enriched rows are emitted (see createConsolidationInsertSql), and
    // it remains the foundation for issue #7's late-event dropping.
    //
    // `declaredPrimaryKey`: the mirror keeps the key because issue #4
    // declared it that way and its behavior must not change. The
    // consolidation pipeline passes false: `tax_calculations` is INSERT-only
    // by domain definition (GLOSSARY, "Tax Calculation": every calculation is
    // new, an existing one is never corrected), so declaring the source
    // append-only is semantically exact. It is also load-bearing there:
    // with the updating (PK-declared) changelog, Flink 1.20.5 silently drops
    // every row a downstream group aggregation receives from the event-time
    // temporal join (verified in the e2e run: the join emits, the aggregate
    // receives, nothing materializes), while with the append-only declaration
    // the same plan materializes correctly.
    private static String createCdcSourceDdl(PipelineConfig cfg, boolean declaredPrimaryKey) {
        String primaryKeyClause = declaredPrimaryKey
                ? ",\n    PRIMARY KEY (id) NOT ENFORCED"
                : "";
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
                    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND%s
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
                    'debezium.publication.autocreate.mode' = 'filtered'
                )
                """.formatted(
                        primaryKeyClause,
                        cfg.sourceHost(),
                        cfg.sourcePort(),
                        cfg.username(),
                        cfg.password(),
                        cfg.databaseName(),
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
    // the tax_calculations source's default `dbz_publication`: both CDC
    // sources run against the same database and 'filtered' autocreate mode
    // must not fight over one publication. NOT ENFORCED because Flink never
    // rechecks the key.
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
    private static String createMerchantsCdcDdl(PipelineConfig cfg) {
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
                    'debezium.publication.autocreate.mode' = 'filtered',
                    'debezium.publication.name' = 'flink_tax_merchants_publication'
                )
                """.formatted(
                        cfg.sourceHost(),
                        cfg.sourcePort(),
                        cfg.username(),
                        cfg.password(),
                        cfg.databaseName(),
                        cfg.merchantsReplicationSlotName());
    }

    // Group aggregation fed by an event-time temporal join (issue #6) instead
    // of a window TVF: a window TVF only emits when the window closes
    // (watermark passes), which cannot grow the certificate live while its
    // period is open. A plain group aggregation emits an updated row per
    // input record and the JDBC upsert sink materializes it incrementally;
    // rows for a period stop changing once no more rows for it arrive, and
    // late rows are not dropped (issue #7's scope).
    //
    // Enrichment (issue #6): each calculation LEFT JOINs the merchant
    // changelog FOR SYSTEM_TIME AS OF its own created_at, so the certificate
    // is a pure function of the source history and replays reproduce
    // identical certificates (the #5 follow-up's idempotency requirement).
    // LEFT join semantics: a withholding whose CUIT has no merchant still
    // consolidates, with null merchant columns (missing master data must not
    // drop withholdings). LAST_VALUE carries the last merchant version seen
    // in the period (a merchant update during a period results in the last
    // version on the certificate), and LAST_VALUE skips NULL inputs, so
    // merchantless rows can never clobber enriched values inside one group.
    //
    // Emission lag: the temporal join buffers each calculation until the
    // combined watermark of BOTH inputs passes its created_at (both sources
    // use 5-second bounded-out-of-orderness watermarks, and the merchants
    // input stops gating the combined watermark while quiet — see the
    // idle-timeout in consolidateCertificates). Emissions therefore lag
    // inserts by at least the watermark bound, and the last row of a quiet
    // stream waits for the next record on either input; this is inherent to
    // the event-time temporal join, and the 5-second bound is pinned by
    // issue #7.
    //
    // Periods are fixed 60-second tumbling windows until issue #7 parametrizes
    // the size and aligns them to America/Argentina/Buenos_Aires. FLOOR is
    // applied after casting created_at to TIMESTAMP(3) because two 1.20.5
    // paths are broken: FLOOR directly on TIMESTAMP_LTZ fails codegen
    // (CompileException) and UNIX_TIMESTAMP(timestamp_ltz) is rejected by the
    // validator; under the UTC pin above the cast is exact. 'RET|_%' ESCAPE
    // '|' keeps the underscore literal.
    private static String createConsolidationInsertSql() {
        return """
                INSERT INTO certificate_items
                SELECT
                    calc.cuit,
                    calc.tax_id,
                    CAST(FLOOR(CAST(calc.created_at AS TIMESTAMP(3)) TO MINUTE) AS TIMESTAMP(6)),
                    calc.tax_rate,
                    LAST_VALUE(merchants.establishment),
                    LAST_VALUE(merchants.name),
                    CAST(FLOOR(CAST(calc.created_at AS TIMESTAMP(3)) TO MINUTE) AS TIMESTAMP(6)) + INTERVAL '60' SECOND,
                    CAST(SUM(calc.base_tax) AS DECIMAL(18, 2)),
                    CAST(SUM(calc.tax_amount) AS DECIMAL(18, 2))
                FROM tax_calculations_cdc calc
                LEFT JOIN merchants_cdc FOR SYSTEM_TIME AS OF calc.created_at AS merchants
                    ON calc.cuit = merchants.cuit
                WHERE calc.tax_id LIKE 'RET|_%' ESCAPE '|'
                GROUP BY
                    calc.cuit,
                    calc.tax_id,
                    calc.tax_rate,
                    FLOOR(CAST(calc.created_at AS TIMESTAMP(3)) TO MINUTE)
                """;
    }

    // Certificate sink DDL: window_start/window_end are declared TIMESTAMP(6)
    // and cast in the INSERT because the JDBC sink's Postgres dialect
    // converter (flink-connector-jdbc 3.3.0) has no converter for
    // TIMESTAMP_LTZ (same finding as the mirror DDL); written into the
    // physical timestamptz columns (docker/postgres/target/init.sql) the UTC
    // instant is preserved. No stringtype=unspecified here: unlike the mirror,
    // there is no uuid column to land. The declared key matches the physical
    // PRIMARY KEY, so the JDBC sink upserts and replays converge idempotently.
    private static String createCertificateSinkDdl(PipelineConfig cfg) {
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
                        cfg.targetHost(),
                        cfg.targetPort(),
                        cfg.databaseName(),
                        cfg.username(),
                        cfg.password());
    }

    // Mirror DDL: the JDBC sink's Postgres dialect converter (flink-connector-jdbc
    // 3.3.0) has no converter for TIMESTAMP_LTZ, so created_at is declared as
    // TIMESTAMP(6) and cast in the INSERT below; written into the physical
    // timestamptz column of tax_calculations_mirror (docker/postgres/target/
    // init.sql) the UTC instant is preserved. stringtype=unspecified lets
    // Postgres infer parameter types from the target columns, which is what
    // lets the STRING transaction_id land in its uuid column.
    private static String createMirrorSinkDdl(PipelineConfig cfg) {
        return """
                CREATE TABLE tax_calculations_mirror (
                    id             BIGINT,
                    transaction_id STRING,
                    cuit           STRING,
                    tax_id         STRING,
                    tax_rate       DECIMAL(5, 2),
                    base_tax       DECIMAL(18, 2),
                    tax_amount     DECIMAL(18, 2),
                    tax_status     STRING,
                    exclusion_rate DECIMAL(5, 2),
                    created_at     TIMESTAMP(6),
                    PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = 'jdbc:postgresql://%s:%s/%s?stringtype=unspecified',
                    'table-name' = 'tax_calculations_mirror',
                    'username' = '%s',
                    'password' = '%s'
                )
                """.formatted(
                        cfg.targetHost(),
                        cfg.targetPort(),
                        cfg.databaseName(),
                        cfg.username(),
                        cfg.password());
    }
}
