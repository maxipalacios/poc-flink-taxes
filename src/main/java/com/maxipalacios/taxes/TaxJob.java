package com.maxipalacios.taxes;

import java.time.ZoneId;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * The Flink job of the tax engine PoC. {@link #main} runs the certificate
 * consolidation pipeline (issue #5): it reads the CDC of the source
 * {@code tax_calculations} table and consolidates the {@code RET_*}
 * withholdings into fixed 60-second certification periods, incrementally
 * upserting the growing certificate rate lines into {@code certificate_items}
 * in the target database.
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

        tables.executeSql(createCdcSourceDdl(cfg));
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

        tables.executeSql(createCdcSourceDdl(cfg));
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
    // watermark is inert for the mirror and for the plain group aggregation,
    // but it is the foundation for issue #7's late-event dropping.
    private static String createCdcSourceDdl(PipelineConfig cfg) {
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
                    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,
                    PRIMARY KEY (id) NOT ENFORCED
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
                        cfg.sourceHost(),
                        cfg.sourcePort(),
                        cfg.username(),
                        cfg.password(),
                        cfg.databaseName(),
                        cfg.replicationSlotName());
    }

    // Group aggregation instead of a window TVF: a window TVF only emits when
    // the window closes (watermark passes), which cannot grow the certificate
    // live while its period is open. A plain group aggregation emits an
    // updated row per input record and the JDBC upsert sink materializes it
    // incrementally; rows for a period stop changing once no more rows for it
    // arrive, and late rows are not dropped (issue #7's scope).
    //
    // Periods are fixed 60-second tumbling windows until issue #7 parametrizes
    // the size and aligns them to America/Argentina/Buenos_Aires. FLOOR is
    // applied after casting created_at to TIMESTAMP(3) because two 1.20.5
    // paths are broken: FLOOR directly on TIMESTAMP_LTZ fails codegen
    // (CompileException) and UNIX_TIMESTAMP(timestamp_ltz) is rejected by the
    // validator; under the UTC pin above the cast is exact. 'RET|_%' ESCAPE
    // '|' keeps the underscore literal. establishment/merchant_name stay NULL:
    // enrichment lands in issue #6 and the physical columns are nullable on
    // purpose.
    private static String createConsolidationInsertSql() {
        return """
                INSERT INTO certificate_items
                SELECT
                    cuit,
                    tax_id,
                    CAST(FLOOR(CAST(created_at AS TIMESTAMP(3)) TO MINUTE) AS TIMESTAMP(6)),
                    tax_rate,
                    CAST(NULL AS STRING),
                    CAST(NULL AS STRING),
                    CAST(FLOOR(CAST(created_at AS TIMESTAMP(3)) TO MINUTE) AS TIMESTAMP(6)) + INTERVAL '60' SECOND,
                    CAST(SUM(base_tax) AS DECIMAL(18, 2)),
                    CAST(SUM(tax_amount) AS DECIMAL(18, 2))
                FROM tax_calculations_cdc
                WHERE tax_id LIKE 'RET|_%' ESCAPE '|'
                GROUP BY
                    cuit,
                    tax_id,
                    tax_rate,
                    FLOOR(CAST(created_at AS TIMESTAMP(3)) TO MINUTE)
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
