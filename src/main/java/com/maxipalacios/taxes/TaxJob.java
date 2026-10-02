package com.maxipalacios.taxes;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Tracer-bullet job (issue #4): takes an initial snapshot of the source
 * {@code tax_calculations} table and then streams inserts into a raw mirror
 * table ({@code tax_calculations_mirror}) in the target database.
 *
 * <p>The mirror is a debugging artifact used to verify the CDC pipeline end
 * to end; it is not a domain table and later issues (#5+) do not consume it.
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

        mirrorTaxCalculations(MirrorConfig.fromEnv(), env).await();
    }

    /**
     * Builds and submits the mirror pipeline. Public for the e2e tests, which
     * run it on the local mini-cluster; rejects {@code env} without
     * checkpointing (see {@link #main}). The returned streaming job runs until
     * cancelled; {@link TableResult#await()} blocks for it.
     */
    public static TableResult mirrorTaxCalculations(MirrorConfig cfg, StreamExecutionEnvironment env) {
        // Fail fast: without checkpointing the CDC snapshot never commits a
        // chunk and the mirror stays silently empty.
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "Checkpointing must be enabled on env before building the mirror "
                            + "pipeline: CDC snapshot chunks commit on checkpoints.");
        }

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
    // TIMESTAMP_LTZ(6). NOT ENFORCED because Flink never rechecks the key.
    private static String createCdcSourceDdl(MirrorConfig cfg) {
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
                    created_at     TIMESTAMP_LTZ(6),
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

    // Mirror DDL: the JDBC sink's Postgres dialect converter (flink-connector-jdbc
    // 3.3.0) has no converter for TIMESTAMP_LTZ, so created_at is declared as
    // TIMESTAMP(6) and cast in the INSERT below; written into the physical
    // timestamptz column of tax_calculations_mirror (docker/postgres/target/
    // init.sql) the UTC instant is preserved. stringtype=unspecified lets
    // Postgres infer parameter types from the target columns, which is what
    // lets the STRING transaction_id land in its uuid column.
    private static String createMirrorSinkDdl(MirrorConfig cfg) {
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
