package com.maxipalacios.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;

import com.maxipalacios.taxes.PipelineConfig;
import com.maxipalacios.taxes.TaxJob;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tracer bullet (issue #4): runs the real CDC mirror job on the
 * in-process mini-cluster against two ephemeral PostgreSQL containers and
 * verifies (1) the startup snapshot copies the pre-existing source rows into
 * {@code tax_calculations_mirror}, and (2) a new source insert streams into
 * the mirror within seconds.
 */
class TaxCalculationsMirrorE2eTest {

    private static final String NEW_ROW_CUIT = "29999999997";
    private static final String NEW_ROW_TAX_AMOUNT = "77.70";

    // Content probe for the initial snapshot: the seed row with no merchant
    // (missing master data), distinctive enough to spot a wrong-value mirror.
    // Values must stay in sync with docker/postgres/source/init.sql.
    private static final String SNAPSHOT_ROW_CUIT = "20149543350";
    private static final String SNAPSHOT_ROW_TAX_ID = "RET_IVA";
    private static final String SNAPSHOT_ROW_TAX_AMOUNT = "210.00";

    // A mirrored created_at further off than this means the TIMESTAMP_LTZ →
    // TIMESTAMP(6) cast drifted across timezones instead of round-tripping
    // the instant.
    private static final Duration CREATED_AT_SKEW_TOLERANCE = Duration.ofMinutes(5);

    @Test
    void mirrorsSourceTaxCalculationsThroughCdc() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {
            PipelineConfig cfg = new PipelineConfig(
                    pair.sourceHost(),
                    Integer.toString(pair.sourcePort()),
                    pair.targetHost(),
                    Integer.toString(pair.targetPort()),
                    PostgresPair.DATABASE,
                    PostgresPair.USERNAME,
                    PostgresPair.PASSWORD,
                    pair.slotName());

            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            // Same order as TaxJob.main: CDC snapshot chunks only commit on
            // checkpoints, so checkpointing must exist before the pipeline is
            // built.
            env.enableCheckpointing(5_000);

            result = TaxJob.mirrorTaxCalculations(cfg, env);

            long snapshotSize = countSourceRows(pair);
            await().atMost(Duration.ofSeconds(90))
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        assertEquals(snapshotSize, countTargetRows(pair),
                                "mirror must hold every row present before the job started");
                        assertSnapshotRowContent(pair);
                    });

            long newId = insertSourceTaxCalculation(pair);
            await().atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        Optional<MirroredRow> mirrored = fetchMirroredRow(pair, newId);
                        assertTrue(mirrored.isPresent(),
                                "mirror must contain the streamed insert, id=" + newId);
                        mirrored.ifPresent(row -> {
                            assertEquals(NEW_ROW_CUIT, row.cuit());
                            assertEquals(0, new BigDecimal(NEW_ROW_TAX_AMOUNT).compareTo(row.taxAmount()),
                                    "tax_amount must match the source insert");
                            assertTrue(Duration.between(row.createdAt(), Instant.now()).abs()
                                            .compareTo(CREATED_AT_SKEW_TOLERANCE) < 0,
                                    "created_at must round-trip the source instant, got " + row.createdAt());
                        });
                    });
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    /** Distinctive insert so the streamed row can never be confused with the seed data. */
    private record MirroredRow(long id, String cuit, BigDecimal taxAmount, Instant createdAt) {
    }

    /** The seed row is only trusted present when its values match the source, not just its count. */
    private void assertSnapshotRowContent(PostgresPair pair) {
        Optional<MirroredRow> mirrored = fetchSeedRowByBusinessKey(pair);
        assertTrue(mirrored.isPresent(),
                "mirror must contain the seeded row " + SNAPSHOT_ROW_CUIT + "/" + SNAPSHOT_ROW_TAX_ID);
        mirrored.ifPresent(row -> {
            assertEquals(SNAPSHOT_ROW_CUIT, row.cuit());
            assertEquals(0, new BigDecimal(SNAPSHOT_ROW_TAX_AMOUNT).compareTo(row.taxAmount()),
                    "seeded snapshot row must mirror the source values");
        });
    }

    private long insertSourceTaxCalculation(PostgresPair pair) {
        // RETURNING id because the column is GENERATED ALWAYS AS IDENTITY; the
        // id must never be guessed. Not executeSourceStatement(): that helper
        // returns void and cannot surface the RETURNING result.
        String sql = """
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate)
                VALUES ('%s', 'RET_IVA_E2E', 7.77, 1000.00, %s, 'CL', 0.00)
                RETURNING id
                """.formatted(NEW_ROW_CUIT, NEW_ROW_TAX_AMOUNT);
        try (Connection connection = pair.openSourceConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong("id");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to insert streaming test row", e);
        }
    }

    private long countSourceRows(PostgresPair pair) {
        try (Connection connection = pair.openSourceConnection()) {
            return countRows("tax_calculations", connection);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows in tax_calculations", e);
        }
    }

    private long countTargetRows(PostgresPair pair) {
        try (Connection connection = pair.openTargetConnection()) {
            return countRows("tax_calculations_mirror", connection);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows in tax_calculations_mirror", e);
        }
    }

    /** The caller owns and closes {@code connection}. */
    private long countRows(String table, Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + table)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private Optional<MirroredRow> fetchMirroredRow(PostgresPair pair, long id) {
        String sql = "SELECT id, cuit, tax_amount, created_at FROM tax_calculations_mirror WHERE id = ?";
        return queryOneMirroredRow(pair, sql, statement -> statement.setLong(1, id));
    }

    /**
     * The seed carries two rows under this business key (the planted
     * missing-master-data row and a later one), so the probe must pick one
     * deterministically: the oldest one is the missing-master-data row this
     * probe inspects. Without the ORDER BY the outcome would depend on the
     * physical tuple order, which the sink's idempotent upserts shuffle (each
     * ON CONFLICT DO UPDATE moves the tuple to the heap end).
     */
    private Optional<MirroredRow> fetchSeedRowByBusinessKey(PostgresPair pair) {
        String sql = "SELECT id, cuit, tax_amount, created_at FROM tax_calculations_mirror "
                + "WHERE cuit = ? AND tax_id = ? "
                + "ORDER BY id LIMIT 1";
        return queryOneMirroredRow(pair, sql, statement -> {
            statement.setString(1, SNAPSHOT_ROW_CUIT);
            statement.setString(2, SNAPSHOT_ROW_TAX_ID);
        });
    }

    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private Optional<MirroredRow> queryOneMirroredRow(PostgresPair pair, String sql, StatementBinder binder) {
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(new MirroredRow(
                        resultSet.getLong("id"),
                        resultSet.getString("cuit"),
                        resultSet.getBigDecimal("tax_amount"),
                        resultSet.getObject("created_at", OffsetDateTime.class).toInstant()));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read mirrored row: " + sql, e);
        }
    }
}
