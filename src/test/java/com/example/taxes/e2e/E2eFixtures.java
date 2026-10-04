package com.example.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import com.example.taxes.CertificationPeriod;
import com.example.taxes.DbConnection;
import com.example.taxes.PipelineConfig;

/**
 * Shared fixtures for the e2e suites: the source inserts and prep statements
 * every scenario plants into its {@link PostgresPair}, the perception-tick
 * pacing discipline, a small target-side count and the pipeline configuration
 * the suites submit. Extracted from verbatim copies that had spread across
 * the suites (same SQL, same constants, same semantics); each suite keeps its
 * scenario-specific helpers (expectation builders, fetchers, comparators)
 * local. The suites pull these in with static imports, like the assertion
 * imports they already use.
 */
final class E2eFixtures {

    /**
     * Real-time gap between the last row under test and the perception tick:
     * the tick only pushes the combined watermark past the last row's
     * created_at if it lands more than the 5-second bounded out-of-orderness
     * later in real time; 6.5 seconds leaves margin for scheduling and JDBC
     * round trips. The same gap also protects rows inserted AFTER a tick: a
     * row dated its own now() stays above the tick's watermark (tick - 5s).
     */
    static final long TICK_GAP_MILLIS = 6_500;

    private E2eFixtures() {
    }

    /**
     * Builds the pipeline configuration the e2e suites submit: the pair's
     * coordinates as both endpoints (the ephemeral pair runs as its bootstrap
     * user on both sides) and the pair's unique replication slot name.
     */
    static PipelineConfig pipelineConfig(PostgresPair pair, CertificationPeriod certificationPeriod) {
        return pipelineConfig(pair, pair.slotName(), certificationPeriod);
    }

    /**
     * Same, with an explicit slot name: the restart-from-scratch suite starts
     * a fresh job against the same pair on a NEW slot (PostgreSQL allows a
     * single consumer per slot, and the fresh job must re-snapshot instead of
     * resuming the abandoned one).
     */
    static PipelineConfig pipelineConfig(PostgresPair pair, String replicationSlotName,
            CertificationPeriod certificationPeriod) {
        return new PipelineConfig(
                new DbConnection(pair.sourceHost(), Integer.toString(pair.sourcePort()),
                        PostgresPair.USERNAME, PostgresPair.PASSWORD, PostgresPair.DATABASE),
                new DbConnection(pair.targetHost(), Integer.toString(pair.targetPort()),
                        PostgresPair.USERNAME, PostgresPair.PASSWORD, PostgresPair.DATABASE),
                replicationSlotName,
                certificationPeriod);
    }

    /** Plain insert whose created_at defaults to now(): the row lands in the currently open certification period. */
    static void insertSourceCalculation(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount) {
        pair.executeSourceStatement("""
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate)
                VALUES ('%s', '%s', %s, %s, %s, 'CL', 0.00)
                """.formatted(cuit, taxId, taxRate.toPlainString(), baseTax.toPlainString(), taxAmount.toPlainString()));
    }

    /** Same insert with an explicit event time, so the row lands in a chosen certification period. */
    static void insertSourceCalculationAt(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount, long createdAtEpochSecond) {
        pair.executeSourceStatement("""
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate, created_at)
                VALUES ('%s', '%s', %s, %s, %s, 'CL', 0.00, to_timestamp(%d))
                """.formatted(cuit, taxId, taxRate.toPlainString(), baseTax.toPlainString(),
                        taxAmount.toPlainString(), createdAtEpochSecond));
    }

    /**
     * Snapshot-row recipe prep (see the suites' javadocs): removes the seed's
     * own calculations so the rows a test plants are the ONLY ones the CDC
     * snapshot carries. The snapshot's delivery order is scrambled (verified
     * empirically), so any planted row older than an already-delivered newer
     * row would be dropped by the late-arrival filter; with the seed gone the
     * planted rows share one five-second band. Must run BEFORE the job starts.
     */
    static void deleteSeedTaxCalculations(PostgresPair pair) {
        pair.executeSourceStatement("DELETE FROM tax_calculations");
    }

    /**
     * Real-time gap between the last row under test and the perception tick:
     * the tick only pushes the combined watermark past the last row's
     * created_at if it lands more than the 5-second bounded out-of-orderness
     * later in real time (see {@link #TICK_GAP_MILLIS}).
     */
    static void waitForWatermarkTickGap() {
        try {
            Thread.sleep(TICK_GAP_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the watermark tick gap", e);
        }
    }

    /**
     * Scalar count against the target, e.g. the suites' global perception
     * checks: {@code sql} must return exactly one numeric column.
     */
    static long countTargetRows(PostgresPair pair, String sql, String failureMessage) {
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(failureMessage, e);
        }
    }

    /** Expected timestamps are instants; the JDBC session offset must not matter. */
    static OffsetDateTime atEpochSecond(long epochSecond) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.UTC);
    }
}
