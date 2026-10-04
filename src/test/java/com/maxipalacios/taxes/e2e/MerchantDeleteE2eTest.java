package com.maxipalacios.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.maxipalacios.taxes.CertificationPeriod;
import com.maxipalacios.taxes.TaxJob;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.maxipalacios.taxes.e2e.E2eFixtures.atEpochSecond;
import static com.maxipalacios.taxes.e2e.E2eFixtures.countTargetRows;
import static com.maxipalacios.taxes.e2e.E2eFixtures.deleteSeedTaxCalculations;
import static com.maxipalacios.taxes.e2e.E2eFixtures.insertSourceCalculation;
import static com.maxipalacios.taxes.e2e.E2eFixtures.waitForWatermarkTickGap;

/**
 * End-to-end test for a merchant that disappears (spec stories #3 and #11):
 * runs the certificate consolidation job with merchant enrichment on the
 * in-process mini-cluster against two ephemeral PostgreSQL containers and
 * pins what a merchant DELETE does to the materialized {@code
 * certificate_items}:
 *
 * <ul>
 *   <li>a merchant UPDATE inside an open certification period consolidates
 *       into the rate line as the new values (the temporal join's as-of
 *       lookup finds the updated version, the same semantics the enrichment
 *       suite pins for LAST_VALUE);</li>
 *   <li>the certification period then CLOSES deterministically (window_end
 *       in the past, achieved by real time simply passing the period's known
 *       end — the line was emitted streaming, so its period start is pinned
 *       by the test);</li>
 *   <li>then the merchant row is DELETEd from the source. {@code merchants}
 *       is a versioned changelog with REPLICA IDENTITY FULL, so the delete
 *       streams as a CDC record with a complete before image; the already
 *       closed certificate must be UNCHANGED afterwards — later merchant
 *       changes only affect future periods, never the frozen ones;</li>
 *   <li>a later withholding in a NEW open period still consolidates (the
 *       pipeline survives the delete), with SQL NULL merchant columns — no
 *       merchant version exists as of its created_at once the row is gone
 *       (see the delete note below).</li>
 * </ul>
 *
 * <p><b>How the delete reaches the join state (verified against Flink
 * 1.20.5).</b> The versioned temporal join's build side receives the DELETE
 * record like any other changelog record, and from that moment a withholding
 * created after the delete finds NO merchant content as of its created_at:
 * the temporal join consolidates it with SQL NULL merchant columns — the
 * LEFT-join semantics that already cover a missing merchant row now cover a
 * deleted one. The closed certificate is untouched by any of it: its
 * withholding was emitted long before, and a merchant-side change never
 * re-processes an already joined withholding.
 *
 * <p>Watermark mechanics and the settle/tick discipline are the ones every
 * other e2e suite pins (see {@code MerchantEnrichmentE2eTest}'s javadoc for
 * the full treatment): rows are released by perception ticks inserted after
 * the tick gap, the first source record waits out the CDC probe window, and
 * inserts are held back until the running minute is young enough for the
 * whole scenario to stay inside one open period. Assertions are on target
 * rows only — except one source-side guard that the DELETE actually
 * committed, so the immutability assertion cannot pass vacuously.
 */
class MerchantDeleteE2eTest {

    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);

    /**
     * Settle wait after starting the job before the FIRST source record of
     * the run (the warm-up tick included): the incremental CDC source probes
     * for its streaming resume position for roughly the first ~12 seconds
     * (observed in the job logs), and records committed inside that window
     * can be skipped when the stream starts past them. Asserted inserts
     * start only after this wait.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    /**
     * The scenario (merchant update, withholding, tick) must land early
     * enough in its minute to stay inside one open certification period;
     * the post-delete part needs the same headroom for its own tick.
     */
    private static final long MINUTE_HEADROOM_SECONDS = 30;

    /**
     * The scenario under test has no snapshot rows: every asserted line is
     * emitted streaming, so its certification period closes deterministically
     * when real time passes the period's known end. This margin is added on
     * top of that end before the test proceeds (window_end strictly in the
     * past, with room for clock and JDBC round-trip skew).
     */
    private static final long PERIOD_CLOSE_MARGIN_MILLIS = 1_500;

    private static final String TAX_ID_RET_IVA = "RET_IVA";
    private static final String TAX_ID_PER_IVA = "PER_IVA";

    // Seeded merchant (docker/postgres/source/init.sql); the cuit is pinned
    // to the seed so the merchant row provably exists before the job's first
    // snapshot and its DELETE removes a real master-data row.
    private static final String CARREFOUR_CUIT = "30584620389";
    private static final String CARREFOUR_UPDATED_NAME = "Carrefour Express";
    private static final String CARREFOUR_UPDATED_ESTABLISHMENT = "10999";

    // The perception ticks' cuit: absent from merchants, and PER_* rows are
    // dropped by the taxonomy filter before anything is written anyway.
    private static final String TICK_CUIT = "27777777771";

    // Part A: the open-period line that must carry the updated merchant
    // values. Part B: the post-delete withholding in the new open period.
    // Non-trivial cents pin exact DECIMAL summation; each constant feeds both
    // the source INSERT and the expected line, so the two cannot drift.
    private static final BigDecimal UPDATED_RATE_6_75 = new BigDecimal("6.75");
    private static final BigDecimal UPDATED_BASE = new BigDecimal("1500.75");
    private static final BigDecimal UPDATED_AMOUNT = new BigDecimal("101.30");
    private static final BigDecimal POST_DELETE_RATE_2_50 = new BigDecimal("2.50");
    private static final BigDecimal POST_DELETE_BASE = new BigDecimal("800.00");
    private static final BigDecimal POST_DELETE_AMOUNT = new BigDecimal("20.00");

    // Perception tick rows' content is irrelevant (dropped by taxonomy).
    private static final BigDecimal PER_TICK_RATE_21_00 = new BigDecimal("21.00");
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void keepsClosedCertificatesImmutableWhenTheMerchantIsDeletedAfterClose() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // Snapshot-row recipe prep (see E2eFixtures.deleteSeedTaxCalculations):
            // this test plants no snapshot rows of its own, but the seed's
            // calculations must go so the only Carrefour lines in
            // certificate_items are the ones this test asserts on.
            deleteSeedTaxCalculations(pair);

            result = startConsolidationJob(pair);

            // Settle + warm-up (see STREAM_ANCHOR_SETTLE_MILLIS): the
            // sacrificial perception row forces any remaining CDC probe to
            // complete; perceptions never consolidate, so it cannot alter
            // any line below.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertPerceptionTick(pair);

            // ---- Part A: the merchant update consolidates with the new
            // values inside one open period. ----
            waitUntilMinuteHeadroom(MINUTE_HEADROOM_SECONDS);
            long periodStart = Instant.now().getEpochSecond() / 60 * 60;

            // The mid-period merchant change: streams into the pipeline as a
            // -U/+U pair; the versioned state keeps the new version under the
            // update's op_ts.
            updateMerchant(pair, CARREFOUR_CUIT, CARREFOUR_UPDATED_NAME, CARREFOUR_UPDATED_ESTABLISHMENT);

            // The withholding, created_at defaults to now(): strictly after
            // the update's commit (so its as-of version is the new one) and
            // inside the same open period thanks to the headroom guard.
            insertSourceCalculation(pair, CARREFOUR_CUIT, TAX_ID_RET_IVA, UPDATED_RATE_6_75,
                    UPDATED_BASE, UPDATED_AMOUNT);

            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            OffsetDateTime periodStartTime = atEpochSecond(periodStart);
            AtomicReference<RateLine> capturedRef = new AtomicReference<>();
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, CARREFOUR_CUIT);
                        List<RateLine> updatedLines = linesFor(lines, TAX_ID_RET_IVA, UPDATED_RATE_6_75,
                                periodStartTime);
                        assertEquals(1, lines.size(),
                                () -> "exactly the updated open-period line must exist for this cuit; got "
                                        + describe(lines));
                        assertEquals(1, updatedLines.size(), "exactly one updated open-period line");
                        RateLine line = updatedLines.get(0);
                        assertEquals(periodStartTime.toInstant().plusSeconds(60), line.windowEnd().toInstant(),
                                "window_end must be window_start + 60s");
                        // The update must consolidate with the new values,
                        // not the seed ones the merchant had at snapshot time.
                        assertEquals(CARREFOUR_UPDATED_ESTABLISHMENT, line.establishment(),
                                "establishment must be the merchant state as of the withholding's created_at");
                        assertEquals(CARREFOUR_UPDATED_NAME, line.merchantName(),
                                "merchant_name must be the merchant state as of the withholding's created_at");
                        assertEquals(0, UPDATED_BASE.compareTo(line.totalBaseTax()),
                                "the line must hold its exact single-row totals");
                        assertEquals(0, UPDATED_AMOUNT.compareTo(line.totalTaxAmount()),
                                "the line must hold its exact single-row totals");
                        capturedRef.set(line);
                    });
            RateLine captured = capturedRef.get();

            // ---- The certification period CLOSES deterministically: the
            // line was emitted streaming, so its period end is exactly
            // periodStart + 60 and real time passing it closes the period
            // (window_end in the past) without any further inserts. ----
            waitUntilEpochSecondHasPassed(periodStart + 60);
            RateLine closedNow = linesFor(fetchRateLines(pair, CARREFOUR_CUIT),
                    TAX_ID_RET_IVA, UPDATED_RATE_6_75, periodStartTime).get(0);
            assertTrue(closedNow.windowEnd().isBefore(OffsetDateTime.now()),
                    "the certification period must be closed (window_end in the past) after the boundary");

            // ---- Part B: the merchant DELETE. merchants is a versioned
            // changelog with REPLICA IDENTITY FULL, so the delete streams as
            // a CDC record with the complete before image. ----
            deleteMerchant(pair, CARREFOUR_CUIT);
            assertMerchantIsGoneFromSource(pair, CARREFOUR_CUIT);

            // New open-period withholding for the same cuit AFTER the delete:
            // fresh-minute headroom so created_at = now() cannot straddle a
            // period boundary, and the tick that releases it stays inside the
            // same period.
            waitUntilMinuteHeadroom(MINUTE_HEADROOM_SECONDS);
            long postDeletePeriodStart = Instant.now().getEpochSecond() / 60 * 60;
            insertSourceCalculation(pair, CARREFOUR_CUIT, TAX_ID_RET_IVA, POST_DELETE_RATE_2_50,
                    POST_DELETE_BASE, POST_DELETE_AMOUNT);

            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            OffsetDateTime postDeletePeriodStartTime = atEpochSecond(postDeletePeriodStart);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> all = fetchRateLines(pair, CARREFOUR_CUIT);
                        List<RateLine> closedLines = linesFor(all, TAX_ID_RET_IVA, UPDATED_RATE_6_75,
                                periodStartTime);
                        List<RateLine> postDeleteLines = linesFor(all, TAX_ID_RET_IVA, POST_DELETE_RATE_2_50,
                                postDeletePeriodStartTime);
                        assertEquals(2, closedLines.size() + postDeleteLines.size(),
                                () -> "exactly the closed pre-delete line and the post-delete line must exist; got "
                                        + describe(all));
                        assertEquals(1, closedLines.size(), "the closed pre-delete line must still exist");

                        // THE criterion: the closed certificate is UNCHANGED
                        // after the merchant delete — same period bounds,
                        // totals and merchant columns, byte for byte. A
                        // merchant-side change must never rewrite a frozen
                        // certificate.
                        assertEquals(captured, closedLines.get(0),
                                "the closed certificate must be unchanged byte for byte after the merchant delete");
                        assertTrue(closedLines.get(0).windowEnd().isBefore(OffsetDateTime.now()),
                                "the closed certificate must have stayed closed");

                        // The post-delete withholding still consolidates (the
                        // pipeline survives the merchant delete) with SQL
                        // NULL merchant columns: from the delete on, no
                        // merchant version exists as of a later withholding's
                        // created_at — the LEFT-join semantics that cover a
                        // missing merchant row cover a deleted one too (see
                        // the class javadoc).
                        assertEquals(1, postDeleteLines.size(), "exactly one post-delete line");
                        RateLine postLine = postDeleteLines.get(0);
                        assertEquals(postDeletePeriodStartTime.toInstant().plusSeconds(60),
                                postLine.windowEnd().toInstant(), "window_end must be window_start + 60s");
                        assertNull(postLine.establishment(),
                                "the merchant was deleted, so a later withholding must consolidate with "
                                        + "SQL NULL establishment (no merchant version as of its created_at)");
                        assertNull(postLine.merchantName(),
                                "the merchant was deleted, so a later withholding must consolidate with "
                                        + "SQL NULL merchant_name (no merchant version as of its created_at)");
                        assertEquals(0, POST_DELETE_BASE.compareTo(postLine.totalBaseTax()),
                                "the post-delete line must hold its exact single-row totals");
                        assertEquals(0, POST_DELETE_AMOUNT.compareTo(postLine.totalTaxAmount()),
                                "the post-delete line must hold its exact single-row totals");

                        // Global sanity: perception ticks (and any seed PER_*
                        // rows) must never reach certificate_items.
                        assertEquals(0, countPerceptionLines(pair),
                                "perception ticks must never consolidate into certificate_items");
                    });
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    /**
     * Builds and starts the consolidation pipeline exactly like TaxJob.main:
     * checkpointing must exist before the pipeline is built, because CDC
     * snapshot chunks only commit on checkpoints.
     */
    private TableResult startConsolidationJob(PostgresPair pair) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        return TaxJob.consolidateCertificates(E2eFixtures.pipelineConfig(pair, CertificationPeriod.parse("60s")), env);
    }

    /**
     * Merchant master-data change: streams into the pipeline as a -U/+U pair
     * on the merchants CDC input.
     */
    private void updateMerchant(PostgresPair pair, String cuit, String name, String establishment) {
        pair.executeSourceStatement(
                "UPDATE merchants SET name = '%s', establishment = '%s' WHERE cuit = '%s'"
                        .formatted(name, establishment, cuit));
    }

    /**
     * The merchant disappears: streams into the pipeline as a DELETE record
     * on the merchants CDC input (REPLICA IDENTITY FULL ships the complete
     * before image, set on the seed's merchants table by the init script).
     */
    private void deleteMerchant(PostgresPair pair, String cuit) {
        pair.executeSourceStatement("DELETE FROM merchants WHERE cuit = '" + cuit + "'");
    }

    /**
     * Fixture guard, not a target assertion: proves the DELETE actually
     * committed in the source, so the immutability assertion below cannot
     * pass vacuously (a no-op delete would stream no changelog record at
     * all).
     */
    private void assertMerchantIsGoneFromSource(PostgresPair pair, String cuit) {
        String sql = "SELECT count(*) FROM merchants WHERE cuit = ?";
        try (Connection connection = pair.openSourceConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, cuit);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertEquals(0, resultSet.getLong(1),
                        "the merchant row must be gone from the source before the assertions run");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to verify the merchant delete in the source", e);
        }
    }

    /**
     * Perception tick: a PER_IVA row whose created_at defaults to now(). The
     * taxonomy filter drops PER_* before anything is written, so the tick can
     * never touch certificate_items; it only advances the combined watermark
     * of the CDC inputs to (now - 5s), releasing the quiet stream's buffered
     * rows for emission.
     */
    private void insertPerceptionTick(PostgresPair pair) {
        insertSourceCalculation(pair, TICK_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                PER_TICK_BASE, PER_TICK_AMOUNT);
    }

    /**
     * Holds inserts back until the running minute is young enough that the
     * rest of the scenario (the tick that follows) still lands inside the
     * same open certification period.
     */
    private static void waitUntilMinuteHeadroom(long minHeadroomSeconds) {
        long secondsIntoMinute = Instant.now().getEpochSecond() % 60;
        long secondsToWait = secondsIntoMinute >= minHeadroomSeconds ? 60 - secondsIntoMinute : 0;
        if (secondsToWait > 0) {
            try {
                // +100ms so the resumed clock is safely past the boundary.
                Thread.sleep(secondsToWait * 1000 + 100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for a fresh minute", e);
            }
        }
    }

    /**
     * Waits until the given epoch second is safely in the past, so the
     * certification period that ends there has closed: the deterministic
     * real-time close for a line whose period start the test pinned.
     */
    private static void waitUntilEpochSecondHasPassed(long epochSecond) {
        long millisRemaining = epochSecond * 1000 - System.currentTimeMillis() + PERIOD_CLOSE_MARGIN_MILLIS;
        if (millisRemaining > 0) {
            try {
                Thread.sleep(millisRemaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for the period to close", e);
            }
        }
    }

    /** All rate lines of one cuit, in fetch order (tax_id, then tax_rate, then window_start). */
    private List<RateLine> fetchRateLines(PostgresPair pair, String cuit) {
        String sql = """
                SELECT cuit, tax_id, window_start, window_end, tax_rate, establishment, merchant_name,
                       total_base_tax, total_tax_amount
                FROM certificate_items
                WHERE cuit = ?
                ORDER BY tax_id, tax_rate, window_start
                """;
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, cuit);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<RateLine> lines = new ArrayList<>();
                while (resultSet.next()) {
                    lines.add(new RateLine(
                            resultSet.getString("cuit"),
                            resultSet.getString("tax_id"),
                            resultSet.getObject("window_start", OffsetDateTime.class),
                            resultSet.getObject("window_end", OffsetDateTime.class),
                            resultSet.getBigDecimal("tax_rate"),
                            resultSet.getString("establishment"),
                            resultSet.getString("merchant_name"),
                            resultSet.getBigDecimal("total_base_tax"),
                            resultSet.getBigDecimal("total_tax_amount")));
                }
                return lines;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read certificate_items for cuit " + cuit, e);
        }
    }

    /** Rate lines matching one (tax_id, tax_rate, window_start) certificate key. */
    private static List<RateLine> linesFor(List<RateLine> lines, String taxId, BigDecimal taxRate,
            OffsetDateTime windowStart) {
        return lines.stream()
                .filter(line -> taxId.equals(line.taxId()))
                .filter(line -> taxRate.compareTo(line.taxRate()) == 0)
                .filter(line -> windowStart.toInstant().equals(line.windowStart().toInstant()))
                .collect(Collectors.toList());
    }

    private long countPerceptionLines(PostgresPair pair) {
        return countTargetRows(pair, "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'",
                "Failed to count perception lines");
    }

    private static String describe(List<RateLine> lines) {
        return lines.stream()
                .map(line -> line.taxId() + "@" + line.taxRate().toPlainString()
                        + " base=" + line.totalBaseTax().toPlainString()
                        + " amount=" + line.totalTaxAmount().toPlainString()
                        + " establishment=" + line.establishment()
                        + " merchant_name=" + line.merchantName()
                        + " period=[" + line.windowStart() + ", " + line.windowEnd() + ")")
                .collect(Collectors.joining("; "));
    }

    /** One materialized rate line of certificate_items, as read back from the target. */
    private record RateLine(
            String cuit,
            String taxId,
            OffsetDateTime windowStart,
            OffsetDateTime windowEnd,
            BigDecimal taxRate,
            String establishment,
            String merchantName,
            BigDecimal totalBaseTax,
            BigDecimal totalTaxAmount) {
    }
}
