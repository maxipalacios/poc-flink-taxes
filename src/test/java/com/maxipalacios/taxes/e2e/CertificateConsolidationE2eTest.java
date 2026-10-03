package com.maxipalacios.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.maxipalacios.taxes.PipelineConfig;
import com.maxipalacios.taxes.TaxJob;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for issue #5 (consolidation) and issue #6 (enrichment):
 * runs the real certificate consolidation job on the in-process mini-cluster
 * against two ephemeral PostgreSQL containers and checks the materialized
 * {@code certificate_items} against the pinned domain semantics:
 *
 * <ul>
 *   <li>only withholdings ({@code RET_*} family) consolidate; perceptions
 *       ({@code PER_*}) never appear, not even the seeded ones;</li>
 *   <li>one rate line per (cuit, tax_id, certification period, tax_rate), with
 *       exact DECIMAL totals;</li>
 *   <li>certification periods are 60-second tumbling windows of the source
 *       {@code created_at};</li>
 *   <li>emission is incremental: each source insert upserts the live
 *       certificate, and rows stop changing once their period closes;</li>
 *   <li>merchant enrichment (issue #6): CUITs without a merchant row
 *       consolidate with SQL NULL merchant columns (LEFT join), and
 *       enrichment only ever appears for CUITs that have master data in the
 *       source.</li>
 * </ul>
 */
class CertificateConsolidationE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);
    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);

    /**
     * Settle wait after starting the job before any streaming insert the test
     * asserts on: the incremental CDC source probes for its streaming resume
     * position for roughly the first ~12 seconds (observed in the job logs)
     * and rows committed inside that window can be skipped when the stream
     * starts past them. The wait plus the sacrificial warm-up row above keeps
     * asserted inserts outside that window.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    /**
     * Real-time gap long enough that both CDC inputs have certainly flipped
     * to idle (2-second table.exec.source.idle-timeout) between the two
     * perception flushes of the snapshot test, so the second flush
     * re-activates the tax input and releases its watermark into the
     * combined one. Distinct from {@link #STREAM_ANCHOR_SETTLE_MILLIS}: this
     * wait spaces out flushes, it does not gate when asserted rows are
     * inserted.
     */
    private static final long IDLE_FLIP_SETTLE_MILLIS = 12_000;

    /**
     * Wave 1 (test 2) must land early enough in its minute for wave 2 to
     * follow while the period is still open: an insert in the last seconds of
     * a minute could reach the job after the period closed. Inserts are held
     * back until the running minute is this many seconds old.
     */
    private static final long MINUTE_HEADROOM_SECONDS = 50;

    private static final String TAX_ID_RET_IVA = "RET_IVA";

    // Snapshot cuit (test 1): deliberately absent from the seed's merchants,
    // so its lines must consolidate with SQL NULL merchant columns (LEFT join:
    // missing master data must not drop withholdings).
    private static final String SNAPSHOT_CUIT = "27777777771";

    // Non-trivial cents pin exact DECIMAL summation. Each rate constant feeds
    // both the source INSERT and the expected line, so the two cannot drift.
    private static final BigDecimal IVA_RATE_3_50 = new BigDecimal("3.50");
    private static final BigDecimal IVA_RATE_5_00 = new BigDecimal("5.00");
    private static final BigDecimal GANANCIAS_RATE_3_00 = new BigDecimal("3.00");
    private static final BigDecimal PER_IVA_RATE_21_00 = new BigDecimal("21.00");
    private static final BigDecimal PER_IIBB_RATE_5_00 = new BigDecimal("5.00");

    private static final BigDecimal IVA_35_BASE_1 = new BigDecimal("1234.56");
    private static final BigDecimal IVA_35_AMOUNT_1 = new BigDecimal("43.21");
    private static final BigDecimal IVA_35_BASE_2 = new BigDecimal("765.43");
    private static final BigDecimal IVA_35_AMOUNT_2 = new BigDecimal("26.79");
    private static final BigDecimal IVA_50_BASE = new BigDecimal("1000.00");
    private static final BigDecimal IVA_50_AMOUNT = new BigDecimal("50.00");
    private static final BigDecimal GANANCIAS_BASE = new BigDecimal("2000.00");
    private static final BigDecimal GANANCIAS_AMOUNT = new BigDecimal("60.00");
    private static final BigDecimal PER_IVA_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_IVA_AMOUNT = new BigDecimal("210.00");
    private static final BigDecimal PER_IIBB_BASE = new BigDecimal("5000.00");
    private static final BigDecimal PER_IIBB_AMOUNT = new BigDecimal("250.00");

    // Open-period activity (test 1) runs under a second cuit so it can never
    // touch the closed snapshot lines it is compared against.
    private static final String OPEN_PERIOD_CUIT = "27888888882";
    private static final BigDecimal OPEN_PERIOD_BASE = new BigDecimal("500.00");
    private static final BigDecimal OPEN_PERIOD_AMOUNT = new BigDecimal("17.50");

    // Incremental emission (test 2): two waves upserting the same primary key.
    // Like SNAPSHOT_CUIT, deliberately absent from the seed's merchants, so
    // its lines must consolidate with SQL NULL merchant columns.
    private static final String INCREMENTAL_CUIT = "27999999993";
    private static final BigDecimal WAVE_1_BASE = new BigDecimal("1000.00");
    private static final BigDecimal WAVE_1_AMOUNT = new BigDecimal("50.00");
    private static final BigDecimal WAVE_2_BASE = new BigDecimal("500.00");
    private static final BigDecimal WAVE_2_AMOUNT = new BigDecimal("25.00");
    private static final BigDecimal PER_MID_STREAM_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_MID_STREAM_AMOUNT = new BigDecimal("210.00");

    // Watermark-flush rows (see flushWatermarkWithPerception): any PER_* row
    // works because the taxonomy filter drops it, so its money values can
    // never reach a certificate line.
    private static final BigDecimal FLUSH_PER_BASE = new BigDecimal("1000.00");
    private static final BigDecimal FLUSH_PER_AMOUNT = new BigDecimal("210.00");

    @Test
    void consolidatesSnapshotWithholdingsByRateAndFreezesClosedPeriods() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // One shared mid-minute instant about 10 minutes in the past: the
            // +30s offset keeps the rows away from minute boundaries, so every
            // row floors into the same single, already closed 60-second period
            // no matter which second the test runs at.
            long minuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            long midMinute = minuteStart + 30;

            // Every row exists before the job starts, so the CDC snapshot (not
            // streaming) must consolidate it. Perceptions share the cuit and
            // must be dropped by the taxonomy filter.
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50, IVA_35_BASE_1, IVA_35_AMOUNT_1, midMinute);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50, IVA_35_BASE_2, IVA_35_AMOUNT_2, midMinute);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00, IVA_50_BASE, IVA_50_AMOUNT, midMinute);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "RET_GANANCIAS", GANANCIAS_RATE_3_00, GANANCIAS_BASE, GANANCIAS_AMOUNT, midMinute);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "PER_IVA", PER_IVA_RATE_21_00, PER_IVA_BASE, PER_IVA_AMOUNT, midMinute);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "PER_IIBB_CABA", PER_IIBB_RATE_5_00, PER_IIBB_BASE, PER_IIBB_AMOUNT, midMinute);

            result = startConsolidationJob(pair);

            List<RateLine> expected = expectedSnapshotLines(minuteStart);
            // The temporal join buffers every calculation until the combined
            // watermark of both inputs passes its created_at (see TaxJob's
            // consolidation comment). The merchants input idles ~2s after its
            // snapshot (its watermark is stuck at epoch 0, so idling is the
            // only way it stops gating the combined watermark), but the race
            // between the two inputs' idle flips can freeze the combined
            // watermark below every buffered row: once BOTH inputs are idle,
            // the combined watermark never recomputes, and a watermark that
            // arrived while the merchants input was still active stays stuck
            // in its input's partial watermark. Two perception flushes close
            // that hole deterministically: the first, dated ~60s ahead of
            // now, advances the tax input's watermark immediately; the
            // second, sent after both inputs have certainly gone idle,
            // re-activates the tax input and releases its watermark into the
            // combined one. Perceptions never consolidate (taxonomy filter),
            // so neither flush can alter any asserted line.
            flushWatermarkWithPerception(pair, SNAPSHOT_CUIT);
            Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
            flushWatermarkWithPerception(pair, SNAPSHOT_CUIT);
            await().atMost(SNAPSHOT_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        assertRateLinesMatch(expected, fetchRateLines(pair, SNAPSHOT_CUIT));
                        // Global checks: the seed's own PER_* rows and every
                        // other merchant's lines are in scope here too.
                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                        assertEnrichedCuitsHaveMasterData(pair);
                    });

            List<RateLine> closedPeriodLines = fetchRateLines(pair, SNAPSHOT_CUIT);
            for (RateLine line : closedPeriodLines) {
                assertPeriodClosed(line);
            }

            // Closed-period stability: ongoing open-period activity for a
            // different cuit must not disturb the closed lines.
            insertSourceCalculation(pair, OPEN_PERIOD_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50, OPEN_PERIOD_BASE, OPEN_PERIOD_AMOUNT);
            // The temporal join only emits a calculation once the combined
            // watermark of both inputs passes its created_at (see TaxJob's
            // consolidation comment), so this lone streaming insert would sit
            // buffered forever: nothing else pushes the watermark past it.
            // The perception flush below is dated ~60s ahead of everything
            // already inserted, which advances the watermark past the open
            // row; perceptions never consolidate, so it cannot alter any
            // asserted line.
            flushWatermarkWithPerception(pair, OPEN_PERIOD_CUIT);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, OPEN_PERIOD_CUIT);
                        assertEquals(1, lines.size(),
                                "an open-period insert must consolidate into exactly one rate line");
                        RateLine line = lines.get(0);
                        assertEquals(TAX_ID_RET_IVA, line.taxId());
                        assertEquals(0, IVA_RATE_3_50.compareTo(line.taxRate()),
                                "streamed line must carry the source tax rate");
                        assertEquals(0, OPEN_PERIOD_BASE.compareTo(line.totalBaseTax()),
                                "streamed line totals must match the single source row");
                        assertEquals(0, OPEN_PERIOD_AMOUNT.compareTo(line.totalTaxAmount()),
                                "streamed line totals must match the single source row");
                        assertEquals(line.windowStart().toInstant().plusSeconds(60), line.windowEnd().toInstant(),
                                "certification periods are 60-second tumbling windows");
                    });

            // The closed certificate must have survived the open-period
            // activity byte for byte, money compared by value.
            assertRateLinesMatch(closedPeriodLines, fetchRateLines(pair, SNAPSHOT_CUIT));
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    @Test
    void upsertsOpenPeriodWithholdingsIncrementallyWithoutDuplicates() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {
            result = startConsolidationJob(pair);

            // The incremental CDC source probes for its streaming resume
            // position for roughly the first ~12 seconds after job start, and
            // rows committed inside that window can be skipped when the real
            // stream starts past them (observed as flaky wave-1 losses). A
            // sacrificial perception row forces that probe to complete, and
            // the settle wait keeps every asserted insert below outside the
            // window; perceptions never consolidate, so the warm-up row is
            // invisible in certificate_items either way.
            insertSourceCalculation(pair, INCREMENTAL_CUIT, "PER_IVA", PER_IVA_RATE_21_00,
                    PER_MID_STREAM_BASE, PER_MID_STREAM_AMOUNT);
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);

            // Wave 1 goes into the currently open period (created_at defaults
            // to now()); aligned to a fresh minute so wave 2 below has room.
            waitUntilMinuteHeadroom();
            insertSourceCalculation(pair, INCREMENTAL_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00, WAVE_1_BASE, WAVE_1_AMOUNT);
            // The temporal join buffers every calculation until the combined
            // watermark passes its created_at (see TaxJob's consolidation
            // comment), so wave 1 would never emit on its own: nothing else
            // is due to arrive before it. The perception flush is dated ~60s
            // ahead of wave 1, pushing the watermark past both wave 1 and the
            // mid-period wave 2 pinned below (period_start + 30s); perceptions
            // never consolidate, so it cannot alter any asserted line.
            flushWatermarkWithPerception(pair, INCREMENTAL_CUIT);

            AtomicReference<OffsetDateTime> periodStartRef = new AtomicReference<>();
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofMillis(250))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, INCREMENTAL_CUIT);
                        assertEquals(1, lines.size(), "wave 1 must emit exactly one rate line");
                        RateLine line = lines.get(0);
                        assertEquals(TAX_ID_RET_IVA, line.taxId());
                        assertEquals(0, IVA_RATE_5_00.compareTo(line.taxRate()),
                                "streamed line must carry the source tax rate");
                        assertEquals(0, WAVE_1_BASE.compareTo(line.totalBaseTax()),
                                "the first emission must already hold the full wave-1 totals");
                        assertEquals(0, WAVE_1_AMOUNT.compareTo(line.totalTaxAmount()),
                                "the first emission must already hold the full wave-1 totals");
                        periodStartRef.set(line.windowStart());
                    });
            OffsetDateTime periodStart = periodStartRef.get();

            // Wave 2 hits the same primary key, pinned to mid-period
            // (window_start + 30s) so it lands inside the same period even if
            // this code runs near a minute boundary. The perception row
            // arrives mid-stream; its absence is asserted below, after the
            // wave-2 upsert proves the pipeline consumed source changes that
            // came after it.
            insertSourceCalculationAt(pair, INCREMENTAL_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00,
                    WAVE_2_BASE, WAVE_2_AMOUNT, periodStart.toEpochSecond() + 30);
            insertSourceCalculation(pair, INCREMENTAL_CUIT, "PER_IVA", PER_IVA_RATE_21_00,
                    PER_MID_STREAM_BASE, PER_MID_STREAM_AMOUNT);
            // Wave 2 is dated below the watermark the flush above already
            // pushed (its created_at is pinned mid-period), so it is a late
            // row: the temporal join buffers it and only emits on the next
            // watermark advance, which a quiet stream never produces. This
            // second perception flush, dated ~60s ahead of now, provides that
            // advance; perceptions never consolidate, so it cannot alter any
            // asserted line.
            flushWatermarkWithPerception(pair, INCREMENTAL_CUIT);

            BigDecimal expectedBase = WAVE_1_BASE.add(WAVE_2_BASE);
            BigDecimal expectedAmount = WAVE_1_AMOUNT.add(WAVE_2_AMOUNT);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, INCREMENTAL_CUIT);
                        // One line total means both the second wave and the
                        // mid-stream perception grew the certificate by
                        // upsert, never by adding rows.
                        assertEquals(1, lines.size(),
                                "the certificate must grow by upserting the existing line only");
                        RateLine line = lines.get(0);
                        assertEquals(TAX_ID_RET_IVA, line.taxId());
                        assertEquals(periodStart.toInstant(), line.windowStart().toInstant(),
                                "window_start is part of the upsert key and must not move");
                        assertEquals(0, expectedBase.compareTo(line.totalBaseTax()),
                                "totals must be the exact DECIMAL sum of both waves");
                        assertEquals(0, expectedAmount.compareTo(line.totalTaxAmount()),
                                "totals must be the exact DECIMAL sum of both waves");
                        assertEquals(1, countPrimaryKeyRows(pair, INCREMENTAL_CUIT, TAX_ID_RET_IVA, periodStart, IVA_RATE_5_00),
                                "exactly one row may exist for the (cuit, tax_id, window_start, tax_rate) key");
                        assertNull(line.establishment(),
                                "the CUIT has no merchant row, so enrichment must be SQL NULL");
                        assertNull(line.merchantName(),
                                "the CUIT has no merchant row, so enrichment must be SQL NULL");
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
        PipelineConfig cfg = new PipelineConfig(
                pair.sourceHost(),
                Integer.toString(pair.sourcePort()),
                pair.targetHost(),
                Integer.toString(pair.targetPort()),
                PostgresPair.DATABASE,
                PostgresPair.USERNAME,
                PostgresPair.PASSWORD,
                pair.slotName());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        return TaxJob.consolidateCertificates(cfg, env);
    }

    /** Expected rate lines for the snapshot cuit, in the fetch order (tax_id, then tax_rate). */
    private static List<RateLine> expectedSnapshotLines(long minuteStart) {
        OffsetDateTime windowStart = atEpochSecond(minuteStart);
        OffsetDateTime windowEnd = atEpochSecond(minuteStart + 60);
        return List.of(
                new RateLine(SNAPSHOT_CUIT, "RET_GANANCIAS", windowStart, windowEnd, GANANCIAS_RATE_3_00, null, null,
                        GANANCIAS_BASE, GANANCIAS_AMOUNT),
                new RateLine(SNAPSHOT_CUIT, TAX_ID_RET_IVA, windowStart, windowEnd, IVA_RATE_3_50, null, null,
                        IVA_35_BASE_1.add(IVA_35_BASE_2), IVA_35_AMOUNT_1.add(IVA_35_AMOUNT_2)),
                new RateLine(SNAPSHOT_CUIT, TAX_ID_RET_IVA, windowStart, windowEnd, IVA_RATE_5_00, null, null,
                        IVA_50_BASE, IVA_50_AMOUNT));
    }

    /** Expected timestamps are instants; the JDBC session offset must not matter. */
    private static OffsetDateTime atEpochSecond(long epochSecond) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.UTC);
    }

    /**
     * Full-field comparison of the rate lines, in fetch order. Money compares
     * by value (scale must not matter) and timestamps by instant (session time
     * zone must not matter). Also re-derives the 60-second period span from
     * the returned timestamps instead of trusting the expected values alone.
     */
    private static void assertRateLinesMatch(List<RateLine> expected, List<RateLine> actual) {
        assertEquals(expected.size(), actual.size(),
                () -> "certificate must hold exactly the expected rate lines; expected "
                        + describe(expected) + " but got " + describe(actual));
        for (int i = 0; i < expected.size(); i++) {
            RateLine expectedLine = expected.get(i);
            RateLine actualLine = actual.get(i);
            String where = expectedLine.taxId() + "@" + expectedLine.taxRate().toPlainString();
            assertEquals(expectedLine.cuit(), actualLine.cuit(), "cuit of line " + where);
            assertEquals(expectedLine.taxId(), actualLine.taxId(), "tax_id of line " + where);
            assertEquals(0, expectedLine.taxRate().compareTo(actualLine.taxRate()), "tax_rate of line " + where);
            assertEquals(expectedLine.windowStart().toInstant(), actualLine.windowStart().toInstant(),
                    "window_start of line " + where);
            assertEquals(expectedLine.windowEnd().toInstant(), actualLine.windowEnd().toInstant(),
                    "window_end of line " + where);
            assertEquals(actualLine.windowStart().toInstant().plusSeconds(60), actualLine.windowEnd().toInstant(),
                    "window_end must be window_start + 60s for line " + where);
            assertNull(actualLine.establishment(),
                    "the CUIT has no merchant row, so enrichment must be SQL NULL");
            assertNull(actualLine.merchantName(),
                    "the CUIT has no merchant row, so enrichment must be SQL NULL");
            assertEquals(0, expectedLine.totalBaseTax().compareTo(actualLine.totalBaseTax()),
                    "total_base_tax of line " + where);
            assertEquals(0, expectedLine.totalTaxAmount().compareTo(actualLine.totalTaxAmount()),
                    "total_tax_amount of line " + where);
        }
    }

    /** Certificate finality is derived: a period is closed once window_end is in the past. */
    private static void assertPeriodClosed(RateLine line) {
        assertTrue(line.windowEnd().isBefore(OffsetDateTime.now()),
                "the certification period must be closed (window_end in the past), got window_end="
                        + line.windowEnd());
    }

    private static String describe(List<RateLine> lines) {
        return lines.stream()
                .map(line -> line.taxId() + "@" + line.taxRate().toPlainString()
                        + " base=" + line.totalBaseTax().toPlainString()
                        + " amount=" + line.totalTaxAmount().toPlainString()
                        + " window=[" + line.windowStart() + ", " + line.windowEnd() + ")")
                .collect(Collectors.joining("; "));
    }

    /** Plain insert whose created_at defaults to now(): the row lands in the currently open period. */
    private void insertSourceCalculation(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount) {
        pair.executeSourceStatement("""
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate)
                VALUES ('%s', '%s', %s, %s, %s, 'CL', 0.00)
                """.formatted(cuit, taxId, taxRate.toPlainString(), baseTax.toPlainString(), taxAmount.toPlainString()));
    }

    /** Same insert with an explicit event time, so the row lands in a chosen certification period. */
    private void insertSourceCalculationAt(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount, long createdAtEpochSecond) {
        pair.executeSourceStatement("""
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate, created_at)
                VALUES ('%s', '%s', %s, %s, %s, 'CL', 0.00, to_timestamp(%d))
                """.formatted(cuit, taxId, taxRate.toPlainString(), baseTax.toPlainString(),
                        taxAmount.toPlainString(), createdAtEpochSecond));
    }

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

    /**
     * Perceptions are counted globally: the seed carries PER_* rows of its
     * own, so the taxonomy filter must hold across every merchant, not only
     * for the cuits a test inserted.
     */
    private long countPerceptionLines(PostgresPair pair) {
        return countTargetRows(pair, "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'",
                "Failed to count perception lines");
    }

    /**
     * Enrichment invariant that holds regardless of how much master data a
     * run contains (issue #6): a line may only carry merchant data when its
     * CUIT actually has a merchant row in the SOURCE. CUITs without master
     * data must consolidate with SQL NULL merchant columns (LEFT join), so an
     * enriched line outside the source's merchant CUITs would mean the join
     * fabricated data.
     */
    private void assertEnrichedCuitsHaveMasterData(PostgresPair pair) {
        Set<String> merchantCuits = fetchMerchantCuits(pair);
        List<String> enrichedCuits = fetchEnrichedCuits(pair);
        assertTrue(enrichedCuits.stream().allMatch(merchantCuits::contains),
                () -> "enrichment must only appear for CUITs with a merchant row; merchant CUITs "
                        + merchantCuits + " but enriched line CUITs " + enrichedCuits);
    }

    /** CUITs that have a merchant row in the source (master data actually present). */
    private Set<String> fetchMerchantCuits(PostgresPair pair) {
        try (Connection connection = pair.openSourceConnection();
             PreparedStatement statement = connection.prepareStatement("SELECT cuit FROM merchants");
             ResultSet resultSet = statement.executeQuery()) {
            Set<String> cuits = new HashSet<>();
            while (resultSet.next()) {
                cuits.add(resultSet.getString(1));
            }
            return cuits;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read merchant CUITs from the source", e);
        }
    }

    /** CUITs of every materialized line that carries any merchant data. */
    private List<String> fetchEnrichedCuits(PostgresPair pair) {
        String sql = "SELECT DISTINCT cuit FROM certificate_items "
                + "WHERE establishment IS NOT NULL OR merchant_name IS NOT NULL";
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            List<String> cuits = new ArrayList<>();
            while (resultSet.next()) {
                cuits.add(resultSet.getString(1));
            }
            return cuits;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read enriched CUITs from certificate_items", e);
        }
    }

    /**
     * Pushes the source watermark past everything already inserted by this
     * test: the event-time temporal join emits a calculation only once the
     * combined watermark of both inputs passes its created_at, so a quiet
     * stream holds its last rows buffered forever (see TaxJob's consolidation
     * comment). The flush row is a perception dated ~60 seconds ahead of now;
     * perceptions never consolidate (taxonomy filter), so it can never alter
     * any asserted line.
     */
    private void flushWatermarkWithPerception(PostgresPair pair, String cuit) {
        insertSourceCalculationAt(pair, cuit, "PER_IVA", PER_IVA_RATE_21_00,
                FLUSH_PER_BASE, FLUSH_PER_AMOUNT, Instant.now().getEpochSecond() + 60);
    }

    private long countPrimaryKeyRows(PostgresPair pair, String cuit, String taxId,
            OffsetDateTime windowStart, BigDecimal taxRate) {
        String sql = "SELECT count(*) FROM certificate_items "
                + "WHERE cuit = ? AND tax_id = ? AND window_start = ? AND tax_rate = ?";
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, cuit);
            statement.setString(2, taxId);
            statement.setObject(3, windowStart);
            statement.setBigDecimal(4, taxRate);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows for primary key " + cuit + "/" + taxId, e);
        }
    }

    private long countTargetRows(PostgresPair pair, String sql, String failureMessage) {
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException(failureMessage, e);
        }
    }

    /**
     * Holds inserts back until the running minute is young enough that wave 2
     * can still follow inside the same open period.
     */
    private static void waitUntilMinuteHeadroom() {
        long secondsIntoMinute = Instant.now().getEpochSecond() % 60;
        long secondsToWait = secondsIntoMinute >= MINUTE_HEADROOM_SECONDS ? 60 - secondsIntoMinute : 0;
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
