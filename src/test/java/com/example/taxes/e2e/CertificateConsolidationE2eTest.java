package com.example.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.example.taxes.CertificationPeriod;
import com.example.taxes.TaxJob;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.example.taxes.e2e.E2eFixtures.atEpochSecond;
import static com.example.taxes.e2e.E2eFixtures.countTargetRows;
import static com.example.taxes.e2e.E2eFixtures.deleteSeedTaxCalculations;
import static com.example.taxes.e2e.E2eFixtures.insertSourceCalculation;
import static com.example.taxes.e2e.E2eFixtures.insertSourceCalculationAt;
import static com.example.taxes.e2e.E2eFixtures.waitForWatermarkTickGap;

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
 *       {@code created_at} — this suite runs the default period size (every
 *       test passes "60s" explicitly); issue #7's other period specs ("1s",
 *       "daily", "monthly") are pinned by their own suite,
 *       {@code CertificationPeriodE2eTest};</li>
 *   <li>emission is incremental: each source insert upserts the live
 *       certificate, and rows stop changing once their period closes;</li>
 *   <li>merchant enrichment (issue #6): CUITs without a merchant row
 *       consolidate with SQL NULL merchant columns (LEFT join), and
 *       enrichment only ever appears for CUITs that have master data in the
 *       source;</li>
 *   <li>the issue #7 late-arrival drop: a calculation whose created_at is
 *       already below the source watermark when it arrives (more than the
 *       5-second bounded out-of-orderness behind newer events) never
 *       consolidates.</li>
 * </ul>
 *
 * <p><b>Snapshot-row recipe.</b> This suite plants its snapshot rows with
 * the recipe documented in full in {@code CertificationPeriodE2eTest}'s
 * javadoc (issue #7 mechanics, verified empirically): the CDC snapshot's
 * delivery order is scrambled, so with the late-arrival filter any snapshot
 * row older than an already-delivered newer row would be dropped — the seed's
 * own rows included. The suites therefore delete the seed's calculations and
 * plant exactly the rows they assert on (all within a five-second band here),
 * streaming everything dated after the band.
 */
class CertificateConsolidationE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);
    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);

    /**
     * Settle wait after starting the job before the FIRST source record of
     * the run (warm-up rows and perception ticks included): the incremental
     * CDC source probes for its streaming resume position for roughly the
     * first ~12 seconds (observed in the job logs), and records committed
     * inside that window can be skipped when the stream starts past them —
     * worse, a tick dance committed inside it raced the snapshot's chunk
     * commit and permanently dropped snapshot rows behind an already-advanced
     * watermark (empirically diagnosed: waiting this settle out before the
     * first tick made every run materialize completely).
     * Asserted inserts start only after this wait.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    /**
     * Real-time gap long enough that both CDC inputs have certainly flipped
     * to idle (2-second table.exec.source.idle-timeout) between the two
     * perception ticks of the snapshot test, so the second tick re-activates
     * the tax input and releases its watermark into the combined one.
     * Distinct from {@link #STREAM_ANCHOR_SETTLE_MILLIS}: this wait spaces
     * out ticks, it does not gate when asserted rows are inserted.
     */
    private static final long IDLE_FLIP_SETTLE_MILLIS = 12_000;

    /**
     * Wave 1 (test 2) must land early enough in its minute for wave 2 (dated
     * wave1Epoch + 3) to follow while the period is still open: an insert in
     * the last seconds of a minute could reach the job after the period
     * closed. Inserts are held back until the running minute is this many
     * seconds old, so wave 2 lands at most at second 52.
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

    // Perception tick rows (see insertPerceptionTick): any PER_* row works
    // because the taxonomy filter drops it, so its money values can never
    // reach a certificate line.
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void consolidatesSnapshotWithholdingsByRateAndFreezesClosedPeriods() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // All six rows exist before the job starts, so the CDC snapshot
            // (not streaming) must consolidate them. Perceptions share the
            // cuit and must be dropped by the taxonomy filter.
            //
            // Snapshot-row recipe (see the class javadoc): the seed's own
            // calculations are deleted first, so these six are the ONLY rows
            // the snapshot carries, and they share one ~10-minutes-back
            // period, one row per consecutive second (+28..+33 of the
            // 60-second period starting at minuteStart). Within the five-
            // second band every delivery order the snapshot produces keeps
            // them above the late-drop filter's watermark, and they all
            // floor into one single, already closed period no matter which
            // second the test runs at.
            deleteSeedTaxCalculations(pair);
            long minuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50,
                    IVA_35_BASE_1, IVA_35_AMOUNT_1, minuteStart + 28);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50,
                    IVA_35_BASE_2, IVA_35_AMOUNT_2, minuteStart + 29);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00,
                    IVA_50_BASE, IVA_50_AMOUNT, minuteStart + 30);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "RET_GANANCIAS", GANANCIAS_RATE_3_00,
                    GANANCIAS_BASE, GANANCIAS_AMOUNT, minuteStart + 31);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "PER_IVA", PER_IVA_RATE_21_00,
                    PER_IVA_BASE, PER_IVA_AMOUNT, minuteStart + 32);
            insertSourceCalculationAt(pair, SNAPSHOT_CUIT, "PER_IIBB_CABA", PER_IIBB_RATE_5_00,
                    PER_IIBB_BASE, PER_IIBB_AMOUNT, minuteStart + 33);

            result = startConsolidationJob(pair);

            List<RateLine> expected = expectedSnapshotLines(minuteStart);
            // The settle wait (see STREAM_ANCHOR_SETTLE_MILLIS) must elapse
            // before the first tick: records committed inside the CDC source's
            // streaming-resume probe window race the snapshot's chunk commit
            // and can permanently drop the snapshot rows this test asserts on.
            // Then: the temporal join buffers every calculation until the
            // combined watermark of both inputs passes its created_at (see
            // TaxJob's consolidation comment). After the snapshot the tax
            // input's watermark already sits past these rows (the newest row
            // it read is minutes newer), but the merchants input idles ~2s
            // after its snapshot (its watermark is stuck at epoch 0, so
            // idling is the only way it stops gating the combined watermark),
            // and the race between the two inputs' idle flips can freeze the
            // combined watermark below every buffered row: once BOTH inputs
            // are idle, the combined watermark never recomputes, and a
            // watermark that arrived while the merchants input was still
            // active stays stuck in its input's partial watermark. Two
            // perception ticks close that hole deterministically: the first,
            // dated now, re-activates the tax input and advances its
            // watermark; the second, sent after both inputs have certainly
            // gone idle again, re-activates the tax input once more and
            // releases its watermark into the combined one. The ticks are
            // always on time for the late-drop filter (the watermark they
            // must clear is the snapshot's, minutes behind), and perceptions
            // never consolidate (taxonomy filter), so neither tick can alter
            // any asserted line.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertPerceptionTick(pair, SNAPSHOT_CUIT);
            Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
            insertPerceptionTick(pair, SNAPSHOT_CUIT);
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
            // The perception tick below is dated now(): after the tick gap it
            // lands more than 5 real seconds after the open row, so its
            // watermark (tick - 5s) clears the row's created_at while the row
            // itself arrived above the late-drop filter's watermark. The old
            // future-dated flush is gone: it pushed the watermark ~60s ahead
            // of now, which would make any row dated its own now arrive late
            // (issue #7). Perceptions never consolidate, so the tick cannot
            // alter any asserted line.
            waitForWatermarkTickGap();
            insertPerceptionTick(pair, OPEN_PERIOD_CUIT);
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

            // The settle wait (see STREAM_ANCHOR_SETTLE_MILLIS) must elapse
            // before the first source record: records committed inside the
            // CDC source's streaming-resume probe window race the snapshot's
            // chunk commit and can permanently drop snapshot rows behind an
            // already-advanced watermark. After the wait, the sacrificial
            // perception row forces any remaining probe to complete; the
            // asserted wave inserts below are then safely outside the window,
            // and the warm-up row itself is invisible in certificate_items
            // either way (perceptions never consolidate).
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertSourceCalculation(pair, INCREMENTAL_CUIT, "PER_IVA", PER_IVA_RATE_21_00,
                    PER_MID_STREAM_BASE, PER_MID_STREAM_AMOUNT);

            // Wave 1 carries an EXPLICIT created_at (epoch captured just
            // before the insert): wave 2 below must be dated relative to it —
            // both inside one open 60-second period — while still arriving
            // above the live watermark. Aligned to a fresh minute so wave 2
            // has room: wave 1 sits at most at second 49, wave 2 (wave1 + 3)
            // at most at second 52.
            waitUntilMinuteHeadroom();
            long wave1Epoch = Instant.now().getEpochSecond();
            insertSourceCalculationAt(pair, INCREMENTAL_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00, WAVE_1_BASE, WAVE_1_AMOUNT, wave1Epoch);
            // The temporal join buffers every calculation until the combined
            // watermark passes its created_at (see TaxJob's consolidation
            // comment), so wave 1 would never emit on its own: nothing else
            // is due to arrive before it. The perception tick below is dated
            // now(); after the tick gap it lands more than 5 real seconds
            // after wave 1, so its watermark (tick - 5s, about wave1Epoch +
            // 1.5s) clears wave 1 — and wave 1 itself arrived above the
            // late-drop filter's watermark. Perceptions never consolidate, so
            // it cannot alter any asserted line.
            waitForWatermarkTickGap();
            insertPerceptionTick(pair, INCREMENTAL_CUIT);

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

            // Wave 2 hits the same primary key, dated wave1Epoch + 3: still
            // inside wave 1's open period (the headroom guard above holds
            // wave 1 to at most second 49 of its minute, so +3 seconds cannot
            // cross the boundary) and still ABOVE the watermark the first
            // tick left behind (about wave1Epoch + 1.5s), so the late-drop
            // filter keeps it; the temporal join buffers it until the next
            // watermark advance, which a quiet stream never produces. The
            // mid-stream perception row (dated now) and the second tick below
            // provide that advance; perceptions never consolidate, so neither
            // can alter any asserted line.
            insertSourceCalculationAt(pair, INCREMENTAL_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00,
                    WAVE_2_BASE, WAVE_2_AMOUNT, wave1Epoch + 3);
            insertSourceCalculation(pair, INCREMENTAL_CUIT, "PER_IVA", PER_IVA_RATE_21_00,
                    PER_MID_STREAM_BASE, PER_MID_STREAM_AMOUNT);
            waitForWatermarkTickGap();
            insertPerceptionTick(pair, INCREMENTAL_CUIT);

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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        return TaxJob.consolidateCertificates(E2eFixtures.pipelineConfig(pair, CertificationPeriod.parse("60s")), env);
    }

    /** Expected rate lines for the snapshot cuit, in the fetch order (tax_id, then tax_rate). */
    private static List<RateLine> expectedSnapshotLines(long periodStartEpoch) {
        OffsetDateTime windowStart = atEpochSecond(periodStartEpoch);
        OffsetDateTime windowEnd = atEpochSecond(periodStartEpoch + 60);
        return List.of(
                new RateLine(SNAPSHOT_CUIT, "RET_GANANCIAS", windowStart, windowEnd, GANANCIAS_RATE_3_00, null, null,
                        GANANCIAS_BASE, GANANCIAS_AMOUNT),
                new RateLine(SNAPSHOT_CUIT, TAX_ID_RET_IVA, windowStart, windowEnd, IVA_RATE_3_50, null, null,
                        IVA_35_BASE_1.add(IVA_35_BASE_2), IVA_35_AMOUNT_1.add(IVA_35_AMOUNT_2)),
                new RateLine(SNAPSHOT_CUIT, TAX_ID_RET_IVA, windowStart, windowEnd, IVA_RATE_5_00, null, null,
                        IVA_50_BASE, IVA_50_AMOUNT));
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
     * Perceptions are counted globally: tests that keep the seed carry PER_*
     * rows of the seed's own, and every test adds perception ticks, so the
     * taxonomy filter must hold across every merchant, not only for the cuits
     * a test inserted.
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
     * Perception tick: a PER_IVA row whose created_at defaults to now(). The
     * taxonomy filter drops PER_* before anything is written, so the tick can
     * never touch certificate_items; it only re-activates the tax CDC input
     * and advances its watermark to (now - 5s), releasing the quiet stream's
     * buffered rows for emission. It replaces issue #7's previous future-dated
     * flush: a flush dated ~60s ahead pushed the watermark so far up that
     * every LATER legitimate row (created_at = its own now) arrived below it
     * and was dropped by the late-arrival filter, while a now-dated tick
     * keeps the watermark ~5 seconds behind real time — rows inserted after
     * the tick gap stay on time.
     */
    private void insertPerceptionTick(PostgresPair pair, String cuit) {
        insertSourceCalculation(pair, cuit, "PER_IVA", PER_IVA_RATE_21_00,
                PER_TICK_BASE, PER_TICK_AMOUNT);
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

    /**
     * Holds inserts back until the running minute is young enough that wave 2
     * (dated wave1Epoch + 3) can still follow inside the same open period.
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
