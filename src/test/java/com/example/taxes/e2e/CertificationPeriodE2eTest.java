package com.example.taxes.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.example.taxes.CertificationPeriod;
import com.example.taxes.TaxJob;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.example.taxes.e2e.E2eFixtures.countTargetRows;
import static com.example.taxes.e2e.E2eFixtures.deleteSeedTaxCalculations;
import static com.example.taxes.e2e.E2eFixtures.insertSourceCalculation;
import static com.example.taxes.e2e.E2eFixtures.insertSourceCalculationAt;
import static com.example.taxes.e2e.E2eFixtures.waitForWatermarkTickGap;

/**
 * End-to-end tests for issue #7's parametrized certification periods: each
 * test runs the certificate consolidation job with its own period spec
 * ("1s", "daily", "monthly") on its own ephemeral PostgreSQL pair and checks
 * that the materialized {@code certificate_items} periods align exactly to
 * the configured period kind:
 *
 * <ul>
 *   <li>"1s" — one-second tumbling certification periods, epoch-aligned: two
 *       calculations one second apart land in consecutive periods, and a calculation more
 *       than the 5-second bounded out-of-orderness behind newer events is
 *       dropped by the late-arrival filter and never consolidates;</li>
 *   <li>"daily" — one calendar day aligned to Buenos Aires midnight (the
 *       job's session time zone): a withholding from 26 hours back
 *       materializes into ITS day, a streamed withholding into today's;</li>
 *   <li>"monthly" — one calendar month aligned to the first of the month: a
 *       backdated withholding (40 days) materializes into ITS month, a
 *       streamed withholding into the current one.</li>
 * </ul>
 *
 * <p>Watermark mechanics the scenarios are shaped around (the same model the
 * other e2e suites pin, verified on Flink 1.20.5): an enriched withholding
 * only reaches the target once the combined watermark of BOTH CDC inputs
 * passes its created_at, each input's watermark lags 5 seconds behind its
 * newest event, and the last row(s) of a quiet stream wait for the NEXT
 * record on either input. Every batch of rows is therefore released by a
 * perception tick: a {@code PER_IVA} row whose created_at defaults to now(),
 * inserted at least ~6 real seconds after the last row under test. The
 * taxonomy filter drops PER_* before anything is written, so a tick can never
 * touch certificate_items — asserted globally in every test.
 *
 * <p><b>Snapshot-row recipe (issue #7 watermark mechanics, verified
 * empirically).</b> The consolidation WHERE drops every calculation whose
 * created_at is below the source watermark at its arrival, and during the
 * snapshot that watermark is max(created_at delivered so far) - 5 seconds.
 * An id-order delivery assumption (ascending created_at) does NOT hold: the
 * snapshot's arrival order was recorded by heap ctid and caught scrambled
 * (rows interleaved near-even/odd, varying run to run), so ANY snapshot row
 * older than an already-delivered newer row is
 * dropped — the seed's own old rows included. The deterministic recipe is
 * therefore: delete the seed's own calculations ({@code DELETE FROM
 * tax_calculations}) and plant exactly the snapshot rows the test asserts
 * on, ALL dated within a five-second band. The calendar tests plant a single
 * backdated row each (immune to any delivery order by being alone) and stream
 * their second, newer row after the snapshot — a planted newer row would
 * scramble ahead of the backdated one and drop it.
 */
class CertificationPeriodE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);
    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);

    /**
     * Settle wait after starting the job before the FIRST source record of
     * the run (warm-up rows and perception ticks included), and before any
     * streaming insert the test asserts on: the incremental CDC source probes
     * for its streaming resume position for roughly the first ~12 seconds
     * (observed in the job logs), and records committed inside that window
     * can be skipped when the stream starts past them — worse, records
     * committed inside it race the snapshot's chunk commit and can
     * permanently drop snapshot rows behind an already-advanced watermark
     * (empirically diagnosed: waiting this settle out before the first tick
     * made every run materialize completely). Asserted inserts start only
     * after this wait.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    /**
     * Real-time gap long enough that both CDC inputs have certainly flipped
     * to idle (2-second table.exec.source.idle-timeout) between the two
     * perception ticks that release the snapshot-phase rows of the calendar
     * tests, so the second tick re-activates the tax input and releases its
     * watermark into the combined one (the both-inputs-idle freeze race
     * documented in the consolidation suite's test 1).
     */
    private static final long IDLE_FLIP_SETTLE_MILLIS = 12_000;

    // Calendar-period expectations are computed against the job's pinned
    // session time zone (TaxJob.SESSION_ZONE, the single source of truth):
    // Buenos Aires. Never against the JVM default, which is UTC in every
    // runtime of this project (standing repo invariant).

    private static final String TAX_ID_RET_IVA = "RET_IVA";
    private static final String TAX_ID_RET_GANANCIAS = "RET_GANANCIAS";
    private static final String TAX_ID_PER_IVA = "PER_IVA";
    // Fresh cuits, absent from the seed and from merchants (LEFT join: their
    // lines must consolidate with SQL NULL merchant columns). The calendar
    // tests delete the seed's own calculations and plant exactly the rows
    // they assert on; the one-second test keeps the seed (all of its asserted
    // rows stream, and none of its assertions touch seed rows).
    private static final String ONE_SECOND_CUIT = "27555555556";
    private static final String DAILY_PREVIOUS_DAY_CUIT = "27666666667";
    private static final String DAILY_CURRENT_DAY_CUIT = "27666666668";
    private static final String MONTHLY_PREVIOUS_MONTH_CUIT = "27666666669";
    private static final String MONTHLY_CURRENT_MONTH_CUIT = "27666666670";

    // The perception ticks' cuit (and the one-second test's warm-up row): any
    // cuit works because PER_* rows are dropped by the taxonomy filter before
    // anything is written.
    private static final String TICK_CUIT = "27777777771";

    private static final BigDecimal RET_RATE_5_00 = new BigDecimal("5.00");
    private static final BigDecimal RET_RATE_3_50 = new BigDecimal("3.50");
    private static final BigDecimal RET_RATE_3_00 = new BigDecimal("3.00");

    // One-second test: two on-time withholdings in consecutive 1-second
    // periods, plus a third calculation dated 7 seconds behind (late).
    private static final BigDecimal H1_BASE = new BigDecimal("1000.00");
    private static final BigDecimal H1_AMOUNT = new BigDecimal("35.00");
    private static final BigDecimal H2_BASE = new BigDecimal("500.00");
    private static final BigDecimal H2_AMOUNT = new BigDecimal("17.50");
    private static final BigDecimal LATE_BASE = new BigDecimal("2000.00");
    private static final BigDecimal LATE_AMOUNT = new BigDecimal("70.00");

    // Daily test: the planted previous-day withholding's money.
    private static final BigDecimal PREVIOUS_DAY_BASE = new BigDecimal("40000.00");
    private static final BigDecimal PREVIOUS_DAY_AMOUNT = new BigDecimal("2000.00");
    private static final BigDecimal CURRENT_DAY_BASE = new BigDecimal("8000.00");
    private static final BigDecimal CURRENT_DAY_AMOUNT = new BigDecimal("400.00");

    // Monthly test: the planted backdated withholding's money.
    private static final BigDecimal PREVIOUS_MONTH_BASE = new BigDecimal("30000.00");
    private static final BigDecimal PREVIOUS_MONTH_AMOUNT = new BigDecimal("900.00");
    private static final BigDecimal CURRENT_MONTH_BASE = new BigDecimal("1000.00");
    private static final BigDecimal CURRENT_MONTH_AMOUNT = new BigDecimal("35.00");

    // Perception tick rows' content is irrelevant (dropped by taxonomy).
    private static final BigDecimal PER_TICK_RATE_21_00 = new BigDecimal("21.00");
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void consolidatesOneSecondPeriodsAndDropsLateCalculations() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {
            result = startConsolidationJob(pair, "1s");

            // The settle wait (see STREAM_ANCHOR_SETTLE_MILLIS) must elapse
            // before the first source record: records committed inside the
            // CDC source's streaming-resume probe window race the snapshot's
            // chunk commit and can permanently drop snapshot rows. After the
            // wait, the sacrificial perception warm-up row forces any
            // remaining probe to complete so the asserted inserts below
            // cannot be skipped; perceptions never consolidate, so the
            // warm-up row is invisible in certificate_items either way.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertSourceCalculation(pair, TICK_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                    PER_TICK_BASE, PER_TICK_AMOUNT);

            // Two withholdings one second apart, inserted newest-first: H2's
            // created_at (T-1) still sits above the watermark H1 leaves
            // behind (T - 5s, the bounded out-of-orderness), so both arrive
            // on time for the late-drop filter, and each floors into its own
            // 1-second period: H1 -> [T, T+1s), H2 -> [T-1s, T). One second
            // of out-of-orderness must consolidate; only lateness beyond the
            // 5-second bound is dropped.
            long h1Epoch = Instant.now().getEpochSecond();
            insertSourceCalculationAt(pair, ONE_SECOND_CUIT, TAX_ID_RET_IVA, RET_RATE_3_50,
                    H1_BASE, H1_AMOUNT, h1Epoch);
            insertSourceCalculationAt(pair, ONE_SECOND_CUIT, TAX_ID_RET_IVA, RET_RATE_3_50,
                    H2_BASE, H2_AMOUNT, h1Epoch - 1);

            // The temporal join buffers both withholdings until the combined
            // watermark passes their created_at; a quiet stream never
            // advances it. The perception tick below (dated now(), after the
            // tick gap) pushes the watermark to about T + 1.5s — past both
            // rows — and never consolidates itself.
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, ONE_SECOND_CUIT);
                        assertEquals(2, lines.size(),
                                () -> "exactly the two on-time withholdings must consolidate, each into its own "
                                        + "1-second period; got " + describe(lines));
                        // Fetch order is window_start ascending: H2's window
                        // [T-1s, T) sorts before H1's [T, T+1s).
                        RateLine h2Line = lines.get(0);
                        RateLine h1Line = lines.get(1);
                        assertEquals(Instant.ofEpochSecond(h1Epoch - 1), h2Line.windowStart().toInstant(),
                                "H2 must land in the 1-second period starting at T-1s");
                        assertEquals(h2Line.windowStart().toInstant().plusSeconds(1), h2Line.windowEnd().toInstant(),
                                "one-second periods: window_end = window_start + 1s");
                        assertEquals(0, H2_BASE.compareTo(h2Line.totalBaseTax()),
                                "H2's period must hold exactly H2's totals");
                        assertEquals(0, H2_AMOUNT.compareTo(h2Line.totalTaxAmount()),
                                "H2's period must hold exactly H2's totals");
                        assertEquals(Instant.ofEpochSecond(h1Epoch), h1Line.windowStart().toInstant(),
                                "H1 must land in the 1-second period starting at T");
                        assertEquals(h1Line.windowStart().toInstant().plusSeconds(1), h1Line.windowEnd().toInstant(),
                                "one-second periods: window_end = window_start + 1s");
                        assertEquals(0, H1_BASE.compareTo(h1Line.totalBaseTax()),
                                "H1's period must hold exactly H1's totals");
                        assertEquals(0, H1_AMOUNT.compareTo(h1Line.totalTaxAmount()),
                                "H1's period must hold exactly H1's totals");
                        assertNull(h1Line.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(h1Line.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(h2Line.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(h2Line.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });

            List<RateLine> beforeLateRow = fetchRateLines(pair, ONE_SECOND_CUIT);

            // A calculation dated T-7 sits more than 5 seconds behind the
            // newest event the source has seen (its watermark is about
            // T + 1.5s after the tick above): the late-drop filter must
            // discard it at arrival, before the temporal join. The second
            // tick below gives a hypothetically-buffered copy every chance
            // to emit afterwards, so the assertion genuinely pins the drop.
            insertSourceCalculationAt(pair, ONE_SECOND_CUIT, TAX_ID_RET_IVA, RET_RATE_3_50,
                    LATE_BASE, LATE_AMOUNT, h1Epoch - 7);
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        assertSameLines(beforeLateRow, fetchRateLines(pair, ONE_SECOND_CUIT),
                                "the late calculation must never consolidate: still exactly the two on-time "
                                        + "lines, totals unchanged, no third line");
                        // Global: the warm-up row and both ticks must never
                        // reach certificate_items.
                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    @Test
    void alignsDailyPeriodsToBuenosAiresDays() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // Snapshot-row recipe (see the class javadoc): the seed's own
            // calculations are deleted and ONE withholding is planted for
            // the previous Buenos Aires day — 26 hours back, which always
            // falls on an earlier calendar day than the run (two instants
            // more than 24 hours apart can never share a calendar day). A
            // lone snapshot row cannot be scrambled against any other, so
            // the late-drop filter keeps it and the snapshot consolidates
            // it; its daily period closed hours ago. The expected period is
            // computed from the planted epoch in Java against the job's
            // session zone.
            deleteSeedTaxCalculations(pair);
            long previousDayEpoch = Instant.now().getEpochSecond() - 26L * 60 * 60;
            Instant previousDayStart = baDayStart(Instant.ofEpochSecond(previousDayEpoch));
            insertSourceCalculationAt(pair, DAILY_PREVIOUS_DAY_CUIT, TAX_ID_RET_IVA, RET_RATE_5_00,
                    PREVIOUS_DAY_BASE, PREVIOUS_DAY_AMOUNT, previousDayEpoch);

            result = startConsolidationJob(pair, "daily");

            // The settle wait (see STREAM_ANCHOR_SETTLE_MILLIS) must elapse
            // before the first tick: records committed inside the CDC
            // source's streaming-resume probe window race the snapshot's
            // chunk commit and can permanently drop the planted row this
            // test asserts on. Then the planted row — still buffered by the
            // temporal join until the combined watermark of both inputs
            // passes it, with the both-inputs-idle freeze race (documented
            // in the consolidation suite's test 1) able to strand buffered
            // rows. Two now-dated perception ticks — the second after both
            // inputs have certainly gone idle — release the combined
            // watermark deterministically; perceptions never consolidate.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertPerceptionTick(pair);
            Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
            insertPerceptionTick(pair);

            await().atMost(SNAPSHOT_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, DAILY_PREVIOUS_DAY_CUIT);
                        assertEquals(1, lines.size(),
                                () -> "the previous-day withholding must consolidate into exactly one "
                                        + "daily certification period; got " + describe(lines));
                        RateLine line = lines.get(0);
                        assertEquals(previousDayStart, line.windowStart().toInstant(),
                                "daily periods align to the Buenos Aires midnight of the row's created_at");
                        assertEquals(previousDayStart.plus(1, ChronoUnit.DAYS), line.windowEnd().toInstant(),
                                "daily periods: window_end = window_start + 1 calendar day");
                        assertTrue(line.windowEnd().isBefore(OffsetDateTime.now()),
                                "the previous-day withholding's certification period must already be closed");
                        assertEquals(0, PREVIOUS_DAY_BASE.compareTo(line.totalBaseTax()),
                                "the daily line must hold the withholding's exact totals");
                        assertEquals(0, PREVIOUS_DAY_AMOUNT.compareTo(line.totalTaxAmount()),
                                "the daily line must hold the withholding's exact totals");
                        assertNull(line.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(line.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });

            // Cross-check that the daily boundary actually separates days: a
            // second withholding STREAMED now (created_at = its own now, on
            // time for the late-drop filter) must materialize into TODAY's
            // Buenos Aires day — a different period than the planted row's.
            // Streaming the newer row is load-bearing, not style: a second
            // PLANTED row dated today could be delivered by the scrambled
            // snapshot ahead of the previous-day row, whose created_at would
            // then sit below the watermark and be dropped.
            long currentDayEpoch = Instant.now().getEpochSecond();
            Instant currentDayStart = baDayStart(Instant.ofEpochSecond(currentDayEpoch));
            assertNotEquals(previousDayStart, currentDayStart,
                    "the cross-check row must sit in a different Buenos Aires day than the planted row");
            insertSourceCalculationAt(pair, DAILY_CURRENT_DAY_CUIT, TAX_ID_RET_IVA, RET_RATE_3_50,
                    CURRENT_DAY_BASE, CURRENT_DAY_AMOUNT, currentDayEpoch);
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> currentLines = fetchRateLines(pair, DAILY_CURRENT_DAY_CUIT);
                        assertEquals(1, currentLines.size(),
                                () -> "the streamed withholding must consolidate into exactly one daily "
                                        + "period; got " + describe(currentLines));
                        RateLine currentLine = currentLines.get(0);
                        assertEquals(currentDayStart, currentLine.windowStart().toInstant(),
                                "the streamed withholding materializes into TODAY's Buenos Aires day");
                        assertEquals(currentDayStart.plus(1, ChronoUnit.DAYS), currentLine.windowEnd().toInstant(),
                                "daily periods: window_end = window_start + 1 calendar day");
                        assertEquals(0, CURRENT_DAY_BASE.compareTo(currentLine.totalBaseTax()),
                                "the daily line must hold the withholding's exact totals");
                        assertEquals(0, CURRENT_DAY_AMOUNT.compareTo(currentLine.totalTaxAmount()),
                                "the daily line must hold the withholding's exact totals");
                        assertNull(currentLine.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(currentLine.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");

                        // The planted previous-day line stayed untouched in
                        // its own day period.
                        List<RateLine> previousLines = fetchRateLines(pair, DAILY_PREVIOUS_DAY_CUIT);
                        assertEquals(1, previousLines.size(),
                                "the previous-day line must still exist");
                        assertEquals(previousDayStart, previousLines.get(0).windowStart().toInstant(),
                                "the previous-day line must keep its own day period");

                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    @Test
    void alignsMonthlyPeriodsToBuenosAiresMonths() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // Snapshot-row recipe (see the class javadoc): the seed's own
            // calculations are deleted and ONE withholding is planted 40
            // days before now — always a PREVIOUS calendar month, since no
            // month is 40 days long. A lone snapshot row cannot be scrambled
            // against any other, so the late-drop filter keeps it and the
            // snapshot consolidates it; its monthly period closed weeks ago.
            // The expected period is computed from the planted epoch in Java
            // against the job's session zone.
            deleteSeedTaxCalculations(pair);
            long previousMonthEpoch = Instant.now().getEpochSecond() - 40L * 24 * 60 * 60;
            Instant previousMonthStart = baMonthStart(Instant.ofEpochSecond(previousMonthEpoch));
            // One CALENDAR month later: month arithmetic must go through the
            // zone (Instant has no month unit), which lands on the next
            // first-of-month whatever the month lengths in between.
            Instant previousMonthEnd = previousMonthStart.atZone(TaxJob.SESSION_ZONE).plusMonths(1).toInstant();
            insertSourceCalculationAt(pair, MONTHLY_PREVIOUS_MONTH_CUIT, TAX_ID_RET_GANANCIAS, RET_RATE_3_00,
                    PREVIOUS_MONTH_BASE, PREVIOUS_MONTH_AMOUNT, previousMonthEpoch);

            result = startConsolidationJob(pair, "monthly");

            // The settle wait (see STREAM_ANCHOR_SETTLE_MILLIS) must elapse
            // before the first tick: records committed inside the CDC
            // source's streaming-resume probe window race the snapshot's
            // chunk commit and can permanently drop the planted row this
            // test asserts on. Then the planted row — still buffered by the
            // temporal join until the combined watermark of both inputs
            // passes it, with the both-inputs-idle freeze race (documented
            // in the consolidation suite's test 1) able to strand buffered
            // rows. Two now-dated perception ticks — the second after both
            // inputs have certainly gone idle — release the combined
            // watermark deterministically; perceptions never consolidate.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertPerceptionTick(pair);
            Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
            insertPerceptionTick(pair);

            await().atMost(SNAPSHOT_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchRateLines(pair, MONTHLY_PREVIOUS_MONTH_CUIT);
                        assertEquals(1, lines.size(),
                                () -> "the backdated withholding must consolidate into exactly one monthly "
                                        + "period; got " + describe(lines));
                        RateLine line = lines.get(0);
                        assertEquals(previousMonthStart, line.windowStart().toInstant(),
                                "monthly periods align to the first of the Buenos Aires month");
                        assertEquals(previousMonthEnd, line.windowEnd().toInstant(),
                                "monthly periods: window_end = window_start + 1 calendar month");
                        assertTrue(line.windowEnd().isBefore(OffsetDateTime.now()),
                                "the backdated withholding's certification period must already be closed");
                        assertEquals(0, PREVIOUS_MONTH_BASE.compareTo(line.totalBaseTax()),
                                "the monthly line must hold the withholding's exact totals");
                        assertEquals(0, PREVIOUS_MONTH_AMOUNT.compareTo(line.totalTaxAmount()),
                                "the monthly line must hold the withholding's exact totals");
                        assertNull(line.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(line.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });

            // Cross-check that the monthly boundary actually separates
            // months: a second withholding STREAMED now (created_at = its
            // own now, on time for the late-drop filter) must materialize
            // into the CURRENT Buenos Aires month — a different period than
            // the planted row's. Streaming the newer row is load-bearing,
            // not style: a second PLANTED row dated now could be delivered
            // by the scrambled snapshot ahead of the backdated row, whose
            // created_at would then sit below the watermark and be dropped.
            long currentMonthEpoch = Instant.now().getEpochSecond();
            Instant currentMonthStart = baMonthStart(Instant.ofEpochSecond(currentMonthEpoch));
            Instant currentMonthEnd = currentMonthStart.atZone(TaxJob.SESSION_ZONE).plusMonths(1).toInstant();
            assertNotEquals(previousMonthStart, currentMonthStart,
                    "the cross-check row must sit in a different calendar month than the planted row");
            insertSourceCalculationAt(pair, MONTHLY_CURRENT_MONTH_CUIT, TAX_ID_RET_IVA, RET_RATE_3_50,
                    CURRENT_MONTH_BASE, CURRENT_MONTH_AMOUNT, currentMonthEpoch);
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> currentLines = fetchRateLines(pair, MONTHLY_CURRENT_MONTH_CUIT);
                        assertEquals(1, currentLines.size(),
                                () -> "the streamed withholding must consolidate into exactly one monthly "
                                        + "period; got " + describe(currentLines));
                        RateLine currentLine = currentLines.get(0);
                        assertEquals(currentMonthStart, currentLine.windowStart().toInstant(),
                                "the streamed withholding materializes into the CURRENT Buenos Aires month");
                        assertEquals(currentMonthEnd, currentLine.windowEnd().toInstant(),
                                "monthly periods: window_end = window_start + 1 calendar month");
                        assertEquals(0, CURRENT_MONTH_BASE.compareTo(currentLine.totalBaseTax()),
                                "the monthly line must hold the withholding's exact totals");
                        assertEquals(0, CURRENT_MONTH_AMOUNT.compareTo(currentLine.totalTaxAmount()),
                                "the monthly line must hold the withholding's exact totals");
                        assertNull(currentLine.establishment(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");
                        assertNull(currentLine.merchantName(),
                                "the cuit has no merchant row, so enrichment must be SQL NULL (LEFT join)");

                        // The planted backdated line stayed untouched in its
                        // own month period.
                        List<RateLine> previousLines = fetchRateLines(pair, MONTHLY_PREVIOUS_MONTH_CUIT);
                        assertEquals(1, previousLines.size(),
                                "the backdated line must still exist");
                        assertEquals(previousMonthStart, previousLines.get(0).windowStart().toInstant(),
                                "the backdated line must keep its own month period");

                        assertEquals(0, countPerceptionLines(pair),
                                "perceptions must never consolidate into certificate_items");
                    });
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    /**
     * Builds and starts the consolidation pipeline exactly like TaxJob.main,
     * with the test's certification period spec: checkpointing must exist
     * before the pipeline is built, because CDC snapshot chunks only commit
     * on checkpoints.
     */
    private TableResult startConsolidationJob(PostgresPair pair, String certificationPeriodSpec) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        return TaxJob.consolidateCertificates(
                E2eFixtures.pipelineConfig(pair, CertificationPeriod.parse(certificationPeriodSpec)), env);
    }

    /** Buenos Aires midnight floor of an instant (the daily period start). */
    private static Instant baDayStart(Instant instant) {
        return LocalDate.ofInstant(instant, TaxJob.SESSION_ZONE).atStartOfDay(TaxJob.SESSION_ZONE).toInstant();
    }

    /** Buenos Aires first-of-month floor of an instant (the monthly period start). */
    private static Instant baMonthStart(Instant instant) {
        return YearMonth.from(instant.atZone(TaxJob.SESSION_ZONE)).atDay(1).atStartOfDay(TaxJob.SESSION_ZONE).toInstant();
    }

    private static String describe(List<RateLine> lines) {
        return lines.stream()
                .map(line -> line.taxId() + "@" + line.taxRate().toPlainString()
                        + " base=" + line.totalBaseTax().toPlainString()
                        + " amount=" + line.totalTaxAmount().toPlainString()
                        + " window=[" + line.windowStart() + ", " + line.windowEnd() + ")")
                .collect(Collectors.joining("; "));
    }

    /** Field-wise comparison of two rate-line lists (money by value, timestamps by instant). */
    private static void assertSameLines(List<RateLine> expected, List<RateLine> actual, String message) {
        assertEquals(expected.size(), actual.size(), message);
        for (int i = 0; i < expected.size(); i++) {
            RateLine expectedLine = expected.get(i);
            RateLine actualLine = actual.get(i);
            assertEquals(expectedLine.cuit(), actualLine.cuit(), message);
            assertEquals(expectedLine.taxId(), actualLine.taxId(), message);
            assertEquals(0, expectedLine.taxRate().compareTo(actualLine.taxRate()), message);
            assertEquals(expectedLine.windowStart().toInstant(), actualLine.windowStart().toInstant(), message);
            assertEquals(expectedLine.windowEnd().toInstant(), actualLine.windowEnd().toInstant(), message);
            assertEquals(0, expectedLine.totalBaseTax().compareTo(actualLine.totalBaseTax()), message);
            assertEquals(0, expectedLine.totalTaxAmount().compareTo(actualLine.totalTaxAmount()), message);
            assertEquals(expectedLine.establishment(), actualLine.establishment(), message);
            assertEquals(expectedLine.merchantName(), actualLine.merchantName(), message);
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

    /**
     * Perceptions are counted globally: the one-second test keeps the seed
     * (whose PER_* rows are in scope) and every test adds perception ticks,
     * so the taxonomy filter must hold across every merchant, not only for
     * the cuits under test.
     */
    private long countPerceptionLines(PostgresPair pair) {
        return countTargetRows(pair, "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'",
                "Failed to count perception lines");
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
