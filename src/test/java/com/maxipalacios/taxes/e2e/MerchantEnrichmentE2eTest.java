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
import java.util.List;
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
 * End-to-end tests for issue #6: runs the certificate consolidation job with
 * merchant enrichment on the in-process mini-cluster against two ephemeral
 * PostgreSQL containers and checks the materialized {@code certificate_items}
 * against the pinned domain semantics:
 *
 * <ul>
 *   <li>each withholding enriches with the merchant state as of the
 *       withholding's {@code created_at} (event-time temporal join, FOR
 *       SYSTEM_TIME AS OF);</li>
 *   <li>LEFT join semantics: a withholding whose CUIT has no merchant still
 *       consolidates, with null merchant columns — missing master data must
 *       not drop withheld money;</li>
 *   <li>a merchant update inside an open certification period surfaces on the
 *       rate line as the last version seen in the period (LAST_VALUE of
 *       establishment and name);</li>
 *   <li>a merchant update after a period closed never rewrites the closed
 *       certificate — a regular join would rewrite the merchant columns of
 *       every joined line; the temporal join must not.</li>
 * </ul>
 *
 * <p>Watermark mechanics the scenarios are shaped around (verified Flink 1.20.5
 * behavior): an enriched withholding only reaches the target once the combined
 * watermark of BOTH CDC inputs passes the withholding's {@code created_at}, and
 * each input's watermark lags 5 seconds behind its newest event. The last
 * row(s) of a quiet stream therefore wait for the NEXT record on either input
 * (the merchants dimension is usually quiet and stops gating the combined
 * watermark once its input goes idle after 10 seconds). Every scenario thus
 * ends with a perception tick: a {@code PER_IVA} row whose {@code created_at}
 * defaults to now(), inserted at least ~6 real seconds after the last row
 * under test. The taxonomy filter drops {@code PER_*} before anything is
 * written, so a tick can never touch {@code certificate_items} — asserted
 * globally below.
 *
 * <p>Two more pinned behaviors the assertions respect: {@code LAST_VALUE}
 * skips NULL inputs (a merchantless withholding can never clobber an enriched
 * value already aggregated into the same rate line), and the merchants CDC
 * snapshot records carry the snapshot read time as their {@code op_ts} — so a
 * withholding whose {@code created_at} predates the job's snapshot read (all
 * backfilled ones do) enriches to NULL, while anything inserted after it sees
 * the snapshot version as of its own {@code created_at}.
 */
class MerchantEnrichmentE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);
    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);

    /**
     * Real-time gap between the last row under test and the perception tick:
     * the tick only pushes the combined watermark past the last row's
     * created_at if it lands more than the 5-second bounded out-of-orderness
     * later; 6.5 seconds leaves margin for scheduling and JDBC round trips.
     */
    private static final long TICK_GAP_MILLIS = 6_500;

    /**
     * Part A (test 2) must land early enough in its minute for the whole
     * sequence (first withholding, merchant update, second withholding, tick)
     * to stay inside one open certification period. Stricter than the issue
     * #5 suite's threshold because more steps follow the first insert.
     */
    private static final long MINUTE_HEADROOM_SECONDS = 30;

    /**
     * Settle wait after starting the job before any streaming insert the test
     * asserts on. Two startup windows must pass first: the merchants CDC
     * snapshot read completes within a few seconds of job start (every
     * merchant-column expectation relies on the withholdings' created_at
     * landing after it — the versioned state stamps snapshot records with the
     * read time, so an as-of lookup against an earlier created_at finds no
     * version at all), and the incremental tax CDC source probes for its
     * streaming resume position for roughly the first ~12 seconds (observed
     * in the job logs), skipping rows committed inside that window. The wait
     * plus the sacrificial perception warm-up rows keeps asserted inserts
     * outside both windows.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    private static final String TAX_ID_RET_IVA = "RET_IVA";
    private static final String TAX_ID_RET_IIBB_CABA = "RET_IIBB_CABA";
    private static final String TAX_ID_RET_GANANCIAS = "RET_GANANCIAS";
    private static final String TAX_ID_PER_IVA = "PER_IVA";

    // Seeded merchants (docker/postgres/source/init.sql); the seed values are
    // pinned here so as-of expectations cannot drift from the seed silently.
    private static final String FARMACITY_CUIT = "30692138747";
    private static final String FARMACITY_NAME = "Farmacity S.A.";
    private static final String FARMACITY_ESTABLISHMENT = "10001";

    private static final String CARREFOUR_CUIT = "30584620389";
    private static final String CARREFOUR_UPDATED_NAME = "Carrefour Express";
    private static final String CARREFOUR_UPDATED_ESTABLISHMENT = "10999";

    private static final String ALMACENES_CUIT = "23123456783";
    private static final String ALMACENES_UPDATED_NAME = "Almacenes del Sur II";
    private static final String ALMACENES_UPDATED_ESTABLISHMENT = "10998";

    // Deliberately absent from merchants (the same choice as the issue #5
    // suite's snapshot cuit): the LEFT join must keep the withholding with
    // null merchant columns. Also the cuit of the perception ticks, which the
    // taxonomy filter drops before anything is written.
    private static final String MERCHANTLESS_CUIT = "27777777771";

    // Test 1's rates are unused by the seed, so the two lines this test
    // creates are uniquely identifiable among the seed's own lines for the
    // cuit (the seed already carries RET_IVA 3.50 and 5.00 lines for it).
    private static final BigDecimal SNAPSHOT_RATE_4_25 = new BigDecimal("4.25");
    private static final BigDecimal STREAMING_RATE_6_75 = new BigDecimal("6.75");
    private static final BigDecimal MERCHANTLESS_RATE_3_50 = new BigDecimal("3.50");
    private static final BigDecimal IIBB_RATE_2_00 = new BigDecimal("2.00");
    private static final BigDecimal GANANCIAS_RATE_3_00 = new BigDecimal("3.00");
    private static final BigDecimal PER_TICK_RATE_21_00 = new BigDecimal("21.00");

    // Non-trivial cents pin exact DECIMAL summation. Each constant feeds both
    // the source INSERT and the expected line, so the two cannot drift.
    private static final BigDecimal SNAPSHOT_BASE = new BigDecimal("2500.00");
    private static final BigDecimal SNAPSHOT_AMOUNT = new BigDecimal("106.25");
    private static final BigDecimal STREAMING_BASE = new BigDecimal("1500.75");
    private static final BigDecimal STREAMING_AMOUNT = new BigDecimal("101.30");
    private static final BigDecimal MERCHANTLESS_BASE = new BigDecimal("640.00");
    private static final BigDecimal MERCHANTLESS_AMOUNT = new BigDecimal("22.40");

    // Part A (test 2): two withholdings upserting one open-period rate line.
    private static final BigDecimal MID_PERIOD_BASE_1 = new BigDecimal("1000.00");
    private static final BigDecimal MID_PERIOD_AMOUNT_1 = new BigDecimal("20.00");
    private static final BigDecimal MID_PERIOD_BASE_2 = new BigDecimal("500.00");
    private static final BigDecimal MID_PERIOD_AMOUNT_2 = new BigDecimal("10.00");

    // Part B (test 2): one closed-period withholding, then one in a new open
    // period after the merchant update.
    private static final BigDecimal CLOSED_BASE = new BigDecimal("2000.00");
    private static final BigDecimal CLOSED_AMOUNT = new BigDecimal("60.00");
    private static final BigDecimal AFTER_CLOSE_BASE = new BigDecimal("800.00");
    private static final BigDecimal AFTER_CLOSE_AMOUNT = new BigDecimal("24.00");

    // The perception tick's row content is irrelevant (dropped by taxonomy).
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void enrichesCalculationsWithMerchantStateAsOfCreatedAt() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {

            // Snapshot phase (before the job starts): one withholding for a
            // seeded merchant, ~10 minutes back and pinned mid-minute so it
            // floors into a single, already closed certification period. The
            // seed's own rows for this cuit are newer still, so the source
            // watermark after the snapshot already sits past this row's
            // created_at and it consolidates without needing a tick.
            long minuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            long midMinute = minuteStart + 30;
            insertSourceCalculationAt(pair, FARMACITY_CUIT, TAX_ID_RET_IVA, SNAPSHOT_RATE_4_25,
                    SNAPSHOT_BASE, SNAPSHOT_AMOUNT, midMinute);

            result = startConsolidationJob(pair);

            // Wait for the snapshot rate line before streaming anything: this
            // proves the snapshot phase ran to completion, so every row
            // inserted below streams while the merchant's snapshot version is
            // already in the join state (op_ts <= now, which pins the as-of
            // lookups of all streaming rows). The line's merchant columns are
            // pinned to NULL: the snapshot record was read at job start, so
            // the as-of lookup against this withholding's created_at (10
            // minutes before the job) finds no merchant version at all.
            await().atMost(SNAPSHOT_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = linesFor(fetchRateLines(pair, FARMACITY_CUIT),
                                TAX_ID_RET_IVA, SNAPSHOT_RATE_4_25, atEpochSecond(minuteStart));
                        assertEquals(1, lines.size(),
                                "the snapshot withholding must consolidate into exactly one closed rate line");
                        RateLine line = lines.get(0);
                        assertEquals(atEpochSecond(minuteStart + 60).toInstant(), line.windowEnd().toInstant(),
                                "window_end must be window_start + 60s");
                        assertTrue(line.windowEnd().isBefore(OffsetDateTime.now()),
                                "the snapshot withholding's certification period must already be closed");
                        assertNull(line.establishment(),
                                "a withholding created before the job's merchants snapshot read must not "
                                        + "enrich: no merchant version existed as of its created_at");
                        assertNull(line.merchantName(),
                                "a withholding created before the job's merchants snapshot read must not "
                                        + "enrich: no merchant version existed as of its created_at");
                        assertEquals(0, SNAPSHOT_BASE.compareTo(line.totalBaseTax()),
                                "the closed snapshot line must hold its exact snapshot totals");
                        assertEquals(0, SNAPSHOT_AMOUNT.compareTo(line.totalTaxAmount()),
                                "the closed snapshot line must hold its exact snapshot totals");
                    });

            // Streaming phase (created_at defaults to now()): one withholding
            // for the seeded merchant and one for a cuit with no merchant row
            // anywhere. Before them, a sacrificial perception warm-up row
            // plus a settle wait: the incremental CDC source probes for its
            // streaming resume position for roughly the first ~12 seconds
            // after job start and rows committed inside that window can be
            // skipped when the stream starts past them (observed in the job
            // logs). The warm-up forces the probe to complete and perceptions
            // never consolidate, so it cannot alter any line below.
            insertSourceCalculation(pair, MERCHANTLESS_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                    PER_TICK_BASE, PER_TICK_AMOUNT);
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertSourceCalculation(pair, FARMACITY_CUIT, TAX_ID_RET_IVA, STREAMING_RATE_6_75,
                    STREAMING_BASE, STREAMING_AMOUNT);
            insertSourceCalculation(pair, MERCHANTLESS_CUIT, TAX_ID_RET_IVA, MERCHANTLESS_RATE_3_50,
                    MERCHANTLESS_BASE, MERCHANTLESS_AMOUNT);

            // Watermark tick gap: the tick below only advances the combined
            // watermark past the streaming rows' created_at if it lands more
            // than the 5-second out-of-orderness later in real time.
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> all = fetchRateLines(pair, FARMACITY_CUIT);
                        List<RateLine> closedLines = linesFor(all, TAX_ID_RET_IVA, SNAPSHOT_RATE_4_25,
                                atEpochSecond(minuteStart));
                        List<RateLine> streamingLines = withRate(all, STREAMING_RATE_6_75);
                        assertEquals(2, closedLines.size() + streamingLines.size(),
                                () -> "exactly the closed snapshot line and the open-period streaming line must "
                                        + "exist for this test's rates; got " + describe(all));
                        assertEquals(1, closedLines.size(), "exactly one closed snapshot line");
                        assertEquals(1, streamingLines.size(), "exactly one open-period streaming line");
                        RateLine closedLine = closedLines.get(0);
                        RateLine streamingLine = streamingLines.get(0);

                        // The closed snapshot line still holds exactly its
                        // snapshot totals; merchant columns unpinned, not
                        // asserted.
                        assertEquals(atEpochSecond(minuteStart + 60).toInstant(), closedLine.windowEnd().toInstant(),
                                "the closed snapshot line must keep its original certification period");
                        assertEquals(0, SNAPSHOT_BASE.compareTo(closedLine.totalBaseTax()),
                                "the closed snapshot line must keep its exact snapshot totals");
                        assertEquals(0, SNAPSHOT_AMOUNT.compareTo(closedLine.totalTaxAmount()),
                                "the closed snapshot line must keep its exact snapshot totals");

                        // The streaming line is enriched with the merchant
                        // state as of its created_at: the merchant's snapshot
                        // version, the only version that existed then.
                        assertTrue(streamingLine.windowStart().toInstant().isAfter(closedLine.windowStart().toInstant()),
                                "the streaming line must sit in a later certification period than the closed "
                                        + "snapshot line");
                        assertEquals(streamingLine.windowStart().toInstant().plusSeconds(60),
                                streamingLine.windowEnd().toInstant(), "window_end must be window_start + 60s");
                        assertEquals(FARMACITY_ESTABLISHMENT, streamingLine.establishment(),
                                "establishment must be the merchant state as of the withholding's created_at");
                        assertEquals(FARMACITY_NAME, streamingLine.merchantName(),
                                "merchant_name must be the merchant state as of the withholding's created_at");
                        assertEquals(0, STREAMING_BASE.compareTo(streamingLine.totalBaseTax()),
                                "the streaming line must hold its exact single-row totals");
                        assertEquals(0, STREAMING_AMOUNT.compareTo(streamingLine.totalTaxAmount()),
                                "the streaming line must hold its exact single-row totals");

                        // LEFT join: the merchantless withholding still
                        // consolidates, with null merchant columns — missing
                        // master data must not drop withheld money.
                        List<RateLine> merchantlessLines = fetchRateLines(pair, MERCHANTLESS_CUIT);
                        assertEquals(1, merchantlessLines.size(),
                                "a cuit without a merchant must still consolidate into exactly one rate line");
                        RateLine merchantlessLine = merchantlessLines.get(0);
                        assertEquals(TAX_ID_RET_IVA, merchantlessLine.taxId());
                        assertEquals(0, MERCHANTLESS_RATE_3_50.compareTo(merchantlessLine.taxRate()),
                                "the merchantless line must carry the source tax rate");
                        assertNull(merchantlessLine.establishment(),
                                "LEFT join: no merchant row anywhere, establishment must stay SQL NULL");
                        assertNull(merchantlessLine.merchantName(),
                                "LEFT join: no merchant row anywhere, merchant_name must stay SQL NULL");
                        assertEquals(0, MERCHANTLESS_BASE.compareTo(merchantlessLine.totalBaseTax()),
                                "the merchantless line must hold its exact single-row totals");
                        assertEquals(0, MERCHANTLESS_AMOUNT.compareTo(merchantlessLine.totalTaxAmount()),
                                "the merchantless line must hold its exact single-row totals");
                        assertEquals(merchantlessLine.windowStart().toInstant().plusSeconds(60),
                                merchantlessLine.windowEnd().toInstant(), "window_end must be window_start + 60s");

                        // Global sanity: perception ticks (and the seed's own
                        // PER_* rows) must never reach certificate_items.
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

    @Test
    void reflectsLastMerchantVersionSeenInPeriodAndKeepsClosedCertificatesImmutable() throws Exception {
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {
            result = startConsolidationJob(pair);

            // ---- Part A: mid-period merchant update -> LAST_VALUE wins ----
            // Let the merchants snapshot read and the tax CDC streaming probe
            // settle first (see STREAM_ANCHOR_SETTLE_MILLIS): a sacrificial
            // perception warm-up row forces the probe to complete, and the
            // settle wait keeps the withholdings below outside the skip
            // window. Then hold the sequence back until the running minute is
            // young enough for both withholdings and the tick to stay inside
            // one open certification period.
            insertSourceCalculation(pair, MERCHANTLESS_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                    PER_TICK_BASE, PER_TICK_AMOUNT);
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            waitUntilMinuteHeadroom(MINUTE_HEADROOM_SECONDS);
            long periodStart = Instant.now().getEpochSecond() / 60 * 60;

            // First withholding, created_at defaults to now(): strictly after
            // the merchants snapshot read (so its as-of version is the seed
            // one) and strictly before the update below commits.
            insertSourceCalculation(pair, CARREFOUR_CUIT, TAX_ID_RET_IIBB_CABA, IIBB_RATE_2_00,
                    MID_PERIOD_BASE_1, MID_PERIOD_AMOUNT_1);

            // The mid-period merchant change: commits between the two
            // withholdings. The merchants CDC source delivers the update as a
            // -U/+U pair; the versioned state keeps the pre-update version
            // under the snapshot read's op_ts and the new version under the
            // update's op_ts.
            updateMerchant(pair, CARREFOUR_CUIT, CARREFOUR_UPDATED_NAME, CARREFOUR_UPDATED_ESTABLISHMENT);

            // Second withholding, created_at defaults to now(): strictly after
            // the update's commit (so its as-of version is the new one) and
            // still inside the same open period thanks to the headroom guard.
            insertSourceCalculation(pair, CARREFOUR_CUIT, TAX_ID_RET_IIBB_CABA, IIBB_RATE_2_00,
                    MID_PERIOD_BASE_2, MID_PERIOD_AMOUNT_2);

            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            BigDecimal expectedBase = MID_PERIOD_BASE_1.add(MID_PERIOD_BASE_2);
            BigDecimal expectedAmount = MID_PERIOD_AMOUNT_1.add(MID_PERIOD_AMOUNT_2);
            OffsetDateTime periodStartTime = atEpochSecond(periodStart);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> all = fetchRateLines(pair, CARREFOUR_CUIT);
                        List<RateLine> lines = linesFor(all, TAX_ID_RET_IIBB_CABA, IIBB_RATE_2_00, periodStartTime);
                        assertEquals(1, lines.size(),
                                () -> "both withholdings must upsert the single open-period rate line; got "
                                        + describe(all));
                        RateLine line = lines.get(0);
                        assertEquals(0, expectedBase.compareTo(line.totalBaseTax()),
                                "totals must be the exact DECIMAL sum of both withholdings");
                        assertEquals(0, expectedAmount.compareTo(line.totalTaxAmount()),
                                "totals must be the exact DECIMAL sum of both withholdings");
                        // LAST_VALUE = last version seen in the period: the
                        // updated values, not the pre-update ones the first
                        // withholding was enriched with. (Even a NULL first
                        // enrichment — a withholding created before the
                        // merchants snapshot read — would be skipped by
                        // LAST_VALUE and could never clobber the line.)
                        assertEquals(CARREFOUR_UPDATED_ESTABLISHMENT, line.establishment(),
                                "establishment must be the last merchant version seen in the period");
                        assertEquals(CARREFOUR_UPDATED_NAME, line.merchantName(),
                                "merchant_name must be the last merchant version seen in the period");
                        assertEquals(1, countPrimaryKeyRows(pair, CARREFOUR_CUIT, TAX_ID_RET_IIBB_CABA,
                                periodStartTime, IIBB_RATE_2_00),
                                "exactly one row may exist for the (cuit, tax_id, window_start, tax_rate) key");
                    });

            // ---- Part B: post-close update -> closed certificate immutable ----
            // A third seeded merchant. The withholding below is inserted while
            // the job streams but carries a created_at ~10 minutes back: its
            // certification period closed long ago, and its created_at also
            // predates the merchants snapshot read, so the as-of lookup finds
            // no merchant version and the line must consolidate with NULL
            // merchant columns. The line is captured as read back and must
            // never change afterwards.
            long closedMinuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            insertSourceCalculationAt(pair, ALMACENES_CUIT, TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00,
                    CLOSED_BASE, CLOSED_AMOUNT, closedMinuteStart + 30);

            // The backdated withholding's emission timer is already behind the
            // current combined watermark when the row is registered, and Flink
            // only fires event-time timers on the next watermark ADVANCE — a
            // quiet stream would leave it buffered forever. The perception
            // tick below (after the usual gap) provides that advance; it never
            // consolidates, so it cannot alter the line under test.
            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            AtomicReference<RateLine> capturedRef = new AtomicReference<>();
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = linesFor(fetchRateLines(pair, ALMACENES_CUIT),
                                TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00, atEpochSecond(closedMinuteStart));
                        assertEquals(1, lines.size(),
                                "the closed-period withholding must consolidate into exactly one rate line");
                        RateLine captured = lines.get(0);
                        assertNull(captured.establishment(),
                                "the closed-period withholding's created_at predates the merchants snapshot "
                                        + "read, so no merchant version existed as of it");
                        assertNull(captured.merchantName(),
                                "the closed-period withholding's created_at predates the merchants snapshot "
                                        + "read, so no merchant version existed as of it");
                        capturedRef.set(captured);
                    });
            RateLine capturedClosedLine = capturedRef.get();

            // The merchant change happens strictly after that period closed.
            updateMerchant(pair, ALMACENES_CUIT, ALMACENES_UPDATED_NAME, ALMACENES_UPDATED_ESTABLISHMENT);

            // New open-period withholding for the same (cuit, tax_id, rate):
            // fresh-minute headroom so created_at = now() cannot straddle a
            // period boundary, and the as-of version is the updated merchant.
            waitUntilMinuteHeadroom(MINUTE_HEADROOM_SECONDS);
            long openPeriodStart = Instant.now().getEpochSecond() / 60 * 60;
            insertSourceCalculation(pair, ALMACENES_CUIT, TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00,
                    AFTER_CLOSE_BASE, AFTER_CLOSE_AMOUNT);

            waitForWatermarkTickGap();
            insertPerceptionTick(pair);

            OffsetDateTime openPeriodStartTime = atEpochSecond(openPeriodStart);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> all = fetchRateLines(pair, ALMACENES_CUIT);
                        List<RateLine> openPeriodLines = linesFor(all, TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00,
                                openPeriodStartTime);
                        assertEquals(1, openPeriodLines.size(),
                                () -> "the new open-period withholding must consolidate into its own rate line; got "
                                        + describe(all));
                        RateLine openLine = openPeriodLines.get(0);
                        assertEquals(0, AFTER_CLOSE_BASE.compareTo(openLine.totalBaseTax()),
                                "the open-period line must hold the new withholding only");
                        assertEquals(0, AFTER_CLOSE_AMOUNT.compareTo(openLine.totalTaxAmount()),
                                "the open-period line must hold the new withholding only");
                        assertEquals(ALMACENES_UPDATED_ESTABLISHMENT, openLine.establishment(),
                                "the post-close update must take effect for the new period");
                        assertEquals(ALMACENES_UPDATED_NAME, openLine.merchantName(),
                                "the post-close update must take effect for the new period");

                        // The closed certificate stayed untouched: the closed
                        // rate line is still exactly the captured row (same
                        // period bounds, totals and merchant columns). A
                        // regular join would have rewritten its merchant
                        // columns on the merchant UPDATE; the temporal join
                        // must not.
                        List<RateLine> closedNow = linesFor(all, TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00,
                                atEpochSecond(closedMinuteStart));
                        assertEquals(1, closedNow.size(), "the closed rate line must still exist");
                        assertEquals(capturedClosedLine, closedNow.get(0),
                                "the closed certificate must be unchanged byte for byte after the post-close "
                                        + "merchant update");
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

    /** Expected timestamps are instants; the JDBC session offset must not matter. */
    private static OffsetDateTime atEpochSecond(long epochSecond) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.UTC);
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

    /** Plain insert whose created_at defaults to now(): the row lands in the currently open certification period. */
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

    /**
     * Perception tick: a PER_IVA row whose created_at defaults to now(). The
     * taxonomy filter drops PER_* before anything is written, so the tick can
     * never touch certificate_items; it only advances the combined watermark
     * of the CDC inputs to (now - 5s), releasing the quiet stream's last rows
     * for emission.
     */
    private void insertPerceptionTick(PostgresPair pair) {
        insertSourceCalculation(pair, MERCHANTLESS_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                PER_TICK_BASE, PER_TICK_AMOUNT);
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
     * Real-time gap between the last row under test and the perception tick:
     * the tick only pushes the combined watermark past the last row's
     * created_at if it lands more than the 5-second bounded out-of-orderness
     * later in real time.
     */
    private static void waitForWatermarkTickGap() {
        try {
            // Watermark tick gap: the next perception tick must arrive more
            // than the 5-second out-of-orderness after the last row under
            // test for its watermark to release that row.
            Thread.sleep(TICK_GAP_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the watermark tick gap", e);
        }
    }

    /**
     * Holds inserts back until the running minute is young enough that the
     * rest of the scenario (a second withholding plus the tick) still lands
     * inside the same open certification period.
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

    /** Rate lines matching one rate, whatever their certification period. */
    private static List<RateLine> withRate(List<RateLine> lines, BigDecimal taxRate) {
        return lines.stream()
                .filter(line -> taxRate.compareTo(line.taxRate()) == 0)
                .collect(Collectors.toList());
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

    /**
     * Perceptions are counted globally: the seed carries PER_* rows of its
     * own and the tests add perception ticks, so the taxonomy filter must
     * hold across every merchant, not only for the cuits under test.
     */
    private long countPerceptionLines(PostgresPair pair) {
        return countTargetRows(pair, "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'",
                "Failed to count perception lines");
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
