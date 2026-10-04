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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

import static com.maxipalacios.taxes.e2e.E2eFixtures.atEpochSecond;
import static com.maxipalacios.taxes.e2e.E2eFixtures.countTargetRows;
import static com.maxipalacios.taxes.e2e.E2eFixtures.deleteSeedTaxCalculations;
import static com.maxipalacios.taxes.e2e.E2eFixtures.insertSourceCalculation;
import static com.maxipalacios.taxes.e2e.E2eFixtures.insertSourceCalculationAt;

/**
 * End-to-end test for the restart-from-scratch story (spec story #22): the
 * test runs the consolidation job, plants source rows, waits for them to
 * consolidate — and then stops the job WITHOUT any savepoint or
 * checkpoint-state reuse. A FRESH job is started against the SAME databases
 * on a NEW replication slot name (PostgreSQL allows a single consumer per
 * slot; a fresh job re-snapshots instead of resuming the abandoned slot), and
 * the target must converge to EXACTLY the same certificate rows — no
 * duplicates, no lost rows. This is the snapshot-plus-idempotent-PK-upserts
 * contract, and the deliberate counterpart to {@code
 * CheckpointRecoveryE2eTest}: that suite proves the failover from a mid-run
 * checkpoint; this one proves a cold re-run reproduces the same certificate
 * from the source data alone.
 *
 * <p><b>What "without checkpoint-state reuse" means here.</b> The job is
 * cancelled through its JobClient, like every suite's teardown. No savepoint
 * is taken and no checkpoint storage is configured, so the job's state lives
 * only in the default in-memory JobManager checkpoint storage and dies with
 * the mini cluster: the fresh job starts cold — brand-new aggregation state,
 * brand-new CDC sources, a brand-new replication slot, therefore a FULL
 * re-snapshot of both tables.
 *
 * <p><b>Designing around allowed-lateness-0 (the snapshot-row recipe).</b>
 * The consolidation WHERE drops every calculation whose created_at is below
 * the source watermark at its arrival, and during a snapshot that watermark
 * is max(created_at delivered so far) - 5 seconds — with the snapshot's
 * delivery order scrambled (verified empirically, see {@code
 * CertificationPeriodE2eTest}'s javadoc). Every calculation this test plants
 * is therefore dated inside ONE five-second band ~10 minutes in the past,
 * pinned mid-minute, before the first job starts: whatever delivery order
 * either snapshot produces keeps every row above the late-drop filter. Two
 * consequences shape the rest of the scenario:
 *
 * <ul>
 *   <li><b>no streaming rows anywhere.</b> A now-dated row (the perception
 *       ticks that release the snapshot rows are exactly that) is minutes
 *       newer than the band. Before the fresh job starts, the perception
 *       ticks are deleted from the source — done only after the first job is
 *       fully stopped, so the delete itself never streams anywhere — because
 *       a tick riding the fresh snapshot would push its late-drop watermark
 *       past the band rows and nondeterministically drop them. What remains
 *       in the table is exactly the band, so the fresh snapshot re-reads
 *       precisely the rows the first snapshot read.</li>
 *   <li><b>enrichment stays NULL under both jobs.</b> The merchants CDC
 *       snapshot stamps the rows it reads with the snapshot read time, so a
 *       withholding created before that read finds no merchant version as of
 *       its created_at and consolidates with SQL NULL merchant columns. Both
 *       jobs' snapshot reads happen AFTER every planted created_at, so both
 *       consolidate the same rows with the same NULL enrichment. The seeded
 *       merchant row (present before the first snapshot, unchanged after)
 *       keeps the master-data side identical across the two jobs.</li>
 * </ul>
 *
 * <p>The release discipline is the suites' standard settle + double-tick
 * dance (the CDC probe window and the both-inputs-idle freeze race, see
 * {@code MerchantEnrichmentE2eTest}'s javadoc): each job waits out the
 * settle, then receives two perception ticks, the second after both inputs
 * have certainly gone idle. The ticks are PER_* rows and never consolidate.
 */
class RestartFromScratchE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);

    /**
     * How long the test waits for the first job's two replication slots (the
     * tax slot and the derived merchants slot) to go INACTIVE after the
     * cancel: the proof that the job's CDC connections are gone before the
     * fresh job starts, and that the pair was left the way a from-scratch
     * restart finds it in production — old slots abandoned, no consumer.
     */
    private static final Duration SLOT_RELEASE = Duration.ofSeconds(60);

    /**
     * Settle wait after starting a job before its FIRST source record (the
     * perception ticks of the release dance included): the incremental CDC
     * source probes for its streaming resume position for roughly the first
     * ~12 seconds (observed in the job logs), and records committed inside
     * that window can be skipped — worse, they race the snapshot's chunk
     * commit and can permanently drop snapshot rows behind an
     * already-advanced watermark. Applies to BOTH jobs.
     */
    private static final long STREAM_ANCHOR_SETTLE_MILLIS = 15_000;

    /**
     * Real-time gap long enough that both CDC inputs have certainly flipped
     * to idle (2-second table.exec.source.idle-timeout) between the two
     * perception ticks that release the snapshot rows, so the second tick
     * re-activates the tax input and releases its watermark into the
     * combined one (the both-inputs-idle freeze race documented in the
     * consolidation suite's test 1).
     */
    private static final long IDLE_FLIP_SETTLE_MILLIS = 12_000;

    private static final String TAX_ID_RET_IVA = "RET_IVA";
    private static final String TAX_ID_RET_GANANCIAS = "RET_GANANCIAS";
    private static final String TAX_ID_RET_IIBB_CABA = "RET_IIBB_CABA";
    private static final String TAX_ID_PER_IVA = "PER_IVA";

    // One seeded merchant cuit and one merchantless cuit: the seeded merchant
    // row exists before BOTH jobs' snapshots and is never touched, so the
    // master-data side of the two runs is identical (see the class javadoc's
    // enrichment note). The merchantless cuit is absent from the seed.
    private static final String FARMACITY_CUIT = "30692138747";
    private static final String MERCHANTLESS_CUIT = "27999999994";

    // The perception ticks' cuit: any cuit works because PER_* rows are
    // dropped by the taxonomy filter before anything is written.
    private static final String TICK_CUIT = "27999999995";

    // The band's money, one row per (cuit, tax_id, tax_rate) line. Non-trivial
    // cents pin exact DECIMAL summation; every constant feeds both the source
    // INSERT and the expected line, so the two cannot drift. The (tax_id,
    // tax_rate) pairs are all distinct so the fetch order has no ties.
    private static final BigDecimal IVA_RATE_3_50 = new BigDecimal("3.50");
    private static final BigDecimal GANANCIAS_RATE_3_00 = new BigDecimal("3.00");
    private static final BigDecimal IVA_RATE_5_00 = new BigDecimal("5.00");
    private static final BigDecimal IIBB_RATE_2_00 = new BigDecimal("2.00");

    private static final BigDecimal IVA_35_BASE = new BigDecimal("1234.56");
    private static final BigDecimal IVA_35_AMOUNT = new BigDecimal("43.21");
    private static final BigDecimal GANANCIAS_BASE = new BigDecimal("2000.00");
    private static final BigDecimal GANANCIAS_AMOUNT = new BigDecimal("60.00");
    private static final BigDecimal IVA_50_BASE = new BigDecimal("1000.00");
    private static final BigDecimal IVA_50_AMOUNT = new BigDecimal("50.00");
    private static final BigDecimal IIBB_BASE = new BigDecimal("5000.00");
    private static final BigDecimal IIBB_AMOUNT = new BigDecimal("150.00");

    // Perception tick rows' content is irrelevant (dropped by taxonomy).
    private static final BigDecimal PER_TICK_RATE_21_00 = new BigDecimal("21.00");
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void freshJobOnANewSlotReproducesTheSameCertificates() throws Exception {
        try (PostgresPair pair = PostgresPair.start()) {

            // Snapshot-row recipe prep: the seed's own calculations go, the
            // band below is the ONLY thing both snapshots ever read.
            deleteSeedTaxCalculations(pair);
            long minuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            List<MoneyRow> band = plantBand(pair, minuteStart);

            // ---- Run 1: the original job, its own replication slot. ----
            List<RateLine> firstJobLines;
            TableResult firstJob = startConsolidationJob(pair, pair.slotName());
            try {
                firstJobLines = releaseAndFetch(pair, band);
            } finally {
                // Stop WITHOUT savepoint or checkpoint-state reuse: the
                // in-memory checkpoint storage dies with the job.
                firstJob.getJobClient().ifPresent(JobClient::cancel);
            }
            // The slots must go INACTIVE before anything else happens: the
            // proof that the job's CDC connections are gone, and that the
            // pair is left the way a from-scratch restart finds it in
            // production — old slots abandoned, no consumer.
            await().atMost(SLOT_RELEASE)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> countInactiveSlots(pair) == 2);

            // The perception ticks of run 1's release dance are minutes newer
            // than the band; a tick riding run 2's snapshot would push its
            // late-drop watermark past the band rows and drop them. The first
            // job is fully stopped by now, so this delete never streams
            // anywhere; run 2's slot (created below, after the delete)
            // snapshots a table holding exactly the band.
            deletePerceptionRows(pair);

            // ---- Run 2: the FRESH job, same databases, NEW slot name — a
            // cold start with a full re-snapshot of both tables. ----
            String freshSlotName = pair.slotName() + "_restart";
            TableResult secondJob = startConsolidationJob(pair, freshSlotName);
            try {
                List<RateLine> secondJobLines = releaseAndFetch(pair, band);

                // THE criterion: the target converged to exactly the same
                // certificate rows. Both comparisons are full-field; the
                // first pins the expected set, the second pins "the same as
                // before the restart" against run 1's actually observed rows.
                assertRateLinesMatch(firstJobLines, secondJobLines);

                // Global sanity: perception ticks (and any seed PER_* rows)
                // must never reach certificate_items.
                assertEquals(0, countPerceptionLines(pair),
                        "perception ticks must never consolidate into certificate_items");
            } finally {
                // The job runs until cancelled; stop it before the containers go.
                secondJob.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    /**
     * Plants the test's snapshot band: four withholdings inside one
     * five-second window ~10 minutes back (rows at +28..+31 of the period
     * starting at minuteStart), two for the seeded merchant and two for a
     * merchantless cuit, plus the expectation rows for the comparison.
     */
    private List<MoneyRow> plantBand(PostgresPair pair, long minuteStart) {
        List<MoneyRow> band = new ArrayList<>();
        band.add(plantAt(pair, FARMACITY_CUIT, TAX_ID_RET_IVA, IVA_RATE_3_50,
                IVA_35_BASE, IVA_35_AMOUNT, minuteStart + 28));
        band.add(plantAt(pair, FARMACITY_CUIT, TAX_ID_RET_GANANCIAS, GANANCIAS_RATE_3_00,
                GANANCIAS_BASE, GANANCIAS_AMOUNT, minuteStart + 29));
        band.add(plantAt(pair, MERCHANTLESS_CUIT, TAX_ID_RET_IVA, IVA_RATE_5_00,
                IVA_50_BASE, IVA_50_AMOUNT, minuteStart + 30));
        band.add(plantAt(pair, MERCHANTLESS_CUIT, TAX_ID_RET_IIBB_CABA, IIBB_RATE_2_00,
                IIBB_BASE, IIBB_AMOUNT, minuteStart + 31));
        return band;
    }

    /**
     * Plants one band row and returns it for the expectation builder — the
     * epoch passed here is exactly the created_at the row carries, so the
     * period and late-drop behavior are pinned by the test.
     */
    private MoneyRow plantAt(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount, long createdAtEpochSecond) {
        insertSourceCalculationAt(pair, cuit, taxId, taxRate, baseTax, taxAmount, createdAtEpochSecond);
        return new MoneyRow(cuit, taxId, taxRate, createdAtEpochSecond, baseTax, taxAmount);
    }

    /**
     * The settle + double-tick release dance for a job that just started on
     * the planted band (see the class javadoc), followed by the exact-match
     * convergence wait: the target must hold exactly the band's certificate
     * rows. Returns the observed rows for the run-over-run comparison.
     */
    private List<RateLine> releaseAndFetch(PostgresPair pair, List<MoneyRow> band) throws InterruptedException {
        Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
        insertPerceptionTick(pair);
        Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
        insertPerceptionTick(pair);

        List<RateLine> expected = expectedRateLines(band);
        List<RateLine> observed = new ArrayList<>();
        await().atMost(SNAPSHOT_CONVERGENCE)
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    observed.clear();
                    observed.addAll(fetchAllRateLines(pair));
                    assertRateLinesMatch(expected, observed);
                });
        return observed;
    }

    /**
     * Builds and starts the consolidation pipeline exactly like TaxJob.main,
     * on the given replication slot name: checkpointing must exist before the
     * pipeline is built, because CDC snapshot chunks only commit on
     * checkpoints. Deliberately default otherwise — no checkpoint storage, no
     * restart strategy — because the restart under test is a from-scratch
     * one, not a failover.
     */
    private TableResult startConsolidationJob(PostgresPair pair, String replicationSlotName) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5_000);
        return TaxJob.consolidateCertificates(
                E2eFixtures.pipelineConfig(pair, replicationSlotName, CertificationPeriod.parse("60s")), env);
    }

    /**
     * Removes the perception ticks from the source once the first job is
     * fully stopped (see the class javadoc): the fresh job's snapshot must
     * read exactly the band.
     */
    private void deletePerceptionRows(PostgresPair pair) {
        pair.executeSourceStatement("DELETE FROM tax_calculations WHERE tax_id LIKE 'PER%'");
    }

    /** Perception tick: a PER_IVA row whose created_at defaults to now(); the taxonomy filter drops it. */
    private void insertPerceptionTick(PostgresPair pair) {
        insertSourceCalculation(pair, TICK_CUIT, TAX_ID_PER_IVA, PER_TICK_RATE_21_00,
                PER_TICK_BASE, PER_TICK_AMOUNT);
    }

    /**
     * Number of the pair's two replication slots (the tax slot and the
     * derived merchants slot) that EXIST and are INACTIVE: the cancel proof.
     */
    private long countInactiveSlots(PostgresPair pair) {
        String sql = "SELECT count(*) FROM pg_replication_slots "
                + "WHERE slot_name IN (?, ?) AND active = false";
        try (Connection connection = pair.openSourceConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, pair.slotName());
            statement.setString(2, pair.slotName() + "_merchants");
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect the pair's replication slots", e);
        }
    }

    private long countPerceptionLines(PostgresPair pair) {
        return countTargetRows(pair, "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'",
                "Failed to count perception lines");
    }

    // ------------------------------------------------------------------
    // Expectation building and the exact-match comparison
    // ------------------------------------------------------------------

    /**
     * One planted band row and the exact values it was inserted with: the
     * expectation builder replays the job's own arithmetic over these, so the
     * expected certificate can never disagree with what was planted.
     */
    private record MoneyRow(
            String cuit,
            String taxId,
            BigDecimal taxRate,
            long createdAtEpochSecond,
            BigDecimal baseTax,
            BigDecimal taxAmount) {
    }

    /**
     * Derives the expected rate lines from the planted band: the job's
     * 60-second tumbling periods are epoch floors of created_at, so the same
     * floor groups the rows, and the totals are the plain sums per group.
     * The result is ordered like the fetch (tax_id, then tax_rate, then
     * window_start) for the full-field comparison.
     */
    private static List<RateLine> expectedRateLines(List<MoneyRow> rows) {
        record GroupKey(String cuit, String taxId, BigDecimal taxRate, long windowStartEpochSecond) {
        }
        record Totals(BigDecimal base, BigDecimal amount) {
            Totals merge(Totals other) {
                return new Totals(base.add(other.base), amount.add(other.amount));
            }
        }
        Map<GroupKey, Totals> sums = new LinkedHashMap<>();
        for (MoneyRow row : rows) {
            GroupKey key = new GroupKey(row.cuit(), row.taxId(), row.taxRate(),
                    row.createdAtEpochSecond() / 60 * 60);
            sums.merge(key, new Totals(row.baseTax(), row.taxAmount()), Totals::merge);
        }
        return sums.entrySet().stream()
                .map(entry -> {
                    GroupKey key = entry.getKey();
                    long windowStart = key.windowStartEpochSecond();
                    return new RateLine(key.cuit(), key.taxId(), atEpochSecond(windowStart),
                            atEpochSecond(windowStart + 60), key.taxRate(), null, null,
                            entry.getValue().base(), entry.getValue().amount());
                })
                .sorted(Comparator.comparing(RateLine::taxId)
                        .thenComparing(RateLine::taxRate)
                        .thenComparing(line -> line.windowStart().toInstant()))
                .toList();
    }

    /**
     * Every materialized rate line, globally ordered like the comparison
     * expects (tax_id, then tax_rate, then window_start): the seed's rows are
     * deleted and ticks never materialize, so ALL lines of certificate_items
     * belong to this test and the global list is the strictest comparison
     * surface for the convergence, duplicate and lost-row criteria alike.
     */
    private List<RateLine> fetchAllRateLines(PostgresPair pair) {
        String sql = """
                SELECT cuit, tax_id, window_start, window_end, tax_rate, establishment, merchant_name,
                       total_base_tax, total_tax_amount
                FROM certificate_items
                ORDER BY tax_id, tax_rate, window_start
                """;
        try (Connection connection = pair.openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
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
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read certificate_items", e);
        }
    }

    /**
     * Full-field comparison of the rate lines, in fetch order. Money compares
     * by value (scale must not matter) and timestamps by instant (session
     * time zone must not matter). Every line must carry SQL NULL merchant
     * columns: the seeded merchant's snapshot version is stamped with each
     * job's snapshot read time, which is after every planted created_at, and
     * the merchantless cuit has no merchant row at all (see the class
     * javadoc's enrichment note). Duplicates cannot hide here either: the
     * physical primary key forbids them, and inflated totals from a
     * double-counted replay fail the money comparison.
     */
    private static void assertRateLinesMatch(List<RateLine> expected, List<RateLine> actual) {
        assertEquals(expected.size(), actual.size(),
                () -> "certificate must hold exactly the expected rate lines; expected " + describe(expected)
                        + " but got " + describe(actual));
        for (int i = 0; i < expected.size(); i++) {
            RateLine expectedLine = expected.get(i);
            RateLine actualLine = actual.get(i);
            String where = expectedLine.cuit() + " " + expectedLine.taxId() + "@"
                    + expectedLine.taxRate().toPlainString() + " window=" + expectedLine.windowStart();
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
                    "every planted withholding predates both jobs' merchant snapshot reads (or has no merchant "
                            + "at all), so enrichment must be SQL NULL for line " + where);
            assertNull(actualLine.merchantName(),
                    "every planted withholding predates both jobs' merchant snapshot reads (or has no merchant "
                            + "at all), so enrichment must be SQL NULL for line " + where);
            assertEquals(0, expectedLine.totalBaseTax().compareTo(actualLine.totalBaseTax()),
                    "total_base_tax of line " + where);
            assertEquals(0, expectedLine.totalTaxAmount().compareTo(actualLine.totalTaxAmount()),
                    "total_tax_amount of line " + where);
        }
    }

    private static String describe(List<RateLine> lines) {
        return lines.stream()
                .map(line -> line.cuit() + " " + line.taxId() + "@" + line.taxRate().toPlainString()
                        + " base=" + line.totalBaseTax().toPlainString()
                        + " amount=" + line.totalTaxAmount().toPlainString()
                        + " window=[" + line.windowStart() + ", " + line.windowEnd() + ")")
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
