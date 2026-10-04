package com.example.taxes.e2e;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.example.taxes.CertificationPeriod;
import com.example.taxes.DbConnection;
import com.example.taxes.PipelineConfig;
import com.example.taxes.TaxJob;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import static com.example.taxes.e2e.E2eFixtures.atEpochSecond;
import static com.example.taxes.e2e.E2eFixtures.deleteSeedTaxCalculations;
import static com.example.taxes.e2e.E2eFixtures.insertSourceCalculationAt;
import static com.example.taxes.e2e.E2eFixtures.waitForWatermarkTickGap;

/**
 * End-to-end test for issue #8's automated checkpoint-recovery criterion: a
 * mid-run failure of the JDBC sink's database must be survived by the
 * restart strategy's failover, every operator must come back from the LAST
 * COMPLETED checkpoint, and the consolidation must converge to exactly the
 * certificate a failure-free run would produce — no duplicate and no missing
 * rate line.
 *
 * <p><b>What maps to what.</b> On a real cluster, losing a TaskManager
 * mid-run fails its tasks, the restart strategy restores the whole job from
 * the last completed checkpoint, the CDC source resumes from its replication
 * slot's checkpointed LSN and replays the WAL, and the idempotent JDBC
 * upserts converge. The local mini cluster cannot do a programmatic
 * savepoint/checkpoint restore (on Flink 1.20.5
 * {@code PipelineExecutorUtils.getJobGraph} takes SavepointRestoreSettings
 * only from the CLI accessors, so {@code execution.savepoint.path} is
 * ignored for local execution), so this test proves recovery exactly the way
 * a killed TaskManager is recovered: it STOPS the target PostgreSQL
 * container with docker-java, the JDBC sink's flushes start failing, the
 * sink retries a failed flush a few times with short backoff and then
 * throws, and the sink task fails — the injected outage IS the task failure
 * a cluster operator would cause by killing a TaskManager, just aimed at the
 * sink chain. The job must cycle {@code RUNNING -> RESTARTING -> RUNNING}
 * (observed through the JobClient, which makes the failover assertion
 * deterministic: while the target is down every restart cycle fails its
 * flush again and goes back to RESTARTING), restore from the last completed
 * checkpoint, re-claim the replication slots, and replay the WAL.
 *
 * <p><b>The blackout row.</b> While the target is down, one more {@code
 * RET_*} calculation is inserted into the SOURCE (which stays up — like a
 * real outage of only the sink database). That row may or may not have been
 * consumed before the failover — it sits at or after the restored
 * checkpoint's LSN either way — so after recovery it must appear EXACTLY
 * ONCE: replayed from the slot past the checkpoint LSN, aggregated on top of
 * the restored state, and idempotently upserted. It is the sharpest probe of
 * the source's slot-position restore: a source that restored to an older
 * position would double-count rows already held by the restored aggregation
 * state, and a source that restored past it would lose it.
 *
 * <p><b>Why the convergence is the assertion.</b> The final check compares
 * the target against the PLAIN SUMS of every row this test planted across
 * four phases: wave 1 (snapshot rows, one closed 60-second period ~10
 * minutes in the past), wave 2 (streaming rows materialized BEFORE the
 * kill), the blackout row (committed DURING the kill), and wave 3 (streaming
 * rows after recovery). Duplicated state would surface as more than one row
 * per (cuit, tax_id, window_start, tax_rate) primary key or as inflated
 * totals; a restore that lost the checkpointed aggregation state would
 * surface as pre-kill contributions missing from the totals (restored state
 * + slot replay only re-delivers WAL after the checkpoint LSN). Exact totals
 * plus exactly one row per key is therefore the whole criterion.
 *
 * <p><b>Checkpoint mechanics.</b> The job checkpoints every
 * {@link TaxJob#CHECKPOINT_INTERVAL_MS} (the production interval, pinned by
 * issue #8 and by {@code TaxJobTest}) into a file checkpoint storage under a
 * JUnit {@code @TempDir}. Before injecting the failure the test polls for
 * {@code chk-N/_metadata} files: the {@code _metadata} marker is only
 * written when a checkpoint COMPLETED, so the failover provably restores
 * from a real checkpoint rather than from a cold start (a cold start would
 * fail this test, because every pre-kill contribution would be missing from
 * the restored aggregation state).
 *
 * <p><b>Watermark mechanics</b> (the same model every other e2e suite pins,
 * verified empirically on Flink 1.20.5 — see {@code
 * CertificationPeriodE2eTest}'s javadoc for the full treatment): the
 * temporal join buffers every calculation until the combined watermark of
 * BOTH CDC inputs passes its created_at; a quiet stream never advances on
 * its own; rows more than the 5-second bounded out-of-orderness behind the
 * watermark are dropped (allowed lateness 0); and the snapshot's delivery
 * order is scrambled. Consequences this test inherits:
 *
 * <ul>
 *   <li><b>Snapshot-row recipe:</b> the seed's own calculations are deleted
 *       first and the snapshot rows are planted in one tight 5-second band
 *       inside a single 60-second period ~10 minutes in the past, so every
 *       delivery order keeps them above the late-drop filter;</li>
 *   <li><b>the settle + tick dance:</b> the first source record of a fresh
 *       job waits out STREAM_ANCHOR_SETTLE (the incremental CDC source
 *       probes for its streaming resume position for ~12 seconds and records
 *       committed inside that window can be skipped), then perception ticks
 *       dated now() — inserted after the 6.5-second tick gap and, around the
 *       snapshot, after the idle-flip settle — re-activate the tax input and
 *       push its watermark past the buffered rows. PER_* rows are dropped by
 *       the taxonomy filter, so ticks can never alter an asserted line;</li>
 *   <li><b>the blackout row needs its own tick</b> to reach the failing
 *       sink: it is dated its own now(), so the temporal join buffers it
 *       until a later record pushes the watermark past it. The tick inserted
 *       ~6.5 seconds into the blackout provides that push — without it the
 *       row would sit buffered, the sink would have nothing to flush, and
 *       the outage would be absorbed without any failover at all.</li>
 * </ul>
 *
 * <p><b>How the target is addressed (a docker-29 finding, verified
 * empirically).</b> The recovery cycle must not disturb the coordinates the
 * JDBC sink connects to, and this environment's docker daemon (29.8.0,
 * nftables era) re-creates published host ports of a container only
 * UNRELIABLY across a daemon-side stop/start cycle: the container comes back
 * healthy on the same IP, {@code NetworkSettings.Ports} still reports the
 * binding, but no host listener ever re-appears and every connection to the
 * mapped port is refused forever (observed for stop+start via docker-java
 * AND for the atomic restart command, for both Testcontainers-created and
 * CLI-created containers; the v6 side re-publishes while the v4 side does
 * not). The test therefore does NOT depend on the mapped port for the
 * target: both the pipeline's sink DDL and every target connection of the
 * test use the container's direct docker-bridge IP and the physical port
 * 5432, which need no proxy or DNAT at all and survive the restart cycle
 * untouched (the IP is pinned after the restart and asserted, so a docker
 * IP reassignment fails fast instead of corrupting the scenario). The source
 * container is never restarted, so it keeps the pair's usual mapped-port
 * coordinates.
 *
 * <p>This suite runs the default 60-second period spec, like {@code
 * CertificateConsolidationE2eTest}; the other period specs are pinned by
 * their own suite. All asserted rows use a CUIT absent from the seed's
 * {@code merchants}, so every materialized line must carry SQL NULL merchant
 * columns (LEFT join: missing master data must not drop withholdings), and
 * no enriched line can appear anywhere else either, because the seed's
 * calculations are deleted and perception ticks never materialize.
 */
class CheckpointRecoveryE2eTest {

    private static final Duration SNAPSHOT_CONVERGENCE = Duration.ofSeconds(90);
    private static final Duration STREAMING_CONVERGENCE = Duration.ofSeconds(60);
    private static final Duration POST_RECOVERY_CONVERGENCE = Duration.ofSeconds(90);

    /**
     * Settle wait after starting the job before the FIRST source record of
     * the run (perception ticks included): the incremental CDC source probes
     * for its streaming resume position for roughly the first ~12 seconds and
     * records committed inside that window can be skipped — worse, they race
     * the snapshot's chunk commit and can permanently drop snapshot rows
     * behind an already-advanced watermark (the full diagnosis lives in the
     * consolidation suite's javadoc). Asserted inserts start only after
     * this wait.
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

    /**
     * Generous restart budget for the failover: the sink task fails on every
     * restart cycle while the target is down (each cycle flushes into the
     * dead database and dies again), and transient replication-slot claim
     * races between the dying and restarting CDC tasks also surface as
     * restarts. 200 attempts at a 2-second delay absorb a blackout of many
     * minutes; this test only needs ~20 seconds of it.
     */
    private static final int RESTART_ATTEMPTS = 200;
    private static final long RESTART_DELAY_SECONDS = 2;

    /**
     * How long the test is willing to wait for the observed proof of the
     * failover (the job leaving RUNNING for RESTARTING). The sink only sees
     * its first flush failure after the blackout row has been released by
     * the blackout tick, and its retry loop runs a few times before the
     * fatal throw, so the first RESTARTING is expected within ~15 seconds of
     * the tick; the budget covers much slower retry schedules too.
     */
    private static final Duration FAILOVER_OBSERVATION = Duration.ofSeconds(90);

    /**
     * Extra hold with the target still down after RESTARTING was first
     * observed: at least one more restart cycle fails its flush into the
     * dead target, proving the job survives REPEATED failovers (not just
     * one) before the database comes back.
     */
    private static final long BLACKOUT_HOLD_MILLIS = 5_000;

    /**
     * The docker-java container start returns immediately and no
     * Testcontainers wait strategy runs for it, so Postgres is polled with a
     * plain JDBC round trip until it accepts connections again.
     */
    private static final Duration TARGET_RECOVERY = Duration.ofSeconds(60);

    /** Wait for the job to come back RUNNING once the target accepts connections. */
    private static final Duration JOB_RUNNING_AFTER_FAILOVER = Duration.ofSeconds(90);

    /** Wait for the first COMPLETED checkpoint (10-second production interval). */
    private static final Duration CHECKPOINT_COMPLETION = Duration.ofSeconds(60);

    /**
     * docker stop sends SIGTERM, which postmaster (PID 1 of the postgres
     * image) treats as SMART shutdown: it would wait for every client to
     * disconnect — including the sink's idle connection, which never will —
     * until the kill timeout. 2 seconds caps that: the sink's connection
     * dies with the container and Postgres crash-recovers on the next start.
     */
    private static final int CONTAINER_STOP_TIMEOUT_SECONDS = 2;

    /** Postgres listens on the image's default port inside the container. */
    private static final int TARGET_POSTGRES_PORT = 5432;

    // Resolved at test start (see the class javadoc's docker-29 finding):
    // the target is addressed by its docker-bridge IP and the physical
    // postgres port, NOT by the mapped host port, so the kill/restart cycle
    // cannot disturb the coordinates the JDBC sink and this test read
    // through. JUnit instantiates a fresh test instance per method, so this
    // instance field cannot leak between tests.
    private String targetIp;

    private static final String TAX_ID_RET_IVA = "RET_IVA";

    // The one CUIT every asserted row uses: deliberately absent from the
    // seed's merchants, so its lines must consolidate with SQL NULL merchant
    // columns (LEFT join: missing master data must not drop withholdings),
    // and no other line can carry enrichment (the seed's calculations are
    // deleted; perception ticks never materialize).
    private static final String RECOVERY_CUIT = "27404040404";

    // Non-trivial cents pin exact DECIMAL summation across the four phases.
    // Each constant feeds both the source INSERT and the expected line, so
    // the two cannot drift.
    private static final BigDecimal RATE_3_50 = new BigDecimal("3.50");
    private static final BigDecimal RATE_5_00 = new BigDecimal("5.00");
    private static final BigDecimal RATE_2_50 = new BigDecimal("2.50");

    // Wave 1 (snapshot): two RET_IVA@3.50 rows and one RET_IVA@5.00 row in
    // one closed 60-second period ~10 minutes in the past.
    private static final BigDecimal W1_BASE_1 = new BigDecimal("1234.56");
    private static final BigDecimal W1_AMOUNT_1 = new BigDecimal("43.21");
    private static final BigDecimal W1_BASE_2 = new BigDecimal("765.43");
    private static final BigDecimal W1_AMOUNT_2 = new BigDecimal("26.79");
    private static final BigDecimal W1_BASE_3 = new BigDecimal("1000.00");
    private static final BigDecimal W1_AMOUNT_3 = new BigDecimal("50.00");

    // Wave 2 (streaming, before the kill): two rows on one RET_IVA@2.50 line.
    private static final BigDecimal W2_BASE_1 = new BigDecimal("200.00");
    private static final BigDecimal W2_AMOUNT_1 = new BigDecimal("5.00");
    private static final BigDecimal W2_BASE_2 = new BigDecimal("300.00");
    private static final BigDecimal W2_AMOUNT_2 = new BigDecimal("7.50");

    // The blackout row: one more RET_IVA@2.50 row committed while the target
    // is down. Sharing wave 2's tax_id and rate sharpens the probe: if the
    // two land in the same 60-second period, the line's total must grow from
    // the restored state (wave 2) plus the replayed blackout row — restored
    // state without replay shows only wave 2, replay without restored state
    // shows only the blackout, a double-counted replay inflates the sum.
    private static final BigDecimal BLACKOUT_BASE = new BigDecimal("150.00");
    private static final BigDecimal BLACKOUT_AMOUNT = new BigDecimal("3.75");

    // Wave 3 (streaming, after recovery): two more RET_IVA@2.50 rows proving
    // the recovered job still consolidates new work.
    private static final BigDecimal W3_BASE_1 = new BigDecimal("400.00");
    private static final BigDecimal W3_AMOUNT_1 = new BigDecimal("10.00");
    private static final BigDecimal W3_BASE_2 = new BigDecimal("600.00");
    private static final BigDecimal W3_AMOUNT_2 = new BigDecimal("15.00");

    // Perception tick rows (see insertPerceptionTick): any PER_* row works
    // because the taxonomy filter drops it, so its money values can never
    // reach a certificate line.
    private static final BigDecimal PER_TICK_BASE = new BigDecimal("1000.00");
    private static final BigDecimal PER_TICK_AMOUNT = new BigDecimal("210.00");

    @Test
    void recoversFromMidRunKillAndConvergesWithoutDuplicateOrMissingRateLines(@TempDir Path tempDir) throws Exception {
        Path checkpointRoot = tempDir.resolve("checkpoints");
        DockerClient docker = DockerClientFactory.instance().client();
        TableResult result = null;
        try (PostgresPair pair = PostgresPair.start()) {
            String targetId = pair.targetContainerId();
            targetIp = docker.inspectContainerCmd(targetId).exec().getNetworkSettings().getIpAddress();

            // Snapshot-row recipe (see the class javadoc and
            // CertificationPeriodE2eTest's): the seed's own calculations are
            // deleted first, so the rows planted below are the ONLY ones the
            // CDC snapshot carries, and their shared 5-second band keeps
            // every scrambled delivery order above the late-drop filter.
            deleteSeedTaxCalculations(pair);

            // ----- Phase 1: wave 1, the snapshot phase. All three rows
            // exist before the job starts, so the CDC snapshot (not
            // streaming) must consolidate them into one closed 60-second
            // period ~10 minutes in the past (one row per consecutive
            // second, +28..+30 of the period starting at minuteStart). -----
            List<MoneyRow> moneyRows = new ArrayList<>();
            long minuteStart = Instant.now().getEpochSecond() / 60 * 60 - 600;
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_3_50,
                    W1_BASE_1, W1_AMOUNT_1, minuteStart + 28));
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_3_50,
                    W1_BASE_2, W1_AMOUNT_2, minuteStart + 29));
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_5_00,
                    W1_BASE_3, W1_AMOUNT_3, minuteStart + 30));

            result = startConsolidationJob(pair, checkpointRoot);
            JobClient jobClient = result.getJobClient().orElseThrow(
                    () -> new IllegalStateException("the consolidation INSERT must expose a JobClient"));

            // The settle + two-tick dance (see the class javadoc and the
            // consolidation suite's test 1): the settle must elapse before
            // the first record, then the first tick re-activates the tax
            // input, and the second — after both inputs have certainly gone
            // idle — releases its watermark into the combined one. The ticks
            // are always on time for the late-drop filter (the watermark
            // they must clear is the snapshot's, minutes behind) and never
            // consolidate, so they cannot alter any asserted line.
            Thread.sleep(STREAM_ANCHOR_SETTLE_MILLIS);
            insertPerceptionTick(pair, RECOVERY_CUIT);
            Thread.sleep(IDLE_FLIP_SETTLE_MILLIS);
            insertPerceptionTick(pair, RECOVERY_CUIT);

            List<RateLine> expectedAfterWave1 = expectedRateLines(moneyRows);
            await().atMost(SNAPSHOT_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        assertRateLinesMatch(expectedAfterWave1, fetchAllRateLines());
                        assertEquals(0, countPerceptionLines(),
                                "perceptions must never consolidate into certificate_items");
                    });

            // ----- Proof that a checkpoint has COMPLETED: the failover
            // below must restore from a real checkpoint, not from a cold
            // start. File checkpoint storage lays out
            // <root>/<jobId>/chk-N/_metadata, and the _metadata file is only
            // written once the checkpoint COMPLETED (a started checkpoint
            // only creates its chk-N directory). -----
            await().atMost(CHECKPOINT_COMPLETION)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> countCompletedCheckpoints(checkpointRoot) >= 1);

            // ----- Phase 2: wave 2, streaming rows dated now, materialized
            // BEFORE the kill. Both rows land on one RET_IVA@2.50 line (two
            // consecutive seconds; the expected grouping below floors them
            // into their period, so even a minute boundary would still be
            // asserted exactly). The perception tick releases them the same
            // way every quiet stream is released (see the class javadoc). -----
            long wave2Epoch = Instant.now().getEpochSecond();
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_2_50,
                    W2_BASE_1, W2_AMOUNT_1, wave2Epoch));
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_2_50,
                    W2_BASE_2, W2_AMOUNT_2, wave2Epoch + 1));
            waitForWatermarkTickGap();
            insertPerceptionTick(pair, RECOVERY_CUIT);

            List<RateLine> expectedAfterWave2 = expectedRateLines(moneyRows);
            await().atMost(STREAMING_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertRateLinesMatch(expectedAfterWave2, fetchAllRateLines()));

            // ----- Phase 3: THE KILL (mid-run). Stopping the target breaks
            // the JDBC sink's connection: the blackout row released by the
            // tick below reaches the sink, its flush fails, the sink
            // exhausts its retries and throws, and the sink task fails —
            // the same task failure a killed TaskManager produces on a real
            // cluster, which is what triggers the restart strategy's
            // failover from the last completed checkpoint. -----
            stopContainer(docker, targetId);
            try {
                // The blackout row: committed to the source (which stays up)
                // while the target is down. Dated its own now(), so it is
                // above the live watermark (the newest row in the WAL until
                // wave 3) and passes the late-drop filter both now and at
                // replay time.
                moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_2_50,
                        BLACKOUT_BASE, BLACKOUT_AMOUNT, Instant.now().getEpochSecond()));

                // The temporal join buffers the blackout row until the
                // combined watermark passes its created_at; this tick
                // (dated now, after the tick gap) provides that push and
                // shoves the row into the failing sink. Without it the
                // outage would be absorbed silently and no failover would
                // ever happen. Perceptions never consolidate, so the tick
                // cannot alter any asserted line.
                waitForWatermarkTickGap();
                insertPerceptionTick(pair, RECOVERY_CUIT);

                // The deterministic failover proof: the job must LEAVE
                // RUNNING. Nothing else in this run can cause it — the
                // source database is untouched — so observing RESTARTING
                // here means the sink task failed and the restart strategy
                // took over. While the target stays down the job keeps
                // cycling RESTARTING/RUNNING, so a 100 ms poll cannot miss
                // the (>= 2 s) RESTARTING dwell of the first cycle.
                await().atMost(FAILOVER_OBSERVATION)
                        .pollInterval(Duration.ofMillis(100))
                        .until(() -> jobStatus(jobClient) == JobStatus.RESTARTING);

                // Hold the target down through at least one more restart
                // cycle: repeated failovers must be survivable, not just one.
                Thread.sleep(BLACKOUT_HOLD_MILLIS);
            } finally {
                // ----- Phase 4: RECOVER. Whatever happened above, the
                // target comes back before the pair closes (a stopped
                // container would break the pair's own stop path).
                startContainer(docker, targetId);
            }

            // The docker-java start returns immediately and no Testcontainers
            // wait strategy runs for it. First the network attach is polled
            // back: the direct-IP addressing above relies on docker handing
            // the container its old bridge IP, so a reassignment must fail
            // fast here instead of corrupting the scenario. Then Postgres is
            // polled with a plain JDBC round trip until it accepts
            // connections again (direct IP, no host port publishing — see
            // the class javadoc's docker-29 finding).
            await().atMost(TARGET_RECOVERY)
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> targetIp.equals(
                            docker.inspectContainerCmd(targetId).exec().getNetworkSettings().getIpAddress()));
            await().atMost(TARGET_RECOVERY)
                    .pollInterval(Duration.ofMillis(500))
                    .until(this::targetAcceptsConnections);
            // The job cycles RESTARTING during the failover; wait until the
            // completed failover has all tasks RUNNING again before feeding
            // the recovered pipeline.
            await().atMost(JOB_RUNNING_AFTER_FAILOVER)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> jobStatus(jobClient) == JobStatus.RUNNING);

            // ----- Phase 5: wave 3, post-recovery streaming rows dated now,
            // above the rebuilt watermark (the newest row before them is the
            // blackout tick, ~a minute behind by now). The perception tick
            // releases them exactly like every other quiet stretch. -----
            long wave3Epoch = Instant.now().getEpochSecond();
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_2_50,
                    W3_BASE_1, W3_AMOUNT_1, wave3Epoch));
            moneyRows.add(insertCalculationAt(pair, RECOVERY_CUIT, TAX_ID_RET_IVA, RATE_2_50,
                    W3_BASE_2, W3_AMOUNT_2, wave3Epoch + 1));
            waitForWatermarkTickGap();
            insertPerceptionTick(pair, RECOVERY_CUIT);

            // THE criterion: the certificate must converge to the plain sums
            // of ALL planted rows — wave 1 (restored state), wave 2 (restored
            // state), the blackout row (slot replay past the checkpoint LSN)
            // and wave 3 (fresh post-recovery work) — with exactly one row
            // per primary key. Duplicates would show as count-per-key > 1;
            // missing restored state would show as pre-kill totals missing.
            List<RateLine> fullyExpected = expectedRateLines(moneyRows);
            await().atMost(POST_RECOVERY_CONVERGENCE)
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        List<RateLine> lines = fetchAllRateLines();
                        assertRateLinesMatch(fullyExpected, lines);
                        for (RateLine line : fullyExpected) {
                            assertEquals(1, countPrimaryKeyRows(line),
                                    "exactly one row may exist for the (cuit, tax_id, window_start, tax_rate) key of "
                                            + line.taxId() + "@" + line.taxRate().toPlainString());
                        }
                        assertEquals(0, countPerceptionLines(),
                                "perceptions must never consolidate into certificate_items");
                    });
            assertEquals(JobStatus.RUNNING, jobStatus(jobClient),
                    "the job must still be RUNNING after the recovery converged");
        } finally {
            // The job runs until cancelled; stop it before the containers go.
            if (result != null) {
                result.getJobClient().ifPresent(JobClient::cancel);
            }
        }
    }

    /**
     * Builds and starts the consolidation pipeline exactly like
     * TaxJob.main, with the issue-#8 recovery configuration on top:
     *
     * <ul>
     *   <li>the checkpoint interval is the production constant
     *       ({@link TaxJob#CHECKPOINT_INTERVAL_MS}), so the failover
     *       restores against checkpoints taken at the cadence issue #8
     *       pins;</li>
     *   <li>an explicit fixed-delay restart strategy (see
     *       {@link #RESTART_ATTEMPTS}): the sink task must survive repeated
     *       failovers while the target is down instead of failing
     *       terminally;</li>
     *   <li>a file checkpoint storage under the JUnit temp dir, so the test
     *       can poll for {@code chk-N/_metadata} and prove a checkpoint
     *       COMPLETED before the kill.</li>
     * </ul>
     *
     * <p>Checkpointing must exist before the pipeline is built, because CDC
     * snapshot chunks only commit on checkpoints (same as TaxJob.main).
     *
     * <p>Deliberate deprecation: on Flink 1.20.5 the programmatic
     * {@code setRestartStrategy} / {@code setCheckpointStorage(String)}
     * pair is deprecated in favor of configuration options, but it is the
     * explicit form this test wants — the env is configured exactly like
     * production code would (TaxJob.main enables checkpointing on the env
     * the same way), the settings ride the ExecutionConfig into the job
     * graph that the mini cluster executes, and the deprecation is
     * warning-only.
     */
    private TableResult startConsolidationJob(PostgresPair pair, Path checkpointRoot) {
        PipelineConfig cfg = new PipelineConfig(
                new DbConnection(pair.sourceHost(), Integer.toString(pair.sourcePort()),
                        PostgresPair.USERNAME, PostgresPair.PASSWORD, PostgresPair.DATABASE),
                // Direct container coordinates for the sink (see the class
                // javadoc's docker-29 finding): the sink's JDBC URL must keep
                // working across the target's stop/start cycle, which the
                // mapped host port does not.
                new DbConnection(targetIp, Integer.toString(TARGET_POSTGRES_PORT),
                        PostgresPair.USERNAME, PostgresPair.PASSWORD, PostgresPair.DATABASE),
                pair.slotName(),
                CertificationPeriod.parse("60s"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(TaxJob.CHECKPOINT_INTERVAL_MS);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                RESTART_ATTEMPTS, Duration.ofSeconds(RESTART_DELAY_SECONDS)));
        env.getCheckpointConfig().setCheckpointStorage(checkpointRoot.toUri().toString());
        return TaxJob.consolidateCertificates(cfg, env);
    }

    // ------------------------------------------------------------------
    // Failure injection (docker-java; the docker socket is mounted into
    // the test-runner container, exactly like Testcontainers itself).
    // ------------------------------------------------------------------

    private static void stopContainer(DockerClient docker, String containerId) {
        try {
            docker.stopContainerCmd(containerId).withTimeout(CONTAINER_STOP_TIMEOUT_SECONDS).exec();
        } catch (NotModifiedException e) {
            // Already stopped — nothing to inject.
        }
    }

    private static void startContainer(DockerClient docker, String containerId) {
        try {
            docker.startContainerCmd(containerId).exec();
        } catch (NotModifiedException e) {
            // Already running — nothing to recover.
        }
    }

    /**
     * Plain JDBC round trip against the target over its direct container
     * address: false while Postgres is still down or crash-recovering
     * (connection refused, "the database system is starting up", ...), true
     * once it accepts queries again.
     */
    private boolean targetAcceptsConnections() {
        try (Connection connection = openTargetConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            return true;
        } catch (SQLException stillStarting) {
            return false;
        }
    }

    /**
     * Opens a connection to the target over its direct container address
     * (see the class javadoc's docker-29 finding): unlike the pair's
     * mapped-port URL this survives the target's stop/start cycle, which is
     * exactly what the recovery scenario exercises.
     */
    private Connection openTargetConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://" + targetIp + ":" + TARGET_POSTGRES_PORT + "/" + PostgresPair.DATABASE,
                PostgresPair.USERNAME,
                PostgresPair.PASSWORD);
    }

    /** Reads the job status (the future completes quickly on the mini cluster). */
    private static JobStatus jobStatus(JobClient jobClient) {
        try {
            return jobClient.getJobStatus().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while querying the job status", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to query the job status", e);
        }
    }

    /**
     * Counts COMPLETED checkpoints under the file checkpoint storage root:
     * any {@code chk-N/_metadata} file. The {@code _metadata} marker is only
     * written when a checkpoint completed, so counting files (not
     * directories) is the completion proof the failover relies on.
     */
    private static long countCompletedCheckpoints(Path checkpointRoot) throws IOException {
        if (!Files.isDirectory(checkpointRoot)) {
            return 0;
        }
        try (var paths = Files.walk(checkpointRoot)) {
            return paths.filter(path -> path.getFileName().toString().equals("_metadata"))
                    .filter(path -> path.getParent() != null
                            && path.getParent().getFileName().toString().startsWith("chk-"))
                    .count();
        }
    }

    // ------------------------------------------------------------------
    // Expectation building
    // ------------------------------------------------------------------

    /**
     * One planted money row and the exact values it was inserted with: the
     * expectation builder replays the job's own arithmetic over these, so
     * the expected certificate can never disagree with what was planted.
     */
    private record MoneyRow(
            String taxId,
            BigDecimal taxRate,
            long createdAtEpochSecond,
            BigDecimal baseTax,
            BigDecimal taxAmount) {
    }

    /**
     * Derives the expected rate lines from every planted row: the job's
     * 60-second tumbling periods are epoch floors of created_at, so the same
     * floor groups the rows, and the totals are the plain sums per group.
     * Grouping (instead of hand-written per-phase lines) keeps the
     * expectation exact no matter which minute boundary any wave straddles.
     * The result is ordered like the fetch (tax_id, then tax_rate, then
     * window_start) for the full-field comparison.
     */
    private static List<RateLine> expectedRateLines(List<MoneyRow> rows) {
        record GroupKey(String taxId, BigDecimal taxRate, long windowStartEpochSecond) {
        }
        record Totals(BigDecimal base, BigDecimal amount) {
            Totals merge(Totals other) {
                return new Totals(base.add(other.base), amount.add(other.amount));
            }
        }
        Map<GroupKey, Totals> sums = new LinkedHashMap<>();
        for (MoneyRow row : rows) {
            GroupKey key = new GroupKey(row.taxId(), row.taxRate(), row.createdAtEpochSecond() / 60 * 60);
            sums.merge(key, new Totals(row.baseTax(), row.taxAmount()), Totals::merge);
        }
        return sums.entrySet().stream()
                .map(entry -> {
                    GroupKey key = entry.getKey();
                    long windowStart = key.windowStartEpochSecond();
                    return new RateLine(RECOVERY_CUIT, key.taxId(), atEpochSecond(windowStart),
                            atEpochSecond(windowStart + 60), key.taxRate(), null, null,
                            entry.getValue().base(), entry.getValue().amount());
                })
                .sorted(Comparator.comparing(RateLine::taxId)
                        .thenComparing(RateLine::taxRate)
                        .thenComparing(line -> line.windowStart().toInstant()))
                .toList();
    }

    /**
     * Full-field comparison of the rate lines, in fetch order. Money compares
     * by value (scale must not matter) and timestamps by instant (session
     * time zone must not matter). Every line must also carry SQL NULL
     * merchant columns (the CUIT has no merchant row) and re-derive its
     * 60-second period span, so the comparison re-checks the enrichment and
     * period semantics on the way.
     */
    private static void assertRateLinesMatch(List<RateLine> expected, List<RateLine> actual) {
        assertEquals(expected.size(), actual.size(),
                () -> "certificate must hold exactly the expected rate lines; expected " + describe(expected)
                        + " but got " + describe(actual));
        for (int i = 0; i < expected.size(); i++) {
            RateLine expectedLine = expected.get(i);
            RateLine actualLine = actual.get(i);
            String where = expectedLine.taxId() + "@" + expectedLine.taxRate().toPlainString()
                    + " window=" + expectedLine.windowStart();
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

    private static String describe(List<RateLine> lines) {
        return lines.stream()
                .map(line -> line.taxId() + "@" + line.taxRate().toPlainString()
                        + " base=" + line.totalBaseTax().toPlainString()
                        + " amount=" + line.totalTaxAmount().toPlainString()
                        + " window=[" + line.windowStart() + ", " + line.windowEnd() + ")")
                .reduce((a, b) -> a + "; " + b)
                .orElse("");
    }

    // ------------------------------------------------------------------
    // Source/target helpers. The generic fixtures (inserts, seed prep, tick
    // pacing, timestamp shaping) are shared in E2eFixtures; what stays here
    // is this suite's own recovery machinery and its global fetch/comparison
    // helpers, which read ALL of certificate_items instead of one cuit.
    // ------------------------------------------------------------------

    /**
     * Same insert with an explicit event time, so the row lands in a chosen
     * certification period. Returns the planted row for the expectation
     * builder — the epoch passed here is exactly the created_at the row
     * carries, so the period and late-drop behavior are pinned by the test.
     */
    private MoneyRow insertCalculationAt(PostgresPair pair, String cuit, String taxId,
            BigDecimal taxRate, BigDecimal baseTax, BigDecimal taxAmount, long createdAtEpochSecond) {
        insertSourceCalculationAt(pair, cuit, taxId, taxRate, baseTax, taxAmount, createdAtEpochSecond);
        return new MoneyRow(taxId, taxRate, createdAtEpochSecond, baseTax, taxAmount);
    }

    /**
     * Perception tick: a PER_IVA row whose created_at defaults to now(). The
     * taxonomy filter drops PER_* before anything is written, so the tick can
     * never touch certificate_items; it only re-activates the tax CDC input
     * and advances its watermark to (now - 5s), releasing the quiet stream's
     * buffered rows for emission.
     */
    private void insertPerceptionTick(PostgresPair pair, String cuit) {
        pair.executeSourceStatement("""
                INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate)
                VALUES ('%s', 'PER_IVA', 21.00, %s, %s, 'CL', 0.00)
                """.formatted(cuit, PER_TICK_BASE.toPlainString(), PER_TICK_AMOUNT.toPlainString()));
    }

    /**
     * Every materialized rate line, globally ordered like the comparison
     * expects (tax_id, then tax_rate, then window_start): the seed's rows are
     * deleted and ticks never materialize, so ALL lines of certificate_items
     * belong to this test and the global list is the strictest comparison
     * surface for convergence, duplicates and missing lines alike.
     */
    private List<RateLine> fetchAllRateLines() {
        String sql = """
                SELECT cuit, tax_id, window_start, window_end, tax_rate, establishment, merchant_name,
                       total_base_tax, total_tax_amount
                FROM certificate_items
                ORDER BY tax_id, tax_rate, window_start
                """;
        try (Connection connection = openTargetConnection();
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

    /** Physical row count behind one (cuit, tax_id, window_start, tax_rate) key. */
    private long countPrimaryKeyRows(RateLine line) {
        String sql = "SELECT count(*) FROM certificate_items "
                + "WHERE cuit = ? AND tax_id = ? AND window_start = ? AND tax_rate = ?";
        try (Connection connection = openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, line.cuit());
            statement.setString(2, line.taxId());
            statement.setObject(3, line.windowStart());
            statement.setBigDecimal(4, line.taxRate());
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count rows for primary key " + line.taxId(), e);
        }
    }

    /**
     * Perceptions are counted globally: the run adds many perception ticks
     * (settle dance, one per wave, the blackout), so the taxonomy filter
     * must hold across every merchant, not only for the asserted cuit.
     */
    private long countPerceptionLines() {
        String sql = "SELECT count(*) FROM certificate_items WHERE tax_id LIKE 'PER%'";
        try (Connection connection = openTargetConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to count perception lines", e);
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
