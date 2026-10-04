package com.example.taxes;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

/**
 * The Flink job of the tax engine PoC. {@link #main} runs the certificate
 * consolidation pipeline (issues #5 and #6): it reads the CDC of the source
 * {@code tax_calculations} table, consolidates the {@code RET_*} withholdings
 * into certification periods sized by {@link CertificationPeriod} (60 seconds
 * by default, configurable per deployment), enriches every rate line with
 * the merchant master data as of each calculation's {@code created_at}
 * (event-time temporal join against the CDC of {@code merchants}), and
 * incrementally upserts the growing certificate rate lines into
 * {@code certificate_items} in the target database. Period boundaries align
 * to America/Argentina/Buenos_Aires, and calculations that arrive after the
 * watermark has passed their {@code created_at} are dropped (issue #7).
 *
 * <p>The connector CREATE TABLE DDL lives in {@link PipelineDdl}, built from
 * the validated {@link PipelineConfig}; this class orchestrates the pipeline
 * and generates the consolidation INSERT.
 */
public final class TaxJob {

    // Session time zone of the consolidation pipeline's table environment,
    // pinned by consolidateCertificates: America/Argentina/Buenos_Aires, a
    // fixed -03:00 offset (no DST). Every certification period boundary is a
    // Buenos Aires calendar boundary, and the JDBC sink conversion in
    // createConsolidationInsertSql derives its offset interval literal from
    // this zone, so both must stay the same constant. Public so the e2e
    // suites compute their expected calendar boundaries against the exact
    // zone the job pins instead of carrying their own copy.
    public static final ZoneId SESSION_ZONE = ZoneId.of("America/Argentina/Buenos_Aires");

    // Package-private so the argument-parser unit test can assert against it.
    static final String USAGE =
            "Usage: TaxJob [--certification-period <spec>] where <spec> is '<n>s' (n >= 1, e.g. '60s'), 'daily' or 'monthly'";

    // CDC incremental snapshots commit read chunks on checkpoints, so the
    // production job enables checkpointing before the table environment
    // exists (see main). Issue #8 pins the 10-second interval as an
    // acceptance criterion ("checkpointing remains enabled at 10-second
    // intervals"), so the value lives in this one constant, which feeds both
    // main() and the checkpoint-recovery e2e test: the failover that test
    // injects restores from checkpoints taken at exactly the production
    // cadence, and this constant keeps the two from drifting apart.
    public static final long CHECKPOINT_INTERVAL_MS = 10_000;

    private TaxJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // CDC incremental snapshots commit read chunks on checkpoints, so
        // checkpointing must be enabled before the table environment exists.
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        // Recovery is proven, not assumed (issue #8): pin the fixed-delay
        // strategy the checkpoint-recovery e2e test exercises, instead of
        // relying on the cluster's implicit default for checkpointing jobs.
        // Unbounded attempts: a killed TaskManager must always fail over from
        // the last completed checkpoint; a persistent failure still surfaces
        // as a permanently RESTARTING job in the Flink UI and logs.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                Integer.MAX_VALUE, Duration.ofSeconds(1)));

        // Env vars stay the single configuration surface (fromEnv); the
        // optional --certification-period flag only overrides the period spec.
        PipelineConfig cfg = PipelineConfig.fromEnv();
        String periodOverride = certificationPeriodArg(args);
        if (periodOverride != null) {
            cfg = cfg.withCertificationPeriod(periodOverride);
        }

        consolidateCertificates(cfg, env).await();
    }

    // Parses the optional --certification-period submission flag. The whole
    // command line must be empty or carry exactly one flag, in the separated
    // (--certification-period daily) or '=' (--certification-period=daily)
    // form; anything else — an unknown flag, a trailing extra argument, a
    // flag without a value — fails fast with the one-line usage instead of
    // being silently ignored, because a mis-typed flag would otherwise run
    // the demo with the wrong period size. Returns null when no flag is
    // present so fromEnv()'s env var keeps deciding. The spec VALUE is
    // validated later by CertificationPeriod.parse (via withCertificationPeriod).
    // Package-private for the argument-parser unit test.
    static String certificationPeriodArg(String[] args) {
        if (args.length == 0) {
            return null;
        }
        if (args.length == 2 && args[0].equals("--certification-period") && !args[1].isEmpty()) {
            return args[1];
        }
        if (args.length == 1 && args[0].startsWith("--certification-period=")) {
            String value = args[0].substring("--certification-period=".length());
            if (!value.isEmpty()) {
                return value;
            }
        }
        throw new IllegalArgumentException(USAGE);
    }

    // Fail fast: without checkpointing the CDC snapshot never commits a
    // chunk and the pipeline's output silently stays empty.
    private static void requireCheckpointing(StreamExecutionEnvironment env, String pipeline) {
        if (!env.getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException(
                    "Checkpointing must be enabled on env before building the " + pipeline
                            + " pipeline: CDC snapshot chunks commit on checkpoints.");
        }
    }

    /**
     * Builds and submits the certificate consolidation pipeline. Public for
     * the e2e tests, which run it on the local mini-cluster; rejects
     * {@code env} without checkpointing (see {@link #main}). The returned
     * streaming job runs until cancelled; {@link TableResult#await()} blocks
     * for it.
     */
    public static TableResult consolidateCertificates(PipelineConfig cfg, StreamExecutionEnvironment env) {
        requireCheckpointing(env, "certificate consolidation");

        StreamTableEnvironment tables = StreamTableEnvironment.create(env);

        // Pinned to Buenos Aires so the certification period boundaries are
        // Buenos Aires calendar boundaries (issue #7): FLOOR(... TO DAY/MONTH)
        // floors the session wall clock, and the TIMESTAMP_LTZ -> TIMESTAMP
        // cast renders the session-TZ wall clock (verified on Flink 1.20.5:
        // CAST(TO_TIMESTAMP_LTZ(0, 0) AS TIMESTAMP(6)) under this pin yields
        // 1969-12-31 21:00:00). The zone is fixed-offset (-03:00, no DST),
        // which the sink conversion in createConsolidationInsertSql relies
        // on to express the offset as one interval literal.
        tables.getConfig().setLocalTimeZone(SESSION_ZONE);

        // The event-time temporal join in the consolidation INSERT buffers
        // every calculation until the combined watermark of BOTH inputs
        // passes the calculation's created_at. The merchants dimension is
        // quiet in streaming (merchant changes are rare) and its watermark
        // freezes at its last record's op_ts minus 5 seconds — the snapshot
        // read time — which sits below every later streaming calculation, so
        // without help it would hold every buffered calculation back
        // forever. Flink's default table.exec.source.idle-timeout is 0
        // (disabled); after 2 seconds without records the merchants input is
        // marked idle and stops gating the combined watermark while no
        // merchant changes stream in, and a real merchant change re-activates
        // it instantly. The value is short ON PURPOSE: both inputs idle on
        // this same timeout, and once BOTH are idle the combined watermark
        // never recomputes, so if the tax input idled before the merchants
        // one, every buffered row would freeze below its emission watermark
        // (observed as a total stall in the e2e runs with a 10-second
        // timeout). 2 seconds lets the merchants input — whose tiny snapshot
        // finishes first — reliably go idle first.
        tables.getConfig().set(
                ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT, Duration.ofSeconds(2));

        tables.executeSql(PipelineDdl.createCdcSourceDdl(cfg));
        tables.executeSql(PipelineDdl.createMerchantsCdcDdl(cfg));
        tables.executeSql(PipelineDdl.createCertificateSinkDdl(cfg));

        // The config carries the parsed period; no re-parse needed here.
        return tables.executeSql(createConsolidationInsertSql(cfg.certificationPeriod(), SESSION_ZONE));
    }

    // Group aggregation fed by an event-time temporal join (issue #6) instead
    // of a window TVF: a window TVF only emits when the window closes
    // (watermark passes), which cannot grow the certificate live while its
    // period is open. A plain group aggregation emits an updated row per
    // input record and the JDBC upsert sink materializes it incrementally;
    // rows for a period stop changing once no more rows for it arrive.
    //
    // Enrichment (issue #6): each calculation LEFT JOINs the merchant
    // changelog FOR SYSTEM_TIME AS OF its own created_at, so the certificate
    // is a pure function of the source history and replays reproduce
    // identical certificates (the #5 follow-up's idempotency requirement).
    // LEFT join semantics: a withholding whose CUIT has no merchant still
    // consolidates, with null merchant columns (missing master data must not
    // drop withholdings). LAST_VALUE carries the last merchant version seen
    // in the period (a merchant update during a period results in the last
    // version on the certificate), and LAST_VALUE skips NULL inputs, so
    // merchantless rows can never clobber enriched values inside one group.
    //
    // Emission lag: the temporal join buffers each calculation until the
    // combined watermark of BOTH inputs passes its created_at (both sources
    // use 5-second bounded-out-of-orderness watermarks, and the merchants
    // input stops gating the combined watermark while quiet — see the
    // idle-timeout in consolidateCertificates). Emissions therefore lag
    // inserts by at least the watermark bound, and the last row of a quiet
    // stream waits for the next record on either input; this is inherent to
    // the event-time temporal join, and the 5-second bound is pinned by
    // issue #7.
    //
    // Certification period (issue #7): the size comes from the parsed
    // CertificationPeriod and the window expressions are generated per kind.
    // All kinds align to the session wall clock (issue #1 story #14: period
    // boundaries are local calendar boundaries): 'daily'/'monthly' FLOOR the
    // session wall clock TO DAY/TO MONTH, and fixed n-second periods floor
    // the epoch in local-wall-clock coordinates (see the SECONDS branch). The
    // SELECT derives window_start/window_end from the GROUP BY's period key
    // with scalar functions, the same way the FLOOR expression used to be
    // repeated. FLOOR is applied after casting created_at to TIMESTAMP(3)
    // because two 1.20.5 paths are broken: FLOOR directly on TIMESTAMP_LTZ
    // fails codegen (CompileException) and UNIX_TIMESTAMP(timestamp_ltz) is
    // rejected by the validator. 'RET|_%' ESCAPE '|' keeps the underscore
    // literal; it is written 'RET|_%%' in the template so .formatted()
    // renders the single %.
    //
    // Sink conversion (the repo's standing JDBC invariant): the sink DDL
    // declares window_start/window_end as TIMESTAMP(6) (flink-connector-jdbc
    // 3.3.0 has no TIMESTAMP_LTZ converter) and the physical columns are
    // timestamptz; the JDBC/pgjdbc stack interprets a TIMESTAMP value in the
    // JVM default time zone, which is UTC in every runtime of this project
    // (compose containers and test host). Under the Buenos Aires session pin,
    // CAST(<ltz expr> AS TIMESTAMP(6)) renders the BA wall clock (verified on
    // 1.20.5), so every window boundary is shifted by the zone's fixed offset
    // interval below — minus a negative interval = plus — writing the UTC
    // wall clock of the boundary instant, which lands exactly like the UTC
    // pin's values did.
    //
    // Late-event dropping (issue #7): the WHERE drops arrivals whose
    // created_at is already below the source watermark at the moment the row
    // is evaluated. CURRENT_WATERMARK returns the source watermark at
    // evaluation time, so with the 5-second bounded out-of-orderness a
    // calculation arriving more than 5 seconds behind newer events is dropped
    // before the temporal join and never consolidates: allowed lateness is 0,
    // and pure-SQL Flink has no late-event recovery. COALESCE keeps every row
    // until the first watermark exists — before any watermark arrives
    // CURRENT_WATERMARK is NULL and nothing can be judged late. The
    // deliberate interaction with the temporal join: a row that arrived ON
    // time and is buffered by the join still emits when the watermark passes
    // it — the filter removes late arrivals, not buffered rows.
    static String createConsolidationInsertSql(CertificationPeriod period, ZoneId zone) {
        String offsetInterval = offsetIntervalLiteral(zone);
        WindowSql sql = switch (period.kind()) {
            case SECONDS -> {
                // The GROUP BY key is the local-wall-clock-aligned epoch
                // floor. UNIX_TIMESTAMP(DATE_FORMAT(...)) renders the session
                // wall clock (Buenos Aires by the session pin) without millis
                // and returns exact epoch seconds — source instants are
                // second-granular, so nothing is lost. Adding the zone's
                // fixed offset turns the epoch into the "local pseudo-epoch"
                // (what the wall clock would read if it were UTC); flooring
                // THAT to a multiple of n and subtracting the offset again
                // yields boundary epochs aligned to local wall-clock
                // multiples of n (local midnight, 00:00:07, ... for a 7s
                // period), as issue #1 story #14 requires. Buenos Aires holds
                // a fixed -03:00 forever — offsetIntervalLiteral above
                // fail-fasts on any zone with a future transition, so the
                // offset read here is constant for the job's lifetime.
                //
                // When n divides the offset's second count (60s, 1s, 2s, ...),
                // the shift cancels algebraically (FLOOR((x + s)/n) = FLOOR(x/n)
                // + s/n for integer s/n), so the plain epoch floor of the
                // pre-alignment formula is emitted unchanged — byte-identical
                // SQL, identical boundaries for the demo sizes.
                long offsetSeconds = zone.getRules().getOffset(Instant.now()).getTotalSeconds();
                String epochSeconds =
                        "UNIX_TIMESTAMP(DATE_FORMAT(CAST(calc.created_at AS TIMESTAMP(3)), "
                                + "'yyyy-MM-dd HH:mm:ss'))";
                String epochFloor;
                if (offsetSeconds % period.seconds() == 0) {
                    epochFloor = "CAST(FLOOR(" + epochSeconds
                            + " / " + period.seconds()
                            + ") * " + period.seconds() + " AS BIGINT)";
                } else {
                    epochFloor = "CAST(FLOOR((" + epochSeconds + signed(offsetSeconds)
                            + ") / " + period.seconds()
                            + ") * " + period.seconds() + signed(-offsetSeconds) + " AS BIGINT)";
                }
                String windowStart = "CAST(TO_TIMESTAMP_LTZ(" + epochFloor + ", 0) AS TIMESTAMP(6)) - " + offsetInterval;
                yield new WindowSql(windowStart,
                        windowStart + " + INTERVAL '" + period.seconds() + "' SECOND", epochFloor);
            }
            case CALENDAR_DAY -> {
                // Buenos Aires midnight by the session pin.
                String dayFloor = "FLOOR(CAST(calc.created_at AS TIMESTAMP(3)) TO DAY)";
                String windowStart = "CAST(" + dayFloor + " AS TIMESTAMP(6)) - " + offsetInterval;
                yield new WindowSql(windowStart, windowStart + " + INTERVAL '1' DAY", dayFloor);
            }
            case CALENDAR_MONTH -> {
                // Buenos Aires first of month by the session pin. Adding one
                // MONTH is calendar-aware and always lands on the next first
                // of month, so no clamping is needed.
                String monthFloor = "FLOOR(CAST(calc.created_at AS TIMESTAMP(3)) TO MONTH)";
                String windowStart = "CAST(" + monthFloor + " AS TIMESTAMP(6)) - " + offsetInterval;
                yield new WindowSql(windowStart, windowStart + " + INTERVAL '1' MONTH", monthFloor);
            }
        };
        return """
                INSERT INTO certificate_items
                SELECT
                    calc.cuit,
                    calc.tax_id,
                    %s,
                    calc.tax_rate,
                    LAST_VALUE(merchants.establishment),
                    LAST_VALUE(merchants.name),
                    %s,
                    CAST(SUM(calc.base_tax) AS DECIMAL(18, 2)),
                    CAST(SUM(calc.tax_amount) AS DECIMAL(18, 2))
                FROM tax_calculations_cdc calc
                LEFT JOIN merchants_cdc FOR SYSTEM_TIME AS OF calc.created_at AS merchants
                    ON calc.cuit = merchants.cuit
                WHERE calc.tax_id LIKE 'RET|_%%' ESCAPE '|'
                    AND calc.created_at >= COALESCE(CURRENT_WATERMARK(calc.created_at), calc.created_at)
                GROUP BY
                    calc.cuit,
                    calc.tax_id,
                    calc.tax_rate,
                    %s
                """.formatted(sql.windowStart(), sql.windowEnd(), sql.groupKey());
    }

    // Renders a second count as a signed arithmetic SQL fragment (" + 10800"
    // or " - 10800"); zero renders as " + 0", which callers avoid.
    private static String signed(long seconds) {
        return seconds < 0 ? " - " + -seconds : " + " + seconds;
    }

    // The three SQL fragments one period kind generates: the window_start and
    // window_end sink expressions and the GROUP BY period key.
    private record WindowSql(String windowStart, String windowEnd, String groupKey) {
    }

    // The zone's UTC offset as a SQL interval literal, used above to shift
    // the session wall clock (Buenos Aires) to the UTC wall clock the JDBC
    // sink needs (see the sink-conversion paragraph in
    // createConsolidationInsertSql). The literal must be HOUR TO MINUTE:
    // Flink 1.20.5 rejects INTERVAL '-10800' SECOND ("exceeds precision of
    // SECOND(2)") and '-10800' SECOND(6) ("DAY_INTERVAL_TYPES precision is
    // not supported"), while '-3:0' HOUR TO MINUTE validates. The sign rides
    // on the first field of the absolute value: Buenos Aires (-03:00) yields
    // INTERVAL '-3:0' HOUR TO MINUTE, so `expr - (-3:0)` adds 3 hours and
    // recovers the UTC wall clock of the same instant.
    //
    // The zone must be effectively fixed-offset, checked as "no transition
    // after the current instant". ZoneRules.isFixedOffset() is NOT usable:
    // it also returns false for America/Argentina/Buenos_Aires, which
    // observed DST until 2009, although its rules have no further transition
    // and -03:00 holds forever from now on. nextTransition(...) == null
    // means one offset literal covers every event from the job's start
    // onward; a DST-observing zone has a future transition and is rejected
    // fail-fast, because one literal cannot carry a varying offset. For a
    // fixed zone the instant passed to getOffset is irrelevant. Accepted
    // limitation of the one-literal approach: a row dated INSIDE the zone's
    // historical DST era (pre-2009 for Buenos Aires) floors to the correct
    // local wall clock but converts back with today's offset, so its stored
    // boundary instant would be one hour off; no such data exists in this
    // PoC's horizons.
    private static String offsetIntervalLiteral(ZoneId zone) {
        if (zone.getRules().nextTransition(Instant.now()) != null) {
            throw new IllegalStateException(
                    "Certification windows require a time zone whose UTC offset no longer changes, but " + zone
                            + " has a future transition (DST); its varying offset cannot be expressed as one SQL interval literal.");
        }
        int totalSeconds = zone.getRules().getOffset(Instant.now()).getTotalSeconds();
        int hours = Math.abs(totalSeconds) / 3600;
        int minutes = Math.abs(totalSeconds) % 3600 / 60;
        String sign = totalSeconds < 0 ? "-" : "";
        return "INTERVAL '" + sign + hours + ":" + minutes + "' HOUR TO MINUTE";
    }
}
