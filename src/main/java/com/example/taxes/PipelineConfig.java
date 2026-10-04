package com.example.taxes;

import java.util.regex.Pattern;

/**
 * Configuration of the certificate consolidation pipeline: where to read the
 * source {@code tax_calculations} table from ({@link #source}), where to
 * write the {@code certificate_items} consolidation sink in the target
 * database ({@link #target}), the source's replication slot and the
 * {@link CertificationPeriod} that sizes the periods (issue #7, see
 * {@link CertificationPeriod} for the valid forms).
 *
 * <p>Both endpoints are typed {@link DbConnection}s, so the source/target
 * coordinates travel as one value each instead of a loose list of Strings,
 * and the period is carried in its parsed form — callers never re-parse the
 * spec. Every interpolated field is validated at construction (see
 * {@link DbConnection} and the slot-name check below), so the DDL builders in
 * {@code PipelineDdl} interpolate only vetted values.
 *
 * <p>The consolidation pipeline (issues #5 and #6) reads two CDC sources from
 * the same database — {@code tax_calculations} and {@code merchants} — so it
 * consumes two replication slots: {@link #replicationSlotName} for the tax
 * calculations and {@link #merchantsReplicationSlotName()} for the merchants.
 *
 * <p>{@link #fromEnv()} resolves the docker-compose service names, so
 * {@code flink run} on the cluster needs no configuration; the e2e tests pass
 * container coordinates through the constructor instead. Environment variables:
 *
 * <ul>
 *     <li>{@code SOURCE_POSTGRES_HOST} (default {@code postgres-source}) and
 *         {@code SOURCE_POSTGRES_PORT} (default {@code 5432}) — the CDC source;</li>
 *     <li>{@code SOURCE_POSTGRES_USER} (default {@code flink_cdc}) and
 *         {@code TARGET_POSTGRES_USER} (default {@code flink_sink}) — the two
 *         databases use separate users: the source needs replication rights,
 *         the target only sink write rights;</li>
 *     <li>{@code TARGET_POSTGRES_HOST} (default {@code postgres-target}) and
 *         {@code TARGET_POSTGRES_PORT} (default {@code 5432}) — the JDBC sink;</li>
 *     <li>{@code POSTGRES_DB} (default {@code taxes}) and
 *         {@code POSTGRES_PASSWORD} (default {@code flink}) — shared by both
 *         endpoints;</li>
 *     <li>{@code CDC_SLOT_NAME} (default {@code flink_tax_certificates});</li>
 *     <li>{@code CERTIFICATION_PERIOD} (default
 *         {@code CertificationPeriod.DEFAULT_SPEC}).</li>
 * </ul>
 *
 * <p>Every value is validated eagerly — the {@link DbConnection}s in their
 * compact constructor, the slot name below, the period when
 * {@link #fromEnv()} parses it — so a bad env var or flag value fails before
 * any pipeline object exists.
 */
public record PipelineConfig(DbConnection source, DbConnection target, String replicationSlotName,
        CertificationPeriod certificationPeriod) {

    // PostgreSQL slot names allow only lowercase letters, digits and
    // underscores, capped at 63 chars (the merchants slot's derived value is
    // re-checked in merchantsReplicationSlotName, because its "_merchants"
    // suffix must not push it over the cap).
    private static final Pattern SLOT_NAME_PATTERN = Pattern.compile("[a-z0-9_]{1,63}");

    public PipelineConfig {
        if (certificationPeriod == null) {
            // Fail fast: a missing period must surface at construction, not
            // deep inside the job submission after CDC sources are already
            // declared. Specs are validated when they are parsed (fromEnv and
            // withCertificationPeriod), so only null can get this far.
            throw new IllegalArgumentException("Certification period must not be null; parse a spec with CertificationPeriod.parse");
        }
        if (replicationSlotName == null || !SLOT_NAME_PATTERN.matcher(replicationSlotName).matches()) {
            throw new IllegalArgumentException(
                    "Invalid replication slot name '" + replicationSlotName
                            + "': expected lowercase letters, digits or underscores (up to 63 chars)");
        }
    }

    public static PipelineConfig fromEnv() {
        // Parse here, not in the compact constructor: the record carries the
        // parsed period, and this is where a bad spec value must fail fast —
        // at startup, not deep inside the job submission after CDC sources
        // are already declared.
        CertificationPeriod certificationPeriod = CertificationPeriod.parse(envOr("CERTIFICATION_PERIOD",
                // The fallback matches the compose demo default (seed and
                // README): an unconfigured deployment behaves exactly like the
                // pre-issue-#7 pipeline with its 60-second periods.
                CertificationPeriod.DEFAULT_SPEC));
        return new PipelineConfig(
                new DbConnection(
                        envOr("SOURCE_POSTGRES_HOST", "postgres-source"),
                        envOr("SOURCE_POSTGRES_PORT", "5432"),
                        envOr("SOURCE_POSTGRES_USER", "flink_cdc"),
                        envOr("POSTGRES_PASSWORD", "flink"),
                        envOr("POSTGRES_DB", "taxes")),
                new DbConnection(
                        envOr("TARGET_POSTGRES_HOST", "postgres-target"),
                        // 5432, not the compose host mapping 5433: the job runs
                        // INSIDE the cluster network (flink run from the jobmanager
                        // container), where the postgres-target service listens on
                        // its physical 5432. The default must be the in-network port
                        // so the documented plain `flink run` works without env vars
                        // (issue #8's savepoint exercise hit the 5433 crash loop);
                        // deployments that publish the target differently override
                        // this via TARGET_POSTGRES_PORT.
                        envOr("TARGET_POSTGRES_PORT", "5432"),
                        envOr("TARGET_POSTGRES_USER", "flink_sink"),
                        envOr("POSTGRES_PASSWORD", "flink"),
                        envOr("POSTGRES_DB", "taxes")),
                // The fallback names the certificate pipeline's slot.
                envOr("CDC_SLOT_NAME", "flink_tax_certificates"),
                certificationPeriod);
    }

    // Returns a copy with the certification period overridden. Used by
    // TaxJob.main so the --certification-period flag can override the
    // env-resolved value without re-listing every connection coordinate;
    // parsing validates the new spec, keeping the copy fail-fast.
    public PipelineConfig withCertificationPeriod(String newSpec) {
        return new PipelineConfig(source, target, replicationSlotName, CertificationPeriod.parse(newSpec));
    }

    // Second replication slot for the merchants CDC source (issue #6).
    // PostgreSQL allows a single consumer per slot, and both CDC sources run
    // against the same database, so each needs its own. Derived from the main
    // slot name rather than a new record component, and the "_merchants"
    // suffix keeps the pair identifiable per deployment. The derived value
    // must satisfy the same slot-name rules as the base (see
    // SLOT_NAME_PATTERN), so a base name near the 63-char cap fails here
    // instead of producing a slot name PostgreSQL would reject.
    public String merchantsReplicationSlotName() {
        String derived = replicationSlotName + "_merchants";
        if (!SLOT_NAME_PATTERN.matcher(derived).matches()) {
            throw new IllegalStateException(
                    "Derived merchants replication slot name '" + derived
                            + "' violates the PostgreSQL slot-name rules (lowercase letters, digits, underscores, "
                            + "max 63 chars); shorten CDC_SLOT_NAME");
        }
        return derived;
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
