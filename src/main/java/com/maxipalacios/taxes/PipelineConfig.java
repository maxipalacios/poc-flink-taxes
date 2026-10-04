package com.maxipalacios.taxes;

/**
 * Configuration shared by the tax pipelines: the connection coordinates —
 * where to read the source {@code tax_calculations} table from and where to
 * write in the target database (the {@code certificate_items} consolidation
 * sink, or the {@code tax_calculations_mirror} debugging table for the issue
 * #4 pipeline) — plus, since issue #7, pipeline parameters beyond connection
 * coordinates: {@link #certificationPeriodSpec} sizes the consolidation
 * pipeline's certification periods (see {@link CertificationPeriod} for the
 * valid forms).
 *
 * <p>The consolidation pipeline (issues #5 and #6) reads two CDC sources from
 * the same database — {@code tax_calculations} and {@code merchants} — so it
 * consumes two replication slots: {@link #replicationSlotName} for the tax
 * calculations and {@link #merchantsReplicationSlotName()} for the merchants.
 *
 * <p>{@link #fromEnv()} resolves the docker-compose service names, so
 * {@code flink run} on the cluster needs no configuration; the e2e tests
 * pass container coordinates through the constructor instead. The period spec
 * is validated eagerly in the compact constructor, so a bad env var or flag
 * value fails before any pipeline object exists.
 */
public record PipelineConfig(
        String sourceHost,
        String sourcePort,
        String targetHost,
        String targetPort,
        String databaseName,
        String username,
        String password,
        String replicationSlotName,
        String certificationPeriodSpec) {

    public PipelineConfig {
        // Fail fast: an invalid spec must surface at construction, not deep
        // inside the job submission after CDC sources are already declared.
        CertificationPeriod.parse(certificationPeriodSpec);
    }

    public static PipelineConfig fromEnv() {
        return new PipelineConfig(
                envOr("SOURCE_POSTGRES_HOST", "postgres-source"),
                envOr("SOURCE_POSTGRES_PORT", "5432"),
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
                envOr("POSTGRES_DB", "taxes"),
                envOr("POSTGRES_USER", "flink"),
                envOr("POSTGRES_PASSWORD", "flink"),
                // The fallback names the certificate pipeline's slot: main()
                // runs consolidation, while the mirror pipeline is only
                // exercised by its e2e test, which passes its own unique slot
                // name.
                envOr("CDC_SLOT_NAME", "flink_tax_certificates"),
                // The fallback matches the compose demo default (seed and
                // README): an unconfigured deployment behaves exactly like the
                // pre-issue-#7 pipeline with its 60-second periods.
                envOr("CERTIFICATION_PERIOD", CertificationPeriod.DEFAULT_SPEC));
    }

    // Returns a copy with the certification period spec overridden. Used by
    // TaxJob.main so the --certification-period flag can override the
    // env-resolved value without re-listing every connection coordinate; the
    // compact constructor re-validates the new spec.
    public PipelineConfig withCertificationPeriod(String newSpec) {
        return new PipelineConfig(sourceHost, sourcePort, targetHost, targetPort,
                databaseName, username, password, replicationSlotName, newSpec);
    }

    // Second replication slot for the merchants CDC source (issue #6).
    // PostgreSQL allows a single consumer per slot, and both CDC sources run
    // against the same database, so each needs its own. Derived from the main
    // slot name rather than a new record component: both e2e tests construct
    // the record positionally, and a "_merchants" suffix keeps the pair
    // identifiable per deployment. Slot names only allow lowercase letters,
    // digits and underscores, capped at 63 chars, which the derived values
    // ("flink_tax_certificates_merchants") satisfy.
    public String merchantsReplicationSlotName() {
        return replicationSlotName + "_merchants";
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
