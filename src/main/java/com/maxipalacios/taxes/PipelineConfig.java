package com.maxipalacios.taxes;

/**
 * Connection coordinates shared by the tax pipelines: where to read the
 * source {@code tax_calculations} table from and where to write in the target
 * database (the {@code certificate_items} consolidation sink, or the
 * {@code tax_calculations_mirror} debugging table for the issue #4 pipeline).
 *
 * <p>The consolidation pipeline (issues #5 and #6) reads two CDC sources from
 * the same database — {@code tax_calculations} and {@code merchants} — so it
 * consumes two replication slots: {@link #replicationSlotName} for the tax
 * calculations and {@link #merchantsReplicationSlotName()} for the merchants.
 *
 * <p>{@link #fromEnv()} resolves the docker-compose service names, so
 * {@code flink run} on the cluster needs no configuration; the e2e tests
 * pass container coordinates through the constructor instead.
 */
public record PipelineConfig(
        String sourceHost,
        String sourcePort,
        String targetHost,
        String targetPort,
        String databaseName,
        String username,
        String password,
        String replicationSlotName) {

    public static PipelineConfig fromEnv() {
        return new PipelineConfig(
                envOr("SOURCE_POSTGRES_HOST", "postgres-source"),
                envOr("SOURCE_POSTGRES_PORT", "5432"),
                envOr("TARGET_POSTGRES_HOST", "postgres-target"),
                envOr("TARGET_POSTGRES_PORT", "5433"),
                envOr("POSTGRES_DB", "taxes"),
                envOr("POSTGRES_USER", "flink"),
                envOr("POSTGRES_PASSWORD", "flink"),
                // The fallback names the certificate pipeline's slot: main()
                // runs consolidation, while the mirror pipeline is only
                // exercised by its e2e test, which passes its own unique slot
                // name.
                envOr("CDC_SLOT_NAME", "flink_tax_certificates"));
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
