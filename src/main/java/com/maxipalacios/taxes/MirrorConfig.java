package com.maxipalacios.taxes;

/**
 * Connection coordinates for the CDC mirror pipeline: where to read the
 * source {@code tax_calculations} table from and where to write the mirror
 * table in the target database.
 *
 * <p>{@link #fromEnv()} resolves the docker-compose service names, so
 * {@code flink run} on the cluster needs no configuration; the e2e tests
 * pass container coordinates through the constructor instead.
 */
public record MirrorConfig(
        String sourceHost,
        String sourcePort,
        String targetHost,
        String targetPort,
        String databaseName,
        String username,
        String password,
        String replicationSlotName) {

    public static MirrorConfig fromEnv() {
        return new MirrorConfig(
                envOr("SOURCE_POSTGRES_HOST", "postgres-source"),
                envOr("SOURCE_POSTGRES_PORT", "5432"),
                envOr("TARGET_POSTGRES_HOST", "postgres-target"),
                envOr("TARGET_POSTGRES_PORT", "5433"),
                envOr("POSTGRES_DB", "taxes"),
                envOr("POSTGRES_USER", "flink"),
                envOr("POSTGRES_PASSWORD", "flink"),
                envOr("CDC_SLOT_NAME", "flink_tax_mirror"));
    }

    private static String envOr(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
