package com.example.taxes.e2e;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Reusable ephemeral source+target PostgreSQL pair for the e2e tests.
 *
 * <p>The source runs with {@code wal_level=logical} (the CDC connector needs
 * logical replication) and mounts the repo's seed script; the target mounts
 * the repo's target init script. Both mirror the docker-compose setup:
 * postgres:17, db/user/password taxes/flink/flink. Every later e2e test
 * reuses this pair via try-with-resources; each start generates a unique
 * replication slot name so parallel or repeated runs never collide.
 */
public final class PostgresPair implements AutoCloseable {

    public static final String DATABASE = "taxes";
    public static final String USERNAME = "flink";
    public static final String PASSWORD = "flink";

    private static final int POSTGRES_PORT = 5432;

    private final PostgreSQLContainer<?> source;
    private final PostgreSQLContainer<?> target;
    private final String slotName;

    private PostgresPair() {
        // Relative paths work because Gradle's test working directory is the
        // project root.
        File sourceSeed = new File("docker/postgres/source/init.sql");
        File targetSeed = new File("docker/postgres/target/init.sql");
        if (!sourceSeed.isFile() || !targetSeed.isFile()) {
            throw new IllegalStateException(
                "Seed script not found (Gradle's test working directory must be the project root): "
                    + sourceSeed.getAbsolutePath() + ", " + targetSeed.getAbsolutePath());
        }

        slotName = newSlotName();

        source = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName(DATABASE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            // Same flags as the postgres-source service in docker-compose.yml.
            .withCommand("postgres", "-c", "wal_level=logical",
                "-c", "max_wal_senders=10", "-c", "max_replication_slots=10");
        // The mounted script is load-bearing beyond the seed data: it also
        // pre-creates the two CDC publications (dbz_publication for
        // tax_calculations, flink_tax_merchants_publication for merchants)
        // that both CDC sources require — PipelineDdl runs them with
        // publication autocreation DISABLED, because PostgreSQL 17 only lets
        // superusers or table owners create publications. The script runs as
        // the bootstrap superuser here, exactly like in docker-compose.
        source.withCopyFileToContainer(
            MountableFile.forHostPath(sourceSeed.toPath()),
            "/docker-entrypoint-initdb.d/001-init.sql");

        target = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName(DATABASE)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);
        target.withCopyFileToContainer(
            MountableFile.forHostPath(targetSeed.toPath()),
            "/docker-entrypoint-initdb.d/001-init.sql");
    }

    /** Starts the source+target pair; stops whatever was started on failure. */
    public static PostgresPair start() {
        PostgresPair pair = new PostgresPair();
        try {
            pair.source.start();
            pair.target.start();
            return pair;
        } catch (RuntimeException | Error e) {
            pair.close();
            throw e;
        }
    }

    // PostgreSQL slot names allow only lowercase letters, digits and
    // underscores, capped at 63 chars.
    private static String newSlotName() {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        return "flink_tax_poc_" + suffix;
    }

    public String sourceHost() {
        return source.getHost();
    }

    public int sourcePort() {
        return source.getMappedPort(POSTGRES_PORT);
    }

    public String targetHost() {
        return target.getHost();
    }

    public int targetPort() {
        return target.getMappedPort(POSTGRES_PORT);
    }

    /**
     * Docker container id of the target, for tests that inject an
     * infrastructure failure by stopping and starting the container through
     * docker-java (the issue #8 checkpoint-recovery test kills the JDBC
     * sink's database mid-run to force the job's failover).
     */
    public String targetContainerId() {
        return target.getContainerId();
    }

    /** Unique replication slot name for this run. */
    public String slotName() {
        return slotName;
    }

    /** Opens a connection to the source; the caller closes it. */
    public Connection openSourceConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(sourceHost(), sourcePort()), USERNAME, PASSWORD);
    }

    /** Opens a connection to the target; the caller closes it. */
    public Connection openTargetConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(targetHost(), targetPort()), USERNAME, PASSWORD);
    }

    /** Convenience for inserting test rows into the source. */
    public void executeSourceStatement(String sql) {
        try (Connection connection = openSourceConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute: " + sql, e);
        }
    }

    @Override
    public void close() {
        stopIfRunning(source);
        stopIfRunning(target);
    }

    private static void stopIfRunning(PostgreSQLContainer<?> container) {
        if (container.isRunning()) {
            container.stop();
        }
    }

    private static String jdbcUrl(String host, int port) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + DATABASE;
    }
}
