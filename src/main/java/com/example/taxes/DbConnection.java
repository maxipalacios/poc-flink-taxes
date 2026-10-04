package com.example.taxes;

import java.util.regex.Pattern;

/**
 * Connection coordinates of one PostgreSQL node: host, port, credentials and
 * database name. {@link PipelineConfig} carries one for the CDC source and
 * one for the JDBC sink, replacing the flat String list that made the two
 * connection data clumps easy to mix up.
 *
 * <p>Every field that the DDL builders interpolate into connector DDL is
 * validated here in the compact constructor (fail fast, security): host,
 * port, username and database name must match strict allow-list patterns and
 * the password must not carry characters that could break out of the DDL's
 * single-quoted string literals. Because every DDL value comes from a
 * validated {@code DbConnection}, the interpolation in {@code PipelineDdl} is
 * safe by construction.
 */
public record DbConnection(String host, String port, String username, String password, String databaseName) {

    // Docker DNS service names (postgres-source), localhost, IPv4 addresses
    // and Testcontainers hosts are all subsets of this allow-list; anything
    // outside it (spaces, quotes, slashes, @) has no business in a hostname
    // and would be an injection attempt or a misconfiguration.
    private static final Pattern HOST_PATTERN = Pattern.compile("[A-Za-z0-9._-]{1,253}");

    // Postgres identifiers as the connector DDL uses them (unquoted): letters,
    // digits and underscores only, up to 63 chars. Keeps usernames and the
    // database name from carrying quotes or spaces into the DDL literals.
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z0-9_]{1,63}");

    public DbConnection {
        if (host == null || !HOST_PATTERN.matcher(host).matches()) {
            throw new IllegalArgumentException(
                    "Invalid database host '" + host
                            + "': expected letters, digits, dots, underscores or dashes (up to 253 chars), "
                            + "e.g. 'postgres-source', 'localhost' or '127.0.0.1'");
        }
        validatePort(port);
        if (username == null || !IDENTIFIER_PATTERN.matcher(username).matches()) {
            throw new IllegalArgumentException(
                    "Invalid database user '" + username
                            + "': expected letters, digits or underscores (up to 63 chars)");
        }
        // The password is interpolated into single-quoted DDL literals, where
        // a single quote would terminate the literal and everything after it
        // would execute as connector configuration. Rejection beats escaping
        // here: escaping would couple every DDL builder to the quoting rules,
        // and a password that needs quotes, backslashes or semicolons can be
        // rotated to one that does not.
        if (password == null || password.indexOf('\'') >= 0 || password.indexOf('\\') >= 0
                || password.indexOf(';') >= 0 || password.indexOf('\n') >= 0 || password.indexOf('\r') >= 0) {
            throw new IllegalArgumentException(
                    "Invalid database password: must not contain a single quote, backslash, semicolon or newline "
                            + "(the value is interpolated into single-quoted DDL literals)");
        }
        if (databaseName == null || !IDENTIFIER_PATTERN.matcher(databaseName).matches()) {
            throw new IllegalArgumentException(
                    "Invalid database name '" + databaseName
                            + "': expected letters, digits or underscores (up to 63 chars)");
        }
    }

    // Digits only, in range: the port is interpolated into the DDL's 'port'
    // option and into JDBC URLs, so a sign, a space or trailing garbage must
    // fail here rather than produce a subtly broken connection string.
    private static void validatePort(String port) {
        if (port == null || !port.matches("[0-9]+")) {
            throw new IllegalArgumentException(
                    "Invalid database port '" + port + "': expected digits only, e.g. '5432'");
        }
        int value;
        try {
            value = Integer.parseInt(port);
        } catch (NumberFormatException overflow) {
            throw new IllegalArgumentException(
                    "Invalid database port '" + port + "': outside the 1-65535 range");
        }
        if (value < 1 || value > 65535) {
            throw new IllegalArgumentException(
                    "Invalid database port '" + port + "': outside the 1-65535 range");
        }
    }
}
