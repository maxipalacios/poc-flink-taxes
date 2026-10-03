package com.maxipalacios.taxes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The size of a certification period (GLOSSARY, "Certification Period"): the
 * interval during which withholdings consolidate into one certificate.
 * Configured through a spec string accepted by the {@code CERTIFICATION_PERIOD}
 * env var and the {@code --certification-period} submission flag (issue #7).
 * Three forms are valid:
 *
 * <ul>
 *     <li>{@code <n>s} — a tumbling period of {@code n} seconds, {@code n >= 1}
 *         integer (e.g. {@code 1s}, {@code 60s}); boundaries are epoch-aligned
 *         (floor of the event's epoch seconds to a multiple of {@code n}).</li>
 *     <li>{@code daily} — one calendar day, aligned to midnight of the job's
 *         session time zone (America/Argentina/Buenos_Aires).</li>
 *     <li>{@code monthly} — one calendar month, aligned to the first of the
 *         month of the job's session time zone.</li>
 * </ul>
 *
 * <p>Anything else — null, blank, zero or negative seconds, other units
 * ({@code 5m}, {@code 1d}), uppercase {@code S}, surrounding whitespace,
 * garbage — throws {@link IllegalArgumentException} with a message listing
 * the valid forms. Parsing is exact: specs are not trimmed or case-folded.
 */
public record CertificationPeriod(Kind kind, long seconds, String spec) {

    /** Discriminates how a period's boundaries are computed. */
    public enum Kind {
        /** Fixed n-second tumbling periods, floored on the epoch. */
        SECONDS,
        /** One calendar day: session-time-zone midnight to midnight. */
        CALENDAR_DAY,
        /** One calendar month: first of month to first of the next month. */
        CALENDAR_MONTH
    }

    /** The spec every default configuration path falls back to. */
    public static final String DEFAULT_SPEC = "60s";

    // Exact whole-string match: digits followed by a lowercase 's'. The
    // regex already rejects a sign, so "0s" fails on the n >= 1 check
    // below and "-1s" fails right here on the match. Declared before
    // DEFAULT, whose initializer calls parse and needs it.
    private static final Pattern SECONDS_SPEC = Pattern.compile("([0-9]+)s");

    /** The default period: the 60-second demo size the seed and README document. */
    public static final CertificationPeriod DEFAULT = parse(DEFAULT_SPEC);

    public CertificationPeriod {
        if (kind == null) {
            throw new IllegalArgumentException("Certification period kind must not be null");
        }
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("Certification period spec must not be blank");
        }
        if (kind == Kind.SECONDS && seconds < 1) {
            throw new IllegalArgumentException(
                    "Fixed-second certification periods require seconds >= 1, got " + seconds);
        }
        if (kind != Kind.SECONDS && seconds != 0) {
            throw new IllegalArgumentException(
                    "Calendar certification periods carry no second count, got " + seconds);
        }
    }

    /**
     * Parses a certification period spec. See the class javadoc for the valid
     * forms; everything else throws with a message listing them.
     */
    public static CertificationPeriod parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw invalidSpec(spec);
        }
        switch (spec) {
            case "daily":
                return new CertificationPeriod(Kind.CALENDAR_DAY, 0, spec);
            case "monthly":
                return new CertificationPeriod(Kind.CALENDAR_MONTH, 0, spec);
            default:
                Matcher matcher = SECONDS_SPEC.matcher(spec);
                if (!matcher.matches()) {
                    throw invalidSpec(spec);
                }
                long seconds;
                try {
                    seconds = Long.parseLong(matcher.group(1));
                } catch (NumberFormatException overflow) {
                    // The spec has the right shape but no long can hold n.
                    throw invalidSpec(spec);
                }
                if (seconds < 1) {
                    throw invalidSpec(spec);
                }
                return new CertificationPeriod(Kind.SECONDS, seconds, spec);
        }
    }

    private static IllegalArgumentException invalidSpec(String spec) {
        return new IllegalArgumentException(
                "Invalid certification period '" + spec
                        + "': expected '<n>s' with n >= 1 (e.g. '60s', '1s'), 'daily' or 'monthly'");
    }
}
