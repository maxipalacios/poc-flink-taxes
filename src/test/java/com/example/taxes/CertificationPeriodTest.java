package com.example.taxes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CertificationPeriod#parse}: the three valid spec
 * forms, the rejection of everything else, and the error message listing the
 * valid forms. Pure value-type tests; no Flink runtime involved.
 */
class CertificationPeriodTest {

    @Test
    @DisplayName("parse accepts fixed second periods and carries the second count")
    void parsesFixedSecondPeriods() {
        assertEquals(CertificationPeriod.Kind.SECONDS, CertificationPeriod.parse("1s").kind());
        assertEquals(1, CertificationPeriod.parse("1s").seconds());
        assertEquals(CertificationPeriod.Kind.SECONDS, CertificationPeriod.parse("2s").kind());
        assertEquals(2, CertificationPeriod.parse("2s").seconds());
        assertEquals(CertificationPeriod.Kind.SECONDS, CertificationPeriod.parse("60s").kind());
        assertEquals(60, CertificationPeriod.parse("60s").seconds());
        assertEquals(CertificationPeriod.Kind.SECONDS, CertificationPeriod.parse("120s").kind());
        assertEquals(120, CertificationPeriod.parse("120s").seconds());
    }

    @Test
    @DisplayName("parse accepts the calendar forms daily and monthly")
    void parsesCalendarPeriods() {
        assertEquals(CertificationPeriod.Kind.CALENDAR_DAY, CertificationPeriod.parse("daily").kind());
        assertEquals(CertificationPeriod.Kind.CALENDAR_MONTH, CertificationPeriod.parse("monthly").kind());
    }

    @Test
    @DisplayName("parse keeps the original spec for messages")
    void keepsTheOriginalSpec() {
        assertEquals("60s", CertificationPeriod.parse("60s").spec());
        assertEquals("daily", CertificationPeriod.parse("daily").spec());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "  ",
            "0s", "-1s", "60", "60S", "5m", "1d",
            "monthly ", // trailing space: specs are exact, not trimmed
            "abc"})
    @DisplayName("parse rejects specs outside the three valid forms")
    void rejectsInvalidSpecs(String spec) {
        assertThrows(IllegalArgumentException.class, () -> CertificationPeriod.parse(spec));
    }

    @Test
    @DisplayName("parse rejects null")
    void rejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> CertificationPeriod.parse(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  ", "0s", "-1s", "60", "60S", "5m", "1d", "monthly ", "abc"})
    @DisplayName("the rejection message lists every valid form")
    void rejectionMessageListsTheValidForms(String spec) {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> CertificationPeriod.parse(spec));
        String message = error.getMessage();
        assertTrue(message.contains("<n>s"), message);
        assertTrue(message.contains("daily"), message);
        assertTrue(message.contains("monthly"), message);
    }

    @Test
    @DisplayName("the default is the 60-second demo period")
    void defaultIsTheDemoPeriod() {
        assertEquals("60s", CertificationPeriod.DEFAULT_SPEC);
        assertEquals(CertificationPeriod.DEFAULT_SPEC, CertificationPeriod.DEFAULT.spec());
        assertEquals(CertificationPeriod.Kind.SECONDS, CertificationPeriod.DEFAULT.kind());
        assertEquals(60, CertificationPeriod.DEFAULT.seconds());
    }
}
