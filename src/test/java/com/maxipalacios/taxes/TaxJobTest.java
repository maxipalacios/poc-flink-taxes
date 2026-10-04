package com.maxipalacios.taxes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for the {@code --certification-period} submission flag parser
 * (issue #7): the only part of the job-submission surface that is testable
 * without a cluster. The spec VALUE itself is validated downstream by
 * {@link CertificationPeriodTest}; these tests pin the command-line grammar:
 * the flag is optional (env decides), both the separated and '=' forms work,
 * and anything else on the command line fails fast with the usage message
 * instead of being silently ignored — a mis-typed flag would otherwise run
 * the demo with the wrong period size.
 */
class TaxJobTest {

    @Test
    void returnsNullForAnEmptyCommandLineSoTheEnvVarDecides() {
        assertNull(TaxJob.certificationPeriodArg(new String[0]));
    }

    @Test
    void acceptsTheSeparatedForm() {
        assertEquals("daily", TaxJob.certificationPeriodArg(new String[]{"--certification-period", "daily"}));
        assertEquals("60s", TaxJob.certificationPeriodArg(new String[]{"--certification-period", "60s"}));
    }

    @Test
    void acceptsTheEqualsForm() {
        assertEquals("monthly", TaxJob.certificationPeriodArg(new String[]{"--certification-period=monthly"}));
        assertEquals("1s", TaxJob.certificationPeriodArg(new String[]{"--certification-period=1s"}));
    }

    @Test
    void rejectsAnUnknownFlag() {
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> TaxJob.certificationPeriodArg(new String[]{"--period", "daily"}));
        assertEquals(TaxJob.USAGE, thrown.getMessage());
    }

    @Test
    void rejectsATrailingExtraArgumentInsteadOfIgnoringIt() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> TaxJob.certificationPeriodArg(new String[]{"--certification-period", "daily", "extra"}));
        assertEquals(TaxJob.USAGE, thrown.getMessage());
    }

    @Test
    void rejectsAFlagWithoutAValue() {
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> TaxJob.certificationPeriodArg(new String[]{"--certification-period"}));
        assertEquals(TaxJob.USAGE, thrown.getMessage());
    }

    @Test
    void rejectsAnEmptyValueInBothForms() {
        IllegalArgumentException equalsForm =
                assertThrows(IllegalArgumentException.class, () -> TaxJob.certificationPeriodArg(new String[]{"--certification-period="}));
        assertEquals(TaxJob.USAGE, equalsForm.getMessage());
        IllegalArgumentException separatedForm =
                assertThrows(IllegalArgumentException.class, () -> TaxJob.certificationPeriodArg(new String[]{"--certification-period", ""}));
        assertEquals(TaxJob.USAGE, separatedForm.getMessage());
    }

    /**
     * Pins {@link TaxJob#CHECKPOINT_INTERVAL_MS} at the 10-second interval
     * issue #8's acceptance criterion requires ("checkpointing remains
     * enabled at 10-second intervals"). Honest scope: a cluster-free unit
     * test can only pin the constant's VALUE — that the pipeline really
     * checkpoints (and recovers from its checkpoints) at this interval is
     * what {@code CheckpointRecoveryE2eTest} proves against the real mini
     * cluster.
     */
    @Test
    void pinsTheProductionCheckpointInterval() {
        assertEquals(10_000L, TaxJob.CHECKPOINT_INTERVAL_MS);
    }
}
