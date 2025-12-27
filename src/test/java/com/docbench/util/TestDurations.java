package com.docbench.util;

import java.time.Duration;

/**
 * Utility class for creating Duration instances in tests.
 * Java's Duration doesn't have ofMicros, so we provide it here.
 */
public final class TestDurations {

    private TestDurations() {}

    /**
     * Creates a Duration from microseconds.
     */
    public static Duration micros(long microseconds) {
        return Duration.ofNanos(microseconds * 1000L);
    }

    /**
     * Creates a Duration from milliseconds (alias for clarity).
     */
    public static Duration millis(long milliseconds) {
        return Duration.ofMillis(milliseconds);
    }
}
