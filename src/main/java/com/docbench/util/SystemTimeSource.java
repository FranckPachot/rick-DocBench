package com.docbench.util;

import java.time.Instant;

/**
 * System time source using actual system clocks.
 */
public final class SystemTimeSource implements TimeSource {
    static final SystemTimeSource INSTANCE = new SystemTimeSource();

    private SystemTimeSource() {}

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }

    @Override
    public Instant now() {
        return Instant.now();
    }
}
