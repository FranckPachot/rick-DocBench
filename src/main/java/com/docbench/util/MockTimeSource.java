package com.docbench.util;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mock time source for testing.
 * Allows controlled time progression.
 */
public final class MockTimeSource implements TimeSource {
    private final AtomicLong nanoTime;
    private final AtomicReference<Instant> instant;

    public MockTimeSource(long initialNanos, Instant initialInstant) {
        this.nanoTime = new AtomicLong(initialNanos);
        this.instant = new AtomicReference<>(initialInstant);
    }

    @Override
    public long nanoTime() {
        return nanoTime.get();
    }

    @Override
    public Instant now() {
        return instant.get();
    }

    /**
     * Advances time by the specified duration.
     */
    public void advance(Duration duration) {
        nanoTime.addAndGet(duration.toNanos());
        instant.updateAndGet(i -> i.plus(duration));
    }

    /**
     * Sets the nano time to a specific value.
     */
    public void setNanoTime(long nanos) {
        nanoTime.set(nanos);
    }

    /**
     * Sets the instant to a specific value.
     */
    public void setInstant(Instant instant) {
        this.instant.set(instant);
    }
}
