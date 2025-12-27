package com.docbench.util;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstraction over time for testability.
 * Enables deterministic testing of time-dependent code.
 */
public sealed interface TimeSource permits SystemTimeSource, MockTimeSource {

    /**
     * Returns the current value of the running JVM's high-resolution time source, in nanoseconds.
     */
    long nanoTime();

    /**
     * Returns the current instant.
     */
    Instant now();

    /**
     * Calculates the duration between two nano timestamps.
     */
    default Duration elapsed(long startNanos, long endNanos) {
        return Duration.ofNanos(endNanos - startNanos);
    }

    /**
     * Starts a timing context for measuring elapsed time.
     */
    default TimingContext startTiming() {
        return new TimingContext(this);
    }

    /**
     * Returns the system time source.
     */
    static TimeSource system() {
        return SystemTimeSource.INSTANCE;
    }

    /**
     * Returns a mock time source starting at the specified nano time.
     */
    static MockTimeSource mock(long initialNanos) {
        return new MockTimeSource(initialNanos, Instant.now());
    }

    /**
     * Returns a mock time source starting at the specified instant.
     */
    static MockTimeSource mockAt(Instant instant) {
        return new MockTimeSource(0L, instant);
    }

    /**
     * Context for measuring elapsed time.
     * Thread-safe and reusable.
     */
    final class TimingContext {
        private final TimeSource timeSource;
        private final long startNanos;
        private final AtomicReference<Duration> stoppedDuration = new AtomicReference<>();

        TimingContext(TimeSource timeSource) {
            this.timeSource = Objects.requireNonNull(timeSource);
            this.startNanos = timeSource.nanoTime();
        }

        /**
         * Stops timing and returns the elapsed duration.
         * Subsequent calls return the same duration.
         */
        public Duration stop() {
            return stoppedDuration.updateAndGet(existing -> {
                if (existing != null) {
                    return existing;
                }
                return timeSource.elapsed(startNanos, timeSource.nanoTime());
            });
        }

        /**
         * Returns the elapsed duration without stopping.
         */
        public Duration elapsed() {
            Duration stopped = stoppedDuration.get();
            if (stopped != null) {
                return stopped;
            }
            return timeSource.elapsed(startNanos, timeSource.nanoTime());
        }
    }
}
