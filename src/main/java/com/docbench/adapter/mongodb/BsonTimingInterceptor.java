package com.docbench.adapter.mongodb;

import com.docbench.metrics.MetricsCollector;
import com.docbench.util.TimeSource;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MongoDB CommandListener implementation that captures timing metrics.
 * Used for overhead decomposition analysis.
 *
 * <p>Captures:
 * <ul>
 *   <li>Client round-trip time (total client-side latency)</li>
 *   <li>Server execution time (as reported by MongoDB)</li>
 *   <li>Overhead (client round-trip minus server execution)</li>
 * </ul>
 */
public class BsonTimingInterceptor implements CommandListener {

    private final MetricsCollector collector;
    private final TimeSource timeSource;

    // Track pending commands by request ID
    private final Map<Integer, CommandTiming> pendingCommands;

    // Statistics
    private final AtomicLong completedOperations;
    private final AtomicLong failedOperations;

    /**
     * Creates a new timing interceptor.
     *
     * @param collector  the metrics collector
     * @param timeSource the time source
     */
    public BsonTimingInterceptor(MetricsCollector collector, TimeSource timeSource) {
        this.collector = Objects.requireNonNull(collector, "collector must not be null");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
        this.pendingCommands = new ConcurrentHashMap<>();
        this.completedOperations = new AtomicLong(0);
        this.failedOperations = new AtomicLong(0);
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
        CommandTiming timing = new CommandTiming(
                event.getRequestId(),
                event.getCommandName(),
                timeSource.nanoTime()
        );
        pendingCommands.put(event.getRequestId(), timing);
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        CommandTiming timing = pendingCommands.remove(event.getRequestId());

        if (timing == null) {
            // Orphaned event - command started before interceptor was attached
            return;
        }

        long endNanos = timeSource.nanoTime();
        long clientRoundTripNanos = endNanos - timing.startNanos();
        long serverExecutionNanos = event.getElapsedTime(java.util.concurrent.TimeUnit.NANOSECONDS);
        long overheadNanos = clientRoundTripNanos - serverExecutionNanos;

        // Record general metrics
        collector.recordTiming("mongodb.client_round_trip", Duration.ofNanos(clientRoundTripNanos));
        collector.recordTiming("mongodb.server_execution", Duration.ofNanos(serverExecutionNanos));
        collector.recordTiming("mongodb.overhead", Duration.ofNanos(Math.max(0, overheadNanos)));

        // Record command-specific metrics
        String commandName = timing.commandName();
        collector.recordTiming("mongodb." + commandName + ".client_round_trip",
                Duration.ofNanos(clientRoundTripNanos));
        collector.recordTiming("mongodb." + commandName + ".server_execution",
                Duration.ofNanos(serverExecutionNanos));

        completedOperations.incrementAndGet();
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        CommandTiming timing = pendingCommands.remove(event.getRequestId());

        if (timing != null) {
            long endNanos = timeSource.nanoTime();
            long clientRoundTripNanos = endNanos - timing.startNanos();

            collector.recordTiming("mongodb.failed.client_round_trip",
                    Duration.ofNanos(clientRoundTripNanos));
        }

        failedOperations.incrementAndGet();
    }

    /**
     * Returns the number of completed operations.
     */
    public long getCompletedOperationCount() {
        return completedOperations.get();
    }

    /**
     * Returns the number of failed operations.
     */
    public long getFailedOperationCount() {
        return failedOperations.get();
    }

    /**
     * Returns the total number of operations (completed + failed).
     */
    public long getTotalOperationCount() {
        return completedOperations.get() + failedOperations.get();
    }

    /**
     * Returns the number of pending (in-flight) commands.
     */
    public int getPendingCommandCount() {
        return pendingCommands.size();
    }

    /**
     * Resets all statistics and clears pending commands.
     */
    public void reset() {
        pendingCommands.clear();
        completedOperations.set(0);
        failedOperations.set(0);
    }

    /**
     * Internal record for tracking command timing.
     */
    private record CommandTiming(
            int requestId,
            String commandName,
            long startNanos
    ) {
    }
}
