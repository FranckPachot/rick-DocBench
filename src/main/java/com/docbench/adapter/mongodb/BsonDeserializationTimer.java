package com.docbench.adapter.mongodb;

import com.docbench.metrics.MetricsCollector;
import com.docbench.util.TimeSource;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Timer for BSON deserialization operations.
 * Captures field-level traversal timing for overhead decomposition analysis.
 *
 * <p>BSON traversal characteristics measured:
 * <ul>
 *   <li>O(n) sequential field-name scanning at each document level</li>
 *   <li>Nested document access time</li>
 *   <li>Array element access patterns</li>
 * </ul>
 */
public class BsonDeserializationTimer {

    private final MetricsCollector collector;
    private final TimeSource timeSource;
    private final Map<String, OperationContext> operations = new ConcurrentHashMap<>();

    public BsonDeserializationTimer(MetricsCollector collector, TimeSource timeSource) {
        this.collector = Objects.requireNonNull(collector, "collector must not be null");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
    }

    /**
     * Starts timing deserialization for an operation.
     */
    public void startDeserialization(String operationId) {
        operations.put(operationId, new OperationContext(timeSource.nanoTime()));
    }

    /**
     * Records a field access during deserialization.
     */
    public void recordFieldAccess(String operationId, String fieldName) {
        recordFieldAccess(operationId, fieldName, -1);
    }

    /**
     * Records a field access with its position in the document.
     */
    public void recordFieldAccess(String operationId, String fieldName, int position) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        long now = timeSource.nanoTime();
        ctx.recordFieldAccess(fieldName, position, now);

        // Record individual field timing
        collector.recordTiming("bson.field_access." + fieldName,
                Duration.ofNanos(now - ctx.lastEventTime));
        ctx.lastEventTime = now;
    }

    /**
     * Enters a nested document.
     */
    public void enterNestedDocument(String operationId, String fieldName) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        ctx.enterNested();
        ctx.lastEventTime = timeSource.nanoTime();
    }

    /**
     * Exits a nested document.
     */
    public void exitNestedDocument(String operationId) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        ctx.exitNested();
        ctx.lastEventTime = timeSource.nanoTime();
    }

    /**
     * Enters an array.
     */
    public void enterArray(String operationId, String arrayName) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        ctx.enterArray(arrayName);
        ctx.lastEventTime = timeSource.nanoTime();
    }

    /**
     * Exits an array.
     */
    public void exitArray(String operationId) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        ctx.exitArray();
        ctx.lastEventTime = timeSource.nanoTime();
    }

    /**
     * Records an array element access.
     */
    public void recordArrayElementAccess(String operationId, String arrayName, int index) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        ctx.recordArrayElement(arrayName, index);
        ctx.lastEventTime = timeSource.nanoTime();
    }

    /**
     * Ends deserialization timing and records metrics.
     */
    public void endDeserialization(String operationId) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return;

        long endTime = timeSource.nanoTime();
        ctx.endTime = endTime;

        // Record summary metrics
        Duration total = Duration.ofNanos(endTime - ctx.startTime);
        collector.recordTiming("bson.deserialization.total", total);
        collector.addCounter("bson.deserialization.field_count", ctx.fieldAccessCount.get());
    }

    /**
     * Returns the number of field accesses for an operation.
     */
    public int getFieldAccessCount(String operationId) {
        OperationContext ctx = operations.get(operationId);
        return ctx != null ? ctx.fieldAccessCount.get() : 0;
    }

    /**
     * Returns the position of a field in the document.
     */
    public int getFieldPosition(String operationId, String fieldName) {
        OperationContext ctx = operations.get(operationId);
        return ctx != null ? ctx.fieldPositions.getOrDefault(fieldName, -1) : -1;
    }

    /**
     * Returns the maximum nesting depth reached.
     */
    public int getMaxNestingDepth(String operationId) {
        OperationContext ctx = operations.get(operationId);
        return ctx != null ? ctx.maxNestingDepth : 0;
    }

    /**
     * Returns the number of elements accessed in an array.
     */
    public int getArrayElementCount(String operationId, String arrayName) {
        OperationContext ctx = operations.get(operationId);
        return ctx != null ? ctx.arrayElementCounts.getOrDefault(arrayName, 0) : 0;
    }

    /**
     * Returns the total deserialization time for an operation.
     */
    public Duration getTotalDeserializationTime(String operationId) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null || ctx.endTime == 0) return Duration.ZERO;
        return Duration.ofNanos(ctx.endTime - ctx.startTime);
    }

    /**
     * Returns a detailed breakdown of deserialization timing.
     */
    public DeserializationBreakdown getDeserializationBreakdown(String operationId) {
        OperationContext ctx = operations.get(operationId);
        if (ctx == null) return null;

        return new DeserializationBreakdown(
                getTotalDeserializationTime(operationId),
                ctx.fieldAccessCount.get(),
                ctx.maxNestingDepth,
                ctx.lastFieldPosition,
                ctx.totalArrayElements.get()
        );
    }

    /**
     * Clears timing data for an operation.
     */
    public void clear(String operationId) {
        operations.remove(operationId);
    }

    /**
     * Breakdown of deserialization timing.
     */
    public record DeserializationBreakdown(
            Duration totalTime,
            int fieldCount,
            int maxNestingDepth,
            int lastFieldPosition,
            int totalArrayElements
    ) {}

    /**
     * Context for tracking a single deserialization operation.
     */
    private static class OperationContext {
        final long startTime;
        long lastEventTime;
        long endTime;
        final AtomicInteger fieldAccessCount = new AtomicInteger(0);
        final Map<String, Integer> fieldPositions = new ConcurrentHashMap<>();
        final Map<String, Integer> arrayElementCounts = new ConcurrentHashMap<>();
        final AtomicInteger totalArrayElements = new AtomicInteger(0);
        int currentNestingDepth = 0;
        int maxNestingDepth = 0;
        int lastFieldPosition = -1;
        String currentArray = null;

        OperationContext(long startTime) {
            this.startTime = startTime;
            this.lastEventTime = startTime;
        }

        void recordFieldAccess(String fieldName, int position, long timestamp) {
            fieldAccessCount.incrementAndGet();
            if (position >= 0) {
                fieldPositions.put(fieldName, position);
                lastFieldPosition = Math.max(lastFieldPosition, position);
            }
        }

        void enterNested() {
            currentNestingDepth++;
            maxNestingDepth = Math.max(maxNestingDepth, currentNestingDepth);
        }

        void exitNested() {
            if (currentNestingDepth > 0) {
                currentNestingDepth--;
            }
        }

        void enterArray(String arrayName) {
            currentArray = arrayName;
            arrayElementCounts.putIfAbsent(arrayName, 0);
        }

        void exitArray() {
            currentArray = null;
        }

        void recordArrayElement(String arrayName, int index) {
            arrayElementCounts.merge(arrayName, 1, Integer::sum);
            totalArrayElements.incrementAndGet();
        }
    }
}
