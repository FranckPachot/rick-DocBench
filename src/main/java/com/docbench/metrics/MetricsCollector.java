package com.docbench.metrics;

import com.docbench.util.TimeSource;
import org.HdrHistogram.Histogram;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Thread-safe metrics collector using HdrHistogram for high-precision latency tracking.
 * Captures detailed timing breakdowns for overhead decomposition analysis.
 */
public final class MetricsCollector {

    /**
     * Maximum recordable value: 1 hour in nanoseconds.
     */
    private static final long MAX_VALUE = TimeUnit.HOURS.toNanos(1);

    /**
     * Number of significant value digits for histogram precision.
     */
    private static final int SIGNIFICANT_DIGITS = 3;

    private final ConcurrentHashMap<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final TimeSource timeSource;

    /**
     * Creates a new MetricsCollector with the given time source.
     *
     * @param timeSource the time source for timing operations
     */
    public MetricsCollector(TimeSource timeSource) {
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
    }

    /**
     * Creates a new MetricsCollector with the system time source.
     */
    public MetricsCollector() {
        this(TimeSource.system());
    }

    /**
     * Records a timing measurement with nanosecond precision.
     *
     * @param metricName the metric name
     * @param duration   the duration to record
     */
    public void recordTiming(String metricName, Duration duration) {
        Objects.requireNonNull(metricName, "metricName must not be null");
        Objects.requireNonNull(duration, "duration must not be null");

        long nanos = duration.toNanos();
        if (nanos < 0) {
            nanos = 0;
        }
        if (nanos > MAX_VALUE) {
            nanos = MAX_VALUE;
        }

        Histogram histogram = histograms.computeIfAbsent(metricName, k -> createHistogram());
        synchronized (histogram) {
            histogram.recordValue(nanos);
        }
    }

    /**
     * Records a complete overhead breakdown from a single operation.
     * This decomposes the operation into all tracked timing components.
     *
     * @param breakdown the overhead breakdown to record
     */
    public void recordOverheadBreakdown(OverheadBreakdown breakdown) {
        Objects.requireNonNull(breakdown, "breakdown must not be null");

        // Record individual components
        recordTiming("total_latency", breakdown.totalLatency());
        recordTiming("connection_acquisition", breakdown.connectionAcquisition());
        recordTiming("connection_release", breakdown.connectionRelease());
        recordTiming("serialization", breakdown.serializationTime());
        recordTiming("wire_transmit", breakdown.wireTransmitTime());
        recordTiming("server_execution", breakdown.serverExecutionTime());
        recordTiming("server_parse", breakdown.serverParseTime());
        recordTiming("server_traversal", breakdown.serverTraversalTime());
        recordTiming("server_index", breakdown.serverIndexTime());
        recordTiming("server_fetch", breakdown.serverFetchTime());
        recordTiming("wire_receive", breakdown.wireReceiveTime());
        recordTiming("deserialization", breakdown.deserializationTime());
        recordTiming("client_traversal", breakdown.clientTraversalTime());

        // Record derived metrics
        recordTiming("total_traversal", breakdown.traversalOverhead());
        recordTiming("total_overhead", breakdown.totalOverhead());
        recordTiming("network_overhead", breakdown.networkOverhead());
        recordTiming("serialization_overhead", breakdown.serializationOverhead());
        recordTiming("connection_overhead", breakdown.connectionOverhead());

        // Record platform-specific metrics
        breakdown.platformSpecific().forEach(this::recordTiming);
    }

    /**
     * Times an operation and records the duration.
     *
     * @param metricName the metric name
     * @param operation  the operation to time
     */
    public void timeOperation(String metricName, Runnable operation) {
        Objects.requireNonNull(metricName, "metricName must not be null");
        Objects.requireNonNull(operation, "operation must not be null");

        TimeSource.TimingContext context = timeSource.startTiming();
        try {
            operation.run();
        } finally {
            recordTiming(metricName, context.stop());
        }
    }

    /**
     * Times an operation and records the duration, returning the result.
     *
     * @param metricName the metric name
     * @param operation  the operation to time
     * @param <T>        the result type
     * @return the operation result
     */
    public <T> T timeOperation(String metricName, Supplier<T> operation) {
        Objects.requireNonNull(metricName, "metricName must not be null");
        Objects.requireNonNull(operation, "operation must not be null");

        TimeSource.TimingContext context = timeSource.startTiming();
        try {
            return operation.get();
        } finally {
            recordTiming(metricName, context.stop());
        }
    }

    /**
     * Increments a counter by 1.
     *
     * @param counterName the counter name
     */
    public void incrementCounter(String counterName) {
        Objects.requireNonNull(counterName, "counterName must not be null");
        counters.computeIfAbsent(counterName, k -> new LongAdder()).increment();
    }

    /**
     * Adds a value to a counter.
     *
     * @param counterName the counter name
     * @param value       the value to add
     */
    public void addCounter(String counterName, long value) {
        Objects.requireNonNull(counterName, "counterName must not be null");
        counters.computeIfAbsent(counterName, k -> new LongAdder()).add(value);
    }

    /**
     * Returns the current value of a counter.
     *
     * @param counterName the counter name
     * @return the counter value, or 0 if not found
     */
    public long getCounter(String counterName) {
        LongAdder adder = counters.get(counterName);
        return adder != null ? adder.sum() : 0L;
    }

    /**
     * Generates a statistical summary for all recorded metrics.
     *
     * @return the metrics summary
     */
    public MetricsSummary summarize() {
        Map<String, HistogramSummary> summaries = histograms.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> summarizeHistogram(e.getValue())
                ));

        Map<String, Long> counterValues = counters.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().sum()
                ));

        return new MetricsSummary(summaries, counterValues);
    }

    /**
     * Resets all collected metrics.
     */
    public void reset() {
        histograms.clear();
        counters.clear();
    }

    private Histogram createHistogram() {
        return new Histogram(MAX_VALUE, SIGNIFICANT_DIGITS);
    }

    private HistogramSummary summarizeHistogram(Histogram histogram) {
        synchronized (histogram) {
            return new HistogramSummary(
                    histogram.getTotalCount(),
                    histogram.getMean(),
                    histogram.getValueAtPercentile(50),
                    histogram.getValueAtPercentile(90),
                    histogram.getValueAtPercentile(95),
                    histogram.getValueAtPercentile(99),
                    histogram.getValueAtPercentile(99.9),
                    histogram.getMinValue(),
                    histogram.getMaxValue(),
                    histogram.getStdDeviation()
            );
        }
    }
}
