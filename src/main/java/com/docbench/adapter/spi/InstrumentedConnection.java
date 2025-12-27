package com.docbench.adapter.spi;

import com.docbench.metrics.MetricsCollector;

/**
 * Connection wrapper providing timing hooks at protocol boundaries.
 * Implementations capture detailed timing for overhead decomposition.
 */
public interface InstrumentedConnection extends AutoCloseable {

    /**
     * Returns the underlying platform connection.
     *
     * @param connectionType the expected connection type
     * @param <T>            the connection type
     * @return the unwrapped connection
     * @throws ClassCastException if the connection is not of the expected type
     */
    <T> T unwrap(Class<T> connectionType);

    /**
     * Returns true if this connection is valid and usable.
     *
     * @return true if valid
     */
    boolean isValid();

    /**
     * Registers a listener for operation timing events.
     *
     * @param listener the timing listener
     */
    void addTimingListener(TimingListener listener);

    /**
     * Removes a timing listener.
     *
     * @param listener the listener to remove
     */
    void removeTimingListener(TimingListener listener);

    /**
     * Returns accumulated timing metrics since last reset.
     *
     * @return the timing metrics
     */
    ConnectionTimingMetrics getTimingMetrics();

    /**
     * Resets timing accumulators.
     */
    void resetTimingMetrics();

    /**
     * Returns the associated metrics collector.
     *
     * @return the metrics collector
     */
    MetricsCollector getMetricsCollector();

    /**
     * Returns the connection ID for correlation.
     *
     * @return the connection ID
     */
    String getConnectionId();

    /**
     * Closes this connection and releases resources.
     */
    @Override
    void close();
}
