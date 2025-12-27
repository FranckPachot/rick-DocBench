package com.docbench.adapter.spi;

/**
 * Accumulated connection timing metrics.
 */
public record ConnectionTimingMetrics(
        long serializationTimeNanos,
        long wireTransmitTimeNanos,
        long wireReceiveTimeNanos,
        long deserializationTimeNanos,
        long totalBytesSent,
        long totalBytesReceived,
        long operationCount
) {
    public static ConnectionTimingMetrics empty() {
        return new ConnectionTimingMetrics(0, 0, 0, 0, 0, 0, 0);
    }

    public ConnectionTimingMetrics add(ConnectionTimingMetrics other) {
        return new ConnectionTimingMetrics(
                serializationTimeNanos + other.serializationTimeNanos,
                wireTransmitTimeNanos + other.wireTransmitTimeNanos,
                wireReceiveTimeNanos + other.wireReceiveTimeNanos,
                deserializationTimeNanos + other.deserializationTimeNanos,
                totalBytesSent + other.totalBytesSent,
                totalBytesReceived + other.totalBytesReceived,
                operationCount + other.operationCount
        );
    }
}
