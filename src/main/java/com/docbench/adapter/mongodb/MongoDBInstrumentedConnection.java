package com.docbench.adapter.mongodb;

import com.docbench.adapter.spi.ConnectionTimingMetrics;
import com.docbench.adapter.spi.InstrumentedConnection;
import com.docbench.adapter.spi.TimingListener;
import com.docbench.metrics.MetricsCollector;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Instrumented connection wrapper for MongoDB.
 * Provides timing hooks and metrics collection for overhead analysis.
 */
public class MongoDBInstrumentedConnection implements InstrumentedConnection {

    private final String connectionId;
    private final MongoClient client;
    private final String databaseName;
    private final MetricsCollector metricsCollector;
    private final List<TimingListener> listeners;
    private final AtomicBoolean closed;

    // Timing accumulators
    private final AtomicLong serializationTimeNanos;
    private final AtomicLong wireTransmitTimeNanos;
    private final AtomicLong wireReceiveTimeNanos;
    private final AtomicLong deserializationTimeNanos;
    private final AtomicLong totalBytesSent;
    private final AtomicLong totalBytesReceived;
    private final AtomicLong operationCount;

    /**
     * Creates a new instrumented connection.
     *
     * @param client           the underlying MongoDB client
     * @param databaseName     the database name
     * @param metricsCollector the metrics collector
     */
    public MongoDBInstrumentedConnection(
            MongoClient client,
            String databaseName,
            MetricsCollector metricsCollector) {

        this.connectionId = UUID.randomUUID().toString();
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName must not be null");
        this.metricsCollector = Objects.requireNonNull(metricsCollector, "metricsCollector must not be null");
        this.listeners = new CopyOnWriteArrayList<>();
        this.closed = new AtomicBoolean(false);

        this.serializationTimeNanos = new AtomicLong(0);
        this.wireTransmitTimeNanos = new AtomicLong(0);
        this.wireReceiveTimeNanos = new AtomicLong(0);
        this.deserializationTimeNanos = new AtomicLong(0);
        this.totalBytesSent = new AtomicLong(0);
        this.totalBytesReceived = new AtomicLong(0);
        this.operationCount = new AtomicLong(0);
    }

    @Override
    public String getConnectionId() {
        return connectionId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> connectionType) {
        if (connectionType.isInstance(client)) {
            return (T) client;
        }
        throw new ClassCastException("Cannot unwrap to " + connectionType.getName());
    }

    @Override
    public boolean isValid() {
        return !closed.get();
    }

    @Override
    public void addTimingListener(TimingListener listener) {
        listeners.add(Objects.requireNonNull(listener));
    }

    @Override
    public void removeTimingListener(TimingListener listener) {
        listeners.remove(listener);
    }

    @Override
    public ConnectionTimingMetrics getTimingMetrics() {
        return new ConnectionTimingMetrics(
                serializationTimeNanos.get(),
                wireTransmitTimeNanos.get(),
                wireReceiveTimeNanos.get(),
                deserializationTimeNanos.get(),
                totalBytesSent.get(),
                totalBytesReceived.get(),
                operationCount.get()
        );
    }

    @Override
    public void resetTimingMetrics() {
        serializationTimeNanos.set(0);
        wireTransmitTimeNanos.set(0);
        wireReceiveTimeNanos.set(0);
        deserializationTimeNanos.set(0);
        totalBytesSent.set(0);
        totalBytesReceived.set(0);
        operationCount.set(0);
    }

    @Override
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            client.close();
        }
    }

    /**
     * Returns the MongoDB database for this connection.
     */
    public MongoDatabase getDatabase() {
        return client.getDatabase(databaseName);
    }

    /**
     * Returns the underlying MongoClient.
     */
    public MongoClient getClient() {
        return client;
    }

    // Methods for accumulating timing metrics

    void recordSerializationTime(long nanos) {
        serializationTimeNanos.addAndGet(nanos);
    }

    void recordWireTransmitTime(long nanos) {
        wireTransmitTimeNanos.addAndGet(nanos);
    }

    void recordWireReceiveTime(long nanos) {
        wireReceiveTimeNanos.addAndGet(nanos);
    }

    void recordDeserializationTime(long nanos) {
        deserializationTimeNanos.addAndGet(nanos);
    }

    void recordBytesSent(long bytes) {
        totalBytesSent.addAndGet(bytes);
    }

    void recordBytesReceived(long bytes) {
        totalBytesReceived.addAndGet(bytes);
    }

    void incrementOperationCount() {
        operationCount.incrementAndGet();
    }

    // Notify listeners

    void notifySerializationStart(String operationId) {
        for (TimingListener listener : listeners) {
            listener.onSerializationStart(operationId);
        }
    }

    void notifySerializationComplete(String operationId, int bytesSerialized) {
        for (TimingListener listener : listeners) {
            listener.onSerializationComplete(operationId, bytesSerialized);
        }
    }

    void notifyWireTransmitStart(String operationId) {
        for (TimingListener listener : listeners) {
            listener.onWireTransmitStart(operationId);
        }
    }

    void notifyWireTransmitComplete(String operationId, int bytesSent) {
        for (TimingListener listener : listeners) {
            listener.onWireTransmitComplete(operationId, bytesSent);
        }
    }

    void notifyWireReceiveStart(String operationId) {
        for (TimingListener listener : listeners) {
            listener.onWireReceiveStart(operationId);
        }
    }

    void notifyWireReceiveComplete(String operationId, int bytesReceived) {
        for (TimingListener listener : listeners) {
            listener.onWireReceiveComplete(operationId, bytesReceived);
        }
    }

    void notifyDeserializationStart(String operationId) {
        for (TimingListener listener : listeners) {
            listener.onDeserializationStart(operationId);
        }
    }

    void notifyDeserializationComplete(String operationId, int fieldsDeserialized) {
        for (TimingListener listener : listeners) {
            listener.onDeserializationComplete(operationId, fieldsDeserialized);
        }
    }
}
