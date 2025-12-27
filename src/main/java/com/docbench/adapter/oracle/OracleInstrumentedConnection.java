package com.docbench.adapter.oracle;

import com.docbench.adapter.spi.ConnectionTimingMetrics;
import com.docbench.adapter.spi.InstrumentedConnection;
import com.docbench.adapter.spi.TimingListener;
import com.docbench.metrics.MetricsCollector;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Instrumented connection wrapper for Oracle Database with SQL/JSON support.
 * Provides timing hooks and metrics collection for overhead analysis.
 *
 * <p>Key features:
 * <ul>
 *   <li>Native SQL/JSON operations via JDBC</li>
 *   <li>OSON binary format for O(1) field access</li>
 *   <li>Transaction management (commit/rollback)</li>
 *   <li>Connection pooling via UCP</li>
 * </ul>
 */
public class OracleInstrumentedConnection implements InstrumentedConnection {

    private final String connectionId;
    private final PoolDataSource dataSource;
    private final String schemaName;
    private final MetricsCollector metricsCollector;
    private final List<TimingListener> listeners;
    private final AtomicBoolean closed;

    // Active connection - lazily obtained from pool
    private Connection currentConnection;

    // Timing accumulators
    private final AtomicLong serializationTimeNanos;
    private final AtomicLong wireTransmitTimeNanos;
    private final AtomicLong wireReceiveTimeNanos;
    private final AtomicLong deserializationTimeNanos;
    private final AtomicLong totalBytesSent;
    private final AtomicLong totalBytesReceived;
    private final AtomicLong operationCount;

    /**
     * Creates a new instrumented Oracle connection.
     *
     * @param dataSource       the UCP connection pool
     * @param schemaName       the schema/database name
     * @param metricsCollector the metrics collector
     */
    public OracleInstrumentedConnection(
            PoolDataSource dataSource,
            String schemaName,
            MetricsCollector metricsCollector) {

        this.connectionId = UUID.randomUUID().toString();
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.schemaName = Objects.requireNonNull(schemaName, "schemaName must not be null");
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
        if (connectionType.isAssignableFrom(Connection.class)) {
            return (T) getJdbcConnection();
        }
        throw new ClassCastException("Cannot unwrap to " + connectionType.getName());
    }

    @Override
    public boolean isValid() {
        if (closed.get()) {
            return false;
        }
        try {
            if (currentConnection != null) {
                return currentConnection.isValid(5);
            }
            // Try to get a connection to validate
            Connection conn = dataSource.getConnection();
            boolean valid = conn.isValid(5);
            conn.close();
            return valid;
        } catch (SQLException e) {
            return false;
        }
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
            if (currentConnection != null) {
                try {
                    currentConnection.close();
                } catch (SQLException e) {
                    // Log and ignore
                }
                currentConnection = null;
            }
        }
    }

    /**
     * Returns the current JDBC connection, obtaining one from the pool if needed.
     * The connection is kept open for the lifetime of this instrumented connection
     * to support transaction management.
     *
     * @return the JDBC connection
     */
    public Connection getJdbcConnection() {
        if (closed.get()) {
            throw new IllegalStateException("Connection is closed");
        }
        try {
            if (currentConnection == null || currentConnection.isClosed()) {
                currentConnection = dataSource.getConnection();
                currentConnection.setAutoCommit(false);
            }
            return currentConnection;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get connection: " + e.getMessage(), e);
        }
    }

    /**
     * Commits the current transaction.
     */
    public void commit() {
        if (currentConnection != null) {
            try {
                currentConnection.commit();
            } catch (SQLException e) {
                throw new RuntimeException("Commit failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Rolls back the current transaction.
     */
    public void rollback() {
        if (currentConnection != null) {
            try {
                currentConnection.rollback();
            } catch (SQLException e) {
                // Log and ignore on rollback
            }
        }
    }

    /**
     * Returns the schema name.
     */
    public String getSchemaName() {
        return schemaName;
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
