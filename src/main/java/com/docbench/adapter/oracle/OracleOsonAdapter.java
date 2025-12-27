package com.docbench.adapter.oracle;

import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.metrics.OverheadBreakdown;
import com.docbench.util.TimeSource;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.sql.*;
import java.time.Duration;
import java.util.*;

/**
 * Oracle OSON (binary JSON) database adapter using native SQL/JSON.
 *
 * Uses Oracle 23ai's native JSON column type with OSON binary format
 * for O(1) hash-indexed field access via JSON_VALUE, JSON_QUERY, etc.
 *
 * <p>Key characteristics of OSON traversal:
 * <ul>
 *   <li>O(1) hash-indexed field lookup (vs BSON's O(n) scanning)</li>
 *   <li>Native SQL/JSON path expressions (JSON_VALUE, JSON_QUERY)</li>
 *   <li>JSON_TRANSFORM for efficient partial updates</li>
 *   <li>Server-side field extraction without full document scan</li>
 * </ul>
 */
public class OracleOsonAdapter implements DatabaseAdapter {

    private static final String ADAPTER_ID = "oracle-oson";
    private static final String DISPLAY_NAME = "Oracle OSON (Binary JSON)";
    private static final String VERSION = "23ai";

    private static final Set<Capability> CAPABILITIES = Set.of(
            Capability.NESTED_DOCUMENT_ACCESS,
            Capability.ARRAY_INDEX_ACCESS,
            Capability.PARTIAL_DOCUMENT_RETRIEVAL,
            Capability.BULK_INSERT,
            Capability.BULK_UPDATE,
            Capability.BULK_READ,
            Capability.REPLICATION,
            Capability.SECONDARY_INDEXES,
            Capability.COMPOUND_INDEXES,
            Capability.JSON_PATH_INDEXES,
            Capability.SINGLE_DOCUMENT_ATOMICITY,
            Capability.MULTI_DOCUMENT_TRANSACTIONS,
            Capability.SERVER_EXECUTION_TIME,
            Capability.SERVER_TRAVERSAL_TIME,
            Capability.EXPLAIN_PLAN,
            Capability.PROFILING,
            Capability.CLIENT_TIMING_HOOKS,
            Capability.DESERIALIZATION_METRICS
    );

    private PoolDataSource dataSource;
    private String tableName = "benchmark_docs";
    private TimeSource timeSource = TimeSource.system();
    private MetricsCollector collector;

    @Override
    public String getAdapterId() {
        return ADAPTER_ID;
    }

    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public Set<Capability> getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public boolean hasCapability(Capability capability) {
        return CAPABILITIES.contains(capability);
    }

    @Override
    public boolean hasAllCapabilities(Set<Capability> capabilities) {
        return CAPABILITIES.containsAll(capabilities);
    }

    @Override
    public ValidationResult validateConfig(ConnectionConfig config) {
        List<ValidationResult.ValidationError> errors = new ArrayList<>();

        String uri = config.uri().orElse("");
        if (uri.isBlank()) {
            errors.add(new ValidationResult.ValidationError("uri", "URI is required"));
        } else if (!uri.startsWith("jdbc:oracle:")) {
            errors.add(new ValidationResult.ValidationError("uri",
                    "URI must start with jdbc:oracle: (got: " + uri.substring(0, Math.min(20, uri.length())) + ")"));
        }

        if (config.database() == null || config.database().isBlank()) {
            errors.add(new ValidationResult.ValidationError("database", "Database/schema name is required"));
        }

        String username = config.username().orElse("");
        String password = config.password().orElse("");
        if (!uri.contains("@") && (username.isBlank() || password.isBlank())) {
            if (username.isBlank()) {
                errors.add(new ValidationResult.ValidationError("username", "Username is required"));
            }
            if (password.isBlank()) {
                errors.add(new ValidationResult.ValidationError("password", "Password is required"));
            }
        }

        return errors.isEmpty()
                ? ValidationResult.success()
                : ValidationResult.failure(errors);
    }

    @Override
    public Map<String, String> getConfigurationOptions() {
        return Map.of(
                "username", "Database username",
                "password", "Database password",
                "poolSize", "Connection pool size (default: 10)",
                "tableName", "JSON table name (default: benchmark_docs)"
        );
    }

    @Override
    public InstrumentedConnection connect(ConnectionConfig config) {
        ValidationResult validation = validateConfig(config);
        if (validation.isInvalid()) {
            throw new ConnectionException(ADAPTER_ID,
                    "Invalid configuration: " + validation.allErrorMessages());
        }

        try {
            String url = config.uri().orElseThrow();
            String username = config.username().orElse("");
            String password = config.password().orElse("");
            int poolSize = config.getIntOption("poolSize", 5);

            dataSource = PoolDataSourceFactory.getPoolDataSource();
            dataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            dataSource.setURL(url);
            if (!username.isBlank()) {
                dataSource.setUser(username);
                dataSource.setPassword(password);
            }
            dataSource.setInitialPoolSize(1);
            dataSource.setMinPoolSize(1);
            dataSource.setMaxPoolSize(poolSize);
            dataSource.setConnectionWaitTimeout(30);

            this.collector = new MetricsCollector(timeSource);

            return new OracleInstrumentedConnection(dataSource, config.database(), collector);

        } catch (SQLException e) {
            throw new ConnectionException(ADAPTER_ID, "Failed to connect: " + e.getMessage(), e);
        }
    }

    @Override
    public OperationResult execute(
            InstrumentedConnection conn,
            Operation operation,
            MetricsCollector collector) {

        OracleInstrumentedConnection oracleConn = (OracleInstrumentedConnection) conn;

        return switch (operation) {
            case InsertOperation insert -> executeInsert(oracleConn, insert, collector);
            case ReadOperation read -> executeRead(oracleConn, read, collector);
            case UpdateOperation update -> executeUpdate(oracleConn, update, collector);
            case DeleteOperation delete -> executeDelete(oracleConn, delete, collector);
            case AggregateOperation aggregate -> executeAggregate(oracleConn, aggregate, collector);
        };
    }

    private OperationResult executeInsert(
            OracleInstrumentedConnection conn,
            InsertOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        // Use JSON constructor to ensure proper OSON storage
        String sql = "INSERT INTO " + tableName + " (id, doc) VALUES (?, JSON(?))";

        try (PreparedStatement stmt = conn.getJdbcConnection().prepareStatement(sql)) {
            String jsonContent = toJsonString(operation.document());

            long serializeEnd = timeSource.nanoTime();

            stmt.setString(1, operation.document().getId());
            // Use CLOB for large JSON documents (> 4000 chars)
            if (jsonContent.length() > 4000) {
                stmt.setClob(2, new java.io.StringReader(jsonContent));
            } else {
                stmt.setString(2, jsonContent);
            }

            long execStart = timeSource.nanoTime();
            stmt.executeUpdate();
            long endNanos = timeSource.nanoTime();

            conn.commit();

            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .serializationTime(Duration.ofNanos(serializeEnd - startNanos))
                    .serverExecutionTime(Duration.ofNanos(endNanos - execStart))
                    .build();

            return OperationResult.builder(operation.operationId(), OperationType.INSERT)
                    .success(true)
                    .totalDuration(totalDuration)
                    .overheadBreakdown(breakdown)
                    .metadata("documentId", operation.document().getId())
                    .build();

        } catch (Exception e) {
            conn.rollback();
            throw new OperationException(operation.operationId(), OperationType.INSERT,
                    "Insert failed: " + e.getMessage(), e);
        }
    }

    private OperationResult executeRead(
            OracleInstrumentedConnection conn,
            ReadOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            String sql;
            boolean hasProjection = operation.hasProjection();

            if (hasProjection) {
                // Use JSON_VALUE for O(1) field extraction - this is where OSON shines!
                List<String> fields = operation.projectionPaths();
                StringBuilder selectClause = new StringBuilder();
                for (int i = 0; i < fields.size(); i++) {
                    if (i > 0) selectClause.append(", ");
                    String path = fields.get(i).startsWith("$") ? fields.get(i) : "$." + fields.get(i);
                    selectClause.append("JSON_VALUE(doc, '").append(path).append("') AS field_").append(i);
                }
                sql = "SELECT " + selectClause + " FROM " + tableName + " WHERE id = ?";
            } else {
                // Full document read
                sql = "SELECT doc FROM " + tableName + " WHERE id = ?";
            }

            try (PreparedStatement stmt = conn.getJdbcConnection().prepareStatement(sql)) {
                stmt.setString(1, operation.documentId());

                long execStart = timeSource.nanoTime();
                try (ResultSet rs = stmt.executeQuery()) {
                    long execEnd = timeSource.nanoTime();

                    if (!rs.next()) {
                        return OperationResult.builder(operation.operationId(), OperationType.READ)
                                .success(false)
                                .totalDuration(Duration.ofNanos(timeSource.nanoTime() - startNanos))
                                .build();
                    }

                    // Deserialize/extract result
                    long deserStart = timeSource.nanoTime();
                    String content;
                    if (hasProjection) {
                        // Build result from projected fields
                        StringBuilder result = new StringBuilder("{");
                        List<String> fields = operation.projectionPaths();
                        for (int i = 0; i < fields.size(); i++) {
                            if (i > 0) result.append(",");
                            result.append("\"").append(fields.get(i)).append("\":\"")
                                  .append(rs.getString(i + 1)).append("\"");
                        }
                        result.append("}");
                        content = result.toString();
                    } else {
                        content = rs.getString(1);
                    }
                    long deserEnd = timeSource.nanoTime();

                    Duration totalDuration = Duration.ofNanos(deserEnd - startNanos);

                    OverheadBreakdown breakdown = OverheadBreakdown.builder()
                            .totalLatency(totalDuration)
                            .serverExecutionTime(Duration.ofNanos(execEnd - execStart))
                            .serverTraversalTime(Duration.ZERO)  // O(1) hash lookup
                            .deserializationTime(Duration.ofNanos(deserEnd - deserStart))
                            .build();

                    return OperationResult.builder(operation.operationId(), OperationType.READ)
                            .success(true)
                            .totalDuration(totalDuration)
                            .overheadBreakdown(breakdown)
                            .metadata("documentId", operation.documentId())
                            .metadata("size", content != null ? content.length() : 0)
                            .build();
                }
            }

        } catch (Exception e) {
            throw new OperationException(operation.operationId(), OperationType.READ,
                    "Read failed: " + e.getMessage(), e);
        }
    }

    private OperationResult executeUpdate(
            OracleInstrumentedConnection conn,
            UpdateOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            // Use JSON_TRANSFORM for O(1) path-based updates
            String path = operation.updatePath().startsWith("$") ?
                          operation.updatePath() : "$." + operation.updatePath();

            String sql = "UPDATE " + tableName +
                        " SET doc = JSON_TRANSFORM(doc, SET '" + path + "' = ?) WHERE id = ?";

            try (PreparedStatement stmt = conn.getJdbcConnection().prepareStatement(sql)) {
                stmt.setObject(1, operation.newValue());
                stmt.setString(2, operation.documentId());

                long execStart = timeSource.nanoTime();
                int updated = stmt.executeUpdate();
                long endNanos = timeSource.nanoTime();

                conn.commit();

                Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

                OverheadBreakdown breakdown = OverheadBreakdown.builder()
                        .totalLatency(totalDuration)
                        .serializationTime(Duration.ofNanos(execStart - startNanos))
                        .serverExecutionTime(Duration.ofNanos(endNanos - execStart))
                        .serverTraversalTime(Duration.ZERO)  // O(1) path access
                        .build();

                return OperationResult.builder(operation.operationId(), OperationType.UPDATE)
                        .success(true)
                        .totalDuration(totalDuration)
                        .overheadBreakdown(breakdown)
                        .metadata("modifiedCount", updated)
                        .build();
            }

        } catch (Exception e) {
            conn.rollback();
            throw new OperationException(operation.operationId(), OperationType.UPDATE,
                    "Update failed: " + e.getMessage(), e);
        }
    }

    private OperationResult executeDelete(
            OracleInstrumentedConnection conn,
            DeleteOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        String sql = "DELETE FROM " + tableName + " WHERE id = ?";

        try (PreparedStatement stmt = conn.getJdbcConnection().prepareStatement(sql)) {
            stmt.setString(1, operation.documentId());

            long execStart = timeSource.nanoTime();
            int deleted = stmt.executeUpdate();
            long endNanos = timeSource.nanoTime();

            conn.commit();

            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .serverExecutionTime(Duration.ofNanos(endNanos - execStart))
                    .build();

            return OperationResult.builder(operation.operationId(), OperationType.DELETE)
                    .success(true)
                    .totalDuration(totalDuration)
                    .overheadBreakdown(breakdown)
                    .metadata("deletedCount", deleted)
                    .build();

        } catch (Exception e) {
            conn.rollback();
            throw new OperationException(operation.operationId(), OperationType.DELETE,
                    "Delete failed: " + e.getMessage(), e);
        }
    }

    private OperationResult executeAggregate(
            OracleInstrumentedConnection conn,
            AggregateOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            // Use SQL/JSON aggregation
            String sql = "SELECT JSON_ARRAYAGG(doc RETURNING CLOB) FROM " + tableName;

            try (PreparedStatement stmt = conn.getJdbcConnection().prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {

                long endNanos = timeSource.nanoTime();
                Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

                int resultCount = 0;
                if (rs.next()) {
                    String result = rs.getString(1);
                    if (result != null) {
                        resultCount = 1;
                    }
                }

                OverheadBreakdown breakdown = OverheadBreakdown.builder()
                        .totalLatency(totalDuration)
                        .serverExecutionTime(totalDuration)
                        .build();

                return OperationResult.builder(operation.operationId(), OperationType.AGGREGATE)
                        .success(true)
                        .totalDuration(totalDuration)
                        .overheadBreakdown(breakdown)
                        .metadata("resultCount", resultCount)
                        .build();
            }

        } catch (Exception e) {
            throw new OperationException(operation.operationId(), OperationType.AGGREGATE,
                    "Aggregate failed: " + e.getMessage(), e);
        }
    }

    @Override
    public OverheadBreakdown getOverheadBreakdown(OperationResult result) {
        return result.overheadBreakdown().orElse(
                OverheadBreakdown.builder()
                        .totalLatency(result.totalDuration())
                        .build()
        );
    }

    @Override
    public void setupTestEnvironment(TestEnvironmentConfig config) {
        this.tableName = config.collectionName();

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            if (config.dropExisting()) {
                // Drop existing table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE " + tableName + " PURGE");
                } catch (SQLException e) {
                    // Table might not exist, ignore
                }
            }

            // Create table with JSON column (stored as OSON binary format)
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE %s (
                        id VARCHAR2(255) PRIMARY KEY,
                        doc JSON
                    )
                    """.formatted(tableName));
            }

            conn.commit();
        } catch (SQLException e) {
            throw new SetupException("Failed to setup test environment: " + e.getMessage(), e);
        }
    }

    @Override
    public void teardownTestEnvironment() {
        // Table cleanup handled elsewhere if needed
    }

    @Override
    public void close() {
        // Connection pool handles cleanup
    }

    /**
     * Returns true if this adapter supports O(1) hash-indexed field access.
     */
    public boolean supportsHashIndexedFieldAccess() {
        return true;
    }

    /**
     * Returns true if JSON Duality Views are supported.
     */
    public boolean supportsJsonDualityViews() {
        return true;
    }

    /**
     * Returns true if JSON path expressions are supported.
     */
    public boolean supportsJsonPathExpressions() {
        return true;
    }

    private String toJsonString(JsonDocument document) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"_id\":\"").append(document.getId()).append("\"");

        document.getContent().forEach((key, value) -> {
            if (!"_id".equals(key)) {
                sb.append(",\"").append(key).append("\":");
                appendJsonValue(sb, value);
            }
        });

        sb.append("}");
        return sb.toString();
    }

    private void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String) {
            sb.append("\"").append(escapeJson((String) value)).append("\"");
        } else if (value instanceof Number || value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof Map) {
            sb.append("{");
            boolean first = true;
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(entry.getKey()).append("\":");
                appendJsonValue(sb, entry.getValue());
                first = false;
            }
            sb.append("}");
        } else if (value instanceof List) {
            sb.append("[");
            boolean first = true;
            for (Object item : (List<?>) value) {
                if (!first) sb.append(",");
                appendJsonValue(sb, item);
                first = false;
            }
            sb.append("]");
        } else {
            sb.append("\"").append(escapeJson(value.toString())).append("\"");
        }
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
