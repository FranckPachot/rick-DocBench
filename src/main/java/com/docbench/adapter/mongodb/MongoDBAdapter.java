package com.docbench.adapter.mongodb;

import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.metrics.OverheadBreakdown;
import com.docbench.util.TimeSource;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB database adapter implementation.
 * Provides BSON-based document operations with overhead timing instrumentation.
 *
 * <p>Key characteristics of BSON traversal:
 * <ul>
 *   <li>O(n) sequential field-name scanning at each document level</li>
 *   <li>Length-prefixed for sub-document skipping</li>
 *   <li>Field position impacts traversal time</li>
 * </ul>
 */
public class MongoDBAdapter implements DatabaseAdapter {

    private static final String ADAPTER_ID = "mongodb";
    private static final String DISPLAY_NAME = "MongoDB";
    private static final String VERSION = "1.0.0";

    private static final Set<Capability> CAPABILITIES = Set.of(
            // Document access patterns
            Capability.NESTED_DOCUMENT_ACCESS,
            Capability.ARRAY_INDEX_ACCESS,
            Capability.PARTIAL_DOCUMENT_RETRIEVAL,
            Capability.WILDCARD_PATH_ACCESS,

            // Operations
            Capability.BULK_INSERT,
            Capability.BULK_UPDATE,
            Capability.BULK_READ,

            // Topology
            Capability.SHARDING,
            Capability.REPLICATION,

            // Indexing
            Capability.SECONDARY_INDEXES,
            Capability.COMPOUND_INDEXES,
            Capability.JSON_PATH_INDEXES,

            // Transactions
            Capability.SINGLE_DOCUMENT_ATOMICITY,
            Capability.MULTI_DOCUMENT_TRANSACTIONS,

            // Instrumentation
            Capability.SERVER_EXECUTION_TIME,
            Capability.EXPLAIN_PLAN,
            Capability.PROFILING,
            Capability.CLIENT_TIMING_HOOKS,
            Capability.DESERIALIZATION_METRICS
            // Note: SERVER_TRAVERSAL_TIME not included - BSON doesn't expose this
    );

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private BsonTimingInterceptor timingInterceptor;
    private String collectionName = "benchmark_docs";
    private TimeSource timeSource = TimeSource.system();

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
    public ValidationResult validateConfig(ConnectionConfig config) {
        List<ValidationResult.ValidationError> errors = new ArrayList<>();

        Optional<String> uriOpt = config.uri();
        if (uriOpt.isEmpty() || uriOpt.get().isEmpty()) {
            errors.add(new ValidationResult.ValidationError("uri", "MongoDB URI is required"));
            return ValidationResult.failure(errors);
        }

        String uri = uriOpt.get();
        if (!uri.startsWith("mongodb://") && !uri.startsWith("mongodb+srv://")) {
            errors.add(new ValidationResult.ValidationError("uri",
                    "URI must start with mongodb:// or mongodb+srv://"));
            return ValidationResult.failure(errors);
        }

        return errors.isEmpty() ? ValidationResult.success() : ValidationResult.failure(errors);
    }

    @Override
    public Map<String, String> getConfigurationOptions() {
        return Map.of(
                "maxPoolSize", "Maximum connection pool size (default: 100)",
                "minPoolSize", "Minimum connection pool size (default: 0)",
                "connectTimeoutMs", "Connection timeout in milliseconds (default: 10000)",
                "readPreference", "Read preference: primary, secondary, nearest (default: primary)",
                "writeConcern", "Write concern: w1, majority, etc. (default: majority)"
        );
    }

    @Override
    public InstrumentedConnection connect(ConnectionConfig config) {
        ValidationResult validation = validateConfig(config);
        if (validation.isInvalid()) {
            throw new ConnectionException(ADAPTER_ID,
                    "Invalid configuration: " + validation.allErrorMessages());
        }

        String uri = config.uri().orElseThrow();
        String dbName = config.database();

        MetricsCollector collector = new MetricsCollector(timeSource);
        timingInterceptor = new BsonTimingInterceptor(collector, timeSource);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(config.getIntOption("maxPoolSize", 100))
                        .minSize(config.getIntOption("minPoolSize", 0)))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(config.getIntOption("connectTimeoutMs", 10000),
                                TimeUnit.MILLISECONDS))
                .addCommandListener(timingInterceptor)
                .build();

        client = MongoClients.create(settings);
        database = client.getDatabase(dbName);

        return new MongoDBInstrumentedConnection(client, dbName, collector);
    }

    @Override
    public OperationResult execute(
            InstrumentedConnection conn,
            Operation operation,
            MetricsCollector collector) {

        MongoDBInstrumentedConnection mongoConn = (MongoDBInstrumentedConnection) conn;

        return switch (operation) {
            case InsertOperation insert -> executeInsert(mongoConn, insert, collector);
            case ReadOperation read -> executeRead(mongoConn, read, collector);
            case UpdateOperation update -> executeUpdate(mongoConn, update, collector);
            case DeleteOperation delete -> executeDelete(mongoConn, delete, collector);
            case AggregateOperation aggregate -> executeAggregate(mongoConn, aggregate, collector);
        };
    }

    private OperationResult executeInsert(
            MongoDBInstrumentedConnection conn,
            InsertOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            MongoCollection<Document> coll = conn.getDatabase().getCollection(collectionName);

            // Convert JsonDocument to BSON Document
            Document doc = toBsonDocument(operation.document());

            long serializeStart = timeSource.nanoTime();
            // Serialization happens during insert
            coll.insertOne(doc);
            long endNanos = timeSource.nanoTime();

            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .serializationTime(Duration.ofNanos(endNanos - serializeStart))
                    .build();

            return OperationResult.success(
                    operation.operationId(),
                    OperationType.INSERT,
                    totalDuration,
                    breakdown
            );

        } catch (Exception e) {
            long endNanos = timeSource.nanoTime();
            return OperationResult.failure(
                    operation.operationId(),
                    OperationType.INSERT,
                    Duration.ofNanos(endNanos - startNanos),
                    e
            );
        }
    }

    private OperationResult executeRead(
            MongoDBInstrumentedConnection conn,
            ReadOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            MongoCollection<Document> coll = conn.getDatabase().getCollection(collectionName);

            FindIterable<Document> find = coll.find(Filters.eq("_id", operation.documentId()));

            // Apply projection if specified
            if (operation.hasProjection()) {
                Bson projection = buildProjection(operation.projectionPaths());
                find.projection(projection);
            }

            long queryBuildEnd = timeSource.nanoTime();

            // Execute query and measure deserialization
            long deserStart = timeSource.nanoTime();
            Document result = find.first();
            long deserEnd = timeSource.nanoTime();

            // If we have projection, navigate to the fields to measure client traversal
            long clientTraversalTime = 0;
            if (result != null && operation.hasProjection()) {
                long travStart = timeSource.nanoTime();
                for (String path : operation.projectionPaths()) {
                    navigateToPath(result, path);
                }
                clientTraversalTime = timeSource.nanoTime() - travStart;
            }

            long endNanos = timeSource.nanoTime();
            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            // Convert result back to JsonDocument
            JsonDocument jsonResult = result != null ? fromBsonDocument(result, operation.documentId()) : null;

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .deserializationTime(Duration.ofNanos(deserEnd - deserStart))
                    .clientTraversalTime(Duration.ofNanos(clientTraversalTime))
                    .addPlatformSpecific("mongodb.query_build_time",
                            Duration.ofNanos(queryBuildEnd - startNanos))
                    .build();

            collector.recordOverheadBreakdown(breakdown);

            return OperationResult.builder(operation.operationId(), OperationType.READ)
                    .success(true)
                    .totalDuration(totalDuration)
                    .resultData(jsonResult)
                    .overheadBreakdown(breakdown)
                    .build();

        } catch (Exception e) {
            long endNanos = timeSource.nanoTime();
            return OperationResult.failure(
                    operation.operationId(),
                    OperationType.READ,
                    Duration.ofNanos(endNanos - startNanos),
                    e
            );
        }
    }

    private OperationResult executeUpdate(
            MongoDBInstrumentedConnection conn,
            UpdateOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            MongoCollection<Document> coll = conn.getDatabase().getCollection(collectionName);

            Document update = new Document("$set",
                    new Document(operation.updatePath(), operation.newValue()));

            coll.updateOne(
                    Filters.eq("_id", operation.documentId()),
                    update
            );

            long endNanos = timeSource.nanoTime();
            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .build();

            return OperationResult.success(
                    operation.operationId(),
                    OperationType.UPDATE,
                    totalDuration,
                    breakdown
            );

        } catch (Exception e) {
            long endNanos = timeSource.nanoTime();
            return OperationResult.failure(
                    operation.operationId(),
                    OperationType.UPDATE,
                    Duration.ofNanos(endNanos - startNanos),
                    e
            );
        }
    }

    private OperationResult executeDelete(
            MongoDBInstrumentedConnection conn,
            DeleteOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            MongoCollection<Document> coll = conn.getDatabase().getCollection(collectionName);

            coll.deleteOne(Filters.eq("_id", operation.documentId()));

            long endNanos = timeSource.nanoTime();
            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .build();

            return OperationResult.success(
                    operation.operationId(),
                    OperationType.DELETE,
                    totalDuration,
                    breakdown
            );

        } catch (Exception e) {
            long endNanos = timeSource.nanoTime();
            return OperationResult.failure(
                    operation.operationId(),
                    OperationType.DELETE,
                    Duration.ofNanos(endNanos - startNanos),
                    e
            );
        }
    }

    private OperationResult executeAggregate(
            MongoDBInstrumentedConnection conn,
            AggregateOperation operation,
            MetricsCollector collector) {

        long startNanos = timeSource.nanoTime();

        try {
            MongoCollection<Document> coll = conn.getDatabase().getCollection(collectionName);

            List<Document> pipeline = operation.pipeline().stream()
                    .map(Document::parse)
                    .toList();

            List<Document> results = coll.aggregate(pipeline).into(new ArrayList<>());

            long endNanos = timeSource.nanoTime();
            Duration totalDuration = Duration.ofNanos(endNanos - startNanos);

            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(totalDuration)
                    .build();

            return OperationResult.builder(operation.operationId(), OperationType.AGGREGATE)
                    .success(true)
                    .totalDuration(totalDuration)
                    .resultData(results)
                    .overheadBreakdown(breakdown)
                    .build();

        } catch (Exception e) {
            long endNanos = timeSource.nanoTime();
            return OperationResult.failure(
                    operation.operationId(),
                    OperationType.AGGREGATE,
                    Duration.ofNanos(endNanos - startNanos),
                    e
            );
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
        if (database == null) {
            throw new SetupException("Not connected to database");
        }

        collectionName = config.collectionName();

        if (config.dropExisting()) {
            database.getCollection(collectionName).drop();
        }

        // Create indexes
        for (TestEnvironmentConfig.IndexDefinition index : config.indexes()) {
            Document indexDoc = new Document();
            for (String field : index.fields()) {
                indexDoc.append(field, 1);
            }
            database.getCollection(collectionName).createIndex(indexDoc);
        }
    }

    @Override
    public void teardownTestEnvironment() {
        if (database != null && collectionName != null) {
            database.getCollection(collectionName).drop();
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    // Helper methods

    private Bson buildProjection(List<String> paths) {
        if (paths.isEmpty()) {
            return new Document();
        }

        List<Bson> includes = new ArrayList<>();
        for (String path : paths) {
            // Convert dot notation paths
            includes.add(Projections.include(path));
        }
        includes.add(Projections.include("_id"));

        return Projections.fields(includes);
    }

    private Document toBsonDocument(JsonDocument jsonDoc) {
        Document doc = new Document();
        doc.putAll(jsonDoc.getContent());
        return doc;
    }

    private JsonDocument fromBsonDocument(Document doc, String id) {
        Map<String, Object> content = new HashMap<>(doc);
        return JsonDocument.of(id, content);
    }

    @SuppressWarnings("unchecked")
    private Object navigateToPath(Document doc, String path) {
        String[] segments = path.split("\\.");
        Object current = doc;

        for (String segment : segments) {
            if (current == null) {
                return null;
            }

            // Handle array notation
            int bracketIndex = segment.indexOf('[');
            if (bracketIndex != -1) {
                String fieldName = segment.substring(0, bracketIndex);
                int arrayIndex = Integer.parseInt(
                        segment.substring(bracketIndex + 1, segment.indexOf(']'))
                );

                if (current instanceof Document d) {
                    Object arrayObj = d.get(fieldName);
                    if (arrayObj instanceof List<?> list && arrayIndex < list.size()) {
                        current = list.get(arrayIndex);
                    } else {
                        return null;
                    }
                }
            } else {
                if (current instanceof Document d) {
                    current = d.get(segment);
                } else {
                    return null;
                }
            }
        }

        return current;
    }
}
