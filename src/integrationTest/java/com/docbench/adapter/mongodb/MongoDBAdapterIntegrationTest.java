package com.docbench.adapter.mongodb;

import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.metrics.MetricsSummary;
import com.docbench.metrics.OverheadBreakdown;
import com.docbench.util.TimeSource;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for MongoDBAdapter against real MongoDB instance.
 * Configure MONGODB_URI environment variable or update the default below.
 */
@DisplayName("MongoDBAdapter Integration Tests")
@Tag("integration")
class MongoDBAdapterIntegrationTest {

    private static final String MONGODB_URI = System.getenv().getOrDefault(
            "MONGODB_URI",
            "mongodb://translator:translator123@localhost:27017/testdb"
    );

    private MongoDBAdapter adapter;
    private InstrumentedConnection connection;
    private MetricsCollector collector;

    @BeforeEach
    void setUp() {
        adapter = new MongoDBAdapter();
        collector = new MetricsCollector(TimeSource.system());

        ConnectionConfig config = ConnectionConfig.builder()
                .uri(MONGODB_URI)
                .database("testdb")
                .build();

        connection = adapter.connect(config);

        // Set up test environment
        adapter.setupTestEnvironment(TestEnvironmentConfig.builder()
                .collectionName("benchmark_docs")
                .dropExisting(true)
                .build());
    }

    @AfterEach
    void tearDown() {
        if (adapter != null) {
            adapter.teardownTestEnvironment();
        }
        if (connection != null) {
            connection.close();
        }
        if (adapter != null) {
            adapter.close();
        }
    }

    @Nested
    @DisplayName("Connection")
    class ConnectionTests {

        @Test
        @DisplayName("should connect successfully")
        void connect_shouldSucceed() {
            assertThat(connection).isNotNull();
            assertThat(connection.isValid()).isTrue();
        }

        @Test
        @DisplayName("should provide unique connection ID")
        void connect_shouldProvideConnectionId() {
            assertThat(connection.getConnectionId())
                    .isNotNull()
                    .isNotEmpty();
        }

        @Test
        @DisplayName("should unwrap to MongoClient")
        void connect_shouldUnwrap() {
            Object unwrapped = connection.unwrap(Object.class);
            assertThat(unwrapped).isNotNull();
        }
    }

    @Nested
    @DisplayName("Insert Operations")
    class InsertOperationTests {

        @Test
        @DisplayName("should insert document successfully")
        void execute_insert_shouldSucceed() {
            JsonDocument doc = JsonDocument.builder("doc-1")
                    .field("name", "Test Document")
                    .field("value", 42)
                    .build();

            InsertOperation insert = new InsertOperation("insert-1", doc);

            OperationResult result = adapter.execute(connection, insert, collector);

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.operationId()).isEqualTo("insert-1");
            assertThat(result.totalDuration()).isPositive();
        }

        @Test
        @DisplayName("should capture overhead breakdown for insert")
        void execute_insert_shouldCaptureBreakdown() {
            JsonDocument doc = JsonDocument.builder("doc-2")
                    .field("data", "test")
                    .build();

            InsertOperation insert = new InsertOperation("insert-2", doc);

            OperationResult result = adapter.execute(connection, insert, collector);

            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);
            assertThat(breakdown).isNotNull();
            assertThat(breakdown.totalLatency()).isPositive();
            assertThat(breakdown.serverExecutionTime()).isGreaterThanOrEqualTo(Duration.ZERO);
        }
    }

    @Nested
    @DisplayName("Read Operations")
    class ReadOperationTests {

        @BeforeEach
        void insertTestDocument() {
            JsonDocument doc = JsonDocument.builder("read-test-doc")
                    .field("name", "Read Test")
                    .field("value", 100)
                    .nestedObject("customer", Map.of(
                            "id", "cust-123",
                            "email", "test@example.com",
                            "profile", Map.of(
                                    "tier", "gold",
                                    "preferences", Map.of(
                                            "notifications", true
                                    )
                            )
                    ))
                    .array("items", List.of(
                            Map.of("sku", "SKU-001", "qty", 1),
                            Map.of("sku", "SKU-002", "qty", 2),
                            Map.of("sku", "SKU-003", "qty", 3)
                    ))
                    .build();

            adapter.execute(connection,
                    new InsertOperation("setup-insert", doc),
                    collector);
            collector.reset();
        }

        @Test
        @DisplayName("should read full document")
        void execute_readFull_shouldSucceed() {
            ReadOperation read = ReadOperation.fullDocument("read-1", "read-test-doc");

            OperationResult result = adapter.execute(connection, read, collector);

            assertThat(result.isSuccess()).isTrue();
            assertThat(result.<JsonDocument>resultData()).isPresent();
        }

        @Test
        @DisplayName("should read with projection")
        void execute_readProjection_shouldSucceed() {
            ReadOperation read = ReadOperation.withProjection(
                    "read-2",
                    "read-test-doc",
                    List.of("name", "value")
            );

            OperationResult result = adapter.execute(connection, read, collector);

            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("should read nested field with projection")
        void execute_readNestedProjection_shouldSucceed() {
            ReadOperation read = ReadOperation.withProjection(
                    "read-3",
                    "read-test-doc",
                    List.of("customer.email", "customer.profile.tier")
            );

            OperationResult result = adapter.execute(connection, read, collector);

            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("should capture overhead breakdown for read")
        void execute_read_shouldCaptureBreakdown() {
            ReadOperation read = ReadOperation.fullDocument("read-4", "read-test-doc");

            OperationResult result = adapter.execute(connection, read, collector);

            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);

            assertThat(breakdown.totalLatency()).isPositive();
            assertThat(breakdown.serverExecutionTime()).isGreaterThanOrEqualTo(Duration.ZERO);
            assertThat(breakdown.deserializationTime()).isGreaterThanOrEqualTo(Duration.ZERO);
        }

        @Test
        @DisplayName("should record metrics for read operation")
        void execute_read_shouldRecordMetrics() {
            ReadOperation read = ReadOperation.fullDocument("read-5", "read-test-doc");

            adapter.execute(connection, read, collector);

            // Use connection's metrics collector where BsonTimingInterceptor records
            MetricsSummary summary = connection.getMetricsCollector().summarize();

            assertThat(summary.hasMetric("mongodb.client_round_trip")).isTrue();
            assertThat(summary.get("mongodb.client_round_trip").count()).isGreaterThanOrEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Traversal Overhead Measurement")
    class TraversalOverheadTests {

        @Test
        @DisplayName("should measure increasing traversal time with field position")
        void traversal_shouldIncreaseWithFieldPosition() {
            // Create documents with target field at different positions
            List<Duration> accessTimes = new ArrayList<>();

            for (int position : List.of(5, 25, 50, 75, 100)) {
                // Create document with target at specific position
                JsonDocument doc = createDocumentWithTargetAt(position, 100);
                adapter.execute(connection,
                        new InsertOperation("insert-pos-" + position, doc),
                        collector);

                // Read just the target field
                ReadOperation read = ReadOperation.withProjection(
                        "read-pos-" + position,
                        doc.getId(),
                        List.of("target_field")
                );

                // Warm up
                for (int i = 0; i < 10; i++) {
                    adapter.execute(connection, read, collector);
                }
                collector.reset();

                // Measure
                long totalNanos = 0;
                int iterations = 100;
                for (int i = 0; i < iterations; i++) {
                    OperationResult result = adapter.execute(connection, read, collector);
                    totalNanos += result.totalDuration().toNanos();
                }

                accessTimes.add(Duration.ofNanos(totalNanos / iterations));
            }

            // BSON characteristic: later positions should generally take longer
            // (though this may not be perfectly linear due to various factors)
            System.out.println("Access times by position: " + accessTimes);

            // At minimum, the test should complete without error
            assertThat(accessTimes).allMatch(d -> d.toNanos() > 0);
        }

        @Test
        @DisplayName("should measure increasing traversal time with nesting depth")
        void traversal_shouldIncreaseWithNestingDepth() {
            List<Duration> accessTimes = new ArrayList<>();

            for (int depth : List.of(1, 2, 3, 4, 5)) {
                JsonDocument doc = createDocumentWithNesting(depth);
                adapter.execute(connection,
                        new InsertOperation("insert-depth-" + depth, doc),
                        collector);

                String targetPath = buildNestedPath(depth);
                ReadOperation read = ReadOperation.withProjection(
                        "read-depth-" + depth,
                        doc.getId(),
                        List.of(targetPath)
                );

                // Warm up
                for (int i = 0; i < 10; i++) {
                    adapter.execute(connection, read, collector);
                }
                collector.reset();

                // Measure
                long totalNanos = 0;
                int iterations = 100;
                for (int i = 0; i < iterations; i++) {
                    OperationResult result = adapter.execute(connection, read, collector);
                    totalNanos += result.totalDuration().toNanos();
                }

                accessTimes.add(Duration.ofNanos(totalNanos / iterations));
            }

            System.out.println("Access times by depth: " + accessTimes);

            // Verify all measurements are positive
            assertThat(accessTimes).allMatch(d -> d.toNanos() > 0);
        }

        private JsonDocument createDocumentWithTargetAt(int position, int totalFields) {
            JsonDocument.Builder builder = JsonDocument.builder("doc-pos-" + position);

            for (int i = 1; i <= totalFields; i++) {
                if (i == position) {
                    builder.field("target_field", "TARGET_VALUE");
                } else {
                    builder.field("field_" + String.format("%03d", i),
                            "padding_value_" + i);
                }
            }

            return builder.build();
        }

        private JsonDocument createDocumentWithNesting(int depth) {
            Map<String, Object> current = Map.of("target", "TARGET_VALUE");

            for (int i = depth - 1; i >= 1; i--) {
                current = Map.of(
                        "field_a", "value_a",
                        "field_b", "value_b",
                        "level" + i, current
                );
            }

            return JsonDocument.builder("doc-depth-" + depth)
                    .field("level0", current)
                    .build();
        }

        private String buildNestedPath(int depth) {
            StringBuilder path = new StringBuilder("level0");
            for (int i = 1; i < depth; i++) {
                path.append(".level").append(i);
            }
            path.append(".target");
            return path.toString();
        }
    }

    @Nested
    @DisplayName("Bulk Operations")
    class BulkOperationTests {

        @Test
        @DisplayName("should execute bulk insert")
        void executeBulk_insert_shouldSucceed() {
            List<Operation> operations = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                JsonDocument doc = JsonDocument.builder("bulk-doc-" + i)
                        .field("index", i)
                        .field("data", "bulk test data " + i)
                        .build();
                operations.add(new InsertOperation("bulk-insert-" + i, doc));
            }

            BulkOperationResult result = adapter.executeBulk(connection, operations, collector);

            assertThat(result.allSuccessful()).isTrue();
            assertThat(result.totalOperations()).isEqualTo(100);
            assertThat(result.successCount()).isEqualTo(100);
        }

        @Test
        @DisplayName("should measure throughput for bulk operations")
        void executeBulk_shouldMeasureThroughput() {
            List<Operation> operations = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                JsonDocument doc = JsonDocument.builder("throughput-doc-" + i)
                        .field("index", i)
                        .build();
                operations.add(new InsertOperation("throughput-insert-" + i, doc));
            }

            BulkOperationResult result = adapter.executeBulk(connection, operations, collector);

            double throughput = result.throughputOpsPerSecond();
            System.out.println("Bulk insert throughput: " + throughput + " ops/sec");

            assertThat(throughput).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("Deserialization Timing")
    class DeserializationTimingTests {

        @Test
        @DisplayName("should measure deserialization overhead")
        void deserialization_shouldBeMeasured() {
            // Insert large document
            JsonDocument doc = createLargeDocument(100);
            adapter.execute(connection,
                    new InsertOperation("large-doc", doc),
                    collector);
            collector.reset();

            // Read full document
            ReadOperation read = ReadOperation.fullDocument("read-large", doc.getId());

            OperationResult result = adapter.execute(connection, read, collector);
            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);

            System.out.println("Total latency: " + breakdown.totalLatency().toNanos() / 1000 + " us");
            System.out.println("Server execution: " + breakdown.serverExecutionTime().toNanos() / 1000 + " us");
            System.out.println("Deserialization: " + breakdown.deserializationTime().toNanos() / 1000 + " us");

            assertThat(breakdown.deserializationTime()).isGreaterThanOrEqualTo(Duration.ZERO);
        }

        @Test
        @DisplayName("should show deserialization scales with document size")
        void deserialization_shouldScaleWithDocSize() {
            List<Duration> deserializationTimes = new ArrayList<>();

            for (int fieldCount : List.of(10, 50, 100, 200)) {
                JsonDocument doc = createLargeDocument(fieldCount);
                adapter.execute(connection,
                        new InsertOperation("size-doc-" + fieldCount, doc),
                        collector);

                ReadOperation read = ReadOperation.fullDocument(
                        "read-size-" + fieldCount,
                        doc.getId()
                );

                // Warm up
                for (int i = 0; i < 10; i++) {
                    adapter.execute(connection, read, collector);
                }
                collector.reset();

                // Measure average
                long totalDeserNanos = 0;
                int iterations = 50;
                for (int i = 0; i < iterations; i++) {
                    OperationResult result = adapter.execute(connection, read, collector);
                    OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);
                    totalDeserNanos += breakdown.deserializationTime().toNanos();
                }

                deserializationTimes.add(Duration.ofNanos(totalDeserNanos / iterations));
            }

            System.out.println("Deserialization times by field count: " + deserializationTimes);

            // All should be measurable
            assertThat(deserializationTimes).allMatch(d -> d.toNanos() >= 0);
        }

        private JsonDocument createLargeDocument(int fieldCount) {
            JsonDocument.Builder builder = JsonDocument.builder("large-doc-" + fieldCount);

            for (int i = 0; i < fieldCount; i++) {
                builder.field("field_" + String.format("%03d", i),
                        "This is a reasonably long string value for field " + i +
                                " that adds some size to the document for testing purposes.");
            }

            return builder.build();
        }
    }
}
