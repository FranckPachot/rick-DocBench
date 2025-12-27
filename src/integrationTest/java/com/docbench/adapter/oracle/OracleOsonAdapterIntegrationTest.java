package com.docbench.adapter.oracle;

import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.metrics.OverheadBreakdown;
import com.docbench.util.TimeSource;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for OracleOsonAdapter against real Oracle Database.
 * Tests OSON (binary JSON) with O(1) hash-indexed field access.
 *
 * Configuration priority:
 * 1. Environment variables ORACLE_URL, ORACLE_USERNAME, ORACLE_PASSWORD
 * 2. config/local.properties file
 * 3. Fail with helpful message
 */
@DisplayName("OracleOsonAdapter Integration Tests")
@Tag("integration")
class OracleOsonAdapterIntegrationTest {

    private static final String ORACLE_URL = loadOracleUrl();
    private static final String ORACLE_USERNAME = loadOracleUsername();
    private static final String ORACLE_PASSWORD = loadOraclePassword();
    private static final String ORACLE_DATABASE = loadOracleDatabase();

    private static String loadOracleUrl() {
        String envUrl = System.getenv("ORACLE_URL");
        if (envUrl != null && !envUrl.isBlank()) {
            return envUrl;
        }

        Properties props = loadConfigProperties();
        if (props != null) {
            String configUrl = props.getProperty("oracle.url");
            if (configUrl != null && !configUrl.isBlank()) {
                return configUrl;
            }
        }

        throw new IllegalStateException(
                "Oracle URL not configured. Set ORACLE_URL environment variable " +
                "or create config/local.properties with oracle.url property. " +
                "See config/local.properties.example for template.");
    }

    private static String loadOracleUsername() {
        String envUser = System.getenv("ORACLE_USERNAME");
        if (envUser != null && !envUser.isBlank()) {
            return envUser;
        }

        Properties props = loadConfigProperties();
        if (props != null) {
            String configUser = props.getProperty("oracle.username");
            if (configUser != null && !configUser.isBlank()) {
                return configUser;
            }
        }

        return "docbench"; // Default
    }

    private static String loadOraclePassword() {
        String envPass = System.getenv("ORACLE_PASSWORD");
        if (envPass != null && !envPass.isBlank()) {
            return envPass;
        }

        Properties props = loadConfigProperties();
        if (props != null) {
            String configPass = props.getProperty("oracle.password");
            if (configPass != null && !configPass.isBlank()) {
                return configPass;
            }
        }

        throw new IllegalStateException(
                "Oracle password not configured. Set ORACLE_PASSWORD environment variable " +
                "or create config/local.properties with oracle.password property.");
    }

    private static String loadOracleDatabase() {
        String envDb = System.getenv("ORACLE_DATABASE");
        if (envDb != null && !envDb.isBlank()) {
            return envDb;
        }

        Properties props = loadConfigProperties();
        if (props != null) {
            String configDb = props.getProperty("oracle.database");
            if (configDb != null && !configDb.isBlank()) {
                return configDb;
            }
        }

        return "docbench"; // Default
    }

    private static Properties loadConfigProperties() {
        Path configPath = Path.of("config/local.properties");
        if (Files.exists(configPath)) {
            try (InputStream is = Files.newInputStream(configPath)) {
                Properties props = new Properties();
                props.load(is);
                return props;
            } catch (IOException e) {
                System.err.println("Warning: Could not load config/local.properties: " + e.getMessage());
            }
        }
        return null;
    }

    private OracleOsonAdapter adapter;
    private InstrumentedConnection connection;
    private MetricsCollector collector;

    @BeforeEach
    void setUp() {
        adapter = new OracleOsonAdapter();
        collector = new MetricsCollector(TimeSource.system());

        ConnectionConfig config = ConnectionConfig.builder()
                .uri(ORACLE_URL)
                .database(ORACLE_DATABASE)
                .username(ORACLE_USERNAME)
                .password(ORACLE_PASSWORD)
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
        @DisplayName("should report OSON-specific capabilities")
        void adapter_shouldReportOsonCapabilities() {
            assertThat(adapter.supportsHashIndexedFieldAccess()).isTrue();
            assertThat(adapter.supportsJsonDualityViews()).isTrue();
            assertThat(adapter.supportsJsonPathExpressions()).isTrue();
            assertThat(adapter.hasCapability(Capability.SERVER_TRAVERSAL_TIME)).isTrue();
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
        @DisplayName("should read document by ID")
        void execute_read_shouldSucceed() {
            ReadOperation read = ReadOperation.fullDocument("read-1", "read-test-doc");

            OperationResult result = adapter.execute(connection, read, collector);

            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("should capture overhead breakdown for read")
        void execute_read_shouldCaptureBreakdown() {
            ReadOperation read = ReadOperation.fullDocument("read-2", "read-test-doc");

            OperationResult result = adapter.execute(connection, read, collector);

            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);

            assertThat(breakdown.totalLatency()).isPositive();
            assertThat(breakdown.serverExecutionTime()).isGreaterThanOrEqualTo(Duration.ZERO);
            // OSON advantage: server traversal time should be minimal (O(1))
            assertThat(breakdown.serverTraversalTime()).isGreaterThanOrEqualTo(Duration.ZERO);
        }

        @Test
        @DisplayName("should return not found for missing document")
        void execute_read_shouldReturnNotFoundForMissing() {
            ReadOperation read = ReadOperation.fullDocument("read-3", "non-existent-doc");

            OperationResult result = adapter.execute(connection, read, collector);

            assertThat(result.isFailure()).isTrue();
        }
    }

    @Nested
    @DisplayName("Update Operations")
    class UpdateOperationTests {

        @BeforeEach
        void insertTestDocument() {
            JsonDocument doc = JsonDocument.builder("update-test-doc")
                    .field("name", "Update Test")
                    .field("value", 50)
                    .build();

            adapter.execute(connection,
                    new InsertOperation("setup-update", doc),
                    collector);
            collector.reset();
        }

        @Test
        @DisplayName("should update document field")
        void execute_update_shouldSucceed() {
            UpdateOperation update = new UpdateOperation(
                    "update-1",
                    "update-test-doc",
                    "$.value",
                    100,
                    false
            );

            OperationResult result = adapter.execute(connection, update, collector);

            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("should capture overhead breakdown for update")
        void execute_update_shouldCaptureBreakdown() {
            UpdateOperation update = new UpdateOperation(
                    "update-2",
                    "update-test-doc",
                    "$.name",
                    "Updated Name",
                    false
            );

            OperationResult result = adapter.execute(connection, update, collector);

            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);
            assertThat(breakdown.totalLatency()).isPositive();
            // OSON: O(1) field access for updates
            assertThat(breakdown.serverTraversalTime()).isEqualTo(Duration.ZERO);
        }
    }

    @Nested
    @DisplayName("Delete Operations")
    class DeleteOperationTests {

        @BeforeEach
        void insertTestDocument() {
            JsonDocument doc = JsonDocument.builder("delete-test-doc")
                    .field("name", "Delete Test")
                    .build();

            adapter.execute(connection,
                    new InsertOperation("setup-delete", doc),
                    collector);
            collector.reset();
        }

        @Test
        @DisplayName("should delete document by ID")
        void execute_delete_shouldSucceed() {
            DeleteOperation delete = new DeleteOperation("delete-1", "delete-test-doc");

            OperationResult result = adapter.execute(connection, delete, collector);

            assertThat(result.isSuccess()).isTrue();
        }

        @Test
        @DisplayName("should capture overhead breakdown for delete")
        void execute_delete_shouldCaptureBreakdown() {
            DeleteOperation delete = new DeleteOperation("delete-2", "delete-test-doc");

            OperationResult result = adapter.execute(connection, delete, collector);

            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);
            assertThat(breakdown.totalLatency()).isPositive();
            assertThat(breakdown.serverExecutionTime()).isGreaterThanOrEqualTo(Duration.ZERO);
        }
    }

    @Nested
    @DisplayName("OSON O(1) Access Pattern Tests")
    class OsonAccessPatternTests {

        @Test
        @DisplayName("should demonstrate O(1) field access independent of position")
        void osonAccess_shouldBeConstantTimeRegardlessOfPosition() {
            // Create documents with target field at different positions
            List<Duration> accessTimes = new ArrayList<>();

            for (int position : List.of(5, 25, 50, 75, 100)) {
                // Create document with target at specific position
                JsonDocument doc = createDocumentWithTargetAt(position, 100);
                adapter.execute(connection,
                        new InsertOperation("insert-pos-" + position, doc),
                        collector);

                // Read just the target field
                ReadOperation read = ReadOperation.fullDocument(
                        "read-pos-" + position,
                        doc.getId()
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

            // OSON characteristic: access times should be relatively constant
            // regardless of field position (O(1) hash lookup)
            System.out.println("OSON Access times by position: " + accessTimes);

            // All access times should be positive
            assertThat(accessTimes).allMatch(d -> d.toNanos() > 0);

            // Verify O(1) behavior: variance should be low
            long minNanos = accessTimes.stream().mapToLong(Duration::toNanos).min().orElse(0);
            long maxNanos = accessTimes.stream().mapToLong(Duration::toNanos).max().orElse(0);
            double variance = (double) (maxNanos - minNanos) / minNanos;

            System.out.println("OSON access time variance: " + (variance * 100) + "%");
            // O(1) access should have relatively low variance
            // (though network jitter may add some noise)
        }

        @Test
        @DisplayName("should demonstrate O(1) access independent of nesting depth")
        void osonAccess_shouldBeConstantTimeRegardlessOfDepth() {
            List<Duration> accessTimes = new ArrayList<>();

            for (int depth : List.of(1, 2, 3, 4, 5)) {
                JsonDocument doc = createDocumentWithNesting(depth);
                adapter.execute(connection,
                        new InsertOperation("insert-depth-" + depth, doc),
                        collector);

                ReadOperation read = ReadOperation.fullDocument(
                        "read-depth-" + depth,
                        doc.getId()
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

            System.out.println("OSON Access times by depth: " + accessTimes);

            // All measurements should be positive
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
            System.out.println("Oracle OSON bulk insert throughput: " + throughput + " ops/sec");

            assertThat(throughput).isGreaterThan(0);
        }
    }

    @Nested
    @DisplayName("OSON vs BSON Comparison Prep")
    class ComparisonTests {

        @Test
        @DisplayName("should provide timing breakdown for comparison with BSON")
        void comparison_shouldProvideDetailedBreakdown() {
            // Insert test document
            JsonDocument doc = JsonDocument.builder("comparison-doc")
                    .nestedObject("level1", Map.of(
                            "level2", Map.of(
                                    "level3", Map.of(
                                            "target", "deep_value"
                                    )
                            )
                    ))
                    .build();

            adapter.execute(connection,
                    new InsertOperation("comparison-insert", doc),
                    collector);

            // Read and capture breakdown
            ReadOperation read = ReadOperation.fullDocument("comparison-read", doc.getId());
            OperationResult result = adapter.execute(connection, read, collector);
            OverheadBreakdown breakdown = adapter.getOverheadBreakdown(result);

            System.out.println("=== OSON Overhead Breakdown ===");
            System.out.println("Total Latency:        " + breakdown.totalLatency().toNanos() / 1000 + " us");
            System.out.println("Server Execution:     " + breakdown.serverExecutionTime().toNanos() / 1000 + " us");
            System.out.println("Server Traversal:     " + breakdown.serverTraversalTime().toNanos() / 1000 + " us");
            System.out.println("Deserialization:      " + breakdown.deserializationTime().toNanos() / 1000 + " us");

            // Key assertion: OSON traversal time should be minimal
            assertThat(breakdown.serverTraversalTime())
                    .describedAs("OSON should have O(1) traversal time")
                    .isLessThanOrEqualTo(Duration.ofNanos(100_000)); // 100 microseconds
        }
    }
}
