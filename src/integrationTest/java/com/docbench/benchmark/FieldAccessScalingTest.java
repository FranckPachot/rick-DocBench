package com.docbench.benchmark;

import com.docbench.adapter.mongodb.MongoDBAdapter;
import com.docbench.adapter.oracle.OracleOsonAdapter;
import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.util.TimeSource;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Field Access Scaling Test: Demonstrates O(n) vs O(1) complexity.
 *
 * This test varies the NUMBER OF FIELDS in documents and measures
 * the time to extract the LAST field. This isolates the O(n) vs O(1)
 * complexity difference:
 *
 * - BSON O(n): Time to find last field should increase linearly with field count
 * - OSON O(1): Time to find any field should remain constant regardless of field count
 *
 * Test methodology:
 * 1. Create documents with 10, 50, 100, 200, 500, 1000 fields
 * 2. Always extract the LAST field (worst case for O(n))
 * 3. Compare how access time scales with field count
 */
@DisplayName("O(n) vs O(1) Field Access Scaling")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FieldAccessScalingTest {

    // Field counts to test - should show linear scaling for BSON
    private static final int[] FIELD_COUNTS = {10, 50, 100, 200, 500, 1000};

    // Configuration
    private static final int WARMUP_ITERATIONS = 100;
    private static final int MEASUREMENT_ITERATIONS = 200;

    // Adapters and connections
    private static MongoDBAdapter mongoAdapter;
    private static OracleOsonAdapter oracleAdapter;
    private static InstrumentedConnection mongoConnection;
    private static InstrumentedConnection oracleConnection;
    private static MetricsCollector mongoCollector;
    private static MetricsCollector oracleCollector;

    // Results storage
    private static final Map<Integer, ScalingResult> results = new LinkedHashMap<>();

    @BeforeAll
    static void setupAdapters() {
        Properties props = loadConfigProperties();

        // Setup MongoDB
        mongoAdapter = new MongoDBAdapter();
        mongoCollector = new MetricsCollector(TimeSource.system());
        ConnectionConfig mongoConfig = ConnectionConfig.builder()
                .uri(props.getProperty("mongodb.uri"))
                .database(props.getProperty("mongodb.database", "testdb"))
                .build();
        mongoConnection = mongoAdapter.connect(mongoConfig);
        mongoAdapter.setupTestEnvironment(TestEnvironmentConfig.builder()
                .collectionName("scaling_test")
                .dropExisting(true)
                .build());

        // Setup Oracle
        oracleAdapter = new OracleOsonAdapter();
        oracleCollector = new MetricsCollector(TimeSource.system());
        ConnectionConfig oracleConfig = ConnectionConfig.builder()
                .uri(props.getProperty("oracle.url"))
                .database("docbench")
                .username(props.getProperty("oracle.username"))
                .password(props.getProperty("oracle.password"))
                .build();
        oracleConnection = oracleAdapter.connect(oracleConfig);
        oracleAdapter.setupTestEnvironment(TestEnvironmentConfig.builder()
                .collectionName("scaling_test")
                .dropExisting(true)
                .build());

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  O(n) vs O(1) Field Access Scaling Test");
        System.out.println("  Testing: Extract LAST field from documents with varying field counts");
        System.out.println("  Expected: BSON time scales with field count, OSON time stays constant");
        System.out.println("=".repeat(80));

        // Global warmup
        runGlobalWarmup();
    }

    private static void runGlobalWarmup() {
        System.out.println("\nRunning global warmup...");

        JsonDocument warmupDoc = createDocument("warmup", 100);
        mongoAdapter.execute(mongoConnection, new InsertOperation("warmup-mongo", warmupDoc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("warmup-oracle", warmupDoc), oracleCollector);

        ReadOperation read = ReadOperation.withProjection("warmup-read", "warmup", List.of("field_099"));
        for (int i = 0; i < 200; i++) {
            mongoAdapter.execute(mongoConnection, read, mongoCollector);
            oracleAdapter.execute(oracleConnection, read, oracleCollector);
        }

        mongoCollector.reset();
        oracleCollector.reset();
        System.out.println("Global warmup complete.\n");
    }

    @AfterAll
    static void teardownAndReport() {
        if (mongoAdapter != null) {
            mongoAdapter.teardownTestEnvironment();
            mongoAdapter.close();
        }
        if (oracleAdapter != null) {
            oracleAdapter.teardownTestEnvironment();
            oracleAdapter.close();
        }
        if (mongoConnection != null) mongoConnection.close();
        if (oracleConnection != null) oracleConnection.close();

        printScalingReport();
    }

    private static Properties loadConfigProperties() {
        Path configPath = Path.of("config/local.properties");
        if (Files.exists(configPath)) {
            try (InputStream is = Files.newInputStream(configPath)) {
                Properties props = new Properties();
                props.load(is);
                return props;
            } catch (IOException e) {
                throw new RuntimeException("Could not load config/local.properties", e);
            }
        }
        throw new RuntimeException("config/local.properties not found");
    }

    // =========================================================================
    // Scaling Tests - Extract LAST field from documents of varying sizes
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("10 fields - extract last field")
    void scaling_10fields() {
        testScaling(10);
    }

    @Test
    @Order(2)
    @DisplayName("50 fields - extract last field")
    void scaling_50fields() {
        testScaling(50);
    }

    @Test
    @Order(3)
    @DisplayName("100 fields - extract last field")
    void scaling_100fields() {
        testScaling(100);
    }

    @Test
    @Order(4)
    @DisplayName("200 fields - extract last field")
    void scaling_200fields() {
        testScaling(200);
    }

    @Test
    @Order(5)
    @DisplayName("500 fields - extract last field")
    void scaling_500fields() {
        testScaling(500);
    }

    @Test
    @Order(6)
    @DisplayName("1000 fields - extract last field")
    void scaling_1000fields() {
        testScaling(1000);
    }

    private void testScaling(int fieldCount) {
        String docId = "scale-" + fieldCount;
        String lastField = "field_" + String.format("%03d", fieldCount - 1);

        // Create and insert document
        JsonDocument doc = createDocument(docId, fieldCount);
        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        // Measure time to extract LAST field
        Duration mongoDuration = measureProjectionTime(mongoAdapter, mongoConnection, mongoCollector, docId, lastField);
        Duration oracleDuration = measureProjectionTime(oracleAdapter, oracleConnection, oracleCollector, docId, lastField);

        results.put(fieldCount, new ScalingResult(fieldCount, mongoDuration, oracleDuration));

        System.out.printf("  %4d fields: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                fieldCount,
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    private Duration measureProjectionTime(DatabaseAdapter adapter, InstrumentedConnection conn,
                                           MetricsCollector collector, String docId, String field) {
        ReadOperation read = ReadOperation.withProjection("read-" + docId, docId, List.of(field));

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            adapter.execute(conn, read, collector);
        }
        collector.reset();

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            OperationResult result = adapter.execute(conn, read, collector);
            totalNanos += result.totalDuration().toNanos();
        }

        return Duration.ofNanos(totalNanos / MEASUREMENT_ITERATIONS);
    }

    private static JsonDocument createDocument(String id, int fieldCount) {
        JsonDocument.Builder builder = JsonDocument.builder(id);

        for (int i = 0; i < fieldCount; i++) {
            builder.field("field_" + String.format("%03d", i),
                    "This is field " + i + " with padding content to ensure consistent field sizes across documents.");
        }

        return builder.build();
    }

    private static void printScalingReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  O(n) vs O(1) SCALING ANALYSIS");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.printf("%-12s %12s %12s %10s %12s %12s%n",
                "Fields", "BSON (us)", "OSON (us)", "Ratio", "BSON Δ%", "OSON Δ%");
        System.out.println("-".repeat(80));

        ScalingResult baseline = results.get(FIELD_COUNTS[0]);
        long baselineBson = baseline != null ? baseline.bsonDuration.toNanos() / 1000 : 1;
        long baselineOson = baseline != null ? baseline.osonDuration.toNanos() / 1000 : 1;

        for (int fieldCount : FIELD_COUNTS) {
            ScalingResult result = results.get(fieldCount);
            if (result == null) continue;

            long bsonUs = result.bsonDuration.toNanos() / 1000;
            long osonUs = result.osonDuration.toNanos() / 1000;
            double ratio = (double) bsonUs / osonUs;

            // Calculate % increase from baseline
            double bsonIncrease = ((double) bsonUs / baselineBson - 1) * 100;
            double osonIncrease = ((double) osonUs / baselineOson - 1) * 100;

            System.out.printf("%-12d %12d %12d %9.2fx %+11.1f%% %+11.1f%%%n",
                    fieldCount, bsonUs, osonUs, ratio, bsonIncrease, osonIncrease);
        }

        System.out.println("-".repeat(80));
        System.out.println();
        System.out.println("SCALING INTERPRETATION:");
        System.out.println("  - If BSON is O(n): BSON time should increase ~linearly with field count");
        System.out.println("  - If OSON is O(1): OSON time should stay relatively constant");
        System.out.println();

        // Calculate scaling factors
        if (results.size() >= 2) {
            ScalingResult first = results.get(FIELD_COUNTS[0]);
            ScalingResult last = results.get(FIELD_COUNTS[FIELD_COUNTS.length - 1]);

            if (first != null && last != null) {
                int fieldRatio = FIELD_COUNTS[FIELD_COUNTS.length - 1] / FIELD_COUNTS[0];
                double bsonScaling = (double) last.bsonDuration.toNanos() / first.bsonDuration.toNanos();
                double osonScaling = (double) last.osonDuration.toNanos() / first.osonDuration.toNanos();

                System.out.printf("  Field count increased: %dx (from %d to %d fields)%n",
                        fieldRatio, FIELD_COUNTS[0], FIELD_COUNTS[FIELD_COUNTS.length - 1]);
                System.out.printf("  BSON time increased:   %.2fx%n", bsonScaling);
                System.out.printf("  OSON time increased:   %.2fx%n", osonScaling);
                System.out.println();

                if (bsonScaling > osonScaling * 1.5) {
                    System.out.println("  ✓ BSON shows O(n) scaling behavior (time increases with field count)");
                    System.out.println("  ✓ OSON shows O(1) behavior (time relatively constant)");
                } else {
                    System.out.println("  Note: Scaling difference less pronounced than expected.");
                    System.out.println("  This may be due to network/connection overhead dominating.");
                }
            }
        }

        System.out.println("=".repeat(80) + "\n");
    }

    private record ScalingResult(
            int fieldCount,
            Duration bsonDuration,
            Duration osonDuration
    ) {}
}
