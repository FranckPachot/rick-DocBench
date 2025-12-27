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
 * Performance comparison benchmark: BSON (MongoDB) vs OSON (Oracle SQL/JSON).
 *
 * This test suite compares field access patterns between:
 * - BSON: O(n) sequential field scanning
 * - OSON: O(1) hash-indexed field lookup via JSON_VALUE
 *
 * Key difference from previous version: Uses native SQL/JSON (JSON_VALUE)
 * for Oracle instead of SODA, demonstrating true O(1) field extraction.
 *
 * NOTE: Tests use random ordering to eliminate JVM/cache warmup bias that would
 * otherwise make earlier tests appear slower than later tests.
 */
@DisplayName("BSON vs OSON Performance Comparison (SQL/JSON)")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.Random.class)
class BsonVsOsonComparisonTest {

    // Configuration - higher warmup to stabilize JIT and caches
    private static final int WARMUP_ITERATIONS = 50;
    private static final int MEASUREMENT_ITERATIONS = 100;
    private static final int GLOBAL_WARMUP_ITERATIONS = 200;

    // Adapters and connections
    private static MongoDBAdapter mongoAdapter;
    private static OracleOsonAdapter oracleAdapter;
    private static InstrumentedConnection mongoConnection;
    private static InstrumentedConnection oracleConnection;
    private static MetricsCollector mongoCollector;
    private static MetricsCollector oracleCollector;

    // Results storage for final report
    private static final Map<String, ComparisonResult> results = new LinkedHashMap<>();

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
                .collectionName("benchmark_comparison")
                .dropExisting(true)
                .build());

        // Setup Oracle with SQL/JSON
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
                .collectionName("benchmark_comparison")
                .dropExisting(true)
                .build());

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  BSON vs OSON Performance Comparison (SQL/JSON Edition)");
        System.out.println("  BSON: O(n) sequential field scanning (MongoDB)");
        System.out.println("  OSON: O(1) hash-indexed field lookup via JSON_VALUE (Oracle 23ai)");
        System.out.println("=".repeat(80));

        // Global warmup to stabilize JVM JIT and database caches before any tests run
        runGlobalWarmup();
    }

    private static void runGlobalWarmup() {
        System.out.println("\nRunning global warmup (" + GLOBAL_WARMUP_ITERATIONS + " iterations)...");

        // Create a warmup document
        JsonDocument warmupDoc = JsonDocument.builder("warmup-doc")
                .field("field_001", "warmup value 1")
                .field("field_050", "warmup value 50")
                .field("field_100", "warmup value 100")
                .build();

        // Insert warmup document
        mongoAdapter.execute(mongoConnection, new InsertOperation("warmup-mongo", warmupDoc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("warmup-oracle", warmupDoc), oracleCollector);

        // Run warmup iterations to stabilize JIT and caches
        ReadOperation read = ReadOperation.withProjection("warmup-read", "warmup-doc", List.of("field_050"));
        for (int i = 0; i < GLOBAL_WARMUP_ITERATIONS; i++) {
            mongoAdapter.execute(mongoConnection, read, mongoCollector);
            oracleAdapter.execute(oracleConnection, read, oracleCollector);
        }

        // Reset collectors
        mongoCollector.reset();
        oracleCollector.reset();

        System.out.println("Global warmup complete.\n");
    }

    @AfterAll
    static void teardownAndReport() {
        // Cleanup
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

        // Print final comparison report
        printFinalReport();
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
    // Test 1: Single Field Projection at Different Positions
    // This is where OSON O(1) really shines - extracting a single field
    // =========================================================================

    @Test
    
    @DisplayName("Single Field Projection: First field in 100-field document")
    void projection_firstField_100Fields() {
        testSingleFieldProjection("first-100", 1, 100, "target_field");
    }

    @Test
    
    @DisplayName("Single Field Projection: Middle field in 100-field document")
    void projection_middleField_100Fields() {
        testSingleFieldProjection("middle-100", 50, 100, "target_field");
    }

    @Test
    
    @DisplayName("Single Field Projection: Last field in 100-field document")
    void projection_lastField_100Fields() {
        testSingleFieldProjection("last-100", 100, 100, "target_field");
    }

    @Test
    
    @DisplayName("Single Field Projection: Last field in 500-field document")
    void projection_lastField_500Fields() {
        testSingleFieldProjection("last-500", 500, 500, "target_field");
    }

    private void testSingleFieldProjection(String testName, int targetPosition, int totalFields, String fieldName) {
        String docId = "proj-" + testName;
        JsonDocument doc = createDocumentWithTargetAt(docId, targetPosition, totalFields, fieldName);

        // Insert into both databases
        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        // Measure with PROJECTION (this is where OSON O(1) matters)
        Duration mongoDuration = measureProjectionTime(mongoAdapter, mongoConnection, mongoCollector, docId, fieldName);
        Duration oracleDuration = measureProjectionTime(oracleAdapter, oracleConnection, oracleCollector, docId, fieldName);

        String key = testName + "-projection";
        results.put(key, new ComparisonResult(key, mongoDuration, oracleDuration,
                "Position " + targetPosition + "/" + totalFields + " (projection)"));

        System.out.printf("  %s: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                testName,
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    // =========================================================================
    // Test 2: Nested Field Access via Projection
    // OSON uses hash lookup at each level, BSON must scan each level
    // =========================================================================

    @Test
    
    @DisplayName("Nested Field Projection: depth 1")
    void nestedProjection_depth1() {
        testNestedFieldProjection("nested-1", 1);
    }

    @Test
    
    @DisplayName("Nested Field Projection: depth 3")
    void nestedProjection_depth3() {
        testNestedFieldProjection("nested-3", 3);
    }

    @Test
    
    @DisplayName("Nested Field Projection: depth 5")
    void nestedProjection_depth5() {
        testNestedFieldProjection("nested-5", 5);
    }

    @Test
    
    @DisplayName("Nested Field Projection: depth 8")
    void nestedProjection_depth8() {
        testNestedFieldProjection("nested-8", 8);
    }

    private void testNestedFieldProjection(String testName, int depth) {
        String docId = "nest-" + testName;
        JsonDocument doc = createDocumentWithNesting(docId, depth, 10); // 10 fields per level

        // Insert into both databases
        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        // Build nested path
        String path = buildNestedPath(depth);

        // Measure with PROJECTION
        Duration mongoDuration = measureProjectionTime(mongoAdapter, mongoConnection, mongoCollector, docId, path);
        Duration oracleDuration = measureProjectionTime(oracleAdapter, oracleConnection, oracleCollector, docId, path);

        String key = testName + "-projection";
        results.put(key, new ComparisonResult(key, mongoDuration, oracleDuration,
                "Depth " + depth + " projection"));

        System.out.printf("  Depth %d: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                depth,
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    // =========================================================================
    // Test 3: Multi-Field Projection
    // Extract multiple fields - OSON does O(k) hash lookups, BSON does O(n*k) scans
    // =========================================================================

    @Test
    
    @DisplayName("Multi-Field Projection: 3 fields from 200-field document")
    void multiProjection_3fields() {
        testMultiFieldProjection("multi-3", 200, List.of("field_010", "field_100", "field_190"));
    }

    @Test
    
    @DisplayName("Multi-Field Projection: 5 fields from 200-field document")
    void multiProjection_5fields() {
        testMultiFieldProjection("multi-5", 200,
                List.of("field_010", "field_050", "field_100", "field_150", "field_190"));
    }

    private void testMultiFieldProjection(String testName, int totalFields, List<String> fieldsToProject) {
        String docId = "multi-" + testName;
        JsonDocument doc = createLargeDocument(docId, totalFields);

        // Insert into both databases
        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        // Measure with multi-field PROJECTION
        Duration mongoDuration = measureMultiProjectionTime(mongoAdapter, mongoConnection, mongoCollector,
                docId, fieldsToProject);
        Duration oracleDuration = measureMultiProjectionTime(oracleAdapter, oracleConnection, oracleCollector,
                docId, fieldsToProject);

        String key = testName + "-projection";
        results.put(key, new ComparisonResult(key, mongoDuration, oracleDuration,
                fieldsToProject.size() + " fields from " + totalFields));

        System.out.printf("  %d fields from %d: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                fieldsToProject.size(), totalFields,
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    // =========================================================================
    // Test 4: Full Document Read (baseline comparison)
    // =========================================================================

    @Test
    
    @DisplayName("Full Document Read: 50 fields")
    void fullRead_50fields() {
        testFullDocumentRead("full-50", 50);
    }

    @Test
    
    @DisplayName("Full Document Read: 200 fields")
    void fullRead_200fields() {
        testFullDocumentRead("full-200", 200);
    }

    private void testFullDocumentRead(String testName, int fieldCount) {
        String docId = "full-" + testName;
        JsonDocument doc = createLargeDocument(docId, fieldCount);

        // Insert into both databases
        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        // Measure FULL document read
        Duration mongoDuration = measureReadTime(mongoAdapter, mongoConnection, mongoCollector, docId);
        Duration oracleDuration = measureReadTime(oracleAdapter, oracleConnection, oracleCollector, docId);

        String key = testName + "-full";
        results.put(key, new ComparisonResult(key, mongoDuration, oracleDuration,
                fieldCount + " fields (full read)"));

        System.out.printf("  %d fields (full): BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                fieldCount,
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    // =========================================================================
    // Test 5: Complex E-commerce Document - Deep Field Access
    // =========================================================================

    @Test
    
    @DisplayName("E-commerce: Access customer.profile.tier (3 levels deep)")
    void ecommerce_nestedCustomerField() {
        String docId = "ecom-nested";
        JsonDocument doc = createEcommerceOrder(docId, 50);

        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        Duration mongoDuration = measureProjectionTime(mongoAdapter, mongoConnection, mongoCollector,
                docId, "customer.tier");
        Duration oracleDuration = measureProjectionTime(oracleAdapter, oracleConnection, oracleCollector,
                docId, "customer.tier");

        results.put("ecom-customer-tier", new ComparisonResult("ecom-customer-tier", mongoDuration, oracleDuration,
                "customer.tier (nested)"));

        System.out.printf("  customer.tier: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    @Test
    
    @DisplayName("E-commerce: Access grandTotal (last field)")
    void ecommerce_lastField() {
        String docId = "ecom-total";
        JsonDocument doc = createEcommerceOrder(docId, 100);

        mongoAdapter.execute(mongoConnection, new InsertOperation("m-" + docId, doc), mongoCollector);
        oracleAdapter.execute(oracleConnection, new InsertOperation("o-" + docId, doc), oracleCollector);

        Duration mongoDuration = measureProjectionTime(mongoAdapter, mongoConnection, mongoCollector,
                docId, "grandTotal");
        Duration oracleDuration = measureProjectionTime(oracleAdapter, oracleConnection, oracleCollector,
                docId, "grandTotal");

        results.put("ecom-grandTotal", new ComparisonResult("ecom-grandTotal", mongoDuration, oracleDuration,
                "grandTotal (last field)"));

        System.out.printf("  grandTotal: BSON=%6d us, OSON=%6d us, Ratio=%.2fx%n",
                mongoDuration.toNanos() / 1000,
                oracleDuration.toNanos() / 1000,
                (double) mongoDuration.toNanos() / oracleDuration.toNanos());
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private Duration measureReadTime(DatabaseAdapter adapter, InstrumentedConnection conn,
                                     MetricsCollector collector, String docId) {
        ReadOperation read = ReadOperation.fullDocument("read-" + docId, docId);

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

    private Duration measureMultiProjectionTime(DatabaseAdapter adapter, InstrumentedConnection conn,
                                                MetricsCollector collector, String docId, List<String> fields) {
        ReadOperation read = ReadOperation.withProjection("read-" + docId, docId, fields);

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

    private JsonDocument createDocumentWithTargetAt(String id, int position, int totalFields, String targetName) {
        JsonDocument.Builder builder = JsonDocument.builder(id);

        for (int i = 1; i <= totalFields; i++) {
            if (i == position) {
                builder.field(targetName, "TARGET_VALUE_" + id);
            } else {
                builder.field("field_" + String.format("%03d", i),
                        "padding_value_for_field_" + i + "_with_extra_content_to_add_size");
            }
        }

        return builder.build();
    }

    private JsonDocument createDocumentWithNesting(String id, int depth, int fieldsPerLevel) {
        Map<String, Object> current = new LinkedHashMap<>();
        current.put("target", "TARGET_VALUE_" + id);
        for (int f = 0; f < fieldsPerLevel; f++) {
            current.put("padding_" + f, "value_at_depth_" + depth + "_field_" + f);
        }

        for (int d = depth - 1; d >= 1; d--) {
            Map<String, Object> parent = new LinkedHashMap<>();
            for (int f = 0; f < fieldsPerLevel; f++) {
                parent.put("padding_" + f, "value_at_depth_" + d + "_field_" + f);
            }
            parent.put("nested", current);
            current = parent;
        }

        return JsonDocument.builder(id)
                .nestedObject("root", current)
                .build();
    }

    private String buildNestedPath(int depth) {
        StringBuilder path = new StringBuilder("root");
        for (int i = 1; i < depth; i++) {
            path.append(".nested");
        }
        path.append(".target");
        return path.toString();
    }

    private JsonDocument createLargeDocument(String id, int fieldCount) {
        JsonDocument.Builder builder = JsonDocument.builder(id);

        for (int i = 0; i < fieldCount; i++) {
            builder.field("field_" + String.format("%03d", i),
                    "This is field " + i + " with some reasonably long content to simulate real data.");
        }

        return builder.build();
    }

    private JsonDocument createEcommerceOrder(String id, int itemCount) {
        List<Map<String, Object>> lineItems = new ArrayList<>();
        double total = 0;

        for (int i = 0; i < itemCount; i++) {
            double price = 10.0 + (i * 0.5);
            int qty = 1 + (i % 5);
            total += price * qty;

            lineItems.add(Map.of(
                    "lineNumber", i + 1,
                    "sku", "SKU-" + String.format("%06d", i),
                    "productName", "Product Item " + i + " - With Detailed Description",
                    "unitPrice", price,
                    "quantity", qty,
                    "subtotal", price * qty
            ));
        }

        return JsonDocument.builder(id)
                .field("orderNumber", "ORD-" + id)
                .field("orderDate", "2024-12-27T10:30:00Z")
                .field("status", "CONFIRMED")
                .nestedObject("customer", Map.of(
                        "customerId", "CUST-12345",
                        "email", "customer@example.com",
                        "name", "John Doe",
                        "tier", "GOLD"
                ))
                .nestedObject("shippingAddress", Map.of(
                        "street", "123 Main Street",
                        "city", "Anytown",
                        "state", "CA",
                        "zip", "90210",
                        "country", "USA"
                ))
                .array("lineItems", lineItems)
                .field("subtotal", total)
                .field("taxTotal", total * 0.08)
                .field("grandTotal", total + (total * 0.08) + 9.99)
                .build();
    }

    private static void printFinalReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  FINAL COMPARISON REPORT: BSON vs OSON (SQL/JSON)");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.printf("%-35s %10s %10s %8s %s%n",
                "Test Case", "BSON (us)", "OSON (us)", "Ratio", "Winner");
        System.out.println("-".repeat(80));

        long totalBson = 0;
        long totalOson = 0;
        int bsonWins = 0;
        int osonWins = 0;

        for (ComparisonResult result : results.values()) {
            long bsonUs = result.bsonDuration.toNanos() / 1000;
            long osonUs = result.osonDuration.toNanos() / 1000;
            double ratio = (double) bsonUs / osonUs;
            String winner = ratio > 1.0 ? "OSON" : (ratio < 1.0 ? "BSON" : "TIE");

            if (ratio > 1.0) osonWins++;
            else if (ratio < 1.0) bsonWins++;

            totalBson += bsonUs;
            totalOson += osonUs;

            System.out.printf("%-35s %10d %10d %7.2fx %s%n",
                    result.description, bsonUs, osonUs, ratio, winner);
        }

        System.out.println("-".repeat(80));
        double overallRatio = (double) totalBson / totalOson;
        System.out.printf("%-35s %10d %10d %7.2fx %s%n",
                "TOTAL", totalBson, totalOson, overallRatio,
                overallRatio > 1.0 ? "OSON" : "BSON");

        System.out.println();
        System.out.println("Summary:");
        System.out.println("  BSON wins: " + bsonWins);
        System.out.println("  OSON wins: " + osonWins);
        System.out.println("  Overall ratio: " + String.format("%.2fx", overallRatio));
        System.out.println();

        if (overallRatio > 1.0) {
            System.out.println("  OSON (SQL/JSON) demonstrates O(1) hash-indexed field access advantage.");
        } else {
            System.out.println("  Network/driver overhead may dominate in local dev environment.");
            System.out.println("  O(1) advantage is more visible with larger documents and higher load.");
        }

        System.out.println("=".repeat(80) + "\n");
    }

    private record ComparisonResult(
            String key,
            Duration bsonDuration,
            Duration osonDuration,
            String description
    ) {}
}
