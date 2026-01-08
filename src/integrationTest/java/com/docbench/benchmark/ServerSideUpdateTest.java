package com.docbench.benchmark;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import oracle.jdbc.pool.OracleDataSource;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

/**
 * Server-Side Update Test: Compares database-native update operations.
 *
 * This test measures the actual database update path:
 * - Oracle: JSON_TRANSFORM for partial/in-place OSON updates
 * - MongoDB: $set operator for field updates
 */
@DisplayName("Server-Side Update: JSON_TRANSFORM vs MongoDB $set")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ServerSideUpdateTest {

    // Configuration
    private static final int WARMUP_ITERATIONS = 100;
    private static final int MEASUREMENT_ITERATIONS = 1_000;
    // Array tests use fewer iterations since each push permanently grows the array
    private static final int ARRAY_WARMUP_ITERATIONS = 10;
    private static final int ARRAY_MEASUREMENT_ITERATIONS = 100;

    // Database connections
    private static MongoClient mongoClient;
    private static MongoCollection<Document> mongoCollection;
    private static Connection oracleConnection;

    private static final String ORACLE_TABLE = "server_update_test";

    // Results storage
    private static final Map<String, TestResult> results = new LinkedHashMap<>();

    // AWR snapshot tracking - stores [beforeSnapId, afterSnapId] for each category
    private static final Map<String, long[]> awrSnapshots = new LinkedHashMap<>();
    private static final String AWR_REPORT_DIR = "build/reports/awr";
    private static boolean awrEnabled = false;
    private static long dbId = 0;
    private static long instanceNumber = 1;

    private record TestResult(String testId, String description, long mongoNanos, long oracleNanos, String testType) {}

    @BeforeAll
    static void setup() throws SQLException {
        Properties props = loadConfigProperties();

        // MongoDB setup with production-like write concern (w:1, j:true)
        // This ensures durability parity with Oracle (which always waits for redo log sync)
        String mongoUri = props.getProperty("mongodb.uri");
        String mongoDbName = props.getProperty("mongodb.database", "testdb");
        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
        mongoDb.getCollection("server_update_test").drop();
        // Configure write concern: w=1 (primary ack), j=true (wait for journal sync)
        WriteConcern durableWriteConcern = WriteConcern.W1.withJournal(true);
        mongoCollection = mongoDb.getCollection("server_update_test").withWriteConcern(durableWriteConcern);

        // Oracle setup with optimizations:
        // 1. OracleDataSource with statement caching
        // 2. Native JSON type binding property
        String oracleUrl = props.getProperty("oracle.url");
        String oracleUser = props.getProperty("oracle.username");
        String oraclePass = props.getProperty("oracle.password");

        OracleDataSource ods = new OracleDataSource();
        ods.setURL(oracleUrl);
        ods.setUser(oracleUser);
        ods.setPassword(oraclePass);

        // Enable implicit statement caching for prepared statement reuse
        ods.setImplicitCachingEnabled(true);

        oracleConnection = ods.getConnection();
        oracleConnection.setAutoCommit(true);

        // Configure statement cache size on the connection
        if (oracleConnection.isWrapperFor(oracle.jdbc.OracleConnection.class)) {
            oracle.jdbc.OracleConnection oraConn = oracleConnection.unwrap(oracle.jdbc.OracleConnection.class);
            oraConn.setStatementCacheSize(20);
        }

        // Create Oracle table with JSON column
        try (var stmt = oracleConnection.createStatement()) {
            stmt.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + ORACLE_TABLE + "'; EXCEPTION WHEN OTHERS THEN NULL; END;");
            stmt.execute("CREATE TABLE " + ORACLE_TABLE + " (id VARCHAR2(100) PRIMARY KEY, doc JSON)");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  SERVER-SIDE UPDATE TEST: JSON_TRANSFORM vs MongoDB $set");
        System.out.println("  " + "-".repeat(74));
        System.out.println("  Oracle: JSON_TRANSFORM (partial OSON update)");
        System.out.println("    - Statement caching enabled (OracleDataSource)");
        System.out.println("    - ps.setString for scalar bind values (Oracle team recommendation)");
        System.out.println("  MongoDB: $set operator");
        System.out.println("    - WriteConcern: w=1, j=true (journal sync for durability parity)");
        System.out.println("    - Single-member replica set configuration");
        System.out.println("=".repeat(80));

        // Global warmup
        System.out.println("\nRunning global warmup...");
        setupAndWarmup();
        System.out.println("Global warmup complete.\n");

        // Initialize AWR
        initializeAwr();
    }

    @AfterAll
    static void teardown() throws SQLException {
        printFinalReport();

        // Generate AWR reports
        if (awrEnabled) {
            generateAwrReports();
        }

        if (mongoClient != null) {
            mongoClient.getDatabase("testdb").getCollection("server_update_test").drop();
            mongoClient.close();
        }
        if (oracleConnection != null) {
            try (var stmt = oracleConnection.createStatement()) {
                stmt.execute("DROP TABLE " + ORACLE_TABLE);
            } catch (SQLException ignored) {}
            oracleConnection.close();
        }
    }

    private static Properties loadConfigProperties() {
        Path configPath = Path.of("config/local.properties");
        if (Files.exists(configPath)) {
            try (InputStream is = Files.newInputStream(configPath)) {
                Properties props = new Properties();
                props.load(is);
                return props;
            } catch (IOException e) {
                throw new RuntimeException("Could not load config", e);
            }
        }
        throw new RuntimeException("config/local.properties not found");
    }

    private static void setupAndWarmup() throws SQLException {
        // Create a warmup document
        String warmupId = "warmup-doc";
        Document warmupDoc = new Document("_id", warmupId);
        StringBuilder oracleJson = new StringBuilder("{\"_id\":\"" + warmupId + "\"");
        for (int i = 0; i < 100; i++) {
            warmupDoc.append("field_" + i, "value_" + i);
            oracleJson.append(",\"field_").append(i).append("\":\"value_").append(i).append("\"");
        }
        oracleJson.append("}");

        mongoCollection.insertOne(warmupDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, warmupId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Warmup queries - Oracle team recommendation: use ps.setString for scalar values
        for (int i = 0; i < 50; i++) {
            mongoCollection.updateOne(eq("_id", warmupId), Updates.set("field_50", "warmup_" + i));

            try (PreparedStatement ps = oracleConnection.prepareStatement(
                    "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, SET '$.field_50' = ?) WHERE id = ?")) {
                // Oracle team: use ps.setString for scalar values instead of OracleJsonValue
                ps.setString(1, "warmup_" + i);
                ps.setString(2, warmupId);
                ps.executeUpdate();
            }
        }

        // Clean up warmup doc
        mongoCollection.deleteOne(eq("_id", warmupId));
        try (var stmt = oracleConnection.createStatement()) {
            stmt.execute("DELETE FROM " + ORACLE_TABLE + " WHERE id = 'warmup-doc'");
        }
    }

    // =========================================================================
    // Test 0: Protocol Overhead Baseline (non-JSON operations)
    // =========================================================================

    @Test
    @Order(0)
    @DisplayName("Protocol overhead baseline")
    void protocolOverheadBaseline() throws SQLException {
        awrSnapshotBefore("00_baseline");
        measureProtocolOverhead();
        awrSnapshotAfter("00_baseline");
    }

    private static long mongoBaselineNanos;
    private static long oracleBaselineNanos;

    private void measureProtocolOverhead() throws SQLException {
        // Create a simple document with a counter field
        String testId = "baseline-test";
        Document mongoDoc = new Document("_id", testId).append("counter", 0);
        mongoCollection.insertOne(mongoDoc);

        try (var stmt = oracleConnection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS baseline_test (id VARCHAR2(100) PRIMARY KEY, counter NUMBER)");
            stmt.execute("INSERT INTO baseline_test (id, counter) VALUES ('" + testId + "', 0)");
        }

        // Measure MongoDB $inc (simplest possible update)
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            mongoCollection.updateOne(eq("_id", testId), Updates.inc("counter", 1));
        }
        long mongoTotal = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            mongoCollection.updateOne(eq("_id", testId), Updates.inc("counter", 1));
            mongoTotal += System.nanoTime() - start;
        }
        mongoBaselineNanos = mongoTotal / MEASUREMENT_ITERATIONS;

        // Measure Oracle simple UPDATE (no JSON)
        String sql = "UPDATE baseline_test SET counter = counter + 1 WHERE id = ?";
        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                ps.setString(1, testId);
                ps.executeUpdate();
            }
            long oracleTotal = 0;
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                ps.setString(1, testId);
                long start = System.nanoTime();
                ps.executeUpdate();
                oracleTotal += System.nanoTime() - start;
            }
            oracleBaselineNanos = oracleTotal / MEASUREMENT_ITERATIONS;
        }

        // Cleanup
        mongoCollection.deleteOne(eq("_id", testId));
        try (var stmt = oracleConnection.createStatement()) {
            stmt.execute("DROP TABLE baseline_test");
        }

        System.out.println("  PROTOCOL OVERHEAD BASELINE (non-JSON operations):");
        System.out.printf("    MongoDB $inc baseline         : %8d ns%n", mongoBaselineNanos);
        System.out.printf("    Oracle UPDATE baseline        : %8d ns%n", oracleBaselineNanos);
        System.out.printf("    Oracle overhead vs MongoDB    : %8d ns (%.2fx)%n",
                oracleBaselineNanos - mongoBaselineNanos,
                (double) oracleBaselineNanos / mongoBaselineNanos);
        System.out.println();

        results.put("baseline", new TestResult("baseline", "Protocol baseline", mongoBaselineNanos, oracleBaselineNanos, "baseline"));
    }

    // =========================================================================
    // Test 1: Single Field Update
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Single field update - 100 field document")
    void singleFieldUpdate_100fields() throws SQLException {
        awrSnapshotBefore("01_single_field");
        testSingleFieldUpdate("single-100", 100, 50);
    }

    @Test
    @Order(2)
    @DisplayName("Single field update - 500 field document")
    void singleFieldUpdate_500fields() throws SQLException {
        testSingleFieldUpdate("single-500", 500, 250);
    }

    @Test
    @Order(3)
    @DisplayName("Single field update - 1000 field document")
    void singleFieldUpdate_1000fields() throws SQLException {
        testSingleFieldUpdate("single-1000", 1000, 500);
        awrSnapshotAfter("01_single_field");
    }

    private void testSingleFieldUpdate(String testId, int totalFields, int targetField) throws SQLException {
        // Create test document
        createTestDocument(testId, totalFields);
        String fieldName = "field_" + targetField;

        // Measure MongoDB $set
        long mongoNanos = measureMongoSingleUpdate(testId, fieldName);

        // Measure Oracle JSON_TRANSFORM
        long oracleNanos = measureOracleSingleUpdate(testId, fieldName);

        String description = "Single update " + totalFields + " fields";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "single"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);

        // Cleanup
        cleanupTestDocument(testId);
    }

    private long measureMongoSingleUpdate(String docId, String fieldName) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
            mongoCollection.updateOne(eq("_id", docId), Updates.set(fieldName, value));
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
            long start = System.nanoTime();
            mongoCollection.updateOne(eq("_id", docId), Updates.set(fieldName, value));
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOracleSingleUpdate(String docId, String fieldName) throws SQLException {
        // Oracle team recommendation: use ps.setString for scalar values
        String sql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, SET '$." + fieldName + "' = ?) WHERE id = ?";

        // Use single PreparedStatement for all iterations (like MongoDB's connection pooling)
        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
                // Oracle team: use ps.setString for scalar values instead of OracleJsonValue
                ps.setString(1, value);
                ps.setString(2, docId);
                ps.executeUpdate();
            }

            // Measure
            long totalNanos = 0;
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
                // Oracle team: use ps.setString for scalar values
                ps.setString(1, value);
                ps.setString(2, docId);
                long start = System.nanoTime();
                ps.executeUpdate();
                totalNanos += System.nanoTime() - start;
            }

            return totalNanos / MEASUREMENT_ITERATIONS;
        }
    }

    // =========================================================================
    // Test 2: Multiple Field Update
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Multi-field update - 3 fields in 100 field doc")
    void multiFieldUpdate_3fields() throws SQLException {
        awrSnapshotBefore("02_multi_field");
        testMultiFieldUpdate("multi-3", 100, new int[]{10, 50, 90});
    }

    @Test
    @Order(11)
    @DisplayName("Multi-field update - 5 fields in 100 field doc")
    void multiFieldUpdate_5fields() throws SQLException {
        testMultiFieldUpdate("multi-5", 100, new int[]{10, 30, 50, 70, 90});
    }

    @Test
    @Order(12)
    @DisplayName("Multi-field update - 10 fields in 100 field doc")
    void multiFieldUpdate_10fields() throws SQLException {
        testMultiFieldUpdate("multi-10", 100, new int[]{5, 15, 25, 35, 45, 55, 65, 75, 85, 95});
        awrSnapshotAfter("02_multi_field");
    }

    private void testMultiFieldUpdate(String testId, int totalFields, int[] targetFields) throws SQLException {
        // Create test document
        createTestDocument(testId, totalFields);

        String[] fieldNames = new String[targetFields.length];
        for (int i = 0; i < targetFields.length; i++) {
            fieldNames[i] = "field_" + targetFields[i];
        }

        // Measure MongoDB $set (multiple fields)
        long mongoNanos = measureMongoMultiUpdate(testId, fieldNames);

        // Measure Oracle JSON_TRANSFORM (multiple SET operations)
        long oracleNanos = measureOracleMultiUpdate(testId, fieldNames);

        String description = "Multi-update " + targetFields.length + " fields";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "multi"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);

        // Cleanup
        cleanupTestDocument(testId);
    }

    private long measureMongoMultiUpdate(String docId, String[] fieldNames) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
            List<Bson> updates = new ArrayList<>();
            for (String fieldName : fieldNames) {
                updates.add(Updates.set(fieldName, value));
            }
            mongoCollection.updateOne(eq("_id", docId), Updates.combine(updates));
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
            List<Bson> updates = new ArrayList<>();
            for (String fieldName : fieldNames) {
                updates.add(Updates.set(fieldName, value));
            }
            long start = System.nanoTime();
            mongoCollection.updateOne(eq("_id", docId), Updates.combine(updates));
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOracleMultiUpdate(String docId, String[] fieldNames) throws SQLException {
        // Build JSON_TRANSFORM with multiple SET operations
        StringBuilder sql = new StringBuilder("UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc");
        for (int i = 0; i < fieldNames.length; i++) {
            sql.append(", SET '$.").append(fieldNames[i]).append("' = ?");
        }
        sql.append(") WHERE id = ?");

        // Use single PreparedStatement for all iterations
        try (PreparedStatement ps = oracleConnection.prepareStatement(sql.toString())) {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
                // Oracle team: use ps.setString for scalar values
                for (int j = 0; j < fieldNames.length; j++) {
                    ps.setString(j + 1, value);
                }
                ps.setString(fieldNames.length + 1, docId);
                ps.executeUpdate();
            }

            // Measure
            long totalNanos = 0;
            for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";
                // Oracle team: use ps.setString for scalar values
                for (int j = 0; j < fieldNames.length; j++) {
                    ps.setString(j + 1, value);
                }
                ps.setString(fieldNames.length + 1, docId);
                long start = System.nanoTime();
                ps.executeUpdate();
                totalNanos += System.nanoTime() - start;
            }

            return totalNanos / MEASUREMENT_ITERATIONS;
        }
    }

    // =========================================================================
    // Test 3: Large Document Updates (where partial update shines)
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("Large doc update - 10KB document")
    void largeDocUpdate_10kb() throws SQLException {
        awrSnapshotBefore("03a_large_doc_10KB");
        testLargeDocUpdate("large-10kb", 10 * 1024);
        awrSnapshotAfter("03a_large_doc_10KB");
    }

    @Test
    @Order(21)
    @DisplayName("Large doc update - 50KB document")
    void largeDocUpdate_50kb() throws SQLException {
        awrSnapshotBefore("03b_large_doc_50KB");
        testLargeDocUpdate("large-50kb", 50 * 1024);
        awrSnapshotAfter("03b_large_doc_50KB");
    }

    @Test
    @Order(22)
    @DisplayName("Large doc update - 100KB document")
    void largeDocUpdate_100kb() throws SQLException {
        awrSnapshotBefore("03c_large_doc_100KB");
        testLargeDocUpdate("large-100kb", 100 * 1024);
        awrSnapshotAfter("03c_large_doc_100KB");
    }

    @Test
    @Order(23)
    @DisplayName("Large doc update - 1MB document")
    void largeDocUpdate_1mb() throws SQLException {
        awrSnapshotBefore("03d_large_doc_1MB");
        testLargeDocUpdate("large-1mb", 1024 * 1024);
        awrSnapshotAfter("03d_large_doc_1MB");
    }

    @Test
    @Order(24)
    @DisplayName("Large doc update - 4MB document")
    void largeDocUpdate_4mb() throws SQLException {
        awrSnapshotBefore("03e_large_doc_4MB");
        testLargeDocUpdate("large-4mb", 4 * 1024 * 1024);
        awrSnapshotAfter("03e_large_doc_4MB");
    }

    private void testLargeDocUpdate(String testId, int targetSizeBytes) throws SQLException {
        // Create large document with padding to reach target size
        int fieldCount = 50;
        int paddingPerField = (targetSizeBytes / fieldCount) - 20; // Account for field name overhead

        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{\"_id\":\"" + testId + "\"");

        for (int i = 0; i < fieldCount; i++) {
            String padding = "X".repeat(Math.max(10, paddingPerField));
            String value = "value_" + i + "_" + padding;
            mongoDoc.append("field_" + i, value);
            oracleJson.append(",\"field_").append(i).append("\":\"").append(value).append("\"");
        }
        oracleJson.append("}");

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Update a single small field in the large document
        String fieldName = "field_25";

        // Measure MongoDB
        long mongoNanos = measureMongoSingleUpdate(testId, fieldName);

        // Measure Oracle JSON_TRANSFORM
        long oracleNanos = measureOracleSingleUpdate(testId, fieldName);

        String description = "Large doc ~" + (targetSizeBytes / 1024) + "KB";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "large"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);

        // Cleanup
        cleanupTestDocument(testId);
    }

    // =========================================================================
    // Test 4: Array Scalar Push (simple string values)
    // =========================================================================

    @Test
    @Order(30)
    @DisplayName("Scalar array push - 1 element")
    void scalarArrayPush_1() throws SQLException {
        awrSnapshotBefore("04_array_push");
        testScalarArrayPush("scalar-push-1", 1);
    }

    @Test
    @Order(31)
    @DisplayName("Scalar array push - 10 elements")
    void scalarArrayPush_10() throws SQLException {
        testScalarArrayPush("scalar-push-10", 10);
    }

    private void testScalarArrayPush(String testId, int elementsPerIteration) throws SQLException {
        String mongoDocId = testId + "-mongo";
        String oracleDocId = testId + "-oracle";

        createDocumentWithScalarArray(mongoDocId, 10);
        long mongoNanos = measureMongoScalarPush(mongoDocId, elementsPerIteration);
        cleanupTestDocument(mongoDocId);

        createDocumentWithScalarArray(oracleDocId, 10);
        long oracleNanos = measureOracleScalarPush(oracleDocId, elementsPerIteration);
        cleanupTestDocument(oracleDocId);

        String description = "Scalar push " + elementsPerIteration + "x1";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "array-scalar"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);
    }

    // =========================================================================
    // Test 5: Array Complex Object Push (nested documents)
    // =========================================================================

    @Test
    @Order(35)
    @DisplayName("Object array push - 1 element")
    void objectArrayPush_1() throws SQLException {
        testObjectArrayPush("object-push-1", 1);
    }

    @Test
    @Order(36)
    @DisplayName("Object array push - 10 elements")
    void objectArrayPush_10() throws SQLException {
        testObjectArrayPush("object-push-10", 10);
        awrSnapshotAfter("04_array_push");
    }

    private void testObjectArrayPush(String testId, int elementsPerIteration) throws SQLException {
        String mongoDocId = testId + "-mongo";
        String oracleDocId = testId + "-oracle";

        createDocumentWithObjectArray(mongoDocId, 10);
        long mongoNanos = measureMongoObjectPush(mongoDocId, elementsPerIteration);
        cleanupTestDocument(mongoDocId);

        createDocumentWithObjectArray(oracleDocId, 10);
        long oracleNanos = measureOracleObjectPush(oracleDocId, elementsPerIteration);
        cleanupTestDocument(oracleDocId);

        String description = "Object push " + elementsPerIteration + "x1";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "array-object"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);
    }

    // =========================================================================
    // Test 6: Array Scalar Delete
    // =========================================================================

    @Test
    @Order(40)
    @DisplayName("Scalar array delete - beginning")
    void scalarArrayDelete_beginning() throws SQLException {
        awrSnapshotBefore("05_array_delete");
        testScalarArrayDelete("scalar-del-begin", "beginning");
    }

    @Test
    @Order(41)
    @DisplayName("Scalar array delete - middle")
    void scalarArrayDelete_middle() throws SQLException {
        testScalarArrayDelete("scalar-del-middle", "middle");
    }

    @Test
    @Order(42)
    @DisplayName("Scalar array delete - end")
    void scalarArrayDelete_end() throws SQLException {
        testScalarArrayDelete("scalar-del-end", "end");
    }

    private void testScalarArrayDelete(String testId, String position) throws SQLException {
        String mongoDocId = testId + "-mongo";
        String oracleDocId = testId + "-oracle";

        createDocumentWithScalarArray(mongoDocId, 100);
        long mongoNanos = measureMongoScalarDelete(mongoDocId, position);
        cleanupTestDocument(mongoDocId);

        createDocumentWithScalarArray(oracleDocId, 100);
        long oracleNanos = measureOracleScalarDelete(oracleDocId, position);
        cleanupTestDocument(oracleDocId);

        String description = "Scalar delete " + position;
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "array-scalar"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);
    }

    // =========================================================================
    // Test 7: Array Complex Object Delete
    // =========================================================================

    @Test
    @Order(45)
    @DisplayName("Object array delete - beginning")
    void objectArrayDelete_beginning() throws SQLException {
        testObjectArrayDelete("object-del-begin", "beginning");
    }

    @Test
    @Order(46)
    @DisplayName("Object array delete - middle")
    void objectArrayDelete_middle() throws SQLException {
        testObjectArrayDelete("object-del-middle", "middle");
    }

    @Test
    @Order(47)
    @DisplayName("Object array delete - end")
    void objectArrayDelete_end() throws SQLException {
        testObjectArrayDelete("object-del-end", "end");
        awrSnapshotAfter("05_array_delete");
    }

    private void testObjectArrayDelete(String testId, String position) throws SQLException {
        String mongoDocId = testId + "-mongo";
        String oracleDocId = testId + "-oracle";

        createDocumentWithObjectArray(mongoDocId, 100);
        long mongoNanos = measureMongoObjectDelete(mongoDocId, position);
        cleanupTestDocument(mongoDocId);

        createDocumentWithObjectArray(oracleDocId, 100);
        long oracleNanos = measureOracleObjectDelete(oracleDocId, position);
        cleanupTestDocument(oracleDocId);

        String description = "Object delete " + position;
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "array-object"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);
    }

    // =========================================================================
    // Test 8: Large Document Array Operations (1MB and 4MB)
    // =========================================================================

    @Test
    @Order(50)
    @DisplayName("Large array update - 1MB scalar array")
    void largeArrayUpdate_1mb_scalar() throws SQLException {
        awrSnapshotBefore("06_large_array");
        testLargeArrayUpdate("large-array-1mb-scalar", 1024 * 1024, false);
    }

    @Test
    @Order(51)
    @DisplayName("Large array update - 4MB scalar array")
    void largeArrayUpdate_4mb_scalar() throws SQLException {
        testLargeArrayUpdate("large-array-4mb-scalar", 4 * 1024 * 1024, false);
    }

    @Test
    @Order(52)
    @DisplayName("Large array update - 1MB object array")
    void largeArrayUpdate_1mb_object() throws SQLException {
        testLargeArrayUpdate("large-array-1mb-object", 1024 * 1024, true);
    }

    @Test
    @Order(53)
    @DisplayName("Large array update - 4MB object array")
    void largeArrayUpdate_4mb_object() throws SQLException {
        testLargeArrayUpdate("large-array-4mb-object", 4 * 1024 * 1024, true);
        awrSnapshotAfter("06_large_array");
    }

    private void testLargeArrayUpdate(String testId, int targetSizeBytes, boolean useObjects) throws SQLException {
        String mongoDocId = testId + "-mongo";
        String oracleDocId = testId + "-oracle";

        // Create large array documents
        if (useObjects) {
            createLargeObjectArrayDocument(mongoDocId, targetSizeBytes);
            createLargeObjectArrayDocument(oracleDocId, targetSizeBytes);
        } else {
            createLargeScalarArrayDocument(mongoDocId, targetSizeBytes);
            createLargeScalarArrayDocument(oracleDocId, targetSizeBytes);
        }

        // Measure single element push to large array
        long mongoNanos, oracleNanos;
        if (useObjects) {
            mongoNanos = measureMongoObjectPush(mongoDocId, 1);
            oracleNanos = measureOracleObjectPush(oracleDocId, 1);
        } else {
            mongoNanos = measureMongoScalarPush(mongoDocId, 1);
            oracleNanos = measureOracleScalarPush(oracleDocId, 1);
        }

        cleanupTestDocument(mongoDocId);
        cleanupTestDocument(oracleDocId);

        String sizeLabel = targetSizeBytes >= 1024 * 1024
                ? (targetSizeBytes / (1024 * 1024)) + "MB"
                : (targetSizeBytes / 1024) + "KB";
        String typeLabel = useObjects ? "object" : "scalar";
        String description = "Large " + sizeLabel + " " + typeLabel + " array";
        results.put(testId, new TestResult(testId, description, mongoNanos, oracleNanos, "large-array"));

        double ratio = (double) oracleNanos / Math.max(1, mongoNanos);
        String winner = ratio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("  %-35s: MongoDB=%8d ns, OSON=%8d ns, %6.2fx %s%n",
                description, mongoNanos, oracleNanos, ratio, winner);
    }

    private void createLargeScalarArrayDocument(String testId, int targetSizeBytes) throws SQLException {
        // Calculate number of elements needed to reach target size
        // Each scalar element is ~50 bytes on average
        int avgElementSize = 50;
        int elementCount = targetSizeBytes / avgElementSize;

        List<String> initialArray = new ArrayList<>(elementCount);
        StringBuilder oracleArray = new StringBuilder("[");

        for (int i = 0; i < elementCount; i++) {
            String value = "item_" + i + "_padding_" + String.format("%020d", i);
            initialArray.add(value);
            if (i > 0) oracleArray.append(",");
            oracleArray.append("\"").append(value).append("\"");
        }
        oracleArray.append("]");

        Document mongoDoc = new Document("_id", testId)
                .append("items", initialArray)
                .append("counter", 0);

        String oracleJson = "{\"_id\":\"" + testId + "\",\"items\":" + oracleArray + ",\"counter\":0}";

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }
    }

    private void createLargeObjectArrayDocument(String testId, int targetSizeBytes) throws SQLException {
        // Calculate number of elements needed to reach target size
        // Each object element is ~150 bytes on average
        int avgElementSize = 150;
        int elementCount = targetSizeBytes / avgElementSize;

        List<Document> initialArray = new ArrayList<>(elementCount);
        StringBuilder oracleArray = new StringBuilder("[");

        for (int i = 0; i < elementCount; i++) {
            Document obj = new Document()
                    .append("id", i)
                    .append("name", "item_" + i)
                    .append("metadata", new Document()
                            .append("created", "2024-01-01")
                            .append("tags", List.of("tag1", "tag2", "tag3"))
                            .append("priority", i % 5));
            initialArray.add(obj);

            if (i > 0) oracleArray.append(",");
            oracleArray.append("{\"id\":").append(i)
                    .append(",\"name\":\"item_").append(i).append("\"")
                    .append(",\"metadata\":{\"created\":\"2024-01-01\",\"tags\":[\"tag1\",\"tag2\",\"tag3\"],\"priority\":")
                    .append(i % 5).append("}}");
        }
        oracleArray.append("]");

        Document mongoDoc = new Document("_id", testId)
                .append("items", initialArray)
                .append("counter", 0);

        String oracleJson = "{\"_id\":\"" + testId + "\",\"items\":" + oracleArray + ",\"counter\":0}";

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }
    }

    // =========================================================================
    // Scalar Array Helper Methods
    // =========================================================================

    private void createDocumentWithScalarArray(String testId, int initialArraySize) throws SQLException {
        List<String> initialArray = new ArrayList<>();
        StringBuilder oracleArray = new StringBuilder("[");
        for (int i = 0; i < initialArraySize; i++) {
            initialArray.add("item_" + i);
            if (i > 0) oracleArray.append(",");
            oracleArray.append("\"item_").append(i).append("\"");
        }
        oracleArray.append("]");

        Document mongoDoc = new Document("_id", testId)
                .append("items", initialArray)
                .append("counter", 0);

        String oracleJson = "{\"_id\":\"" + testId + "\",\"items\":" + oracleArray + ",\"counter\":0}";

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }
    }

    private long measureMongoScalarPush(String docId, int elementsPerIteration) {
        for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
            for (int j = 0; j < elementsPerIteration; j++) {
                String value = (i % 2 == 0) ? "new_item_" + i + "_" + j : "new_item_" + i + "_" + j + "        ";
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", value));
            }
        }

        long totalNanos = 0;
        for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            for (int j = 0; j < elementsPerIteration; j++) {
                String value = (i % 2 == 0) ? "new_item_" + i + "_" + j : "new_item_" + i + "_" + j + "        ";
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", value));
            }
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
    }

    private long measureOracleScalarPush(String docId, int elementsPerIteration) throws SQLException {
        String sql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, APPEND '$.items' = ?) WHERE id = ?";

        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
                for (int j = 0; j < elementsPerIteration; j++) {
                    String value = (i % 2 == 0) ? "new_item_" + i + "_" + j : "new_item_" + i + "_" + j + "        ";
                    ps.setString(1, value);
                    ps.setString(2, docId);
                    ps.executeUpdate();
                }
            }

            long totalNanos = 0;
            for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
                long start = System.nanoTime();
                for (int j = 0; j < elementsPerIteration; j++) {
                    String value = (i % 2 == 0) ? "new_item_" + i + "_" + j : "new_item_" + i + "_" + j + "        ";
                    ps.setString(1, value);
                    ps.setString(2, docId);
                    ps.executeUpdate();
                }
                totalNanos += System.nanoTime() - start;
            }

            return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
        }
    }

    private long measureMongoScalarDelete(String docId, String position) {
        Bson update = switch (position) {
            case "beginning" -> Updates.popFirst("items");
            case "end" -> Updates.popLast("items");
            default -> Updates.popFirst("items");
        };

        for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
            if (position.equals("middle")) {
                mongoCollection.updateOne(eq("_id", docId), Updates.unset("items.50"));
                mongoCollection.updateOne(eq("_id", docId), Updates.pull("items", null));
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", "item_replaced_" + i));
            } else {
                mongoCollection.updateOne(eq("_id", docId), update);
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", "item_replaced_" + i));
            }
        }

        long totalNanos = 0;
        for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            if (position.equals("middle")) {
                mongoCollection.updateOne(eq("_id", docId), Updates.unset("items.50"));
                mongoCollection.updateOne(eq("_id", docId), Updates.pull("items", null));
            } else {
                mongoCollection.updateOne(eq("_id", docId), update);
            }
            totalNanos += System.nanoTime() - start;
            mongoCollection.updateOne(eq("_id", docId), Updates.push("items", "item_replaced_" + i));
        }

        return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
    }

    private long measureOracleScalarDelete(String docId, String position) throws SQLException {
        String jsonPath = switch (position) {
            case "beginning" -> "$.items[0]";
            case "end" -> "$.items[last]";
            default -> "$.items[50]";
        };

        String deleteSql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, REMOVE '" + jsonPath + "') WHERE id = ?";
        String appendSql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, APPEND '$.items' = ?) WHERE id = ?";

        try (PreparedStatement deletePs = oracleConnection.prepareStatement(deleteSql);
             PreparedStatement appendPs = oracleConnection.prepareStatement(appendSql)) {

            for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
                deletePs.setString(1, docId);
                deletePs.executeUpdate();
                appendPs.setString(1, "item_replaced_" + i);
                appendPs.setString(2, docId);
                appendPs.executeUpdate();
            }

            long totalNanos = 0;
            for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
                deletePs.setString(1, docId);
                long start = System.nanoTime();
                deletePs.executeUpdate();
                totalNanos += System.nanoTime() - start;
                appendPs.setString(1, "item_replaced_" + i);
                appendPs.setString(2, docId);
                appendPs.executeUpdate();
            }

            return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
        }
    }

    // =========================================================================
    // Complex Object Array Helper Methods
    // =========================================================================

    private void createDocumentWithObjectArray(String testId, int initialArraySize) throws SQLException {
        List<Document> initialArray = new ArrayList<>();
        StringBuilder oracleArray = new StringBuilder("[");
        for (int i = 0; i < initialArraySize; i++) {
            // Create complex object with nested fields
            Document obj = new Document()
                    .append("id", i)
                    .append("name", "item_" + i)
                    .append("metadata", new Document()
                            .append("created", "2024-01-01")
                            .append("tags", List.of("tag1", "tag2", "tag3"))
                            .append("priority", i % 5));
            initialArray.add(obj);

            if (i > 0) oracleArray.append(",");
            oracleArray.append("{\"id\":").append(i)
                    .append(",\"name\":\"item_").append(i).append("\"")
                    .append(",\"metadata\":{\"created\":\"2024-01-01\",\"tags\":[\"tag1\",\"tag2\",\"tag3\"],\"priority\":")
                    .append(i % 5).append("}}");
        }
        oracleArray.append("]");

        Document mongoDoc = new Document("_id", testId)
                .append("items", initialArray)
                .append("counter", 0);

        String oracleJson = "{\"_id\":\"" + testId + "\",\"items\":" + oracleArray + ",\"counter\":0}";

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }
    }

    private long measureMongoObjectPush(String docId, int elementsPerIteration) {
        for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
            for (int j = 0; j < elementsPerIteration; j++) {
                Document obj = new Document()
                        .append("id", 1000 + i * 100 + j)
                        .append("name", "new_item_" + i + "_" + j)
                        .append("metadata", new Document()
                                .append("created", "2024-01-02")
                                .append("tags", List.of("new", "dynamic"))
                                .append("priority", (i + j) % 5));
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", obj));
            }
        }

        long totalNanos = 0;
        for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            for (int j = 0; j < elementsPerIteration; j++) {
                Document obj = new Document()
                        .append("id", 1000 + i * 100 + j)
                        .append("name", "new_item_" + i + "_" + j)
                        .append("metadata", new Document()
                                .append("created", "2024-01-02")
                                .append("tags", List.of("new", "dynamic"))
                                .append("priority", (i + j) % 5));
                mongoCollection.updateOne(eq("_id", docId), Updates.push("items", obj));
            }
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
    }

    private long measureOracleObjectPush(String docId, int elementsPerIteration) throws SQLException {
        String sql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, APPEND '$.items' = JSON(?)) WHERE id = ?";

        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
                for (int j = 0; j < elementsPerIteration; j++) {
                    String objJson = "{\"id\":" + (1000 + i * 100 + j) +
                            ",\"name\":\"new_item_" + i + "_" + j + "\"" +
                            ",\"metadata\":{\"created\":\"2024-01-02\",\"tags\":[\"new\",\"dynamic\"],\"priority\":" +
                            ((i + j) % 5) + "}}";
                    ps.setString(1, objJson);
                    ps.setString(2, docId);
                    ps.executeUpdate();
                }
            }

            long totalNanos = 0;
            for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
                long start = System.nanoTime();
                for (int j = 0; j < elementsPerIteration; j++) {
                    String objJson = "{\"id\":" + (1000 + i * 100 + j) +
                            ",\"name\":\"new_item_" + i + "_" + j + "\"" +
                            ",\"metadata\":{\"created\":\"2024-01-02\",\"tags\":[\"new\",\"dynamic\"],\"priority\":" +
                            ((i + j) % 5) + "}}";
                    ps.setString(1, objJson);
                    ps.setString(2, docId);
                    ps.executeUpdate();
                }
                totalNanos += System.nanoTime() - start;
            }

            return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
        }
    }

    private long measureMongoObjectDelete(String docId, String position) {
        Bson update = switch (position) {
            case "beginning" -> Updates.popFirst("items");
            case "end" -> Updates.popLast("items");
            default -> Updates.popFirst("items");
        };

        for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
            if (position.equals("middle")) {
                mongoCollection.updateOne(eq("_id", docId), Updates.unset("items.50"));
                mongoCollection.updateOne(eq("_id", docId), Updates.pull("items", null));
            } else {
                mongoCollection.updateOne(eq("_id", docId), update);
            }
            // Re-add complex object
            Document obj = new Document()
                    .append("id", 2000 + i)
                    .append("name", "replaced_" + i)
                    .append("metadata", new Document()
                            .append("created", "2024-01-03")
                            .append("tags", List.of("replaced"))
                            .append("priority", i % 5));
            mongoCollection.updateOne(eq("_id", docId), Updates.push("items", obj));
        }

        long totalNanos = 0;
        for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            if (position.equals("middle")) {
                mongoCollection.updateOne(eq("_id", docId), Updates.unset("items.50"));
                mongoCollection.updateOne(eq("_id", docId), Updates.pull("items", null));
            } else {
                mongoCollection.updateOne(eq("_id", docId), update);
            }
            totalNanos += System.nanoTime() - start;

            Document obj = new Document()
                    .append("id", 2000 + i)
                    .append("name", "replaced_" + i)
                    .append("metadata", new Document()
                            .append("created", "2024-01-03")
                            .append("tags", List.of("replaced"))
                            .append("priority", i % 5));
            mongoCollection.updateOne(eq("_id", docId), Updates.push("items", obj));
        }

        return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
    }

    private long measureOracleObjectDelete(String docId, String position) throws SQLException {
        String jsonPath = switch (position) {
            case "beginning" -> "$.items[0]";
            case "end" -> "$.items[last]";
            default -> "$.items[50]";
        };

        String deleteSql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, REMOVE '" + jsonPath + "') WHERE id = ?";
        String appendSql = "UPDATE " + ORACLE_TABLE + " SET doc = JSON_TRANSFORM(doc, APPEND '$.items' = JSON(?)) WHERE id = ?";

        try (PreparedStatement deletePs = oracleConnection.prepareStatement(deleteSql);
             PreparedStatement appendPs = oracleConnection.prepareStatement(appendSql)) {

            for (int i = 0; i < ARRAY_WARMUP_ITERATIONS; i++) {
                deletePs.setString(1, docId);
                deletePs.executeUpdate();

                String objJson = "{\"id\":" + (2000 + i) +
                        ",\"name\":\"replaced_" + i + "\"" +
                        ",\"metadata\":{\"created\":\"2024-01-03\",\"tags\":[\"replaced\"],\"priority\":" +
                        (i % 5) + "}}";
                appendPs.setString(1, objJson);
                appendPs.setString(2, docId);
                appendPs.executeUpdate();
            }

            long totalNanos = 0;
            for (int i = 0; i < ARRAY_MEASUREMENT_ITERATIONS; i++) {
                deletePs.setString(1, docId);
                long start = System.nanoTime();
                deletePs.executeUpdate();
                totalNanos += System.nanoTime() - start;

                String objJson = "{\"id\":" + (2000 + i) +
                        ",\"name\":\"replaced_" + i + "\"" +
                        ",\"metadata\":{\"created\":\"2024-01-03\",\"tags\":[\"replaced\"],\"priority\":" +
                        (i % 5) + "}}";
                appendPs.setString(1, objJson);
                appendPs.setString(2, docId);
                appendPs.executeUpdate();
            }

            return totalNanos / ARRAY_MEASUREMENT_ITERATIONS;
        }
    }


    // =========================================================================
    // Helper Methods
    // =========================================================================

    private void createTestDocument(String testId, int totalFields) throws SQLException {
        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{\"_id\":\"" + testId + "\"");

        for (int i = 0; i < totalFields; i++) {
            String fieldName = "field_" + i;
            String value = "value_" + i + "_padding_data";
            mongoDoc.append(fieldName, value);
            oracleJson.append(",\"").append(fieldName).append("\":\"").append(value).append("\"");
        }
        oracleJson.append("}");

        mongoCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }
    }

    private void cleanupTestDocument(String testId) throws SQLException {
        mongoCollection.deleteOne(eq("_id", testId));
        try (var stmt = oracleConnection.createStatement()) {
            stmt.execute("DELETE FROM " + ORACLE_TABLE + " WHERE id = '" + testId + "'");
        }
    }

    // =========================================================================
    // Report Generation
    // =========================================================================

    private static void printFinalReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  FINAL RESULTS: SERVER-SIDE UPDATE PERFORMANCE");
        System.out.println("  MongoDB $set vs Oracle JSON_TRANSFORM");
        System.out.println("=".repeat(80));

        Map<String, List<TestResult>> grouped = new LinkedHashMap<>();
        for (TestResult result : results.values()) {
            grouped.computeIfAbsent(result.testType, k -> new ArrayList<>()).add(result);
        }

        long totalMongo = 0;
        long totalOracle = 0;
        int mongoWins = 0;
        int oracleWins = 0;

        for (Map.Entry<String, List<TestResult>> entry : grouped.entrySet()) {
            String type = entry.getKey();
            List<TestResult> typeResults = entry.getValue();

            String header = switch (type) {
                case "single" -> "SINGLE FIELD UPDATE";
                case "multi" -> "MULTI-FIELD UPDATE";
                case "large" -> "LARGE DOCUMENT UPDATE";
                case "baseline" -> "PROTOCOL OVERHEAD BASELINE";
                case "array-scalar" -> "SCALAR ARRAY OPERATIONS";
                case "array-object" -> "OBJECT ARRAY OPERATIONS";
                case "large-array" -> "LARGE ARRAY OPERATIONS";
                default -> type.toUpperCase();
            };

            System.out.println("\n" + header + ":");
            System.out.printf("%-35s %12s %12s %10s %s%n",
                    "Test Case", "MongoDB (ns)", "OSON (ns)", "Ratio", "Winner");
            System.out.println("-".repeat(80));

            for (TestResult result : typeResults) {
                double ratio = (double) result.oracleNanos / Math.max(1, result.mongoNanos);
                String winner = ratio > 1.0 ? "MongoDB" : "OSON";

                // Don't count baseline in wins/totals
                if (!result.testType.equals("baseline")) {
                    if (ratio > 1.0) mongoWins++;
                    else oracleWins++;

                    totalMongo += result.mongoNanos;
                    totalOracle += result.oracleNanos;
                }

                System.out.printf("%-35s %12d %12d %9.2fx %s%n",
                        result.description, result.mongoNanos, result.oracleNanos, ratio, winner);
            }
        }

        System.out.println("\n" + "=".repeat(80));
        double overallRatio = (double) totalOracle / Math.max(1, totalMongo);
        String overallWinner = overallRatio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("TOTAL %29s %12d %12d %9.2fx %s%n",
                "", totalMongo, totalOracle, overallRatio, overallWinner);

        System.out.println("\nSummary:");
        System.out.println("  MongoDB wins: " + mongoWins);
        System.out.println("  OSON wins: " + oracleWins);
        System.out.printf("  Overall: %s is %.2fx faster for server-side updates%n",
                overallWinner, overallRatio > 1.0 ? overallRatio : 1.0 / overallRatio);

        // Protocol overhead analysis
        if (mongoBaselineNanos > 0 && oracleBaselineNanos > 0) {
            printProtocolOverheadAnalysis(totalMongo, totalOracle, mongoWins, oracleWins);
        }

        System.out.println("=".repeat(80) + "\n");
    }

    private static void printProtocolOverheadAnalysis(long totalMongo, long totalOracle,
                                                       int mongoWins, int oracleWins) {
        System.out.println("\n" + "-".repeat(80));
        System.out.println("PROTOCOL OVERHEAD ANALYSIS");
        System.out.println("-".repeat(80));

        // Calculate the overhead difference (how much more Oracle's protocol costs)
        long oracleOverheadDelta = oracleBaselineNanos - mongoBaselineNanos;

        System.out.printf("  MongoDB baseline (non-JSON $inc): %,d ns%n", mongoBaselineNanos);
        System.out.printf("  Oracle baseline (non-JSON UPDATE): %,d ns%n", oracleBaselineNanos);
        System.out.printf("  Oracle protocol overhead delta: %,d ns (%.2fx MongoDB)%n",
                oracleOverheadDelta, (double) oracleBaselineNanos / mongoBaselineNanos);

        System.out.println("\nADJUSTED OSON PERFORMANCE (subtracting protocol overhead delta):");
        System.out.println("  If Oracle's protocol matched MongoDB's speed, OSON times would be:");

        // Show adjusted results for all non-baseline tests
        Map<String, List<TestResult>> grouped = new LinkedHashMap<>();
        for (TestResult result : results.values()) {
            if (!result.testType.equals("baseline")) {
                grouped.computeIfAbsent(result.testType, k -> new ArrayList<>()).add(result);
            }
        }

        long totalAdjustedOracle = 0;
        int adjustedOracleWins = 0;
        int adjustedMongoWins = 0;

        System.out.printf("\n%-35s %12s %12s %12s %10s%n",
                "Test Case", "MongoDB (ns)", "OSON Adj(ns)", "Saved (ns)", "Adj Ratio");
        System.out.println("-".repeat(80));

        for (Map.Entry<String, List<TestResult>> entry : grouped.entrySet()) {
            for (TestResult result : entry.getValue()) {
                // Adjusted Oracle time = actual - overhead delta (but not less than 0)
                long adjustedOracleNanos = Math.max(0, result.oracleNanos - oracleOverheadDelta);
                long saved = result.oracleNanos - adjustedOracleNanos;
                totalAdjustedOracle += adjustedOracleNanos;

                double adjustedRatio = (double) adjustedOracleNanos / Math.max(1, result.mongoNanos);
                if (adjustedRatio > 1.0) adjustedMongoWins++;
                else adjustedOracleWins++;

                String adjWinner = adjustedRatio > 1.0 ? "MongoDB" : "OSON";
                System.out.printf("%-35s %12d %12d %12d %9.2fx %s%n",
                        result.description, result.mongoNanos, adjustedOracleNanos, saved, adjustedRatio, adjWinner);
            }
        }

        System.out.println("-".repeat(80));
        double adjustedOverallRatio = (double) totalAdjustedOracle / Math.max(1, totalMongo);
        String adjustedOverallWinner = adjustedOverallRatio > 1.0 ? "MongoDB" : "OSON";
        System.out.printf("ADJUSTED TOTAL %20s %12d %12d %12d %9.2fx %s%n",
                "", totalMongo, totalAdjustedOracle, totalOracle - totalAdjustedOracle,
                adjustedOverallRatio, adjustedOverallWinner);

        System.out.println("\nAdjusted Summary:");
        System.out.println("  MongoDB wins (adjusted): " + adjustedMongoWins);
        System.out.println("  OSON wins (adjusted): " + adjustedOracleWins);
        System.out.printf("  If protocol overhead matched: %s would be %.2fx faster%n",
                adjustedOverallWinner, adjustedOverallRatio > 1.0 ? adjustedOverallRatio : 1.0 / adjustedOverallRatio);

        // Calculate what % of Oracle's time is pure protocol overhead
        double overheadPct = (double) oracleOverheadDelta / oracleBaselineNanos * 100;
        System.out.printf("\n  Analysis: Oracle's ~%,d ns protocol overhead accounts for ~%.0f%% of baseline ops%n",
                oracleOverheadDelta, overheadPct);
        System.out.println("  This overhead is a fixed cost regardless of JSON operation complexity.");

        // Generate HTML performance report
        generatePerformanceReport(totalMongo, totalOracle, mongoWins, oracleWins,
                totalAdjustedOracle, adjustedMongoWins, adjustedOracleWins, oracleOverheadDelta);
    }

    private static void generatePerformanceReport(long totalMongo, long totalOracle,
                                                   int mongoWins, int oracleWins,
                                                   long totalAdjustedOracle,
                                                   int adjustedMongoWins, int adjustedOracleWins,
                                                   long oracleOverheadDelta) {
        try {
            Path reportDir = Path.of("reports");
            Files.createDirectories(reportDir);

            StringBuilder html = new StringBuilder();
            html.append("""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>DocBench Performance Report - BSON vs OSON Server-Side Updates</title>
                    <style>
                        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                        h1 { color: #333; border-bottom: 3px solid #0066cc; padding-bottom: 10px; }
                        h2 { color: #0066cc; margin-top: 30px; }
                        h3 { color: #666; }
                        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
                        th, td { border: 1px solid #ddd; padding: 12px; text-align: right; }
                        th { background: #0066cc; color: white; }
                        td:first-child { text-align: left; font-weight: 500; }
                        tr:nth-child(even) { background: #f9f9f9; }
                        tr:hover { background: #f0f7ff; }
                        .winner-mongo { color: #cc6600; font-weight: bold; }
                        .winner-oson { color: #006600; font-weight: bold; }
                        .summary-box { background: #f0f7ff; border: 1px solid #0066cc; border-radius: 8px; padding: 20px; margin: 20px 0; }
                        .highlight { background: #ffffcc; }
                        .metric { font-size: 24px; font-weight: bold; color: #0066cc; }
                        .section { margin: 30px 0; }
                        .note { color: #666; font-style: italic; font-size: 0.9em; }
                        .comparison { display: flex; gap: 20px; }
                        .comparison > div { flex: 1; }
                    </style>
                </head>
                <body>
                <div class="container">
                    <h1>DocBench Performance Report</h1>
                    <p class="note">MongoDB $set vs Oracle JSON_TRANSFORM - Server-Side Update Benchmarks</p>
                """);

            // Executive Summary
            double rawRatio = (double) totalOracle / Math.max(1, totalMongo);
            double adjRatio = (double) totalAdjustedOracle / Math.max(1, totalMongo);

            html.append("""
                <div class="summary-box">
                    <h2>Executive Summary</h2>
                    <div class="comparison">
                        <div>
                            <h3>Raw Performance</h3>
                            <p class="metric">MongoDB %.2fx faster</p>
                            <p>MongoDB wins: %d | OSON wins: %d</p>
                        </div>
                        <div>
                            <h3>Adjusted (Protocol Overhead Removed)</h3>
                            <p class="metric">MongoDB %.2fx faster</p>
                            <p>MongoDB wins: %d | OSON wins: %d</p>
                        </div>
                    </div>
                </div>
                """.formatted(rawRatio, mongoWins, oracleWins, adjRatio, adjustedMongoWins, adjustedOracleWins));

            // Protocol Overhead Section
            html.append("""
                <div class="section">
                    <h2>Protocol Overhead Analysis</h2>
                    <table>
                        <tr><th>Metric</th><th>Value</th></tr>
                        <tr><td>MongoDB baseline (non-JSON $inc)</td><td>%,d ns</td></tr>
                        <tr><td>Oracle baseline (non-JSON UPDATE)</td><td>%,d ns</td></tr>
                        <tr><td>Oracle protocol overhead delta</td><td>%,d ns (%.2fx MongoDB)</td></tr>
                        <tr><td>Overhead as %% of Oracle baseline</td><td>%.1f%%</td></tr>
                    </table>
                    <p class="note">The protocol overhead is a fixed cost per database round-trip, regardless of JSON operation complexity.
                    Oracle's SQL/JDBC protocol has higher latency than MongoDB's binary wire protocol.</p>
                </div>
                """.formatted(
                    mongoBaselineNanos,
                    oracleBaselineNanos,
                    oracleOverheadDelta,
                    (double) oracleBaselineNanos / mongoBaselineNanos,
                    (double) oracleOverheadDelta / oracleBaselineNanos * 100
            ));

            // Raw Results Table
            html.append("""
                <div class="section">
                    <h2>Raw Performance Results</h2>
                    <table>
                        <tr><th>Test Case</th><th>MongoDB (ns)</th><th>OSON (ns)</th><th>Ratio</th><th>Winner</th></tr>
                """);

            for (TestResult result : results.values()) {
                if (result.testType.equals("baseline")) continue;
                double ratio = (double) result.oracleNanos / Math.max(1, result.mongoNanos);
                String winner = ratio > 1.0 ? "MongoDB" : "OSON";
                String winnerClass = ratio > 1.0 ? "winner-mongo" : "winner-oson";
                html.append(String.format(
                        "<tr><td>%s</td><td>%,d</td><td>%,d</td><td>%.2fx</td><td class='%s'>%s</td></tr>%n",
                        result.description, result.mongoNanos, result.oracleNanos, ratio, winnerClass, winner));
            }
            html.append("</table></div>");

            // Adjusted Results Table
            html.append("""
                <div class="section">
                    <h2>Adjusted Performance (Protocol Overhead Removed)</h2>
                    <p class="note">Shows what OSON performance would be if Oracle's protocol matched MongoDB's speed.</p>
                    <table>
                        <tr><th>Test Case</th><th>MongoDB (ns)</th><th>OSON Adjusted (ns)</th><th>Overhead Removed (ns)</th><th>Adj Ratio</th><th>Winner</th></tr>
                """);

            for (TestResult result : results.values()) {
                if (result.testType.equals("baseline")) continue;
                long adjustedOracleNanos = Math.max(0, result.oracleNanos - oracleOverheadDelta);
                long saved = result.oracleNanos - adjustedOracleNanos;
                double adjResultRatio = (double) adjustedOracleNanos / Math.max(1, result.mongoNanos);
                String winner = adjResultRatio > 1.0 ? "MongoDB" : "OSON";
                String winnerClass = adjResultRatio > 1.0 ? "winner-mongo" : "winner-oson";
                String rowClass = adjResultRatio <= 1.0 ? " class='highlight'" : "";
                html.append(String.format(
                        "<tr%s><td>%s</td><td>%,d</td><td>%,d</td><td>%,d</td><td>%.2fx</td><td class='%s'>%s</td></tr>%n",
                        rowClass, result.description, result.mongoNanos, adjustedOracleNanos, saved, adjResultRatio, winnerClass, winner));
            }
            html.append("</table></div>");

            // Key Insights
            html.append("""
                <div class="section">
                    <h2>Key Insights</h2>
                    <ul>
                        <li><strong>Protocol overhead dominates small operations</strong> - Oracle's ~%,d ns fixed overhead masks OSON's JSON manipulation efficiency for typical sub-100KB updates.</li>
                        <li><strong>OSON partial updates shine at scale</strong> - At 1MB+ document sizes, MongoDB's O(n) document rewrite cost exceeds Oracle's protocol overhead.</li>
                        <li><strong>MongoDB's wire protocol is highly optimized</strong> - The binary BSON protocol is ~%.1fx faster than JDBC/SQL for round-trips.</li>
                        <li><strong>OSON wins increase with overhead removed</strong> - From %d to %d wins, particularly for large documents and middle-position array operations.</li>
                    </ul>
                </div>
                """.formatted(
                    oracleOverheadDelta,
                    (double) oracleBaselineNanos / mongoBaselineNanos,
                    oracleWins,
                    adjustedOracleWins
            ));

            html.append("""
                <div class="note" style="margin-top: 40px; text-align: center;">
                    Generated by DocBench - BSON vs OSON Benchmark Suite
                </div>
                </div>
                </body>
                </html>
                """);

            Path reportPath = reportDir.resolve("performance_report.html");
            Files.writeString(reportPath, html.toString());
            System.out.println("\nPerformance report generated: " + reportPath.toAbsolutePath());

        } catch (IOException e) {
            System.out.println("Failed to generate performance report: " + e.getMessage());
        }
    }

    // =========================================================================
    // AWR Snapshot and Report Generation
    // =========================================================================

    private static void initializeAwr() {
        try {
            // Check if AWR is accessible and get database ID
            // Use CON_DBID for PDB context (CON_DBID = DBID for non-CDB, PDB DBID for PDB)
            try (var stmt = oracleConnection.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT con_dbid, instance_number FROM v$database, v$instance")) {
                if (rs.next()) {
                    dbId = rs.getLong(1);
                    instanceNumber = rs.getLong(2);
                    awrEnabled = true;
                    System.out.println("AWR enabled - CON_DBID: " + dbId + ", Instance: " + instanceNumber);

                    // Create report directory
                    Path reportDir = Path.of(AWR_REPORT_DIR);
                    Files.createDirectories(reportDir);
                    System.out.println("AWR reports will be saved to: " + reportDir.toAbsolutePath());
                }
            }
        } catch (Exception e) {
            System.out.println("AWR not available: " + e.getMessage());
            awrEnabled = false;
        }
    }

    private static long createAwrSnapshot(String description) {
        if (!awrEnabled) return -1;

        try (CallableStatement cs = oracleConnection.prepareCall(
                "{ ? = call DBMS_WORKLOAD_REPOSITORY.CREATE_SNAPSHOT() }")) {
            cs.registerOutParameter(1, Types.NUMERIC);
            cs.execute();
            long snapId = cs.getLong(1);
            System.out.println("  AWR Snapshot " + snapId + " created: " + description);
            return snapId;
        } catch (SQLException e) {
            System.out.println("  AWR snapshot failed: " + e.getMessage());
            return -1;
        }
    }

    private static void awrSnapshotBefore(String category) {
        if (!awrEnabled) return;
        long snapId = createAwrSnapshot("Before " + category);
        if (snapId > 0) {
            awrSnapshots.put(category, new long[]{snapId, -1});
        }
    }

    private static void awrSnapshotAfter(String category) {
        if (!awrEnabled) return;
        long[] snaps = awrSnapshots.get(category);
        if (snaps != null && snaps[0] > 0) {
            long snapId = createAwrSnapshot("After " + category);
            snaps[1] = snapId;
        }
    }

    private static void generateAwrReports() {
        if (!awrEnabled || awrSnapshots.isEmpty()) {
            System.out.println("\nNo AWR snapshots to report.");
            return;
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  GENERATING AWR REPORTS");
        System.out.println("=".repeat(80));

        for (Map.Entry<String, long[]> entry : awrSnapshots.entrySet()) {
            String category = entry.getKey();
            long[] snaps = entry.getValue();

            if (snaps[0] <= 0 || snaps[1] <= 0) {
                System.out.println("  Skipping " + category + " - incomplete snapshots");
                continue;
            }

            try {
                String filename = AWR_REPORT_DIR + "/awr_" + category.replaceAll("[^a-zA-Z0-9]", "_") + ".html";
                generateAwrHtmlReport(snaps[0], snaps[1], filename);
                System.out.println("  Generated: " + filename + " (snaps " + snaps[0] + " - " + snaps[1] + ")");
            } catch (Exception e) {
                System.out.println("  Failed to generate report for " + category + ": " + e.getMessage());
            }
        }

        System.out.println("=".repeat(80) + "\n");
    }

    private static void generateAwrHtmlReport(long beginSnap, long endSnap, String filename) throws SQLException, IOException {
        // Use AWR report function to generate HTML report
        String sql = """
            SELECT output FROM TABLE(
                DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(
                    l_dbid => ?,
                    l_inst_num => ?,
                    l_bid => ?,
                    l_eid => ?
                )
            )
            """;

        StringBuilder report = new StringBuilder();
        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            ps.setLong(1, dbId);
            ps.setLong(2, instanceNumber);
            ps.setLong(3, beginSnap);
            ps.setLong(4, endSnap);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String line = rs.getString(1);
                    if (line != null) {
                        report.append(line).append("\n");
                    }
                }
            }
        }

        // Write to file
        Files.writeString(Path.of(filename), report.toString());
    }
}
