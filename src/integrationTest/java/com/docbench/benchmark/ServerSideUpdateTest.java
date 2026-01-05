package com.docbench.benchmark;

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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

    private record TestResult(String testId, String description, long mongoNanos, long oracleNanos, String testType) {}

    @BeforeAll
    static void setup() throws SQLException {
        Properties props = loadConfigProperties();

        // MongoDB setup
        String mongoUri = props.getProperty("mongodb.uri");
        String mongoDbName = props.getProperty("mongodb.database", "testdb");
        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
        mongoDb.getCollection("server_update_test").drop();
        mongoCollection = mongoDb.getCollection("server_update_test");

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
        System.out.println("=".repeat(80));

        // Global warmup
        System.out.println("\nRunning global warmup...");
        setupAndWarmup();
        System.out.println("Global warmup complete.\n");
    }

    @AfterAll
    static void teardown() throws SQLException {
        printFinalReport();

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
    // Test 1: Single Field Update
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Single field update - 100 field document")
    void singleFieldUpdate_100fields() throws SQLException {
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
        testLargeDocUpdate("large-10kb", 10 * 1024);
    }

    @Test
    @Order(21)
    @DisplayName("Large doc update - 50KB document")
    void largeDocUpdate_50kb() throws SQLException {
        testLargeDocUpdate("large-50kb", 50 * 1024);
    }

    @Test
    @Order(22)
    @DisplayName("Large doc update - 100KB document")
    void largeDocUpdate_100kb() throws SQLException {
        testLargeDocUpdate("large-100kb", 100 * 1024);
    }

    @Test
    @Order(23)
    @DisplayName("Large doc update - 1MB document")
    void largeDocUpdate_1mb() throws SQLException {
        testLargeDocUpdate("large-1mb", 1024 * 1024);
    }

    @Test
    @Order(24)
    @DisplayName("Large doc update - 4MB document")
    void largeDocUpdate_4mb() throws SQLException {
        testLargeDocUpdate("large-4mb", 4 * 1024 * 1024);
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
                default -> type.toUpperCase();
            };

            System.out.println("\n" + header + ":");
            System.out.printf("%-35s %12s %12s %10s %s%n",
                    "Test Case", "MongoDB (ns)", "OSON (ns)", "Ratio", "Winner");
            System.out.println("-".repeat(80));

            for (TestResult result : typeResults) {
                double ratio = (double) result.oracleNanos / Math.max(1, result.mongoNanos);
                String winner = ratio > 1.0 ? "MongoDB" : "OSON";

                if (ratio > 1.0) mongoWins++;
                else oracleWins++;

                totalMongo += result.mongoNanos;
                totalOracle += result.oracleNanos;

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
        System.out.println("=".repeat(80) + "\n");
    }
}
