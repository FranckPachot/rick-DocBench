package com.docbench.benchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.junit.jupiter.api.*;

import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/**
 * Update Efficiency Test: Compares BSON vs OSON client-side update operations.
 *
 * This test measures the efficiency of modifying document fields:
 * - BSON: RawBsonDocument is immutable, requires decode → modify → encode cycle
 * - OSON: OracleJsonObject can be mutable, supports O(1) field access
 *
 * Test methodology:
 * 1. Fetch document as raw binary format
 * 2. Locate field to update (O(n) vs O(1))
 * 3. Create modified version
 * 4. Serialize back to bytes
 */
@DisplayName("BSON vs OSON Update Efficiency")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UpdateEfficiencyTest {

    // Configuration
    private static final int WARMUP_ITERATIONS = 1_000;
    private static final int MEASUREMENT_ITERATIONS = 10_000;
    private static final Codec<BsonDocument> BSON_CODEC = new BsonDocumentCodec();

    // MongoDB
    private static MongoClient mongoClient;
    private static MongoCollection<RawBsonDocument> rawCollection;
    private static MongoCollection<Document> docCollection;
    private static String mongoDbName;

    // Oracle
    private static Connection oracleConnection;
    private static OracleJsonFactory jsonFactory;
    private static final String ORACLE_TABLE = "UPDATE_BENCH_DOCS";

    // Results storage
    private static final Map<String, TestResult> results = new LinkedHashMap<>();

    @BeforeAll
    static void setup() throws SQLException {
        Properties props = loadConfigProperties();

        // Setup MongoDB
        String mongoUri = props.getProperty("mongodb.uri");
        mongoDbName = props.getProperty("mongodb.database", "testdb");
        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase db = mongoClient.getDatabase(mongoDbName);
        db.getCollection("update_bench").drop();
        rawCollection = db.getCollection("update_bench", RawBsonDocument.class);
        docCollection = db.getCollection("update_bench");

        // Setup Oracle
        String oracleUrl = props.getProperty("oracle.url");
        String oracleUser = props.getProperty("oracle.username");
        String oraclePass = props.getProperty("oracle.password");
        oracleConnection = DriverManager.getConnection(oracleUrl, oracleUser, oraclePass);
        jsonFactory = new OracleJsonFactory();

        // Create Oracle table
        try (Statement stmt = oracleConnection.createStatement()) {
            try {
                stmt.execute("DROP TABLE " + ORACLE_TABLE + " PURGE");
            } catch (SQLException e) {
                // Table doesn't exist
            }
            stmt.execute("CREATE TABLE " + ORACLE_TABLE + " (id VARCHAR2(100) PRIMARY KEY, doc JSON)");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  BSON vs OSON UPDATE EFFICIENCY TEST");
        System.out.println("  ────────────────────────────────────");
        System.out.println("  BSON: RawBsonDocument (immutable) → decode → modify → encode");
        System.out.println("  OSON: OracleJsonObject → O(1) field access → modify");
        System.out.println("=".repeat(80));

        runGlobalWarmup();
    }

    private static void runGlobalWarmup() throws SQLException {
        System.out.println("\nRunning global warmup...");

        // Insert warmup documents
        Document warmupDoc = new Document("_id", "warmup").append("field_001", "value");
        docCollection.insertOne(warmupDoc);

        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, "warmup");
            ps.setString(2, "{\"field_001\":\"value\"}");
            ps.executeUpdate();
        }

        // Warmup iterations
        for (int i = 0; i < 100; i++) {
            RawBsonDocument raw = rawCollection.find(new Document("_id", "warmup")).first();
            if (raw != null) {
                BsonDocument decoded = raw.decode(BSON_CODEC);
                decoded.put("field_001", new BsonString("updated"));
            }
        }

        System.out.println("Global warmup complete.\n");
    }

    @AfterAll
    static void teardown() {
        printFinalReport();

        if (mongoClient != null) {
            mongoClient.getDatabase(mongoDbName).getCollection("update_bench").drop();
            mongoClient.close();
        }
        if (oracleConnection != null) {
            try {
                try (Statement stmt = oracleConnection.createStatement()) {
                    stmt.execute("DROP TABLE " + ORACLE_TABLE + " PURGE");
                }
                oracleConnection.close();
            } catch (SQLException e) {
                // ignore
            }
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

    // =========================================================================
    // Test 1: Field Location Cost - Time to find field for update
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Field location - position 1")
    void fieldLocation_position1() throws SQLException {
        testFieldLocation("loc-1", 100, 1);
    }

    @Test
    @Order(2)
    @DisplayName("Field location - position 50")
    void fieldLocation_position50() throws SQLException {
        testFieldLocation("loc-50", 100, 50);
    }

    @Test
    @Order(3)
    @DisplayName("Field location - position 100")
    void fieldLocation_position100() throws SQLException {
        testFieldLocation("loc-100", 100, 100);
    }

    @Test
    @Order(4)
    @DisplayName("Field location - position 500")
    void fieldLocation_position500() throws SQLException {
        testFieldLocation("loc-500", 500, 500);
    }

    @Test
    @Order(5)
    @DisplayName("Field location - position 1000")
    void fieldLocation_position1000() throws SQLException {
        testFieldLocation("loc-1000", 1000, 1000);
    }

    private void testFieldLocation(String testId, int totalFields, int targetPosition) throws SQLException {
        String targetField = "field_" + String.format("%04d", targetPosition);

        // Create and insert test document
        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{");
        for (int i = 1; i <= totalFields; i++) {
            String fieldName = "field_" + String.format("%04d", i);
            String value = "value_" + i;
            mongoDoc.append(fieldName, value);
            if (i > 1) oracleJson.append(",");
            oracleJson.append("\"").append(fieldName).append("\":\"").append(value).append("\"");
        }
        oracleJson.append("}");

        docCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Measure BSON field location (on decoded BsonDocument)
        long bsonNanos = measureBsonFieldLocation(testId, targetField);

        // Measure OSON field location
        long osonNanos = measureOsonFieldLocation(testId, targetField);

        String description = "Locate field " + targetPosition + "/" + totalFields;
        results.put(testId, new TestResult(testId, description, bsonNanos, osonNanos, "location"));

        System.out.printf("  %-30s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                description, bsonNanos, osonNanos,
                (double) bsonNanos / Math.max(1, osonNanos));
    }

    private long measureBsonFieldLocation(String docId, String fieldName) {
        // Fetch and decode document ONCE
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);
        BsonDocument decoded = raw.decode(BSON_CODEC);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            decoded.get(fieldName);
        }

        // Measure field location on decoded document
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            decoded.get(fieldName);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOsonFieldLocation(String docId, String fieldName) throws SQLException {
        // Fetch document as OracleJsonObject
        OracleJsonObject jsonObj = fetchOracleJsonObject(docId);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            jsonObj.get(fieldName);
        }

        // Measure field location
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            jsonObj.get(fieldName);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    // =========================================================================
    // Test 2: Decode-Modify-Encode Cycle
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Update cycle - position 1")
    void updateCycle_position1() throws SQLException {
        testUpdateCycle("upd-1", 100, 1);
    }

    @Test
    @Order(11)
    @DisplayName("Update cycle - position 50")
    void updateCycle_position50() throws SQLException {
        testUpdateCycle("upd-50", 100, 50);
    }

    @Test
    @Order(12)
    @DisplayName("Update cycle - position 100")
    void updateCycle_position100() throws SQLException {
        testUpdateCycle("upd-100", 100, 100);
    }

    @Test
    @Order(13)
    @DisplayName("Update cycle - position 500")
    void updateCycle_position500() throws SQLException {
        testUpdateCycle("upd-500", 500, 500);
    }

    private void testUpdateCycle(String testId, int totalFields, int targetPosition) throws SQLException {
        String targetField = "field_" + String.format("%04d", targetPosition);

        // Create and insert test document
        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{");
        for (int i = 1; i <= totalFields; i++) {
            String fieldName = "field_" + String.format("%04d", i);
            String value = "value_" + i;
            mongoDoc.append(fieldName, value);
            if (i > 1) oracleJson.append(",");
            oracleJson.append("\"").append(fieldName).append("\":\"").append(value).append("\"");
        }
        oracleJson.append("}");

        docCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Measure BSON full update cycle
        long bsonNanos = measureBsonUpdateCycle(testId, targetField);

        // Measure OSON full update cycle
        long osonNanos = measureOsonUpdateCycle(testId, targetField);

        String description = "Update cycle pos " + targetPosition + "/" + totalFields;
        results.put(testId, new TestResult(testId, description, bsonNanos, osonNanos, "cycle"));

        System.out.printf("  %-30s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                description, bsonNanos, osonNanos,
                (double) bsonNanos / Math.max(1, osonNanos));
    }

    private long measureBsonUpdateCycle(String docId, String fieldName) {
        // Fetch raw document ONCE (simulating cached raw bytes)
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        // Warmup: full decode → modify → encode cycle
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            BsonDocument decoded = raw.decode(BSON_CODEC);
            decoded.put(fieldName, new BsonString("updated_" + i));
            new RawBsonDocument(decoded, BSON_CODEC);
        }

        // Measure full cycle
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            // 1. Decode
            BsonDocument decoded = raw.decode(BSON_CODEC);
            // 2. Modify
            decoded.put(fieldName, new BsonString("updated_" + i));
            // 3. Encode
            new RawBsonDocument(decoded, BSON_CODEC);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOsonUpdateCycle(String docId, String fieldName) throws SQLException {
        // Fetch original document
        OracleJsonObject original = fetchOracleJsonObject(docId);

        // Warmup: create mutable copy → modify → serialize
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            OracleJsonObject mutable = copyToMutable(original);
            mutable.put(fieldName, jsonFactory.createString("updated_" + i));
            serializeOsonToBytes(mutable);
        }

        // Measure full cycle
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            // 1. Create mutable copy
            OracleJsonObject mutable = copyToMutable(original);
            // 2. Modify
            mutable.put(fieldName, jsonFactory.createString("updated_" + i));
            // 3. Serialize
            serializeOsonToBytes(mutable);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    // =========================================================================
    // Test 3: Nested Field Update
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("Nested update - depth 1")
    void nestedUpdate_depth1() throws SQLException {
        testNestedUpdate("nest-1", 1);
    }

    @Test
    @Order(21)
    @DisplayName("Nested update - depth 3")
    void nestedUpdate_depth3() throws SQLException {
        testNestedUpdate("nest-3", 3);
    }

    @Test
    @Order(22)
    @DisplayName("Nested update - depth 5")
    void nestedUpdate_depth5() throws SQLException {
        testNestedUpdate("nest-5", 5);
    }

    private void testNestedUpdate(String testId, int depth) throws SQLException {
        // Create nested document
        Document mongoDoc = new Document("_id", testId);
        Document current = new Document("target", "original_value");
        for (int i = 0; i < 10; i++) {
            current.append("pad_" + i, "padding_" + i);
        }

        for (int d = depth - 1; d >= 0; d--) {
            Document parent = new Document();
            for (int i = 0; i < 10; i++) {
                parent.append("pad_" + i, "level_" + d);
            }
            parent.append("nested", current);
            current = parent;
        }
        mongoDoc.append("root", current);

        String oracleJson = mongoDoc.toJson();

        docCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }

        // Build path
        String[] pathParts = buildNestedPath(depth);

        // Measure BSON nested update
        long bsonNanos = measureBsonNestedUpdate(testId, pathParts);

        // Measure OSON nested update
        long osonNanos = measureOsonNestedUpdate(testId, pathParts);

        String description = "Nested update depth " + depth;
        results.put(testId, new TestResult(testId, description, bsonNanos, osonNanos, "nested"));

        System.out.printf("  %-30s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                description, bsonNanos, osonNanos,
                (double) bsonNanos / Math.max(1, osonNanos));
    }

    private String[] buildNestedPath(int depth) {
        List<String> parts = new ArrayList<>();
        parts.add("root");
        for (int i = 0; i < depth; i++) {
            parts.add("nested");
        }
        parts.add("target");
        return parts.toArray(new String[0]);
    }

    private long measureBsonNestedUpdate(String docId, String[] path) {
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            BsonDocument decoded = raw.decode(BSON_CODEC);
            BsonDocument target = navigateBsonPath(decoded, path, path.length - 1);
            if (target != null) {
                target.put(path[path.length - 1], new BsonString("updated_" + i));
            }
            new RawBsonDocument(decoded, BSON_CODEC);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            BsonDocument decoded = raw.decode(BSON_CODEC);
            BsonDocument target = navigateBsonPath(decoded, path, path.length - 1);
            if (target != null) {
                target.put(path[path.length - 1], new BsonString("updated_" + i));
            }
            new RawBsonDocument(decoded, BSON_CODEC);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private BsonDocument navigateBsonPath(BsonDocument doc, String[] path, int endIndex) {
        BsonDocument current = doc;
        for (int i = 0; i < endIndex; i++) {
            BsonValue value = current.get(path[i]);
            if (value == null || !value.isDocument()) return null;
            current = value.asDocument();
        }
        return current;
    }

    private long measureOsonNestedUpdate(String docId, String[] path) throws SQLException {
        OracleJsonObject original = fetchOracleJsonObject(docId);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            OracleJsonObject mutable = copyToMutable(original);
            OracleJsonObject target = navigateOsonPath(mutable, path, path.length - 1);
            if (target != null) {
                target.put(path[path.length - 1], jsonFactory.createString("updated_" + i));
            }
            serializeOsonToBytes(mutable);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            OracleJsonObject mutable = copyToMutable(original);
            OracleJsonObject target = navigateOsonPath(mutable, path, path.length - 1);
            if (target != null) {
                target.put(path[path.length - 1], jsonFactory.createString("updated_" + i));
            }
            serializeOsonToBytes(mutable);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private OracleJsonObject navigateOsonPath(OracleJsonObject obj, String[] path, int endIndex) {
        OracleJsonObject current = obj;
        for (int i = 0; i < endIndex; i++) {
            OracleJsonValue value = current.get(path[i]);
            if (value == null || value.getOracleJsonType() != OracleJsonValue.OracleJsonType.OBJECT) {
                return null;
            }
            current = value.asJsonObject();
        }
        return current;
    }

    // =========================================================================
    // Helper Methods
    // =========================================================================

    private OracleJsonObject fetchOracleJsonObject(String docId) throws SQLException {
        String sql = "SELECT doc FROM " + ORACLE_TABLE + " WHERE id = ?";
        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            ps.setString(1, docId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    OracleJsonValue jsonValue = rs.getObject(1, OracleJsonValue.class);
                    if (jsonValue != null && jsonValue.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                        return jsonValue.asJsonObject();
                    }
                }
            }
        }
        throw new RuntimeException("Document not found: " + docId);
    }

    private OracleJsonObject copyToMutable(OracleJsonObject source) {
        // Create a deep mutable copy by recursively copying nested objects
        return deepCopyObject(source);
    }

    private OracleJsonObject deepCopyObject(OracleJsonObject source) {
        OracleJsonObject mutable = jsonFactory.createObject();
        for (String key : source.keySet()) {
            OracleJsonValue value = source.get(key);
            if (value.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                mutable.put(key, deepCopyObject(value.asJsonObject()));
            } else {
                mutable.put(key, value);
            }
        }
        return mutable;
    }

    private byte[] serializeOsonToBytes(OracleJsonObject obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            jsonFactory.createJsonBinaryGenerator(baos).write(obj).close();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize OSON", e);
        }
    }

    // =========================================================================
    // Report Generation
    // =========================================================================

    private static void printFinalReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  FINAL RESULTS: BSON vs OSON UPDATE EFFICIENCY");
        System.out.println("=".repeat(80));

        // Group by test type
        Map<String, List<TestResult>> grouped = new LinkedHashMap<>();
        for (TestResult result : results.values()) {
            grouped.computeIfAbsent(result.testType, k -> new ArrayList<>()).add(result);
        }

        long totalBson = 0;
        long totalOson = 0;
        int bsonWins = 0;
        int osonWins = 0;

        for (Map.Entry<String, List<TestResult>> entry : grouped.entrySet()) {
            String type = entry.getKey();
            List<TestResult> typeResults = entry.getValue();

            System.out.println("\n" + type.toUpperCase() + " TESTS:");
            System.out.printf("%-32s %12s %12s %10s %s%n",
                    "Test Case", "BSON (ns)", "OSON (ns)", "Ratio", "Winner");
            System.out.println("-".repeat(80));

            for (TestResult result : typeResults) {
                double ratio = (double) result.bsonNanos / Math.max(1, result.osonNanos);
                String winner = ratio > 1.0 ? "OSON" : "BSON";

                if (ratio > 1.0) osonWins++;
                else bsonWins++;

                totalBson += result.bsonNanos;
                totalOson += result.osonNanos;

                System.out.printf("%-32s %12d %12d %9.2fx %s%n",
                        result.description, result.bsonNanos, result.osonNanos, ratio, winner);
            }
        }

        System.out.println("\n" + "=".repeat(80));
        double overallRatio = (double) totalBson / Math.max(1, totalOson);
        System.out.printf("%-32s %12d %12d %9.2fx %s%n",
                "TOTAL", totalBson, totalOson, overallRatio,
                overallRatio > 1.0 ? "OSON" : "BSON");

        System.out.println("\nSummary:");
        System.out.println("  BSON wins: " + bsonWins);
        System.out.println("  OSON wins: " + osonWins);
        System.out.printf("  Overall: %s is %.2fx faster for updates%n",
                overallRatio > 1.0 ? "OSON" : "BSON",
                overallRatio > 1.0 ? overallRatio : 1.0 / overallRatio);
        System.out.println("=".repeat(80) + "\n");
    }

    private record TestResult(
            String testId,
            String description,
            long bsonNanos,
            long osonNanos,
            String testType
    ) {}
}
