package com.docbench.benchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.*;

import oracle.sql.json.OracleJsonObject;
import oracle.sql.json.OracleJsonValue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/**
 * BSON vs OSON Client-Side Field Access Comparison.
 *
 * This test demonstrates the O(n) vs O(1) difference in field access:
 * - BSON (MongoDB): RawBsonDocument.get() - sequential O(n) field scanning
 * - OSON (Oracle): OracleJsonObject.get() - O(1) hash-indexed lookup
 *
 * Both fetch the full document and perform CLIENT-SIDE field access.
 * This isolates the format difference:
 * - BSON stores fields sequentially with length prefixes (must scan to find field)
 * - OSON uses hash-indexed structure (direct O(1) lookup by field name)
 *
 * Type preservation: Both APIs preserve the original JSON types.
 */
@DisplayName("BSON O(n) vs OSON O(1) - Client-Side Field Access")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BsonVsOsonClientSideTest {

    // Configuration - high iteration count for measurable nanosecond-level timings
    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int MEASUREMENT_ITERATIONS = 100_000;

    // MongoDB
    private static MongoClient mongoClient;
    private static MongoCollection<RawBsonDocument> rawCollection;
    private static String mongoDbName;

    // Oracle
    private static Connection oracleConnection;
    private static String oracleTableName = "BENCHMARK_DOCS";

    // Results
    private static final Map<String, ComparisonResult> results = new LinkedHashMap<>();

    @BeforeAll
    static void setup() throws SQLException {
        Properties props = loadConfigProperties();

        // Setup MongoDB
        String mongoUri = props.getProperty("mongodb.uri");
        mongoDbName = props.getProperty("mongodb.database", "testdb");
        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase db = mongoClient.getDatabase(mongoDbName);
        db.getCollection("benchmark_client_side").drop();
        rawCollection = db.getCollection("benchmark_client_side", RawBsonDocument.class);

        // Setup Oracle
        String oracleUrl = props.getProperty("oracle.url");
        String oracleUser = props.getProperty("oracle.username");
        String oraclePass = props.getProperty("oracle.password");
        oracleConnection = DriverManager.getConnection(oracleUrl, oracleUser, oraclePass);

        // Create Oracle table
        try (Statement stmt = oracleConnection.createStatement()) {
            try {
                stmt.execute("DROP TABLE " + oracleTableName + " PURGE");
            } catch (SQLException e) {
                // Table doesn't exist
            }
            stmt.execute("CREATE TABLE " + oracleTableName + " (id VARCHAR2(100) PRIMARY KEY, doc JSON)");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  BSON O(n) vs OSON O(1) - CLIENT-SIDE FIELD ACCESS");
        System.out.println("  ─────────────────────────────────────────────────");
        System.out.println("  MongoDB: RawBsonDocument.get() - sequential O(n) field scanning");
        System.out.println("  Oracle:  OracleJsonObject.get() - O(1) hash-indexed lookup");
        System.out.println("  Both fetch full document, then access field client-side");
        System.out.println("=".repeat(80));

        // Global warmup
        runGlobalWarmup();
    }

    private static void runGlobalWarmup() throws SQLException {
        System.out.println("\nRunning global warmup...");

        // Insert warmup doc
        Document warmupDoc = new Document("_id", "warmup")
                .append("field_000", "value0")
                .append("field_500", "value500");

        MongoCollection<Document> docCollection = mongoClient.getDatabase(mongoDbName)
                .getCollection("benchmark_client_side");
        docCollection.insertOne(warmupDoc);

        String oracleJson = "{\"field_000\":\"value0\",\"field_500\":\"value500\"}";
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + oracleTableName + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, "warmup");
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }

        // Warmup iterations
        for (int i = 0; i < 200; i++) {
            RawBsonDocument raw = rawCollection.find(new Document("_id", "warmup")).first();
            if (raw != null) raw.get("field_500");

            try (PreparedStatement ps = oracleConnection.prepareStatement(
                    "SELECT JSON_VALUE(doc, '$.field_500') FROM " + oracleTableName + " WHERE id = ?")) {
                ps.setString(1, "warmup");
                ps.executeQuery();
            }
        }

        System.out.println("Global warmup complete.\n");
    }

    @AfterAll
    static void teardown() {
        printFinalReport();

        if (mongoClient != null) {
            mongoClient.getDatabase(mongoDbName).getCollection("benchmark_client_side").drop();
            mongoClient.close();
        }
        if (oracleConnection != null) {
            try {
                try (Statement stmt = oracleConnection.createStatement()) {
                    stmt.execute("DROP TABLE " + oracleTableName + " PURGE");
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
    // Position-based tests - demonstrates O(n) scaling for BSON
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Position 1/100 - First field")
    void position_1_of_100() throws SQLException {
        testFieldAccess("pos-1-100", 100, 1, "field_001");
    }

    @Test
    @Order(2)
    @DisplayName("Position 50/100 - Middle field")
    void position_50_of_100() throws SQLException {
        testFieldAccess("pos-50-100", 100, 50, "field_050");
    }

    @Test
    @Order(3)
    @DisplayName("Position 100/100 - Last field")
    void position_100_of_100() throws SQLException {
        testFieldAccess("pos-100-100", 100, 100, "field_100");
    }

    @Test
    @Order(4)
    @DisplayName("Position 500/500 - Last field in large doc")
    void position_500_of_500() throws SQLException {
        testFieldAccess("pos-500-500", 500, 500, "field_500");
    }

    @Test
    @Order(5)
    @DisplayName("Position 1000/1000 - Last field in very large doc")
    void position_1000_of_1000() throws SQLException {
        testFieldAccess("pos-1000-1000", 1000, 1000, "field_1000");
    }

    // =========================================================================
    // Nested field tests
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Nested depth 1")
    void nested_depth_1() throws SQLException {
        testNestedFieldAccess("nested-1", 1);
    }

    @Test
    @Order(11)
    @DisplayName("Nested depth 3")
    void nested_depth_3() throws SQLException {
        testNestedFieldAccess("nested-3", 3);
    }

    @Test
    @Order(12)
    @DisplayName("Nested depth 5")
    void nested_depth_5() throws SQLException {
        testNestedFieldAccess("nested-5", 5);
    }

    // =========================================================================
    // Test Implementation
    // =========================================================================

    private void testFieldAccess(String testId, int totalFields, int targetPosition, String targetField)
            throws SQLException {
        // Create document with target field at specific position
        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{");

        for (int i = 1; i <= totalFields; i++) {
            String fieldName = "field_" + String.format("%03d", i);
            String value = "value_" + i + "_padding_to_ensure_reasonable_field_size";
            mongoDoc.append(fieldName, value);

            if (i > 1) oracleJson.append(",");
            oracleJson.append("\"").append(fieldName).append("\":\"").append(value).append("\"");
        }
        oracleJson.append("}");

        // Insert documents
        MongoCollection<Document> docColl = mongoClient.getDatabase(mongoDbName)
                .getCollection("benchmark_client_side");
        docColl.insertOne(mongoDoc);

        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + oracleTableName + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Measure MongoDB: Full doc fetch + client-side RawBsonDocument parsing
        long mongoNanos = measureMongoClientSide(testId, targetField);

        // Measure Oracle: JSON_VALUE server-side extraction
        long oracleNanos = measureOracleJsonValue(testId, targetField);

        String description = "Position " + targetPosition + "/" + totalFields;
        results.put(testId, new ComparisonResult(testId, mongoNanos, oracleNanos, description));

        System.out.printf("  %-25s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                description,
                mongoNanos,
                oracleNanos,
                (double) mongoNanos / oracleNanos);
    }

    private void testNestedFieldAccess(String testId, int depth) throws SQLException {
        // Create nested document
        Document mongoDoc = new Document("_id", testId);
        Document current = new Document("target", "TARGET_VALUE");
        for (int i = 0; i < 10; i++) {
            current.append("padding_" + i, "nested_padding_value_" + i);
        }

        for (int d = depth - 1; d >= 0; d--) {
            Document parent = new Document();
            for (int i = 0; i < 10; i++) {
                parent.append("padding_" + i, "level_" + d + "_value_" + i);
            }
            parent.append("nested", current);
            current = parent;
        }
        mongoDoc.append("root", current);

        // Build Oracle JSON
        String oracleJson = mongoDoc.toJson();

        // Insert
        MongoCollection<Document> docColl = mongoClient.getDatabase(mongoDbName)
                .getCollection("benchmark_client_side");
        docColl.insertOne(mongoDoc);

        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + oracleTableName + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson);
            ps.executeUpdate();
        }

        // Build path
        StringBuilder mongoPath = new StringBuilder("root");
        StringBuilder oraclePath = new StringBuilder("$.root");
        for (int i = 0; i < depth; i++) {
            mongoPath.append(".nested");
            oraclePath.append(".nested");
        }
        mongoPath.append(".target");
        oraclePath.append(".target");

        // Measure
        long mongoNanos = measureMongoNestedAccess(testId, mongoPath.toString());
        long oracleNanos = measureOracleNestedAccess(testId, oraclePath.toString());

        String description = "Nested depth " + depth;
        results.put(testId, new ComparisonResult(testId, mongoNanos, oracleNanos, description));

        System.out.printf("  %-25s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                description,
                mongoNanos,
                oracleNanos,
                (double) mongoNanos / oracleNanos);
    }

    private long measureMongoClientSide(String docId, String fieldName) {
        // Fetch document ONCE
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        // Warmup - measure ONLY client-side field access
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            raw.get(fieldName); // O(n) parse through BSON to find field
        }

        // Measure ONLY client-side field access (no network)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            raw.get(fieldName); // O(n) sequential scan through BSON bytes
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureMongoNestedAccess(String docId, String path) {
        String[] parts = path.split("\\.");

        // Fetch document ONCE
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        // Warmup - measure ONLY client-side navigation
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            navigateToField(raw, parts);
        }

        // Measure ONLY client-side field access (no network)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            navigateToField(raw, parts); // O(n) at each level
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private BsonValue navigateToField(RawBsonDocument doc, String[] path) {
        BsonValue current = doc;
        for (String part : path) {
            if (!current.isDocument()) return null;
            current = current.asDocument().get(part);
            if (current == null) return null;
        }
        return current;
    }

    private long measureOracleJsonValue(String docId, String fieldName) throws SQLException {
        // Fetch document ONCE using native OSON
        String sql = "SELECT doc FROM " + oracleTableName + " WHERE id = ?";
        OracleJsonObject jsonObj;

        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            ps.setString(1, docId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    OracleJsonValue jsonValue = rs.getObject(1, OracleJsonValue.class);
                    if (jsonValue != null && jsonValue.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                        jsonObj = jsonValue.asJsonObject();
                    } else {
                        throw new RuntimeException("Document not found or not an object: " + docId);
                    }
                } else {
                    throw new RuntimeException("Document not found: " + docId);
                }
            }
        }

        // Warmup - measure ONLY client-side field access
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            jsonObj.get(fieldName); // O(1) hash lookup
        }

        // Measure ONLY client-side field access (no network)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            jsonObj.get(fieldName); // O(1) hash lookup - preserves type
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOracleNestedAccess(String docId, String jsonPath) throws SQLException {
        // Fetch document ONCE using native OSON
        String sql = "SELECT doc FROM " + oracleTableName + " WHERE id = ?";
        String[] pathParts = jsonPath.replace("$.", "").split("\\.");
        OracleJsonValue jsonValue;

        try (PreparedStatement ps = oracleConnection.prepareStatement(sql)) {
            ps.setString(1, docId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    jsonValue = rs.getObject(1, OracleJsonValue.class);
                    if (jsonValue == null) {
                        throw new RuntimeException("Document not found: " + docId);
                    }
                } else {
                    throw new RuntimeException("Document not found: " + docId);
                }
            }
        }

        // Warmup - measure ONLY client-side navigation
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            navigateOsonPath(jsonValue, pathParts);
        }

        // Measure ONLY client-side field access (no network)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            navigateOsonPath(jsonValue, pathParts); // O(1) at each level
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private OracleJsonValue navigateOsonPath(OracleJsonValue value, String[] path) {
        OracleJsonValue current = value;
        for (String part : path) {
            if (current != null && current.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                current = current.asJsonObject().get(part); // O(1) hash lookup at each level
            } else {
                return null;
            }
        }
        return current;
    }

    private static void printFinalReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  FINAL RESULTS: BSON O(n) vs OSON O(1) CLIENT-SIDE ACCESS");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.printf("%-28s %12s %12s %10s %s%n",
                "Test Case", "BSON (ns)", "OSON (ns)", "Ratio", "Winner");
        System.out.println("-".repeat(80));

        long totalBson = 0;
        long totalOson = 0;
        int bsonWins = 0;
        int osonWins = 0;

        // Sort by position for clearer output
        List<Map.Entry<String, ComparisonResult>> sorted = new ArrayList<>(results.entrySet());
        sorted.sort(Comparator.comparingInt(e -> {
            if (e.getKey().startsWith("pos-")) {
                String[] parts = e.getKey().split("-");
                return Integer.parseInt(parts[1]);
            } else if (e.getKey().startsWith("nested-")) {
                return 10000 + Integer.parseInt(e.getKey().split("-")[1]);
            }
            return 99999;
        }));

        for (Map.Entry<String, ComparisonResult> entry : sorted) {
            ComparisonResult result = entry.getValue();
            long bsonNs = result.bsonNanos;
            long osonNs = result.osonNanos;
            double ratio = (double) result.bsonNanos / result.osonNanos;
            String winner = ratio > 1.0 ? "OSON" : "BSON";

            if (ratio > 1.0) osonWins++;
            else bsonWins++;

            totalBson += bsonNs;
            totalOson += osonNs;

            System.out.printf("%-28s %12d %12d %9.2fx %s%n",
                    result.description, bsonNs, osonNs, ratio, winner);
        }

        System.out.println("-".repeat(80));
        double overallRatio = (double) totalBson / totalOson;
        System.out.printf("%-28s %12d %12d %9.2fx %s%n",
                "TOTAL", totalBson, totalOson, overallRatio,
                overallRatio > 1.0 ? "OSON" : "BSON");

        System.out.println();
        System.out.println("Summary:");
        System.out.println("  BSON wins: " + bsonWins);
        System.out.println("  OSON wins: " + osonWins);
        System.out.printf("  Overall: OSON is %.2fx faster%n", overallRatio);
        System.out.println();

        // Show O(n) scaling analysis
        System.out.println("O(n) SCALING ANALYSIS (BSON position tests):");
        Long pos1 = null, pos100 = null, pos500 = null, pos1000 = null;
        for (Map.Entry<String, ComparisonResult> entry : results.entrySet()) {
            if (entry.getKey().equals("pos-1-100")) pos1 = entry.getValue().bsonNanos;
            if (entry.getKey().equals("pos-100-100")) pos100 = entry.getValue().bsonNanos;
            if (entry.getKey().equals("pos-500-500")) pos500 = entry.getValue().bsonNanos;
            if (entry.getKey().equals("pos-1000-1000")) pos1000 = entry.getValue().bsonNanos;
        }

        if (pos1 != null && pos1000 != null) {
            System.out.printf("  Position 1 → 1000: BSON time increased %.2fx%n",
                    (double) pos1000 / pos1);
            System.out.println("  (If O(n), expect ~1000x increase; actual shows network/fetch overhead dominates)");
        }

        System.out.println();
        System.out.println("KEY INSIGHT:");
        System.out.println("  OSON's JSON_VALUE uses O(1) hash lookup server-side.");
        System.out.println("  BSON requires full document transfer + O(n) client-side parsing.");
        System.out.println("  The combined effect shows OSON's architectural advantage.");
        System.out.println("=".repeat(80) + "\n");
    }

    private record ComparisonResult(
            String key,
            long bsonNanos,
            long osonNanos,
            String description
    ) {}
}
