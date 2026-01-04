package com.docbench.benchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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
 * - OSON: OracleJsonObject can be mutable, supports in-place modification
 *
 * Test methodology:
 * 1. Fetch document as raw binary format
 * 2. Decode/copy to mutable structure
 * 3. Modify target field
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
    // Test 1: Decode-Modify-Encode Cycle
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Update cycle - position 1")
    void updateCycle_position1() throws SQLException {
        testUpdateCycle("upd-1", 100, 1);
    }

    @Test
    @Order(2)
    @DisplayName("Update cycle - position 50")
    void updateCycle_position50() throws SQLException {
        testUpdateCycle("upd-50", 100, 50);
    }

    @Test
    @Order(3)
    @DisplayName("Update cycle - position 100")
    void updateCycle_position100() throws SQLException {
        testUpdateCycle("upd-100", 100, 100);
    }

    @Test
    @Order(4)
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
        // Alternate value size each iteration (16-byte difference) to force offset recalculation
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";  // 16 spaces
            BsonDocument decoded = raw.decode(BSON_CODEC);
            decoded.put(fieldName, new BsonString(value));
            new RawBsonDocument(decoded, BSON_CODEC);
        }

        // Measure full cycle with alternating value sizes (16-byte difference)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";  // 16 spaces
            long start = System.nanoTime();
            // 1. Decode
            BsonDocument decoded = raw.decode(BSON_CODEC);
            // 2. Modify (alternating size forces offset recalculation)
            decoded.put(fieldName, new BsonString(value));
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
        // Alternate value size each iteration (16-byte difference)
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";  // 16 spaces
            OracleJsonObject mutable = copyToMutable(original);
            mutable.put(fieldName, jsonFactory.createString(value));
            serializeOsonToBytes(mutable);
        }

        // Measure full cycle with alternating value sizes (16-byte difference)
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            String value = (i % 2 == 0) ? "updated_" + i : "updated_" + i + "                ";  // 16 spaces
            long start = System.nanoTime();
            // 1. Create mutable copy
            OracleJsonObject mutable = copyToMutable(original);
            // 2. Modify (alternating size)
            mutable.put(fieldName, jsonFactory.createString(value));
            // 3. Serialize
            serializeOsonToBytes(mutable);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    // =========================================================================
    // Test 2: Nested Field Update
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("Nested update - depth 1")
    void nestedUpdate_depth1() throws SQLException {
        testNestedUpdate("nest-1", 1);
    }

    @Test
    @Order(11)
    @DisplayName("Nested update - depth 3")
    void nestedUpdate_depth3() throws SQLException {
        testNestedUpdate("nest-3", 3);
    }

    @Test
    @Order(12)
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
    // Test 3: Field Insertion (forces offset recalculation for subsequent fields)
    // =========================================================================

    @Test
    @Order(20)
    @DisplayName("Field insertion - at beginning")
    void fieldInsertion_beginning() throws SQLException {
        testFieldInsertion("ins-begin", 0, "insert at beginning");
    }

    @Test
    @Order(21)
    @DisplayName("Field insertion - at middle")
    void fieldInsertion_middle() throws SQLException {
        testFieldInsertion("ins-mid", 50, "insert at middle");
    }

    @Test
    @Order(22)
    @DisplayName("Field insertion - at end")
    void fieldInsertion_end() throws SQLException {
        testFieldInsertion("ins-end", 100, "insert at end");
    }

    private void testFieldInsertion(String testId, int insertPosition, String description) throws SQLException {
        int totalFields = 100;

        // Create document with 100 fields
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

        // Measure BSON field insertion
        long bsonNanos = measureBsonFieldInsertion(testId, insertPosition);

        // Measure OSON field insertion
        long osonNanos = measureOsonFieldInsertion(testId, insertPosition);

        String desc = "Insert field: " + description;
        results.put(testId, new TestResult(testId, desc, bsonNanos, osonNanos, "insertion"));

        System.out.printf("  %-30s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                desc, bsonNanos, osonNanos,
                (double) bsonNanos / Math.max(1, osonNanos));
    }

    private long measureBsonFieldInsertion(String docId, int insertPosition) {
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        String newFieldName = "new_field_" + String.format("%04d", insertPosition);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            BsonDocument decoded = raw.decode(BSON_CODEC);
            // Insert new field - BsonDocument maintains insertion order
            BsonDocument reordered = new BsonDocument();
            int fieldIndex = 0;
            for (Map.Entry<String, BsonValue> entry : decoded.entrySet()) {
                if (fieldIndex == insertPosition) {
                    reordered.put(newFieldName, new BsonString("inserted_value_" + i));
                }
                reordered.put(entry.getKey(), entry.getValue());
                fieldIndex++;
            }
            if (insertPosition >= fieldIndex) {
                reordered.put(newFieldName, new BsonString("inserted_value_" + i));
            }
            new RawBsonDocument(reordered, BSON_CODEC);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            BsonDocument decoded = raw.decode(BSON_CODEC);
            BsonDocument reordered = new BsonDocument();
            int fieldIndex = 0;
            for (Map.Entry<String, BsonValue> entry : decoded.entrySet()) {
                if (fieldIndex == insertPosition) {
                    reordered.put(newFieldName, new BsonString("inserted_value_" + i));
                }
                reordered.put(entry.getKey(), entry.getValue());
                fieldIndex++;
            }
            if (insertPosition >= fieldIndex) {
                reordered.put(newFieldName, new BsonString("inserted_value_" + i));
            }
            new RawBsonDocument(reordered, BSON_CODEC);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOsonFieldInsertion(String docId, int insertPosition) throws SQLException {
        OracleJsonObject original = fetchOracleJsonObject(docId);

        String newFieldName = "new_field_" + String.format("%04d", insertPosition);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            OracleJsonObject mutable = insertFieldAtPosition(original, newFieldName,
                    jsonFactory.createString("inserted_value_" + i), insertPosition);
            serializeOsonToBytes(mutable);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            OracleJsonObject mutable = insertFieldAtPosition(original, newFieldName,
                    jsonFactory.createString("inserted_value_" + i), insertPosition);
            serializeOsonToBytes(mutable);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private OracleJsonObject insertFieldAtPosition(OracleJsonObject source, String newField,
                                                    OracleJsonValue newValue, int position) {
        OracleJsonObject result = jsonFactory.createObject();
        int index = 0;
        for (String key : source.keySet()) {
            if (index == position) {
                result.put(newField, newValue);
            }
            OracleJsonValue value = source.get(key);
            if (value.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                result.put(key, deepCopyObject(value.asJsonObject()));
            } else {
                result.put(key, value);
            }
            index++;
        }
        if (position >= index) {
            result.put(newField, newValue);
        }
        return result;
    }

    // =========================================================================
    // Test 4: Array Growth (adding elements expands array, shifts subsequent offsets)
    // =========================================================================

    @Test
    @Order(30)
    @DisplayName("Array growth - add 1 element")
    void arrayGrowth_add1() throws SQLException {
        testArrayGrowth("arr-1", 1, "add 1 element");
    }

    @Test
    @Order(31)
    @DisplayName("Array growth - add 10 elements")
    void arrayGrowth_add10() throws SQLException {
        testArrayGrowth("arr-10", 10, "add 10 elements");
    }

    @Test
    @Order(32)
    @DisplayName("Array growth - add 100 elements")
    void arrayGrowth_add100() throws SQLException {
        testArrayGrowth("arr-100", 100, "add 100 elements");
    }

    private void testArrayGrowth(String testId, int elementsToAdd, String description) throws SQLException {
        // Create document with array in the middle (fields before and after)
        Document mongoDoc = new Document("_id", testId);
        StringBuilder oracleJson = new StringBuilder("{");

        // Add 50 fields before array
        for (int i = 1; i <= 50; i++) {
            String fieldName = "before_" + String.format("%04d", i);
            mongoDoc.append(fieldName, "value_" + i);
            oracleJson.append("\"").append(fieldName).append("\":\"value_").append(i).append("\",");
        }

        // Add array with 10 initial elements
        List<Integer> initialArray = new ArrayList<>();
        oracleJson.append("\"data_array\":[");
        for (int i = 0; i < 10; i++) {
            initialArray.add(i);
            if (i > 0) oracleJson.append(",");
            oracleJson.append(i);
        }
        oracleJson.append("],");
        mongoDoc.append("data_array", initialArray);

        // Add 50 fields after array
        for (int i = 1; i <= 50; i++) {
            String fieldName = "after_" + String.format("%04d", i);
            mongoDoc.append(fieldName, "value_" + i);
            if (i > 1) oracleJson.append(",");
            oracleJson.append("\"").append(fieldName).append("\":\"value_").append(i).append("\"");
        }
        oracleJson.append("}");

        docCollection.insertOne(mongoDoc);
        try (PreparedStatement ps = oracleConnection.prepareStatement(
                "INSERT INTO " + ORACLE_TABLE + " (id, doc) VALUES (?, ?)")) {
            ps.setString(1, testId);
            ps.setString(2, oracleJson.toString());
            ps.executeUpdate();
        }

        // Measure BSON array growth
        long bsonNanos = measureBsonArrayGrowth(testId, elementsToAdd);

        // Measure OSON array growth
        long osonNanos = measureOsonArrayGrowth(testId, elementsToAdd);

        String desc = "Array growth: " + description;
        results.put(testId, new TestResult(testId, desc, bsonNanos, osonNanos, "array"));

        System.out.printf("  %-30s: BSON=%8d ns, OSON=%8d ns, Ratio=%6.2fx%n",
                desc, bsonNanos, osonNanos,
                (double) bsonNanos / Math.max(1, osonNanos));
    }

    private long measureBsonArrayGrowth(String docId, int elementsToAdd) {
        RawBsonDocument raw = rawCollection.find(new Document("_id", docId)).first();
        if (raw == null) throw new RuntimeException("Document not found: " + docId);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            BsonDocument decoded = raw.decode(BSON_CODEC);
            BsonArray array = decoded.getArray("data_array");
            for (int j = 0; j < elementsToAdd; j++) {
                array.add(new BsonInt32(1000 + j));
            }
            new RawBsonDocument(decoded, BSON_CODEC);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            BsonDocument decoded = raw.decode(BSON_CODEC);
            BsonArray array = decoded.getArray("data_array");
            for (int j = 0; j < elementsToAdd; j++) {
                array.add(new BsonInt32(1000 + j));
            }
            new RawBsonDocument(decoded, BSON_CODEC);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private long measureOsonArrayGrowth(String docId, int elementsToAdd) throws SQLException {
        OracleJsonObject original = fetchOracleJsonObject(docId);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            OracleJsonObject mutable = copyToMutableWithArrayGrowth(original, "data_array", elementsToAdd);
            serializeOsonToBytes(mutable);
        }

        // Measure
        long totalNanos = 0;
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long start = System.nanoTime();
            OracleJsonObject mutable = copyToMutableWithArrayGrowth(original, "data_array", elementsToAdd);
            serializeOsonToBytes(mutable);
            totalNanos += System.nanoTime() - start;
        }

        return totalNanos / MEASUREMENT_ITERATIONS;
    }

    private OracleJsonObject copyToMutableWithArrayGrowth(OracleJsonObject source, String arrayField, int elementsToAdd) {
        OracleJsonObject mutable = jsonFactory.createObject();
        for (String key : source.keySet()) {
            OracleJsonValue value = source.get(key);
            if (key.equals(arrayField) && value.getOracleJsonType() == OracleJsonValue.OracleJsonType.ARRAY) {
                // Copy array and add new elements
                oracle.sql.json.OracleJsonArray srcArray = value.asJsonArray();
                oracle.sql.json.OracleJsonArray newArray = jsonFactory.createArray();
                for (int i = 0; i < srcArray.size(); i++) {
                    newArray.add(srcArray.get(i));
                }
                for (int i = 0; i < elementsToAdd; i++) {
                    newArray.add(jsonFactory.createDecimal(1000 + i));
                }
                mutable.put(key, newArray);
            } else if (value.getOracleJsonType() == OracleJsonValue.OracleJsonType.OBJECT) {
                mutable.put(key, deepCopyObject(value.asJsonObject()));
            } else {
                mutable.put(key, value);
            }
        }
        return mutable;
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
