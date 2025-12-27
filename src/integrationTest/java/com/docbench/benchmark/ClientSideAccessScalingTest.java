package com.docbench.benchmark;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Client-Side BSON Access Scaling Test: Demonstrates O(n) field scanning.
 *
 * This test measures the time to access fields at different POSITIONS
 * within a BSON document on the CLIENT SIDE after fetching the full document.
 *
 * BSON format stores fields sequentially with length prefixes. To access
 * field at position N, the parser must skip over fields 0 through N-1.
 * This is O(n) where n is the field position.
 *
 * Test methodology:
 * 1. Create a document with 1000 fields
 * 2. Fetch the FULL document (no projection)
 * 3. Access field at position 1, 100, 500, 1000
 * 4. Measure client-side field access time
 */
@DisplayName("Client-Side BSON O(n) Field Access")
@Tag("benchmark")
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientSideAccessScalingTest {

    private static final int FIELD_COUNT = 1000;
    private static final int WARMUP_ITERATIONS = 1000;
    private static final int MEASUREMENT_ITERATIONS = 10000;

    // Test positions to access
    private static final int[] POSITIONS = {1, 10, 50, 100, 200, 500, 750, 999};

    private static MongoClient mongoClient;
    private static MongoCollection<Document> collection;
    private static MongoCollection<RawBsonDocument> rawCollection;
    private static String docId;

    // Results
    private static final Map<Integer, Long> documentAccessResults = new LinkedHashMap<>();
    private static final Map<Integer, Long> rawBsonAccessResults = new LinkedHashMap<>();

    @BeforeAll
    static void setup() {
        Properties props = loadConfigProperties();

        String uri = props.getProperty("mongodb.uri");
        String dbName = props.getProperty("mongodb.database", "testdb");

        mongoClient = MongoClients.create(uri);
        MongoDatabase db = mongoClient.getDatabase(dbName);

        // Drop and recreate collection
        db.getCollection("client_access_test").drop();
        collection = db.getCollection("client_access_test");
        rawCollection = db.getCollection("client_access_test", RawBsonDocument.class);

        // Create and insert test document with 1000 fields
        docId = "test-doc-1000";
        Document doc = new Document("_id", docId);
        for (int i = 0; i < FIELD_COUNT; i++) {
            doc.append("field_" + String.format("%03d", i),
                    "Value for field " + i + " with some padding to make fields reasonably sized.");
        }
        collection.insertOne(doc);

        System.out.println("\n" + "=".repeat(80));
        System.out.println("  Client-Side BSON O(n) Field Access Test");
        System.out.println("  Document has " + FIELD_COUNT + " fields");
        System.out.println("  Testing field access at positions: " + Arrays.toString(POSITIONS));
        System.out.println("=".repeat(80));

        // Global warmup
        System.out.println("\nWarming up...");
        Document warmupDoc = collection.find(new Document("_id", docId)).first();
        for (int i = 0; i < 1000; i++) {
            warmupDoc.get("field_500");
        }
        System.out.println("Warmup complete.\n");
    }

    @AfterAll
    static void teardown() {
        if (mongoClient != null) {
            mongoClient.getDatabase("testdb").getCollection("client_access_test").drop();
            mongoClient.close();
        }

        printResults();
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
    // Test 1: Document.get() access at different positions
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("Document.get() - position 1")
    void documentAccess_position1() {
        testDocumentAccess(1);
    }

    @Test
    @Order(2)
    @DisplayName("Document.get() - position 10")
    void documentAccess_position10() {
        testDocumentAccess(10);
    }

    @Test
    @Order(3)
    @DisplayName("Document.get() - position 50")
    void documentAccess_position50() {
        testDocumentAccess(50);
    }

    @Test
    @Order(4)
    @DisplayName("Document.get() - position 100")
    void documentAccess_position100() {
        testDocumentAccess(100);
    }

    @Test
    @Order(5)
    @DisplayName("Document.get() - position 200")
    void documentAccess_position200() {
        testDocumentAccess(200);
    }

    @Test
    @Order(6)
    @DisplayName("Document.get() - position 500")
    void documentAccess_position500() {
        testDocumentAccess(500);
    }

    @Test
    @Order(7)
    @DisplayName("Document.get() - position 750")
    void documentAccess_position750() {
        testDocumentAccess(750);
    }

    @Test
    @Order(8)
    @DisplayName("Document.get() - position 999")
    void documentAccess_position999() {
        testDocumentAccess(999);
    }

    private void testDocumentAccess(int position) {
        String fieldName = "field_" + String.format("%03d", position);

        // Fetch document
        Document doc = collection.find(new Document("_id", docId)).first();
        if (doc == null) throw new AssertionError("Document not found");

        // Warmup field access
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            doc.get(fieldName);
        }

        // Measure field access time
        long startNanos = System.nanoTime();
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            doc.get(fieldName);
        }
        long endNanos = System.nanoTime();

        long avgNanos = (endNanos - startNanos) / MEASUREMENT_ITERATIONS;
        documentAccessResults.put(position, avgNanos);

        System.out.printf("  Position %4d: Document.get() = %6d ns%n", position, avgNanos);
    }

    // =========================================================================
    // Test 2: RawBsonDocument access (truly sequential parsing)
    // =========================================================================

    @Test
    @Order(10)
    @DisplayName("RawBsonDocument - position 1")
    void rawBsonAccess_position1() {
        testRawBsonAccess(1);
    }

    @Test
    @Order(11)
    @DisplayName("RawBsonDocument - position 100")
    void rawBsonAccess_position100() {
        testRawBsonAccess(100);
    }

    @Test
    @Order(12)
    @DisplayName("RawBsonDocument - position 500")
    void rawBsonAccess_position500() {
        testRawBsonAccess(500);
    }

    @Test
    @Order(13)
    @DisplayName("RawBsonDocument - position 999")
    void rawBsonAccess_position999() {
        testRawBsonAccess(999);
    }

    private void testRawBsonAccess(int position) {
        String fieldName = "field_" + String.format("%03d", position);

        // Fetch as RawBsonDocument (not pre-parsed)
        RawBsonDocument rawDoc = rawCollection.find(new Document("_id", docId)).first();
        if (rawDoc == null) throw new AssertionError("RawBsonDocument not found");

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            rawDoc.get(fieldName);
        }

        // Measure
        long startNanos = System.nanoTime();
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            rawDoc.get(fieldName);
        }
        long endNanos = System.nanoTime();

        long avgNanos = (endNanos - startNanos) / MEASUREMENT_ITERATIONS;
        rawBsonAccessResults.put(position, avgNanos);

        System.out.printf("  Position %4d: RawBsonDocument.get() = %6d ns%n", position, avgNanos);
    }

    private static void printResults() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  CLIENT-SIDE BSON ACCESS SCALING RESULTS");
        System.out.println("=".repeat(80));

        System.out.println("\nDocument.get() - Accessing parsed Document object:");
        System.out.printf("%-12s %12s %12s%n", "Position", "Time (ns)", "Ratio to P1");
        System.out.println("-".repeat(40));

        Long baselineDoc = documentAccessResults.get(1);
        for (int pos : POSITIONS) {
            Long time = documentAccessResults.get(pos);
            if (time != null && baselineDoc != null) {
                double ratio = (double) time / baselineDoc;
                System.out.printf("%-12d %12d %11.2fx%n", pos, time, ratio);
            }
        }

        System.out.println("\nRawBsonDocument.get() - Sequential BSON parsing:");
        System.out.printf("%-12s %12s %12s%n", "Position", "Time (ns)", "Ratio to P1");
        System.out.println("-".repeat(40));

        Long baselineRaw = rawBsonAccessResults.get(1);
        for (int pos : new int[]{1, 100, 500, 999}) {
            Long time = rawBsonAccessResults.get(pos);
            if (time != null && baselineRaw != null) {
                double ratio = (double) time / baselineRaw;
                System.out.printf("%-12d %12d %11.2fx%n", pos, time, ratio);
            }
        }

        System.out.println("\n" + "-".repeat(80));
        System.out.println("INTERPRETATION:");
        System.out.println("  - Document.get(): Uses LinkedHashMap internally - O(1) hash lookup");
        System.out.println("  - RawBsonDocument.get(): Parses BSON sequentially - should show O(n)");
        System.out.println("  - If RawBsonDocument shows increasing time with position, that's O(n)");
        System.out.println("=".repeat(80) + "\n");
    }
}
