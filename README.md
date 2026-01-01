# DocBench

**BSON vs OSON Client-Side Field Access Benchmark**

DocBench demonstrates the O(n) vs O(1) algorithmic complexity difference between MongoDB's BSON and Oracle's OSON binary JSON formats for client-side field access.

## The O(n) vs O(1) Problem

| Format | Traversal Strategy | Complexity |
|--------|-------------------|------------|
| BSON (MongoDB) | Sequential field-name scanning | O(n) per level |
| OSON (Oracle) | Hash-indexed jump navigation | O(1) per level |

At scale (large documents, deeply nested paths), this difference compounds significantly.

## Benchmark Results

Client-side field access comparison (100K iterations, network overhead eliminated):

```
================================================================================
  BSON O(n) vs OSON O(1) - Client-Side Field Access
================================================================================
Test Case                       BSON (ns)    OSON (ns)      Ratio
--------------------------------------------------------------------------------
Position 1/100                        394           88      4.48x OSON
Position 50/100                      2729           89     30.66x OSON
Position 100/100                     3227           51     63.27x OSON
Position 500/500                    15566           92    169.20x OSON
Position 1000/1000                  30640           59    519.32x OSON
Nested depth 1                        694          102      6.80x OSON
Nested depth 3                       1300          111     11.71x OSON
Nested depth 5                       1979          154     12.85x OSON
--------------------------------------------------------------------------------
TOTAL                               56529          746     75.78x OSON

O(n) Scaling: Position 1 -> 1000 = BSON time increased 77.8x
================================================================================
```

### Test Descriptions

| Test | Description | What It Proves |
|------|-------------|----------------|
| **Position 1/100** | Access first field in 100-field document | Best case for BSON (minimal scanning) |
| **Position 50/100** | Access middle field in 100-field document | BSON must skip 49 fields |
| **Position 100/100** | Access last field in 100-field document | Worst case: BSON scans all 99 prior fields |
| **Position 500/500** | Access last field in 500-field document | Scaling test with larger document |
| **Position 1000/1000** | Access last field in 1000-field document | Maximum O(n) penalty for BSON |
| **Nested depth 1** | Access `level1.value` | Single level of nesting |
| **Nested depth 3** | Access `level1.level2.level3.value` | Multi-level path traversal |
| **Nested depth 5** | Access 5-level nested path | Deep nesting compounds O(n) cost |

### Key Findings

- **BSON** (`RawBsonDocument.get`): O(n) sequential scanning—time increases with field position
- **OSON** (`OracleJsonObject.get`): O(1) hash lookup—constant ~60-150ns regardless of position
- At position 1000, OSON is **519x faster** than BSON

## Quick Start

### Prerequisites

- Java 21+
- MongoDB 7.0+
- Oracle Database 23ai Free

### Configuration

Create `config/local.properties`:

```properties
# MongoDB
mongodb.uri=mongodb://user:pass@localhost:27017/testdb
mongodb.database=testdb

# Oracle 23ai
oracle.url=jdbc:oracle:thin:@localhost:1521/FREEPDB1
oracle.username=docbench
oracle.password=your_password
```

### Run Benchmarks

```bash
# Run all benchmarks
./gradlew integrationTest

# Run BSON vs OSON comparison only
./gradlew integrationTest --tests "*.BsonVsOsonClientSideTest"

# Run BSON O(n) scaling test only
./gradlew integrationTest --tests "*.ClientSideAccessScalingTest"
```

## How It Works

The benchmark uses:
- **MongoDB**: `RawBsonDocument.get()` - parses raw BSON bytes on each access (O(n))
- **Oracle**: `OracleJsonObject.get()` - native OSON with hash-indexed lookup (O(1))

Both tests:
1. Fetch the full document once (network cost excluded from measurement)
2. Access a specific field 100,000 times
3. Measure only the client-side field access time

This isolates the **format-level** parsing cost, demonstrating why OSON's hash-indexed structure outperforms BSON's sequential layout.

## License

MIT License - see [LICENSE](LICENSE) file.
