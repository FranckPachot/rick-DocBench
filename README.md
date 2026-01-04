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
Position 1/100                        282          102      2.76x OSON
Position 50/100                      2423          113     21.44x OSON
Position 100/100                     3034           51     59.49x OSON
Position 500/500                    14881          101    147.34x OSON
Position 1000/1000                  28819           71    405.90x OSON
Nested depth 1                        599          121      4.95x OSON
Nested depth 3                       1312          128     10.25x OSON
Nested depth 5                       2033          182     11.17x OSON
--------------------------------------------------------------------------------
TOTAL                               53383          869     61.43x OSON

O(n) Scaling: Position 1 -> 1000 = BSON time increased 102.20x
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
- **OSON** (`OracleJsonObject.get`): O(1) hash lookup—constant ~50-180ns regardless of position
- At position 1000, OSON is **406x faster** than BSON
- Overall, OSON is **61.43x faster** for client-side field access

### BSON O(n) Scaling Proof

The `ClientSideAccessScalingTest` isolates BSON behavior:

```
RawBsonDocument.get() - Sequential BSON parsing:
Position        Time (ns)  Ratio to P1
----------------------------------------
1                     173        1.00x
100                  2881       16.65x
500                 14610       84.45x
999                 28626      165.47x
```

This confirms O(n) complexity: accessing position 999 takes **165x longer** than position 1.

### Update Efficiency Benchmark

The `UpdateEfficiencyTest` compares full update cycles (decode → modify → encode):

```
================================================================================
  BSON vs OSON UPDATE EFFICIENCY
================================================================================
UPDATE CYCLE TESTS (with alternating value sizes):
Test Case                           BSON (ns)    OSON (ns)      Ratio Winner
--------------------------------------------------------------------------------
Update cycle pos 1/100                  11541        12503      0.92x BSON
Update cycle pos 50/100                 11372        11015      1.03x OSON
Update cycle pos 100/100                11363        11365      1.00x BSON
Update cycle pos 500/500                54040        59936      0.90x BSON

NESTED UPDATE TESTS:
Test Case                           BSON (ns)    OSON (ns)      Ratio Winner
--------------------------------------------------------------------------------
Nested update depth 1                    2767         3008      0.92x BSON
Nested update depth 3                    4641         3705      1.25x OSON
Nested update depth 5                    7009         5526      1.27x OSON

FIELD INSERTION TESTS:
Test Case                           BSON (ns)    OSON (ns)      Ratio Winner
--------------------------------------------------------------------------------
Insert at beginning                     12667        11753      1.08x OSON
Insert at middle                        12733        11148      1.14x OSON
Insert at end                           12413        11060      1.12x OSON

ARRAY GROWTH TESTS:
Test Case                           BSON (ns)    OSON (ns)      Ratio Winner
--------------------------------------------------------------------------------
Array growth: add 1 element             13833        12029      1.15x OSON
Array growth: add 10 elements           12483        12414      1.01x OSON
Array growth: add 100 elements          14838        15579      0.95x BSON

--------------------------------------------------------------------------------
OVERALL                               181700       181041      1.00x (tie)
================================================================================
```

**Key Findings:**
- Update efficiency is **roughly equal** between BSON and OSON (~1:1 ratio)
- Update cycles alternate value sizes each iteration to force offset recalculation
- For nested updates at depth 3+, OSON is **1.25x faster** due to O(1) navigation
- Field insertion position has **no significant impact** - full document rebuild dominates
- Both formats require full decode → modify → encode cycles, masking structural differences

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

# Run BSON vs OSON field access comparison
./gradlew integrationTest --tests "*.BsonVsOsonClientSideTest"

# Run BSON O(n) scaling test
./gradlew integrationTest --tests "*.ClientSideAccessScalingTest"

# Run update efficiency comparison
./gradlew integrationTest --tests "*.UpdateEfficiencyTest"
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

### Why RawBsonDocument?

We use `RawBsonDocument` (not `Document`) because:
- `Document` uses `LinkedHashMap` internally → O(1) after initial parse
- `RawBsonDocument` parses BSON bytes on each `.get()` → O(n) every time

This reflects real-world scenarios where BSON parsing cost is paid:
- Server-side query processing
- Initial document parsing from network
- Re-parsing cached BSON bytes
- Change stream processing

## License

MIT License - see [LICENSE](LICENSE) file.
