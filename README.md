# DocBench

**BSON vs OSON Performance Benchmark Suite**

DocBench provides comprehensive benchmarks comparing MongoDB's BSON and Oracle's OSON binary JSON formats across multiple dimensions:

1. **Client-Side Field Access**: O(n) vs O(1) algorithmic complexity
2. **Server-Side Updates**: MongoDB `$set` vs Oracle `JSON_TRANSFORM`

## Table of Contents

- [Latest Results](#latest-results)
- [Benchmark Details](#benchmark-details)
- [Test Environment](#test-environment)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [Configuration](#configuration)
- [Running Benchmarks](#running-benchmarks)
- [AWR Report Generation](#awr-report-generation)
- [Understanding Results](#understanding-results)
- [Troubleshooting](#troubleshooting)

---

## Latest Results

### Executive Summary

With **durability parity** (both databases configured for durable writes):

| Metric | Result |
|--------|--------|
| **Server-Side Updates** | OSON 1.19x faster overall |
| **Client-Side Field Access** | OSON 71x faster overall |
| **OSON Wins** | 30 tests |
| **MongoDB Wins** | 3 tests |

---

## Benchmark Details

### 1. Client-Side Field Access (O(n) vs O(1))

**What this measures:** The time to access a single field from an already-fetched document on the client side.

| Technology | Method | Complexity | Description |
|------------|--------|------------|-------------|
| **BSON (MongoDB)** | `RawBsonDocument.get()` | O(n) | Sequential scanning through binary document to find field |
| **OSON (Oracle)** | `OracleJsonObject.get()` | O(1) | Hash-indexed lookup directly to field offset |

**Why it matters:** For documents with many fields, BSON's linear scanning becomes increasingly expensive while OSON maintains constant-time access regardless of field position.

#### Field Position Tests

Tests access fields at different positions (1st, 50th, 100th, etc.) to demonstrate O(n) vs O(1) scaling behavior.

| Test Case | Description | BSON (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|-----------|-----------|-------|--------|
| Position 1/100 | First field in 100-field document | 250 | 99 | 2.53x | **OSON** |
| Position 50/100 | Middle field in 100-field document | 2,491 | 87 | 28.63x | **OSON** |
| Position 100/100 | Last field in 100-field document | 3,250 | 52 | 62.50x | **OSON** |
| Position 500/500 | Last field in 500-field document | 15,699 | 108 | 145.36x | **OSON** |
| Position 1000/1000 | Last field in 1000-field document | 31,195 | 59 | **528.73x** | **OSON** |

#### Nested Field Access Tests

Tests access fields at varying depths of nesting (e.g., `doc.level1.level2.level3.field`).

| Test Case | Description | BSON (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|-----------|-----------|-------|--------|
| Nested depth 1 | Single level nesting | 598 | 110 | 5.44x | **OSON** |
| Nested depth 3 | Three levels deep | 1,363 | 115 | 11.85x | **OSON** |
| Nested depth 5 | Five levels deep | 2,144 | 170 | 12.61x | **OSON** |

**Key Insight:** BSON time increases linearly with field position (O(n) scaling), while OSON remains constant (~100ns) regardless of position (O(1)).

---

### 2. Server-Side Update Performance

**What this measures:** The time to update field(s) in a document stored in the database, including network round-trip and durability guarantees.

| Technology | Method | Description |
|------------|--------|-------------|
| **MongoDB** | `$set` operator | Updates with `WriteConcern(w:1, j:true)` for durability parity |
| **Oracle** | `JSON_TRANSFORM` | SQL function for partial OSON updates |

**Why it matters:** Server-side updates are the most common operation in document databases. OSON's partial update capability allows modifying specific fields without rewriting the entire document.

#### Single Field Update

Updates a single field in documents of varying sizes (100, 500, 1000 fields). Tests whether update cost scales with document size.

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Single update 100 fields | Update 1 field in 100-field doc | 1,900,402 | 1,232,474 | 0.65x | **OSON** |
| Single update 500 fields | Update 1 field in 500-field doc | 1,993,721 | 1,062,068 | 0.53x | **OSON** |
| Single update 1000 fields | Update 1 field in 1000-field doc | 2,006,917 | 1,116,121 | 0.56x | **OSON** |

#### Multi-Field Update

Updates multiple fields (3, 5, 10) in a single operation. Tests batch update efficiency.

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Multi-update 3 fields | Update 3 fields simultaneously | 1,949,823 | 1,158,361 | 0.59x | **OSON** |
| Multi-update 5 fields | Update 5 fields simultaneously | 1,975,875 | 1,156,843 | 0.59x | **OSON** |
| Multi-update 10 fields | Update 10 fields simultaneously | 1,965,112 | 1,126,143 | 0.57x | **OSON** |

#### Large Document Update

Updates a single field in increasingly large documents (10KB to 4MB). Tests whether update cost scales with document size. **MongoDB must rewrite the entire document; OSON performs in-place partial updates.**

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Large doc ~10KB | Single field update | 1,977,267 | 1,037,956 | 0.52x | **OSON** |
| Large doc ~50KB | Single field update | 1,929,809 | 1,050,259 | 0.54x | **OSON** |
| Large doc ~100KB | Single field update | 1,929,778 | 1,059,995 | 0.55x | **OSON** |
| Large doc ~1MB | Single field update | 2,892,836 | 1,113,762 | **0.39x** | **OSON** |
| Large doc ~4MB | Single field update | 7,385,070 | 1,104,344 | **0.15x** | **OSON** |

**Key Finding:** MongoDB's time increases with document size (O(n) rewrite), while OSON remains constant (~1.1ms) regardless of document size (partial update).

#### Array Push Operations

Appends elements to an array field. Tests `$push` (MongoDB) vs `JSON_TRANSFORM APPEND` (Oracle). "10x1" means 10 sequential single-element pushes.

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Scalar push 1x1 | Push 1 integer to array | 1,907,349 | 1,117,662 | 0.59x | **OSON** |
| Scalar push 10x1 | Push 10 integers (10 operations) | 19,196,365 | 11,572,733 | 0.60x | **OSON** |
| Object push 1x1 | Push 1 object to array | 2,028,989 | 1,194,319 | 0.59x | **OSON** |
| Object push 10x1 | Push 10 objects (10 operations) | 20,169,646 | 13,865,948 | 0.69x | **OSON** |

#### Array Delete Operations

Removes elements from different array positions (beginning, middle, end). Tests `$pull` (MongoDB) vs `JSON_TRANSFORM REMOVE` (Oracle). **Middle deletes are particularly expensive for MongoDB due to array shifting.**

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Scalar delete beginning | Remove first array element | 1,647,896 | 1,099,521 | 0.67x | **OSON** |
| Scalar delete middle | Remove middle array element | 4,282,104 | 1,115,265 | **0.26x** | **OSON** |
| Scalar delete end | Remove last array element | 1,640,492 | 1,238,377 | 0.75x | **OSON** |
| Object delete beginning | Remove first object from array | 2,336,335 | 1,039,076 | 0.44x | **OSON** |
| Object delete middle | Remove middle object from array | 4,218,771 | 1,048,887 | **0.25x** | **OSON** |
| Object delete end | Remove last object from array | 2,274,331 | 1,022,622 | 0.45x | **OSON** |

**Key Finding:** MongoDB's middle-position deletes are ~4x slower than end deletes due to array element shifting. OSON maintains consistent ~1.1ms regardless of position.

#### Large Array Operations

Appends elements to very large arrays (1MB, 4MB). Tests scalability with array size. **MongoDB has specific optimizations for scalar arrays.**

| Test Case | Description | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|-------------|--------------|-----------|-------|--------|
| Large 1MB scalar array | Push to 1MB integer array | 3,129,667 | 4,015,566 | 1.28x | MongoDB |
| Large 4MB scalar array | Push to 4MB integer array | 7,660,251 | 22,889,482 | 2.99x | MongoDB |
| Large 1MB object array | Push to 1MB object array | 4,869,838 | 4,817,506 | 0.99x | **OSON** |
| Large 4MB object array | Push to 4MB object array | 12,449,115 | 18,552,229 | 1.49x | MongoDB |

**Key Finding:** MongoDB wins on large scalar arrays (specialized optimization), but performance is closer for object arrays.

---

### Summary by Category

| Category | MongoDB Wins | OSON Wins | Advantage |
|----------|--------------|-----------|-----------|
| Client-Side Field Access | 0 | 8 | OSON 71x faster |
| Single Field Update | 0 | 3 | OSON 1.6-1.9x faster |
| Multi-Field Update | 0 | 3 | OSON 1.7x faster |
| Large Document Update | 0 | 5 | OSON 1.8-6.7x faster |
| Array Push | 0 | 4 | OSON 1.5-1.7x faster |
| Array Delete | 0 | 6 | OSON 1.3-4x faster |
| Large Array | 3 | 1 | MongoDB 1.3-3x faster |
| **TOTAL** | **3** | **30** | **OSON wins overall** |

### Key Findings

- **Client-side OSON dominates**: 71x faster for field access due to O(1) hash lookup vs BSON's O(n) sequential scanning
- **OSON partial updates shine at scale**: For large documents (1-4MB), OSON is 2.6-6.7x faster because it modifies only the changed field
- **Middle-position array operations**: OSON is 3-4x faster for deletes from array middle because it doesn't need to shift elements
- **MongoDB wins only on large scalar arrays**: Specific optimization for homogeneous scalar array operations

---

## Test Environment

### Benchmark Configuration

This benchmark uses **production-like durability settings** on both databases to ensure a fair comparison:

| Database | Configuration | Durability Behavior |
|----------|---------------|---------------------|
| **MongoDB** | Single-member replica set, `w:1`, `j:true` | Waits for journal sync before acknowledging |
| **Oracle** | Standard configuration | Waits for redo log sync before acknowledging (mandatory) |

### Why Single-Member Replica Set?

MongoDB is configured as a **single-member replica set** rather than standalone mode for several reasons:

1. **Durability Parity**: According to the [MongoDB Journaling documentation](https://www.mongodb.com/docs/manual/core/journaling/), WiredTiger syncs journal records to disk immediately when `j:true` is specified only *"for replica set members (primary and secondary members)"*. Standalone instances rely on the default 100ms sync interval regardless of write concern. This ensures MongoDB waits for writes to be persisted to the journal before acknowledging, matching Oracle's mandatory redo log sync behavior.

2. **Production-Like Configuration**: According to [MongoDB documentation](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-for-testing/), replica sets are recommended even for development to test replica set features. The [MongoDB Community](https://www.mongodb.com/community/forums/t/should-i-use-single-node-replica-set-for-production/190558) notes: *"A single-member replica set leaves flexibility for features like Change Streams, adding a hidden secondary for hot backup, and being able to quickly scale back up later."*

3. **Fair Comparison**: Without `j:true`, MongoDB acknowledges writes after they reach server memory but before journal sync. Oracle always waits for redo log sync. This asymmetry would give MongoDB an unfair speed advantage at the cost of durability.

### Write Concern Details

```java
// MongoDB write concern used in benchmarks
WriteConcern durableWriteConcern = WriteConcern.W1.withJournal(true);
```

- **`w:1`**: Write acknowledged by primary (no waiting for replication)
- **`j:true`**: Wait for journal sync to disk before acknowledging

This matches Oracle's behavior where every COMMIT waits for redo log flush (`log file sync` wait event).

### Hardware & Software

| Component | Version/Specification |
|-----------|----------------------|
| **Oracle Database** | 26ai Free (23.26.0.0.0) |
| **MongoDB** | 8.0.16 |
| **Java** | OpenJDK 23.0.2 |
| **Platform** | WSL2 Linux (Windows 11) |
| **Storage** | SSD |

### Oracle Tuning Applied

- Redo logs: 3 x 500MB
- `session_cached_cursors`: 200
- `open_cursors`: 500

### Benchmark Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `WARMUP_ITERATIONS` | 100 | JIT warmup before measurement |
| `MEASUREMENT_ITERATIONS` | 1,000 | Iterations for timing |
| `ARRAY_MEASUREMENT_ITERATIONS` | 100 | Iterations for array tests |

---

## Prerequisites

### Required Software

| Component | Minimum Version | Recommended |
|-----------|-----------------|-------------|
| Java JDK | 21+ | 21 LTS or 23 |
| Gradle | 8.0+ | 8.5+ (wrapper included) |
| MongoDB | 7.0+ | 8.0+ |
| Oracle Database | 23ai Free | 26ai (23.26+) |

### Hardware Recommendations

For accurate benchmark results:
- **CPU**: 4+ cores dedicated to databases
- **Memory**: 16GB+ (8GB for Oracle, 4GB for MongoDB)
- **Storage**: SSD recommended for consistent I/O
- **Network**: Local connections (localhost) to minimize network variance

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/rhoulihan/DocBench.git
cd DocBench
```

### 2. Verify Java Version

```bash
java -version
# Should show: openjdk version "21.x.x" or higher
```

### 3. Build the Project

```bash
./gradlew build -x test
```

---

## Database Setup

### MongoDB Setup (Single-Member Replica Set)

For fair benchmarking with durability parity, MongoDB must be configured as a replica set.

#### Docker Setup (Recommended)

1. **Create a keyfile for replica set authentication:**

```bash
mkdir -p mongodb-keyfile
openssl rand -base64 756 > mongodb-keyfile/mongo-keyfile
chmod 400 mongodb-keyfile/mongo-keyfile
```

2. **Start MongoDB with replica set enabled:**

```bash
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  -v mongodb-keyfile:/data/keyfile \
  mongo:8.0 \
  mongod --replSet rs0 --bind_ip_all --keyFile /data/keyfile/mongo-keyfile
```

3. **Initialize the replica set:**

```bash
docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin \
  --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"
```

4. **Create benchmark user:**

```bash
docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin <<EOF
use testdb
db.createUser({
  user: "translator",
  pwd: "translator",
  roles: [{ role: "readWrite", db: "testdb" }]
})
EOF
```

5. **Verify replica set status:**

```bash
docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin \
  --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr}))"
# Should show: [ { name: 'localhost:27017', state: 'PRIMARY' } ]
```

#### Why Not Standalone?

Per the [MongoDB Journaling documentation](https://www.mongodb.com/docs/manual/core/journaling/):
- Standalone mode does not trigger immediate journal sync with `j:true`; only replica set members sync immediately
- This means standalone relies on the 100ms sync interval, which doesn't provide true durability parity with Oracle
- Single-member replica sets also provide access to transactions and change streams

### Oracle Setup

#### Docker (Recommended)

```bash
# Pull Oracle 23ai Free (or 26ai when available)
docker run -d \
  --name oracle-free \
  -p 1521:1521 \
  -e ORACLE_PWD=YourPassword123 \
  container-registry.oracle.com/database/free:latest

# Wait for database to start (check logs)
docker logs -f oracle-free
# Wait until you see: "DATABASE IS READY TO USE!"
```

#### Create Benchmark User

Connect as SYSDBA and run:

```sql
-- Connect to PDB
ALTER SESSION SET CONTAINER = FREEPDB1;

-- Create user
CREATE USER translator IDENTIFIED BY translator
  DEFAULT TABLESPACE users
  QUOTA UNLIMITED ON users;

-- Grant basic privileges
GRANT CONNECT, RESOURCE TO translator;
GRANT CREATE SESSION TO translator;
GRANT CREATE TABLE TO translator;
GRANT CREATE PROCEDURE TO translator;

-- Grant JSON/SODA privileges
GRANT SODA_APP TO translator;

-- Grant AWR privileges (required for AWR report generation)
GRANT SELECT ON V_$INSTANCE TO translator;
GRANT SELECT ON V_$DATABASE TO translator;
GRANT SELECT ON V_$SESSION TO translator;
GRANT SELECT ON V_$SQLAREA TO translator;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO translator;
```

### Oracle Performance Tuning

**CRITICAL**: Apply these settings for accurate benchmark results.

#### 1. Redo Log Configuration

Small redo logs cause checkpoint waits that skew results. Increase to 500MB each:

```sql
-- Connect as SYSDBA
-- Check current redo log sizes
SELECT group#, bytes/1024/1024 AS size_mb, status FROM v$log;

-- Add new larger log groups (if current logs are < 500MB)
ALTER DATABASE ADD LOGFILE GROUP 4 SIZE 500M;
ALTER DATABASE ADD LOGFILE GROUP 5 SIZE 500M;
ALTER DATABASE ADD LOGFILE GROUP 6 SIZE 500M;

-- Switch logs and drop old small groups
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM CHECKPOINT;

-- Drop old groups (only INACTIVE groups can be dropped)
ALTER DATABASE DROP LOGFILE GROUP 1;
ALTER DATABASE DROP LOGFILE GROUP 2;
ALTER DATABASE DROP LOGFILE GROUP 3;
```

#### 2. Cursor Caching

```sql
ALTER SYSTEM SET session_cached_cursors = 200 SCOPE = SPFILE;
ALTER SYSTEM SET open_cursors = 500 SCOPE = SPFILE;
-- Restart database to apply
```

---

## Configuration

### Create Configuration File

```bash
cp config/local.properties.example config/local.properties
```

Edit `config/local.properties`:

```properties
# MongoDB Configuration (with replica set)
mongodb.uri=mongodb://translator:translator@localhost:27017/testdb?replicaSet=rs0&authSource=testdb
mongodb.database=testdb

# Oracle Configuration
oracle.url=jdbc:oracle:thin:@localhost:1521/FREEPDB1
oracle.username=translator
oracle.password=translator
```

**Important**: Include `replicaSet=rs0` in the MongoDB URI to enable replica set features including `j:true` write concern.

---

## Running Benchmarks

### Run All Benchmarks

```bash
./gradlew clean integrationTest --rerun-tasks
```

### Run Specific Test Suites

```bash
# Client-side field access (O(n) vs O(1))
./gradlew integrationTest --tests "*.BsonVsOsonClientSideTest"

# Server-side updates with AWR reports
./gradlew integrationTest --tests "*.ServerSideUpdateTest"
```

### Expected Output

```
Server-Side Update: JSON_TRANSFORM vs MongoDB $set
  MongoDB: $set operator
    - WriteConcern: w=1, j=true (journal sync for durability parity)
    - Single-member replica set configuration

  Single update 100 fields: MongoDB=1900402 ns, OSON=1232474 ns, 0.65x OSON
  Large doc ~4096KB: MongoDB=7385070 ns, OSON=1104344 ns, 0.15x OSON
```

---

## AWR Report Generation

### Prerequisites

AWR reports require Oracle privileges:

```sql
GRANT SELECT ON V_$INSTANCE TO translator;
GRANT SELECT ON V_$DATABASE TO translator;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO translator;
```

### Generated Reports

After running `ServerSideUpdateTest`, AWR reports are saved to `build/reports/awr/`:

| Report | Description |
|--------|-------------|
| `awr_00_baseline.html` | Protocol overhead baseline (non-JSON operations) |
| `awr_01_single_field.html` | Single field update tests |
| `awr_02_multi_field.html` | Multi-field update tests |
| `awr_03a_large_doc_10KB.html` | 10KB document update |
| `awr_03b_large_doc_50KB.html` | 50KB document update |
| `awr_03c_large_doc_100KB.html` | 100KB document update |
| `awr_03d_large_doc_1MB.html` | 1MB document update |
| `awr_03e_large_doc_4MB.html` | 4MB document update |
| `awr_04_array_push.html` | Array push operations |
| `awr_05_array_delete.html` | Array delete operations |
| `awr_06_large_array.html` | Large array operations |

---

## Understanding Results

### Output Files

| File | Description |
|------|-------------|
| `reports/performance_report.html` | Interactive HTML report with all results and test descriptions |
| `reports/awr/*.html` | Oracle AWR reports per test category |

### Interpreting Ratios

| Ratio | Interpretation |
|-------|----------------|
| `0.65x OSON` | OSON is 1.54x faster (inverse: 1/0.65 = 1.54) |
| `0.15x OSON` | OSON is 6.67x faster (inverse: 1/0.15 = 6.67) |
| `2.99x MongoDB` | MongoDB is 2.99x faster |

---

## Troubleshooting

### MongoDB Replica Set Issues

```
MongoServerError: not primary
```

**Solution**: Verify replica set is initialized:
```bash
docker exec mongodb mongosh -u admin -p password --authenticationDatabase admin --eval "rs.status()"
```

### Oracle Checkpoint Waits

Check for checkpoint waits:
```sql
SELECT event, total_waits FROM v$system_event WHERE event LIKE '%checkpoint%';
```

**Solution**: Increase redo log size to 500MB each.

---

## License

MIT License - see [LICENSE](LICENSE) file.

---

## References

- [MongoDB Write Concern Documentation](https://www.mongodb.com/docs/manual/reference/write-concern/)
- [MongoDB Journaling Documentation](https://www.mongodb.com/docs/manual/core/journaling/)
- [MongoDB Replica Set for Testing](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-for-testing/)
- [MongoDB Single-Node Replica Set Discussion](https://www.mongodb.com/community/forums/t/should-i-use-single-node-replica-set-for-production/190558)
- [Oracle JSON_TRANSFORM Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_TRANSFORM.html)
