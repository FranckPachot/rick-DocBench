# DocBench

**BSON vs OSON Performance Benchmark Suite**

DocBench provides comprehensive benchmarks comparing MongoDB's BSON and Oracle's OSON binary JSON formats across multiple dimensions:

1. **Client-Side Field Access**: O(n) vs O(1) algorithmic complexity
2. **Server-Side Updates**: MongoDB `$set` vs Oracle `JSON_TRANSFORM`

## Table of Contents

- [Latest Results](#latest-results)
- [Test Environment](#test-environment)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [Configuration](#configuration)
- [Running Benchmarks](#running-benchmarks)
- [AWR Report Generation](#awr-report-generation)
- [Understanding Results](#understanding-results)
- [Benchmark Details](#benchmark-details)

---

## Latest Results

### Executive Summary

With **durability parity** (both databases configured for durable writes):

| Metric | Result |
|--------|--------|
| **Server-Side Updates** | OSON 1.16x faster overall |
| **OSON Wins** | 23 tests |
| **MongoDB Wins** | 2 tests |

### Server-Side Update Performance

| Test Case | MongoDB (ns) | OSON (ns) | Ratio | Winner |
|-----------|--------------|-----------|-------|--------|
| Single update 100 fields | 1,952,820 | 1,310,893 | 0.67x | **OSON** |
| Single update 500 fields | 1,987,706 | 1,108,162 | 0.56x | **OSON** |
| Single update 1000 fields | 1,944,092 | 1,074,378 | 0.55x | **OSON** |
| Multi-update 3 fields | 1,971,949 | 1,309,900 | 0.66x | **OSON** |
| Multi-update 5 fields | 2,000,798 | 1,301,165 | 0.65x | **OSON** |
| Multi-update 10 fields | 1,959,636 | 1,303,595 | 0.67x | **OSON** |
| Large doc ~10KB | 1,951,444 | 1,062,652 | 0.54x | **OSON** |
| Large doc ~50KB | 1,986,630 | 1,058,811 | 0.53x | **OSON** |
| Large doc ~100KB | 1,984,275 | 1,103,444 | 0.56x | **OSON** |
| Large doc ~1MB | 2,798,974 | 1,059,747 | **0.38x** | **OSON** |
| Large doc ~4MB | 7,250,427 | 1,108,646 | **0.15x** | **OSON** |
| Scalar push 1x1 | 1,937,191 | 1,137,217 | 0.59x | **OSON** |
| Object push 1x1 | 1,908,510 | 1,247,177 | 0.65x | **OSON** |
| Object delete middle | 4,127,897 | 1,068,444 | **0.26x** | **OSON** |
| Large 1MB scalar array | 3,248,805 | 7,586,679 | 2.34x | MongoDB |
| Large 4MB scalar array | 7,817,765 | 38,106,668 | 4.87x | MongoDB |
| Large 1MB object array | 12,346,461 | 4,747,576 | 0.38x | **OSON** |
| Large 4MB object array | 14,521,214 | 11,346,450 | 0.78x | **OSON** |

### Client-Side Field Access (O(n) vs O(1))

| Test Case | BSON (ns) | OSON (ns) | Ratio | Winner |
|-----------|-----------|-----------|-------|--------|
| Position 1/100 | 309 | 128 | 2.41x | **OSON** |
| Position 50/100 | 2,799 | 96 | 29.16x | **OSON** |
| Position 100/100 | 3,388 | 57 | 59.44x | **OSON** |
| Position 500/500 | 16,275 | 115 | 141.52x | **OSON** |
| Position 1000/1000 | 31,724 | 75 | **422.99x** | **OSON** |
| **TOTAL** | 58,629 | 837 | **70.05x** | **OSON** |

### Key Findings

- **OSON dominates server-side updates**: 1.5-6.5x faster for most operations with durability parity
- **Client-side OSON dominates**: 70x faster for field access due to O(1) hash lookup vs BSON's O(n)
- **Large document updates**: OSON is 2.6-6.5x faster for 1-4MB documents
- **MongoDB wins only on large scalar arrays**: Specific optimization for scalar array operations

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

  Single update 100 fields: MongoDB=1952820 ns, OSON=1310893 ns, 0.67x OSON
  Large doc ~4096KB: MongoDB=7250427 ns, OSON=1108646 ns, 0.15x OSON
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

- `awr_00_baseline.html` - Protocol overhead baseline
- `awr_01_single_field.html` - Single field updates
- `awr_02_multi_field.html` - Multi-field updates
- `awr_03a-e_large_doc_*.html` - Large document updates (10KB to 4MB)
- `awr_04_array_push.html` - Array push operations
- `awr_05_array_delete.html` - Array delete operations
- `awr_06_large_array.html` - Large array operations

---

## Understanding Results

### Output Files

| File | Description |
|------|-------------|
| `reports/performance_report.html` | Interactive HTML report with all results |
| `reports/awr/*.html` | Oracle AWR reports per test category |

### Interpreting Ratios

| Ratio | Interpretation |
|-------|----------------|
| `0.67x OSON` | OSON is 1.5x faster (inverse: 1/0.67 = 1.49) |
| `0.15x OSON` | OSON is 6.7x faster (inverse: 1/0.15 = 6.67) |
| `2.34x MongoDB` | MongoDB is 2.34x faster |

---

## Benchmark Details

### Test Categories

#### 1. Client-Side Field Access (`BsonVsOsonClientSideTest`)

Measures document traversal efficiency:
- **BSON**: Sequential O(n) field scanning
- **OSON**: Hash-indexed O(1) lookup

#### 2. Server-Side Updates (`ServerSideUpdateTest`)

Database-native update operations with durability parity:
- **MongoDB**: `$set`, `$push`, `$pull` with `WriteConcern(w:1, j:true)`
- **Oracle**: `JSON_TRANSFORM` SQL function

Test scenarios:
- Single/multi-field updates (100-1000 fields)
- Large documents (10KB to 4MB)
- Array push/delete operations
- Large array operations (1MB to 4MB)

### Benchmark Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `WARMUP_ITERATIONS` | 100 | JIT warmup before measurement |
| `MEASUREMENT_ITERATIONS` | 1,000 | Iterations for timing |
| `ARRAY_MEASUREMENT_ITERATIONS` | 100 | Iterations for array tests |

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
- [MongoDB Replica Set for Testing](https://www.mongodb.com/docs/manual/tutorial/deploy-replica-set-for-testing/)
- [MongoDB Single-Node Replica Set Discussion](https://www.mongodb.com/community/forums/t/should-i-use-single-node-replica-set-for-production/190558)
- [Oracle JSON_TRANSFORM Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_TRANSFORM.html)
