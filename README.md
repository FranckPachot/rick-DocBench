# DocBench

**BSON vs OSON Performance Benchmark Suite**

DocBench provides comprehensive benchmarks comparing MongoDB's BSON and Oracle's OSON binary JSON formats across multiple dimensions:

1. **Client-Side Field Access**: O(n) vs O(1) algorithmic complexity
2. **Server-Side Updates**: MongoDB `$set` vs Oracle `JSON_TRANSFORM`
3. **Protocol Overhead**: Wire protocol efficiency comparison

## Table of Contents

- [Latest Results](#latest-results)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Database Setup](#database-setup)
  - [MongoDB Setup](#mongodb-setup)
  - [Oracle Setup](#oracle-setup)
  - [Oracle Performance Tuning](#oracle-performance-tuning)
- [Configuration](#configuration)
- [Running Benchmarks](#running-benchmarks)
- [AWR Report Generation](#awr-report-generation)
- [Understanding Results](#understanding-results)
- [Benchmark Details](#benchmark-details)

---

## Latest Results

**Test Environment**: Oracle 26ai (23.26.0.0.0), MongoDB 8.0, Java 21, WSL2 Linux

### Executive Summary

| Metric | Raw Performance | Adjusted (Protocol Overhead Removed) |
|--------|-----------------|--------------------------------------|
| **Server-Side Updates** | MongoDB 1.58x faster | MongoDB 1.32x faster |
| **MongoDB Wins** | 22 tests | 20 tests |
| **OSON Wins** | 3 tests | 5 tests |

### Client-Side Field Access (O(n) vs O(1))

| Test Case | BSON (ns) | OSON (ns) | Ratio | Winner |
|-----------|-----------|-----------|-------|--------|
| Position 1/100 | 388 | 98 | 3.96x | OSON |
| Position 50/100 | 2,474 | 103 | 24.02x | OSON |
| Position 100/100 | 3,183 | 53 | 60.06x | OSON |
| Position 500/500 | 15,599 | 101 | 154.45x | OSON |
| Position 1000/1000 | 30,393 | 59 | **515.14x** | OSON |
| **TOTAL** | 56,062 | 811 | **69.13x** | **OSON** |

### Protocol Overhead Analysis

| Metric | Value |
|--------|-------|
| MongoDB baseline (non-JSON $inc) | 418,096 ns |
| Oracle baseline (non-JSON UPDATE) | 862,908 ns |
| Oracle protocol overhead delta | 444,812 ns (2.06x MongoDB) |

### Key Findings

- **Client-side OSON dominates**: 69x faster for field access due to O(1) hash lookup
- **OSON wins at scale**: 4MB documents show OSON 2x faster than MongoDB
- **Protocol overhead**: Oracle's JDBC/SQL adds ~445µs fixed cost per operation
- **Crossover point**: ~1MB document size where OSON's efficiency exceeds protocol overhead

---

## Prerequisites

### Required Software

| Component | Minimum Version | Recommended |
|-----------|-----------------|-------------|
| Java JDK | 21+ | 21 LTS |
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

### MongoDB Setup

#### Option A: Docker (Recommended)

```bash
# Pull and run MongoDB
docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  mongo:8.0

# Create benchmark user
docker exec -it mongodb mongosh -u admin -p password --authenticationDatabase admin <<EOF
use testdb
db.createUser({
  user: "translator",
  pwd: "translator",
  roles: [{ role: "readWrite", db: "testdb" }]
})
EOF
```

#### Option B: Native Installation

1. Install MongoDB following [official docs](https://www.mongodb.com/docs/manual/installation/)
2. Create database and user:

```javascript
use testdb
db.createUser({
  user: "translator",
  pwd: "translator",
  roles: [{ role: "readWrite", db: "testdb" }]
})
```

### Oracle Setup

#### Option A: Docker (Recommended)

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

#### Option B: Native Installation

1. Download Oracle Database 23ai Free from [Oracle](https://www.oracle.com/database/free/)
2. Follow installation guide for your platform
3. Create pluggable database (PDB) if not using default FREEPDB1

### Create Benchmark User

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

-- Verify grants
SELECT * FROM dba_sys_privs WHERE grantee = 'TRANSLATOR';
```

### Oracle Performance Tuning

**CRITICAL**: Apply these settings for accurate benchmark results.

#### 1. Redo Log Configuration

Small redo logs cause checkpoint waits that skew results. Increase to 500MB each:

```sql
-- Connect as SYSDBA
ALTER SESSION SET CONTAINER = FREEPDB1;

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
-- Repeat SWITCH LOGFILE until old groups show INACTIVE
ALTER DATABASE DROP LOGFILE GROUP 1;
ALTER DATABASE DROP LOGFILE GROUP 2;
ALTER DATABASE DROP LOGFILE GROUP 3;

-- Verify new configuration
SELECT group#, bytes/1024/1024 AS size_mb, status FROM v$log;
-- Should show: 3 groups of 500MB each
```

#### 2. Cursor Caching

Increase cursor caching for prepared statement performance:

```sql
-- Connect as SYSDBA
ALTER SYSTEM SET session_cached_cursors = 200 SCOPE = SPFILE;
ALTER SYSTEM SET open_cursors = 500 SCOPE = SPFILE;

-- Restart database to apply SPFILE changes
SHUTDOWN IMMEDIATE;
STARTUP;

-- Verify settings
SHOW PARAMETER session_cached_cursors;
SHOW PARAMETER open_cursors;
```

#### 3. Verify Configuration

```sql
-- Check redo log configuration (should be 3x500MB)
SELECT group#, bytes/1024/1024 AS size_mb, status FROM v$log;

-- Check cursor settings
SHOW PARAMETER session_cached_cursors;  -- Should be 200
SHOW PARAMETER open_cursors;            -- Should be 500

-- Check for checkpoint waits (should be 0 or very low)
SELECT event, total_waits, time_waited
FROM v$system_event
WHERE event LIKE '%checkpoint%';
```

---

## Configuration

### Create Configuration File

Copy the example and edit with your credentials:

```bash
cp config/local.properties.example config/local.properties
```

Edit `config/local.properties`:

```properties
# MongoDB Configuration
mongodb.uri=mongodb://translator:translator@localhost:27017/testdb?authSource=testdb
mongodb.database=testdb

# Oracle Configuration
oracle.url=jdbc:oracle:thin:@localhost:1521/FREEPDB1
oracle.username=translator
oracle.password=translator
```

### Configuration Options

| Property | Description | Example |
|----------|-------------|---------|
| `mongodb.uri` | MongoDB connection string | `mongodb://user:pass@host:27017/db` |
| `mongodb.database` | Database name | `testdb` |
| `oracle.url` | JDBC connection URL | `jdbc:oracle:thin:@host:1521/SERVICE` |
| `oracle.username` | Oracle username | `translator` |
| `oracle.password` | Oracle password | `translator` |

### Connection String Formats

**MongoDB with authentication:**
```
mongodb://username:password@hostname:27017/database?authSource=authDb
```

**Oracle with service name:**
```
jdbc:oracle:thin:@hostname:1521/SERVICE_NAME
```

**Oracle with SID:**
```
jdbc:oracle:thin:@hostname:1521:SID
```

**Oracle with TNS:**
```
jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=hostname)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=service)))
```

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

# BSON O(n) scaling proof
./gradlew integrationTest --tests "*.ClientSideAccessScalingTest"

# Client-side update efficiency
./gradlew integrationTest --tests "*.UpdateEfficiencyTest"

# Server-side updates with AWR reports
./gradlew integrationTest --tests "*.ServerSideUpdateTest"
```

### Test Options

```bash
# Force fresh run (ignore up-to-date checks)
./gradlew integrationTest --rerun-tasks

# Show detailed output
./gradlew integrationTest --info

# Run with specific JVM options
./gradlew integrationTest -Dorg.gradle.jvmargs="-Xmx4g"
```

### Expected Output

```
> Task :integrationTest

Server-Side Update: JSON_TRANSFORM vs MongoDB $set
  AWR enabled - CON_DBID: 1234567890, Instance: 1
  AWR reports will be saved to: /path/to/DocBench/build/reports/awr

  Single update 100 fields: MongoDB=396494 ns, OSON=1011496 ns, 2.55x MongoDB
  ...
  Large doc ~4096KB: MongoDB=1856564 ns, OSON=920924 ns, 0.50x OSON

BUILD SUCCESSFUL
```

---

## AWR Report Generation

### Prerequisites

AWR (Automatic Workload Repository) reports require:

1. **Oracle Enterprise Edition features** (available in Oracle Free for development)
2. **Privileges granted to benchmark user** (see [Oracle Setup](#create-benchmark-user))

### AWR Privileges Required

```sql
GRANT SELECT ON V_$INSTANCE TO translator;
GRANT SELECT ON V_$DATABASE TO translator;
GRANT SELECT ON V_$SESSION TO translator;
GRANT SELECT ON V_$SQLAREA TO translator;
GRANT EXECUTE ON DBMS_WORKLOAD_REPOSITORY TO translator;
```

### Generated Reports

After running `ServerSideUpdateTest`, AWR reports are saved to:

```
build/reports/awr/
├── awr_00_baseline.html          # Protocol overhead baseline
├── awr_01_single_field.html      # Single field updates
├── awr_02_multi_field.html       # Multi-field updates
├── awr_03a_large_doc_10KB.html   # 10KB document updates
├── awr_03b_large_doc_50KB.html   # 50KB document updates
├── awr_03c_large_doc_100KB.html  # 100KB document updates
├── awr_03d_large_doc_1MB.html    # 1MB document updates
├── awr_03e_large_doc_4MB.html    # 4MB document updates
├── awr_04_array_push.html        # Array push operations
├── awr_05_array_delete.html      # Array delete operations
└── awr_06_large_array.html       # Large array operations
```

### Viewing Reports

Copy reports to `reports/awr/` for version control:

```bash
cp build/reports/awr/*.html reports/awr/
```

Open in browser to analyze Oracle performance metrics including:
- SQL execution statistics
- Wait events
- I/O statistics
- Buffer cache efficiency

---

## Understanding Results

### Output Files

| File | Description |
|------|-------------|
| `reports/performance_report.html` | Interactive HTML report with all results |
| `reports/awr/*.html` | Oracle AWR reports per test category |
| `build/reports/tests/integrationTest/` | JUnit test results |

### Performance Report Sections

1. **Executive Summary**: Overall winner and win counts
2. **Protocol Overhead Analysis**: Fixed cost per database round-trip
3. **Raw Performance Results**: Actual measured times
4. **Adjusted Performance**: Results with protocol overhead removed

### Interpreting Ratios

| Ratio | Interpretation |
|-------|----------------|
| `2.55x MongoDB` | MongoDB is 2.55x faster |
| `0.50x OSON` | OSON is 2x faster (inverse: 1/0.50 = 2) |
| `1.00x` | Equal performance |

### Key Metrics

- **Raw Performance**: Actual end-to-end operation time
- **Adjusted Performance**: Operation time minus protocol overhead
- **Protocol Overhead**: Fixed cost of database round-trip (~445µs for Oracle vs ~418µs for MongoDB)

---

## Benchmark Details

### Test Categories

#### 1. Client-Side Field Access (`BsonVsOsonClientSideTest`)

Measures document traversal efficiency:
- **BSON**: Sequential O(n) field scanning
- **OSON**: Hash-indexed O(1) lookup

Tests positions 1, 50, 100, 500, 1000 in documents of corresponding sizes.

#### 2. BSON Scaling Test (`ClientSideAccessScalingTest`)

Proves BSON's O(n) complexity by measuring access time at different field positions within the same document.

#### 3. Update Efficiency (`UpdateEfficiencyTest`)

Client-side update cycles: decode → modify → encode
- Update at various positions
- Nested updates at depths 1, 3, 5
- Field insertion
- Array growth

#### 4. Server-Side Updates (`ServerSideUpdateTest`)

Database-native update operations:
- **MongoDB**: `$set`, `$push`, `$pull` operators
- **Oracle**: `JSON_TRANSFORM` SQL function

Test scenarios:
- Single/multi-field updates
- Large documents (10KB to 4MB)
- Array push/delete operations
- Large array operations (1MB to 4MB)

### Benchmark Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `WARMUP_ITERATIONS` | 100 | JIT warmup before measurement |
| `MEASUREMENT_ITERATIONS` | 1,000 | Iterations for timing |
| `ARRAY_WARMUP_ITERATIONS` | 10 | Warmup for array tests |
| `ARRAY_MEASUREMENT_ITERATIONS` | 100 | Iterations for array timing |

---

## Troubleshooting

### Common Issues

#### "AWR not available" Error

```
AWR not available: ORA-00942: table or view does not exist
```

**Solution**: Grant AWR privileges (see [AWR Privileges Required](#awr-privileges-required))

#### Slow Oracle Performance

Check for checkpoint waits:
```sql
SELECT event, total_waits FROM v$system_event WHERE event LIKE '%checkpoint%';
```

**Solution**: Increase redo log size (see [Redo Log Configuration](#1-redo-log-configuration))

#### MongoDB Connection Refused

```
MongoSocketOpenException: Exception opening socket
```

**Solution**: Verify MongoDB is running and port 27017 is accessible:
```bash
docker ps | grep mongo
nc -zv localhost 27017
```

#### Oracle Connection Timeout

```
ORA-12170: TNS:Connect timeout occurred
```

**Solution**: Verify Oracle listener is running:
```bash
docker logs oracle-free | tail -20
# Or for native install:
lsnrctl status
```

---

## License

MIT License - see [LICENSE](LICENSE) file.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run all tests: `./gradlew clean integrationTest`
4. Submit a pull request

## Contact

For questions or issues, please open a GitHub issue.
