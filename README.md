# DocBench

**Extensible Database Document Performance Benchmarking Framework**

DocBench measures document database performance with emphasis on **overhead decomposition**—isolating the distinct cost components that comprise total request latency beyond raw data access time.

## The O(n) vs O(1) Problem

Traditional benchmarks measure aggregate throughput but fail to show **where time is actually spent**. DocBench specifically measures the algorithmic complexity difference in binary JSON formats:

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

**Key Findings:**
- **BSON** (`RawBsonDocument.get`): O(n) sequential scanning—time increases with field position
- **OSON** (`OracleJsonObject.get`): O(1) hash lookup—constant ~60-150ns regardless of position
- At position 1000, OSON is **519x faster** than BSON

## Quick Start

### Prerequisites

- Java 21+ (Java 23 recommended)
- Gradle 8.5+
- Docker (for integration tests)
- MongoDB 7.0+
- Oracle Database 23ai Free

### Build & Test

```bash
# Build
./gradlew build

# Unit tests
./gradlew test

# Integration tests (requires MongoDB and Oracle)
./gradlew integrationTest

# Run client-side O(n) vs O(1) benchmark
./gradlew integrationTest --tests "*.BsonVsOsonClientSideTest"
```

### Configuration

Create `config/local.properties`:

```properties
# MongoDB
mongodb.uri=mongodb://user:pass@localhost:27017/docbench
mongodb.database=docbench

# Oracle 23ai
oracle.url=jdbc:oracle:thin:@localhost:1521/FREEPDB1
oracle.username=docbench
oracle.password=your_password
```

### CLI Usage

```bash
# List available workloads and adapters
./gradlew run --args="list --verbose"

# Run specific workload
./gradlew run --args="run -w traverse -a mongodb --iterations 1000"

# Dry run validation
./gradlew run --args="run --all-workloads -a mongodb -a oracle-oson --dry-run"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DocBench CLI                            │
│   [Command Parser] [Config Loader] [Report Generator]          │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                    Benchmark Orchestrator                       │
│   [Workload Registry] [Execution Engine] [Metrics Collector]    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                  Database Adapter Layer (SPI)                   │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────┐ │
│  │ MongoDBAdapter │  │OracleOSONAdapter│  │ [Future Adapters]│ │
│  │  BSON O(n)     │  │  SQL/JSON O(1)  │  │  PostgreSQL, etc │ │
│  └────────────────┘  └─────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Overhead Decomposition**: Breaks down latency into connection, serialization, traversal, network, and deserialization components
- **Binary JSON Comparison**: Compares BSON O(n) traversal vs OSON O(1) hash-indexed navigation
- **Extensible Architecture**: Plugin-based adapter system for adding database platforms
- **Reproducible Results**: Seeded random generation and deterministic document structures
- **Statistical Rigor**: HdrHistogram-based percentile tracking

## Metrics

| Metric | Description |
|--------|-------------|
| `total_latency` | End-to-end operation time |
| `server_execution_time` | DB-reported execution |
| `client_deserialization_time` | Response parsing (client) |
| `serialization_time` | Request preparation |
| `overhead_ratio` | (total - server_fetch) / total |

## Project Structure

```
com.docbench
├── cli/          # Command-line interface (picocli)
├── config/       # Configuration management
├── orchestrator/ # Benchmark execution
├── workload/     # Workload definitions
├── metrics/      # Measurement and collection
├── adapter/      # Database adapter SPI
│   ├── spi/      # Core interfaces
│   ├── mongodb/  # MongoDB/BSON implementation
│   └── oracle/   # Oracle SQL/JSON implementation
├── document/     # Test document generation
├── report/       # Output generation
└── util/         # Utilities (TimeSource, RandomSource)
```

## Development

This project follows **Test-Driven Development** practices with 238 unit tests and 48 integration tests.

```bash
# Run all tests
./gradlew test integrationTest

# Mutation testing
./gradlew pitest
```

## License

MIT License - see [LICENSE](LICENSE) file.
