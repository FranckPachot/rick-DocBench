# DocBench

**Extensible Database Document Performance Benchmarking Framework**

DocBench is a command-line benchmarking utility designed to provide empirical, reproducible measurements of document database performance characteristics with particular emphasis on **operational overhead decomposition**—isolating and measuring the distinct cost components that comprise total request latency beyond raw data access time.

## Key Features

- **Overhead Decomposition**: Breaks down operation latency into measurable components (connection, serialization, traversal, network, deserialization)
- **Binary JSON Comparison**: Compares BSON (MongoDB) O(n) traversal vs OSON (Oracle) O(1) hash-indexed navigation
- **Extensible Architecture**: Plugin-based adapter system for adding new database platforms
- **Reproducible Results**: Seeded random generation and deterministic document structures
- **Statistical Rigor**: HdrHistogram-based percentile tracking with high precision

## The Traversal Problem

Traditional benchmarks measure aggregate throughput but fail to show **where time is actually spent**. DocBench specifically measures:

| Format | Traversal Strategy | Complexity |
|--------|-------------------|------------|
| BSON (MongoDB) | Length-prefixed, sequential field-name scanning | O(n) per level |
| OSON (Oracle) | Hash-indexed jump navigation via SQL/JSON | O(1) per level |

At scale (millions of documents, deeply nested paths), this difference compounds significantly.

## Benchmark Results

Real benchmark results comparing BSON vs OSON field access performance:

```
================================================================================
  BSON vs OSON Performance Comparison (SQL/JSON)
================================================================================

Test Case                            BSON (μs)  OSON (μs)   Ratio  Winner
--------------------------------------------------------------------------------
Position 1/100 (projection)              1519        650   2.34x  OSON
Position 50/100 (projection)             1026        545   1.88x  OSON
Position 100/100 (projection)             820        453   1.81x  OSON
Position 500/500 (projection)             859        448   1.92x  OSON
Depth 1 projection                        831        524   1.59x  OSON
Depth 3 projection                        604        383   1.58x  OSON
Depth 5 projection                        621        363   1.71x  OSON
Depth 8 projection                        657        400   1.64x  OSON
3 fields from 200                         696        400   1.74x  OSON
5 fields from 200                         643        412   1.56x  OSON
50 fields (full read)                     604        751   0.80x  BSON
200 fields (full read)                    699        861   0.81x  BSON
customer.tier (nested)                    587        383   1.53x  OSON
grandTotal (last field)                   522        370   1.41x  OSON
--------------------------------------------------------------------------------
TOTAL                                   10688       6943   1.54x  OSON

Summary:
  BSON wins: 2 (full document reads)
  OSON wins: 12 (field projections)
  Overall: OSON 1.54x faster
================================================================================
```

**Key Finding**: OSON's O(1) hash-indexed access via `JSON_VALUE` is **1.5-2.3x faster** for field projection operations. Full document reads favor BSON slightly.

## Quick Start

### Prerequisites

- Java 21+ (Java 23 recommended)
- Gradle 8.5+
- Docker (for integration tests)
- MongoDB 7.0+
- Oracle Database 23ai Free

### Build

```bash
./gradlew build
```

### Run Tests

```bash
# Unit tests (158 tests)
./gradlew test

# Integration tests (requires Docker with MongoDB and Oracle)
./gradlew integrationTest

# Run BSON vs OSON benchmark comparison
./gradlew integrationTest --tests "*.BsonVsOsonComparisonTest"

# Mutation testing
./gradlew pitest
```

### Configuration

Create `config/local.properties`:

```properties
# MongoDB configuration
mongodb.uri=mongodb://user:pass@localhost:27017/docbench
mongodb.database=docbench

# Oracle configuration (23ai with SQL/JSON)
oracle.url=jdbc:oracle:thin:@localhost:1521/FREEPDB1
oracle.username=docbench
oracle.password=your_password
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DocBench CLI                            │
│   [Command Parser] [Config Loader] [Report Generator] [Progress]│
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
│  │  BSON Metrics  │  │  SQL/JSON O(1)  │  │  PostgreSQL, etc │ │
│  └────────────────┘  └─────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Status

### Completed ✅

- **Phase 1: Core Infrastructure**
  - TimeSource, RandomSource utilities
  - OverheadBreakdown record with timing decomposition
  - MetricsCollector with HdrHistogram integration
  - DatabaseAdapter SPI with full interface hierarchy
  - CLI structure with picocli

- **Phase 2: MongoDB/BSON Adapter**
  - MongoDBAdapter with instrumented connection
  - BsonTimingInterceptor for command timing
  - BsonDeserializationTimer for field access measurement
  - Full CRUD operations with overhead breakdown

- **Phase 3: Oracle/OSON Adapter**
  - OracleOsonAdapter using native SQL/JSON (not SODA)
  - JSON_VALUE for O(1) field extraction
  - JSON_TRANSFORM for O(1) updates
  - Connection pooling via Oracle UCP

- **Benchmark Comparison**
  - 14 comparison tests across document complexities
  - Field position, nesting depth, array size tests
  - Automated results reporting

### Pending 📋

- **Phase 4: Workloads and Reporting**
  - Workload definitions (traverse-shallow, traverse-deep, etc.)
  - Report generators (Console, JSON, CSV, HTML)
  - CLI commands (run, compare, report, list, validate)

## Metrics

### Core Latency Metrics

| Metric | Description |
|--------|-------------|
| `total_latency` | End-to-end operation time |
| `server_execution_time` | DB-reported execution |
| `server_traversal_time` | Document navigation (server) |
| `client_deserialization_time` | Response parsing (client) |
| `serialization_time` | Request preparation |

### Derived Metrics

| Metric | Formula |
|--------|---------|
| `overhead_ratio` | (total - server_fetch) / total |
| `traversal_ratio` | (server_trav + client_trav) / total |
| `efficiency_score` | server_fetch / total |

## Development

This project follows **strict Test-Driven Development** practices:

1. **Red**: Write failing test first
2. **Green**: Write minimum code to pass
3. **Refactor**: Clean up while keeping tests green

### Project Structure

```
com.docbench
├── cli                     # Command-line interface (picocli)
├── config                  # Configuration management
├── orchestrator            # Benchmark execution
├── workload                # Workload definitions
├── metrics                 # Measurement and collection
├── adapter                 # Database adapter SPI
│   ├── spi                 # Core interfaces
│   ├── mongodb             # MongoDB/BSON implementation
│   └── oracle              # Oracle SQL/JSON implementation
├── document                # Test document generation
├── report                  # Output generation
└── util                    # Utilities (TimeSource, RandomSource)
```

### Test Summary

- **158 unit tests** - Core functionality
- **17 Oracle integration tests** - SQL/JSON operations
- **14+ MongoDB integration tests** - BSON operations
- **14 benchmark comparison tests** - BSON vs OSON

### Code Quality

- **Coverage**: 80%+ line, 70%+ branch
- **Mutation Score**: 60%+ (PIT)
- **Java Version**: 21+ (23 recommended)

## License

MIT License - see [LICENSE](LICENSE) file.

## Contributing

Contributions are welcome! Please ensure all code follows TDD practices and includes comprehensive tests.
