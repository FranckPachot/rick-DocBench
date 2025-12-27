# DocBench Implementation Plan

## Test-Driven Development Implementation Strategy

**Architect**: Senior Java Architect (25 years experience)
**Methodology**: Strict TDD with Red-Green-Refactor cycles
**Target Runtime**: Java 21+ (Java 23 recommended)

---

## Current Status

### Phase 1: Foundation ✅ COMPLETE
- [x] Project scaffolding with Gradle 8.5
- [x] TimeSource, RandomSource abstractions
- [x] OverheadBreakdown record with derived calculations
- [x] MetricsCollector with HdrHistogram integration
- [x] DatabaseAdapter SPI with full interface hierarchy
- [x] Operation sealed interface hierarchy
- [x] Capability enum for adapter contracts
- [x] CLI structure with picocli

### Phase 2: MongoDB/BSON Adapter ✅ COMPLETE
- [x] TestContainers MongoDB configuration
- [x] MongoDBConnectionConfig with URI parsing
- [x] MongoDBInstrumentedConnection with timing hooks
- [x] BsonTimingInterceptor for command timing
- [x] BsonDeserializationTimer for field access measurement
- [x] MongoDBAdapter CRUD operations
- [x] Full overhead breakdown extraction
- [x] 126+ unit tests passing

### Phase 3: Oracle/OSON Adapter ✅ COMPLETE
- [x] TestContainers Oracle 23ai Free configuration
- [x] OracleInstrumentedConnection with SQL/JSON support
- [x] OracleOsonAdapter using native SQL/JSON (NOT SODA)
- [x] JSON_VALUE for O(1) field extraction
- [x] JSON_TRANSFORM for O(1) updates
- [x] Connection pooling via Oracle UCP
- [x] 21 unit tests + 17 integration tests passing

### Benchmark Comparison ✅ COMPLETE
- [x] BsonVsOsonComparisonTest with 14 test cases
- [x] Field position impact tests
- [x] Nesting depth tests
- [x] Multi-field projection tests
- [x] Full document read baseline
- [x] Automated results reporting

**Results**: OSON 1.54x faster overall (12/14 wins for field projection)

### Phase 4: Workloads & Reporting 📋 PENDING
- [ ] WorkloadConfig with parameter validation
- [ ] DocumentGenerator with seeded determinism
- [ ] TraverseShallowWorkload
- [ ] TraverseDeepWorkload
- [ ] TraverseScaleWorkload
- [ ] DeserializeFullWorkload
- [ ] DeserializePartialWorkload
- [ ] ConsoleReporter
- [ ] JsonReporter
- [ ] CsvReporter
- [ ] HtmlReporter
- [ ] ComparisonReport

---

## Guiding Principles

### TDD Rules (Kent Beck's Three Laws)
1. **Red**: Write a failing test before writing any production code
2. **Green**: Write the minimum production code to make the test pass
3. **Refactor**: Clean up the code while keeping tests green

### Architectural Constraints
- All public APIs must have tests written FIRST
- No production code without a corresponding test
- Mutation testing (PIT) score > 60% required
- Test isolation: No shared state between tests
- Constructor injection for all dependencies

---

## Phase 1: Foundation (Core Interfaces & Metrics) ✅

### 1.1 Project Setup
```
docbench/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
└── src/
    ├── main/java/com/docbench/
    ├── test/java/com/docbench/
    └── integrationTest/java/com/docbench/
```

**Dependencies**:
- Java 21+ (Java 23 recommended)
- JUnit 5.10+
- Mockito 5+
- AssertJ 3.24+
- HdrHistogram 2.1+
- picocli 4.7+
- Guice 7+
- SnakeYAML 2+
- TestContainers 1.19+
- PIT Mutation Testing

### 1.2 Core Interfaces (Implemented)

1. **TimeSource** - Testable timing abstraction
2. **RandomSource** - Seeded reproducibility
3. **OverheadBreakdown** - Central timing record
4. **MetricsCollector** - HdrHistogram integration
5. **Capability** - Adapter contract enum
6. **Operation** - Sealed interface hierarchy
7. **DatabaseAdapter** - SPI interface
8. **ConnectionConfig** - Immutable configuration
9. **InstrumentedConnection** - Timing hooks

---

## Phase 2: MongoDB Adapter ✅

### Key Components Implemented

1. **MongoDBAdapter** - Full DatabaseAdapter implementation
2. **MongoDBInstrumentedConnection** - Connection wrapper with timing
3. **BsonTimingInterceptor** - CommandListener for server timing
4. **BsonDeserializationTimer** - Field access measurement

### BSON Traversal Characteristics Verified
- O(n) field scanning confirmed
- Field position impacts access time
- Full document deserialization overhead captured

---

## Phase 3: Oracle OSON Adapter ✅

### Key Design Decision: SQL/JSON over SODA

Originally planned to use Oracle SODA API, but switched to native SQL/JSON for:
- **Direct O(1) access**: `JSON_VALUE(doc, '$.path')` uses OSON hash index
- **Lower overhead**: No SODA abstraction layer
- **Better projection**: Server-side field extraction
- **Standard SQL**: Portable query patterns

### Components Implemented

1. **OracleOsonAdapter** - SQL/JSON-based adapter
   - `INSERT INTO table (id, doc) VALUES (?, JSON(?))`
   - `SELECT JSON_VALUE(doc, '$.path') FROM table WHERE id = ?`
   - `UPDATE table SET doc = JSON_TRANSFORM(doc, SET '$.path' = ?) WHERE id = ?`

2. **OracleInstrumentedConnection** - JDBC wrapper with UCP pooling

### OSON Traversal Characteristics Verified
- O(1) hash-indexed field access confirmed
- Field position does NOT impact access time
- 1.5-2.3x faster than BSON for projections

---

## Phase 4: Workloads & Reporting (Pending)

### 4.1 Workload Implementation (TDD)

1. **WorkloadConfig**
   - Test: Parameter validation, defaults, sweeps
   - Impl: Type-safe configuration

2. **DocumentGenerator**
   - Test: Deterministic generation, schema compliance
   - Impl: Builder with seed support

3. **TraverseShallowWorkload**
   - Test: Operation generation, field position control
   - Impl: Single-level targeting

4. **TraverseDeepWorkload**
   - Test: Nested path generation, array access
   - Impl: Multi-level navigation

5. **TraverseScaleWorkload**
   - Test: Batch generation, access patterns
   - Impl: Volume testing

### 4.2 Reporting Implementation (TDD)

1. **ConsoleReporter** - ANSI table output
2. **JsonReporter** - Machine-readable results
3. **CsvReporter** - Spreadsheet export
4. **HtmlReporter** - Visual charts
5. **ComparisonReport** - Multi-adapter deltas

---

## Test Categories & Coverage

### Current Test Counts

| Category | Tests | Status |
|----------|-------|--------|
| Unit Tests | 158 | ✅ Passing |
| MongoDB Integration | 14+ | ✅ Passing |
| Oracle Integration | 17 | ✅ Passing |
| Benchmark Comparison | 14 | ✅ Passing |

### Unit Tests (80%+ line coverage)
- All pure functions
- Record validations
- Configuration parsing
- Report formatting

### Integration Tests (TestContainers)
- MongoDB operations
- Oracle SQL/JSON operations
- End-to-end workflows

### Benchmark Tests
- BSON vs OSON comparison
- Field position impact
- Nesting depth impact
- Document size impact

---

## Continuous Integration

### GitHub Actions Workflow
```yaml
name: CI

on: [push, pull_request]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          java-version: '21'
          distribution: 'temurin'
      - name: Build
        run: ./gradlew build
      - name: Test
        run: ./gradlew test
      - name: Integration Test
        run: ./gradlew integrationTest
      - name: Mutation Test
        run: ./gradlew pitest
```

---

## Definition of Done

Each component is "done" when:
1. All tests pass (unit + integration)
2. Code coverage > 80% lines
3. Mutation score > 60%
4. Javadoc complete for public APIs
5. No FindBugs/SpotBugs warnings
6. Code reviewed

---

*This plan ensures rigorous TDD discipline while building a production-quality benchmarking framework.*
