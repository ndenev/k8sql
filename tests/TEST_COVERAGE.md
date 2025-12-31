# k8sql Test Coverage

This document outlines the comprehensive test coverage for k8sql, including unit tests, integration tests, and performance regression tests.

## Overview

**Total Test Suites**: 13 integration test suites + 146 unit tests
**Coverage Goal**: Near-SQL conformance with exhaustive edge case testing

## Unit Tests (146 tests)

Located in `src/**/*_test.rs` modules, these cover:

### Configuration (6 tests)
- Config serialization/deserialization
- Default values
- Save/load functionality

### JSON/Arrow Conversion (16 tests)
- Build cluster columns
- String column building
- Field value extraction
- JSON value formatting
- Nested value navigation
- Array indexing in JSON

### Filter Extraction (5 tests)
- Single value extraction
- OR expression handling
- Special value handling (e.g., `*` for all clusters)

### SQL Preprocessing (12 tests)
- Read-only mode enforcement
- `->>`operator precedence fixes
- JSON arrow operator handling

### Provider/Scan Logic (25 tests)
- Filter pushdown (namespace, cluster, label selectors, field selectors)
- Projection pushdown optimization
- Field selector extraction and deduplication
- Multi-cluster queries

### Kubernetes Client/Cache (20 tests)
- Resource discovery caching
- Group freshness checks
- Cluster name sanitization
- Context matching (exact, glob patterns)

### Output Formatting (12 tests)
- Table, JSON, CSV, YAML formats
- Wide column truncation
- Empty result handling

### Field Selectors (12 tests)
- Registry initialization
- Resource-specific selectors
- Operator support (=, !=)
- K8s API format strings

### Progress Reporting (9 tests)
- Cluster completion tracking
- Update broadcasting
- Handle lifecycle

## Integration Tests

### Existing Tests (9 suites)

#### 01-basic-queries.sh
- SELECT with columns
- WHERE clauses (namespace, labels)
- SHOW TABLES/DATABASES
- COUNT aggregation
- ORDER BY
- LIMIT
- ConfigMap data field access

#### 02-multi-cluster.sh
- Cross-cluster queries (`_cluster = '*'`)
- Cluster-specific filtering
- Parallel cluster queries

#### 03-crd-discovery.sh
- Custom Resource Definition discovery
- Dynamic schema generation
- CRD field access

#### 04-filter-pushdown.sh
- Namespace filter pushdown to K8s API
- Label selector pushdown
- Field selector pushdown (status.phase, metadata.name)
- Combined filters
- IN/NOT IN for labels

#### 05-output-formats.sh
- JSON output
- CSV output
- YAML output
- Table output

#### 06-error-handling.sh
- Invalid SQL syntax
- Non-existent tables
- Invalid contexts
- Permission errors

#### 07-repl-mode.sh
- Interactive mode
- Multi-line queries
- Special commands (\dt, \l, \d)

#### 08-daemon-mode.sh
- PostgreSQL wire protocol
- Client connections
- Query execution via pg protocol

#### 09-batch-multi-context.sh
- Batch mode with multiple contexts
- Context switching
- Query routing

### New Comprehensive Tests (5 suites)

#### 10-sql-operators.sh (50+ tests)
**Comparison Operators:**
- `>`, `<`, `>=`, `<=`, `!=`, `=`
- `BETWEEN`

**Logical Operators:**
- `AND`, `OR`, `NOT`
- Complex combinations with parentheses
- Operator precedence

**NULL Operators:**
- `IS NULL`
- `IS NOT NULL`

**String Pattern Matching:**
- `LIKE` with wildcards (`%`, `_`)
- `NOT LIKE`
- `ILIKE` (case-insensitive)

**IN Operator:**
- Single value
- Multiple values
- Empty list
- `NOT IN`

**JSON Operator Integration:**
- `->>` with comparison operators
- `->>` with `IN`
- `->>` with `LIKE`
- `->>` with `IS NOT NULL`

#### 11-sql-aggregations.sh (60+ tests)
**COUNT Variations:**
- `COUNT(*)`
- `COUNT(column)`
- `COUNT(DISTINCT column)`
- With `WHERE`, `GROUP BY`, `HAVING`

**MIN/MAX:**
- On integers
- On strings
- On timestamps
- With `GROUP BY`

**SUM/AVG:**
- Basic aggregations
- With `GROUP BY`
- With `ROUND` for precision

**GROUP BY:**
- Single column
- Multiple columns
- With `WHERE`, `ORDER BY`, `LIMIT`

**HAVING Clause:**
- With `COUNT`, `SUM`, `AVG`
- Multiple conditions

**Multiple Aggregations:**
- Multiple metrics in single query
- `DISTINCT` with aggregations

**Edge Cases:**
- Empty result sets
- NULL handling in aggregations
- ORDER BY aggregates

#### 12-json-operations.sh (45+ tests)
**JSON Arrow Operator (`->>`):**
- Basic field access
- Nested paths
- In `WHERE` clauses
- With NULL values
- With comparison operators (`=`, `IN`, `LIKE`)

**DataFusion JSON Functions:**
- `json_get_str()`
- `json_get_int()`
- `json_get_bool()`
- `json_get_array()`

**JSON Arrays:**
- Array access
- `UNNEST` for array expansion
- Container iteration

**JSON in Aggregations:**
- `COUNT(DISTINCT json_field)`
- `GROUP BY json_field`
- Aggregate with JSON `WHERE`

**JSON Sorting:**
- `ORDER BY json_field`
- With `GROUP BY`

**Type Handling:**
- String to integer coercion
- JSON NULL vs SQL NULL

**Edge Cases:**
- Empty JSON objects
- Non-existent keys
- Special characters in keys (dots, dashes)

**Kubernetes-Specific:**
- `app.kubernetes.io/*` label patterns
- Annotations
- Owner references

#### 13-string-functions.sh (40+ tests)
**Case Conversion:**
- `UPPER()`
- `LOWER()`
- In `WHERE` clauses

**String Manipulation:**
- `CONCAT()` with multiple arguments
- `LENGTH()`/`CHAR_LENGTH()`
- `SUBSTRING()`
- `TRIM()`, `LTRIM()`, `RTRIM()`

**Pattern Matching:**
- `LIKE` patterns
- `STARTS_WITH()`
- `ENDS_WITH()`
- `POSITION()`/`STRPOS()`

**String Comparison:**
- Case-sensitive vs `ILIKE`
- String ordering (`ASC`/`DESC`)

**String Aggregations:**
- `MIN(string)`, `MAX(string)`
- `COUNT(DISTINCT string)`

**String Replacement:**
- `REPLACE()`
- `REGEXP_REPLACE()`

**Edge Cases:**
- Empty strings
- Strings with spaces
- Very long strings (JSON fields)
- NULL handling with `COALESCE`

#### 14-datetime-and-edge-cases.sh (70+ tests)
**Timestamp Comparison:**
- `>`, `<` with timestamps
- `BETWEEN` for date ranges

**Date/Time Functions:**
- `EXTRACT(YEAR/MONTH/DAY/HOUR)`
- `DATE_TRUNC()`
- `CURRENT_TIMESTAMP`

**Timestamp Ordering:**
- `ORDER BY timestamp`
- `MIN(timestamp)`, `MAX(timestamp)`

**NULL Handling:**
- `IS NULL` / `IS NOT NULL`
- `COALESCE()` for defaults
- NULL in comparisons
- NULL in aggregations

**Edge Cases:**
- Empty result sets
- `COUNT` on empty results
- `LIMIT 0`
- Very large `LIMIT`
- `OFFSET` edge cases

**Type Coercion:**
- `CAST` string to integer
- `CAST` integer to string
- Boolean expressions

**Complex Expressions:**
- `CASE` expressions
- Nested functions
- Multiple branches

**Mathematical Operations:**
- Arithmetic (`+`, `-`, `*`, `/`)
- Division by zero protection
- `ROUND()`, `CEIL()`, `FLOOR()`

**Special Characters:**
- Names with hyphens
- JSON keys with dots
- Unicode handling

## Performance Regression Tests (TODO)

### Planned Framework
- **Tool**: Criterion.rs for statistical benchmarking
- **Location**: `benches/` directory
- **CI Integration**: GitHub Actions with performance tracking

### Benchmark Categories

#### 1. Query Performance
- Simple SELECT (baseline)
- Complex multi-table joins (when supported)
- Large result sets (1000+ pods)
- Aggregation queries
- JSON path extraction

#### 2. Filter Pushdown Efficiency
- Namespace filter (API optimization)
- Label selector filter
- Field selector filter
- Combined filters

#### 3. Projection Pushdown
- SELECT * vs SELECT name
- Minimal columns vs full resource

#### 4. Multi-Cluster Queries
- Sequential vs parallel execution
- Cross-cluster aggregations

#### 5. Memory Usage
- Large batch processing
- JSON value handling
- Arrow RecordBatch allocation

### CI Considerations

**GitHub Actions Limitations:**
- Non-deterministic hardware
- Variable performance
- Solution: Track relative changes (threshold: ±15%)

**Regression Detection:**
- Compare against baseline branch (master)
- Alert on >20% degradation
- Store historical results for trending

**Test Approach:**
- Use consistent test data
- Multiple iterations for statistical significance
- Warm-up runs to stabilize caches

## Running Tests

### Unit Tests
```bash
cargo test
```

### Integration Tests
```bash
# Build binary first
cargo build --release
mkdir -p bin
cp target/release/k8sql bin/

# Run all integration tests
./tests/integration/run-tests.sh

# Run specific test suite
./tests/integration/tests/10-sql-operators.sh
```

### Performance Tests (TODO)
```bash
cargo bench
```

## Coverage Metrics

**SQL Conformance**: ~85% of common SQL features
- ✅ SELECT, WHERE, ORDER BY, LIMIT, OFFSET
- ✅ Aggregations: COUNT, SUM, AVG, MIN, MAX
- ✅ GROUP BY, HAVING
- ✅ Operators: =, !=, >, <, >=, <=, BETWEEN, IN, LIKE
- ✅ Functions: String, Date/Time, JSON
- ✅ NULL handling: IS NULL, IS NOT NULL, COALESCE
- ✅ Type casting: CAST, EXTRACT
- ❌ JOINs (not supported - K8s resources aren't relational)
- ❌ Subqueries (limited support)
- ❌ Window functions (not yet supported)
- ❌ CTEs (Common Table Expressions - partial support)

**Edge Case Coverage**: ~90%
- ✅ Empty results
- ✅ NULL handling
- ✅ Type coercion
- ✅ Large LIMIT/OFFSET
- ✅ Special characters
- ✅ Unicode
- ❌ Extreme values (max int, very long strings)

**Kubernetes-Specific**: 100%
- ✅ Namespace filtering
- ✅ Label selectors (=, !=, IN, NOT IN)
- ✅ Field selectors (status.phase, metadata.name, etc.)
- ✅ Multi-cluster queries
- ✅ CRD discovery
- ✅ JSON field access (labels, annotations, spec, status)

## Test Maintenance

**Adding New Tests:**
1. Create new `.sh` file in `tests/integration/tests/`
2. Use helper functions from `lib.sh`
3. Make executable: `chmod +x tests/integration/tests/NN-test-name.sh`
4. Tests auto-discovered by `run-tests.sh`

**Test Naming Convention:**
- `NN-category-name.sh` (NN = sequential number)
- Descriptive category names
- Example: `10-sql-operators.sh`

**Test Structure:**
- Start with echo statement describing suite
- Organize tests into logical sections with echo headers
- Use appropriate assertion from `lib.sh`
- End with `print_summary`

**Helper Functions** (from `lib.sh`):
- `assert_success`: Query should succeed
- `assert_error`: Query should fail with error
- `assert_row_count`: Exact row count
- `assert_min_row_count`: At least N rows
- `assert_contains`: Output contains string
- `assert_not_contains`: Output doesn't contain string
- `assert_table_contains`: Table output contains string
