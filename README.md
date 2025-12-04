# DuckDB Ranges Extension

A DuckDB extension that implements PostgreSQL-compatible integer range types and operations, enabling efficient storage and querying of value ranges.

## Features

This extension provides the `INT4RANGE` type and associated functions for working with integer ranges:

- **Custom Type**: `INT4RANGE` - stores integer ranges with configurable inclusive/exclusive bounds
- **Range Construction**: Multiple constructors for creating ranges with different bound specifications
- **Range Operators**: Check for overlaps, containment, and emptiness
- **Range Accessors**: Extract bounds and boundary inclusivity information

## Installation

### Building from Source

```sh
git clone --recurse-submodules https://github.com/abelcha/duckdb-ranges.git
cd duckdb-ranges
make
```

The build produces:
- `./build/release/duckdb` - DuckDB shell with the extension pre-loaded
- `./build/release/extension/ranges/ranges.duckdb_extension` - Loadable extension binary

### Loading the Extension

```sql
LOAD 'build/release/extension/ranges/ranges.duckdb_extension';
```

Or start the shell with the extension already loaded:
```sh
./build/release/duckdb
```

## Usage

### Creating Ranges

The `int4range()` function supports multiple signatures:

#### 1. String Literal Constructor
```sql
SELECT int4range('[1,10)') AS range;
-- Result: [1,10)

SELECT int4range('empty') AS range;
-- Result: empty
```

Syntax: `[lower,upper)` where:
- `[` or `(` indicates inclusive or exclusive lower bound
- `]` or `)` indicates inclusive or exclusive upper bound

#### 2. Integer Bounds with Default Notation
```sql
SELECT int4range(1, 10) AS range;
-- Result: [1,10)  (default is inclusive lower, exclusive upper)
```

#### 3. Integer Bounds with Custom Notation
```sql
SELECT int4range(1, 10, '[]') AS range;
-- Result: [1,10]

SELECT int4range(1, 10, '()') AS range;
-- Result: (1,10)

SELECT int4range(1, 10, '(]') AS range;
-- Result: (1,10]
```

#### 4. Integer Bounds with Explicit Inclusivity
```sql
SELECT int4range(1, 10, true, false) AS range;
-- Result: [1,10)

SELECT int4range(1, 10, false, true) AS range;
-- Result: (1,10]
```

### Range Operations

#### Check if Ranges Overlap
```sql
SELECT range_overlaps(int4range(1, 10), int4range(5, 15)) AS overlaps;
-- Result: true

SELECT range_overlaps(int4range(1, 5), int4range(10, 15)) AS overlaps;
-- Result: false
```

#### Check if Range Contains Value
```sql
SELECT range_contains(int4range('[1,10)'), 5) AS contains;
-- Result: true

SELECT range_contains(int4range('[1,10)'), 10) AS contains;
-- Result: false (upper bound is exclusive)

SELECT range_contains(int4range('[1,10]'), 10) AS contains;
-- Result: true (upper bound is inclusive)
```

#### Check if Range is Empty
```sql
SELECT isempty(int4range(5, 5)) AS is_empty;
-- Result: true

SELECT isempty(int4range('[5,5]')) AS is_empty;
-- Result: false (both bounds inclusive)
```

### Range Accessors

#### Extract Lower Bound
```sql
SELECT lower(int4range('[1,10)')) AS lower_bound;
-- Result: 1
```

#### Extract Upper Bound
```sql
SELECT upper(int4range('[1,10)')) AS upper_bound;
-- Result: 10
```

#### Check Lower Bound Inclusivity
```sql
SELECT lower_inc(int4range('[1,10)')) AS lower_inclusive;
-- Result: true

SELECT lower_inc(int4range('(1,10)')) AS lower_inclusive;
-- Result: false
```

#### Check Upper Bound Inclusivity
```sql
SELECT upper_inc(int4range('[1,10)')) AS upper_inclusive;
-- Result: false

SELECT upper_inc(int4range('[1,10]')) AS upper_inclusive;
-- Result: true
```

### Type Casting

Ranges can be cast to/from VARCHAR for display and storage:

```sql
-- Implicit cast to VARCHAR for display
SELECT int4range(1, 10)::VARCHAR;
-- Result: '[1,10)'

-- Cast from VARCHAR to INT4RANGE
SELECT '[5,15)'::INT4RANGE;
```

## Examples

### Find Overlapping Time Periods
```sql
CREATE TABLE events (
    id INTEGER,
    name VARCHAR,
    period INT4RANGE
);

INSERT INTO events VALUES
    (1, 'Project A', int4range(2020, 2023)),
    (2, 'Project B', int4range(2022, 2025)),
    (3, 'Project C', int4range(2024, 2027));

-- Find all projects that overlap with 2022-2024
SELECT name 
FROM events 
WHERE range_overlaps(period, int4range(2022, 2024));
-- Results: Project A, Project B
```

### Check Value Membership
```sql
-- Find events active in year 2023
SELECT name 
FROM events 
WHERE range_contains(period, 2023);
-- Results: Project A, Project B
```

### Filter by Bound Properties
```sql
-- Find ranges that include their upper bound
SELECT name, period
FROM events
WHERE upper_inc(period);
```

## Implementation Details

- **Storage**: Ranges are stored as BLOB type with alias `INT4RANGE`
- **Binary Format**: 9 bytes (4 bytes lower + 4 bytes upper + 1 byte bounds flags)
- **Empty Ranges**: Canonically represented as `(1,0)` but displayed as `empty`
- **Null Handling**: All functions properly handle NULL inputs

## Running Tests

```sh
make test
```

Tests are located in `test/sql/` and cover:
- Range construction with various notations
- Overlap detection
- Containment checks
- Accessor functions
- Edge cases (empty ranges, boundary conditions)

## API Reference

### Types
- `INT4RANGE` - Integer range type with inclusive/exclusive bounds

### Constructors
- `int4range(varchar)` - Parse from string literal
- `int4range(int, int)` - Create with default bounds `[)`
- `int4range(int, int, varchar)` - Create with bound notation
- `int4range(int, int, boolean, boolean)` - Create with explicit inclusivity

### Operators
- `range_overlaps(INT4RANGE, INT4RANGE) -> BOOLEAN`
- `range_contains(INT4RANGE, INTEGER) -> BOOLEAN`

### Accessors
- `lower(INT4RANGE) -> INTEGER`
- `upper(INT4RANGE) -> INTEGER`
- `lower_inc(INT4RANGE) -> BOOLEAN`
- `upper_inc(INT4RANGE) -> BOOLEAN`
- `isempty(INT4RANGE) -> BOOLEAN`

## License

See [LICENSE](LICENSE) file for details.

## Contributing

This extension is based on the [DuckDB Extension Template](https://github.com/duckdb/extension-template). 

To contribute:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass with `make test`
5. Submit a pull request

