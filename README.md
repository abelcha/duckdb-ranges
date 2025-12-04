# DuckDB Ranges Extension

A DuckDB extension that implements PostgreSQL-compatible range types and operations, enabling efficient storage and querying of value ranges.

## Features

This extension provides multiple range types with associated functions:

### Range Types
- **INT4RANGE** - Integer ranges (32-bit integers)
- **NUMRANGE** - Numeric ranges (double precision floating-point)

### Capabilities
- **Range Construction**: Multiple constructors for creating ranges with different bound specifications
- **Range Operators**: Check for overlaps, containment with PostgreSQL-compatible `@>` and `<@` operators
- **Range Accessors**: Extract bounds and boundary inclusivity information
- **Type Safety**: Proper NULL handling and type casting

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
- `]` or `)` indicates inclusive or exc`lusive upper bound

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

## NUMRANGE Usage

The `NUMRANGE` type works identically to `INT4RANGE` but uses double-precision floating-point numbers.

### Creating Numeric Ranges

```sql
-- String literal
SELECT numrange('[0.0,1.0)') AS probability;
-- Result: [0.000000,1.000000)

-- Bounds with default notation [)
SELECT numrange(1.5, 10.5) AS range;
-- Result: [1.500000,10.500000)

-- Custom bound notation
SELECT numrange(0.0, 100.0, '[]') AS inclusive_range;
-- Result: [0.000000,100.000000]

-- Explicit inclusivity flags
SELECT numrange(-273.15, 100.0, true, false) AS temperature;
-- Result: [-273.150000,100.000000)
```

### NUMRANGE Operations

All the same operations work with NUMRANGE:

```sql
-- Containment
SELECT numrange(0.0, 1.0) @> 0.5;  -- true
SELECT 0.999 <@ numrange(0.0, 1.0);  -- true

-- Overlaps
SELECT range_overlaps(numrange(1.5, 10.5), numrange(5.0, 15.0));  -- true

-- Accessors
SELECT lower(numrange(1.5, 10.5));  -- 1.5
SELECT upper(numrange(1.5, 10.5));  -- 10.5
SELECT isempty(numrange(5.0, 5.0));  -- true
```

### NUMRANGE Examples

#### Price Range Queries
```sql
CREATE TABLE products (
    id INTEGER,
    name VARCHAR,
    price_range NUMRANGE
);

INSERT INTO products VALUES
    (1, 'Budget Widget', numrange(9.99, 29.99)),
    (2, 'Premium Widget', numrange(50.00, 150.00, '[]')),
    (3, 'Luxury Widget', numrange(200.00, 500.00));

-- Find products available at $100
SELECT name, price_range
FROM products
WHERE price_range @> 100.0;
-- Result: Premium Widget

-- Find products under $50
SELECT name 
FROM products 
WHERE lower(price_range) < 50.0;
-- Results: Budget Widget, Premium Widget
```

#### Scientific Measurements
```sql
-- Temperature ranges in Celsius
SELECT 
    numrange(-273.15, 100.0, '[]') AS water_liquid_range,
    numrange(-273.15, 100.0, '[]') @> 20.0 AS room_temp_is_liquid;

-- Probability ranges
SELECT 
    numrange(0.0, 1.0, '[)') AS probability_range,
    0.75 <@ numrange(0.5, 1.0) AS high_probability;
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

### INT4RANGE
- **Storage**: BLOB type with alias `INT4RANGE`
- **Binary Format**: 9 bytes (4 bytes lower + 4 bytes upper + 1 byte bounds flags)
- **Value Range**: -2,147,483,648 to 2,147,483,647 (32-bit signed integer)
- **Empty Representation**: Canonically `(1,0)`, displayed as `empty`

### NUMRANGE
- **Storage**: BLOB type with alias `NUMRANGE`
- **Binary Format**: 17 bytes (8 bytes lower + 8 bytes upper + 1 byte bounds flags)
- **Precision**: Double-precision floating-point (IEEE 754)
- **Empty Representation**: Canonically `(1.0,0.0)`, displayed as `empty`

### General
- **Bounds Encoding**: Single byte with bits for lower/upper inclusivity
- **Null Handling**: All functions properly handle NULL inputs
- **Type Safety**: Separate function overloads prevent type confusion

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
- `INT4RANGE` - Integer range type (32-bit signed integers)
- `NUMRANGE` - Numeric range type (double-precision floating-point)

### Constructors

#### INT4RANGE
- `int4range(varchar)` - Parse from string literal
- `int4range(int, int)` - Create with default bounds `[)`
- `int4range(int, int, varchar)` - Create with bound notation (`[]`, `[)`, `(]`, `()`)
- `int4range(int, int, boolean, boolean)` - Create with explicit inclusivity

#### NUMRANGE
- `numrange(varchar)` - Parse from string literal
- `numrange(double, double)` - Create with default bounds `[)`
- `numrange(double, double, varchar)` - Create with bound notation (`[]`, `[)`, `(]`, `()`)
- `numrange(double, double, boolean, boolean)` - Create with explicit inclusivity

### Operators

Both range types support the following operators:

#### Named Functions
- `range_overlaps(RANGE, RANGE) -> BOOLEAN` - Check if two ranges overlap
- `range_contains(RANGE, VALUE) -> BOOLEAN` - Check if range contains a value

#### PostgreSQL-Compatible Operators
- `@>` - Contains operator: `range @> value` or `INT4RANGE @> INTEGER` or `NUMRANGE @> DOUBLE`
- `<@` - Contained by operator: `value <@ range` or `INTEGER <@ INT4RANGE` or `DOUBLE <@ NUMRANGE`

### Accessors

The following functions work with both INT4RANGE and NUMRANGE:

- `lower(RANGE)` - Extract lower bound (returns INTEGER for INT4RANGE, DOUBLE for NUMRANGE)
- `upper(RANGE)` - Extract upper bound (returns INTEGER for INT4RANGE, DOUBLE for NUMRANGE)
- `lower_inc(RANGE) -> BOOLEAN` - Check if lower bound is inclusive
- `upper_inc(RANGE) -> BOOLEAN` - Check if upper bound is inclusive
- `isempty(RANGE) -> BOOLEAN` - Check if range is empty

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

