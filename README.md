# DuckDB FuncApply Extension

Dynamic function application for DuckDB - call functions by name at runtime.

## Overview

The FuncApply extension provides dynamic function invocation capabilities for DuckDB, allowing you to:

- Call any scalar function or macro by name using `apply()`
- Pass arguments dynamically using `apply_with()`
- Call table functions dynamically using `apply_table()` and `apply_table_with()`
- Check if functions exist with `function_exists()`

This is useful for data-driven transformations, dynamic SQL generation, and building flexible data pipelines.

## Quick Start

```sql
-- Load the extension
LOAD func_apply;

-- Call scalar functions dynamically
SELECT apply('upper', 'hello world');
-- Result: HELLO WORLD

SELECT apply('substr', 'hello world', 7, 5);
-- Result: world

-- Call table functions dynamically
SELECT * FROM apply_table('range', 5);
-- Returns: 0, 1, 2, 3, 4

SELECT * FROM apply_table('generate_series', 1, 10, 2);
-- Returns: 1, 3, 5, 7, 9

-- Check if a function exists before calling
SELECT function_exists('my_custom_func');
-- Result: true/false
```

## Functions

### `apply(func_name, ...args)`

Calls a function by name with the provided arguments.

```sql
-- String functions
SELECT apply('upper', 'hello');           -- HELLO
SELECT apply('concat', 'a', 'b', 'c');    -- abc
SELECT apply('substr', 'hello', 2, 3);    -- ell

-- Numeric functions
SELECT apply('abs', -42);                 -- 42
SELECT apply('round', 3.14159, 2);        -- 3.14

-- List functions (macros)
SELECT apply('list_sum', [1, 2, 3, 4]);   -- 10
SELECT apply('list_reverse', [1, 2, 3]); -- [3, 2, 1]

-- Named parameters are supported
SELECT apply('substr', 'hello world', start := 7, length := 5);  -- world
```

### `apply_with(func_name, args, kwargs)`

Calls a function with arguments provided as a list.

```sql
-- Basic usage
SELECT apply_with('upper', args := ['hello']);
-- Result: HELLO

-- With positional syntax
SELECT apply_with('concat', ['a', 'b', 'c'], NULL);
-- Result: abc
```

**Note:** DuckDB lists must be homogeneous (same type). For mixed-type arguments, use `apply()` directly.

### `apply_table(func_name, ...args)`

Calls a table function by name and returns its results as a table.

```sql
-- Basic range
SELECT * FROM apply_table('range', 5);
-- Returns: 0, 1, 2, 3, 4

-- Generate series with step
SELECT * FROM apply_table('generate_series', 1, 10, 2);
-- Returns: 1, 3, 5, 7, 9

-- Use in joins
SELECT d.*, r.range as idx
FROM my_data d
CROSS JOIN apply_table('range', 3) r;

-- Use in subqueries
SELECT * FROM my_table
WHERE id IN (SELECT range FROM apply_table('range', 100));
```

### `apply_table_with(func_name, args, kwargs)`

Calls a table function with arguments provided as a list and optional named parameters.

```sql
-- Basic usage
SELECT * FROM apply_table_with('range', args := [5]);
-- Returns: 0, 1, 2, 3, 4

-- With named parameters
SELECT * FROM apply_table_with('generate_series',
    args := [1],
    kwargs := {stop: 10, step: 2}
);
-- Returns: 1, 3, 5, 7, 9
```

### `function_exists(name)`

Returns true if a function with the given name exists.

```sql
SELECT function_exists('upper');           -- true
SELECT function_exists('nonexistent');     -- false

-- Use for conditional logic
SELECT CASE
    WHEN function_exists(func_name) THEN apply(func_name, value)
    ELSE 'N/A'
END
FROM my_table;
```

## Supported Function Types

| Type | Supported | Functions to Use | Example |
|------|-----------|------------------|---------|
| Scalar functions | Yes | `apply`, `apply_with` | `upper`, `abs`, `substr` |
| Macros | Yes | `apply`, `apply_with` | `list_sum`, `list_reverse` |
| Table functions | Yes | `apply_table`, `apply_table_with` | `range`, `generate_series` |
| Aggregate functions | No | N/A | `sum`, `avg` |

## Use Cases

### Data-Driven Transformations

```sql
-- Store transformation rules in a table
CREATE TABLE transforms (
    column_name VARCHAR,
    func_name VARCHAR
);

INSERT INTO transforms VALUES
    ('name', 'upper'),
    ('email', 'lower'),
    ('phone', 'trim');

-- Apply transformations dynamically
SELECT apply(t.func_name, d.value) as result
FROM data d
JOIN transforms t ON d.column = t.column_name;
```

### Dynamic Function Selection

```sql
-- Choose function based on data type
SELECT apply(
    CASE typeof(value)
        WHEN 'VARCHAR' THEN 'upper'
        WHEN 'INTEGER' THEN 'abs'
        ELSE 'to_string'
    END,
    value
) FROM my_table;
```

### Validation Before Execution

```sql
-- Only call functions that exist
SELECT
    func_name,
    CASE WHEN function_exists(func_name)
         THEN apply(func_name, 'test')
         ELSE 'Function not found'
    END as result
FROM function_list;
```

### Dynamic Table Generation

```sql
-- Generate dynamic row counts based on configuration
SELECT * FROM apply_table('range', row_count)
WHERE row_count = (SELECT max_rows FROM config);

-- Use table functions in cross joins for data expansion
SELECT d.*, idx.range as position
FROM my_data d
CROSS JOIN apply_table('range', d.repeat_count) idx;
```

## Building

### Prerequisites

DuckDB extensions use VCPKG for dependency management:

```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

```sh
make
```

### Test

```sh
make test
```

## Installation

### From Source

After building, the extension is available at:
```
./build/release/extension/func_apply/func_apply.duckdb_extension
```

Load it in DuckDB:
```sql
LOAD 'path/to/func_apply.duckdb_extension';
```

### Unsigned Extensions

To load unsigned extensions, start DuckDB with:

```shell
duckdb -unsigned
```

Or in Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions': 'true'})
```

## Documentation

See the [docs/](docs/) folder for detailed documentation:

- [API Reference](docs/api.md) - Complete function reference
- [Examples](docs/examples.md) - Usage examples and patterns
- [Internals](docs/internals.md) - Implementation details

## License

MIT License - see LICENSE file for details.
