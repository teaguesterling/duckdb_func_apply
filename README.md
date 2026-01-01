# DuckDB FuncApply Extension

Dynamic function application for DuckDB - call functions by name at runtime.

## Overview

The FuncApply extension provides dynamic function invocation capabilities for DuckDB, allowing you to:

- Call any scalar function or macro by name using `apply()`
- Pass arguments dynamically using `apply_with()`
- Check if functions exist with `function_exists()`

This is useful for data-driven transformations, dynamic SQL generation, and building flexible data pipelines.

## Quick Start

```sql
-- Load the extension
LOAD func_apply;

-- Call functions dynamically
SELECT apply('upper', 'hello world');
-- Result: HELLO WORLD

SELECT apply('substr', 'hello world', start := 7, length := 5);
-- Result: world

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

| Type | Supported | Example |
|------|-----------|---------|
| Scalar functions | Yes | `upper`, `abs`, `substr` |
| Macros | Yes | `list_sum`, `list_reverse` |
| Aggregate functions | No | `sum`, `avg` |
| Table functions | No | `read_csv`, `range` |

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
