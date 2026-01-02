# FuncApply Extension

Dynamic function application for DuckDB.

## Introduction

The FuncApply extension enables dynamic function invocation in DuckDB. Instead of hardcoding function names in your queries, you can pass function names as strings and call them at runtime.

This opens up powerful patterns for:

- **Data-driven transformations** - Store function names in tables and apply them dynamically
- **Flexible pipelines** - Build reusable query templates that work with any function
- **Runtime function selection** - Choose functions based on data types or conditions
- **Safe dynamic SQL** - Call functions by name without SQL injection risks

## Installation

```sql
-- Load the extension
LOAD 'func_apply';
```

## Quick Example

```sql
-- Instead of hardcoding:
SELECT upper('hello');

-- You can use dynamic application:
SELECT apply('upper', 'hello');

-- This enables patterns like:
SELECT apply(transform_func, value)
FROM data
JOIN transforms ON data.type = transforms.data_type;
```

## Core Functions

| Function | Description |
|----------|-------------|
| [`apply()`](api.md#apply) | Call a scalar function by name with arguments |
| [`apply_with()`](api.md#apply_with) | Call a scalar function with args as a list |
| [`apply_table()`](api.md#apply_table) | Call a table function by name with arguments |
| [`apply_table_with()`](api.md#apply_table_with) | Call a table function with args as a list |
| [`function_exists()`](api.md#function_exists) | Check if a function exists |

## Contents

- [API Reference](api.md) - Complete function documentation
- [Examples](examples.md) - Usage patterns and recipes
- [Limitations](limitations.md) - Known limitations and workarounds

## Supported Functions

FuncApply works with:

- **Scalar functions** - `upper`, `lower`, `abs`, `substr`, etc. (via `apply`, `apply_with`)
- **Macros** - `list_sum`, `list_reverse`, custom macros (via `apply`, `apply_with`)
- **Table functions** - `range`, `generate_series`, etc. (via `apply_table`, `apply_table_with`)

Not yet supported:

- Aggregate functions (`sum`, `avg`, `count`)
- Window functions

## Next Steps

- See [Examples](examples.md) for common usage patterns
- Check the [API Reference](api.md) for detailed function documentation
