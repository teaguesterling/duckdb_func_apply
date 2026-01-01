# Limitations

Known limitations and workarounds for the FuncApply extension.

## Unsupported Function Types

### Aggregate Functions

Aggregate functions like `sum`, `avg`, `count` are not supported with `apply()`.

```sql
-- This does NOT work:
SELECT apply('sum', column_name) FROM table;  -- Error!

-- Workaround: Use native SQL
SELECT sum(column_name) FROM table;

-- Or use list_aggregate for list inputs:
SELECT apply('list_aggregate', [1, 2, 3], 'sum');  -- Works!
```

### Table Functions

Table functions like `read_csv`, `range`, `generate_series` are not supported.

```sql
-- This does NOT work:
SELECT * FROM apply('read_csv', 'file.csv');  -- Error!

-- Workaround: Use native SQL
SELECT * FROM read_csv('file.csv');
```

### Window Functions

Window functions are not supported through `apply()`.

```sql
-- This does NOT work:
SELECT apply('row_number') OVER (ORDER BY id);  -- Error!

-- Workaround: Use native SQL
SELECT row_number() OVER (ORDER BY id) FROM table;
```

## Type Limitations

### Homogeneous Lists in apply_with()

DuckDB lists must contain elements of the same type. This limits `apply_with()` for functions with mixed-type arguments.

```sql
-- This does NOT work:
SELECT apply_with('substr', args := ['hello', 2, 3]);  -- Error: mixed types

-- Workaround: Use apply() directly
SELECT apply('substr', 'hello', 2, 3);  -- Works!
```

### Return Type Inference with Dynamic Names

When the function name comes from a column (not a constant), the return type defaults to `VARCHAR`:

```sql
-- Type is inferred correctly:
SELECT typeof(apply('length', 'hello'));  -- BIGINT

-- Type defaults to VARCHAR:
SELECT typeof(apply(func_col, 'hello')) FROM funcs;  -- VARCHAR
```

## Named Parameters (kwargs)

The `kwargs` parameter in `apply_with()` is not yet functional. Named parameters via struct are planned for a future release.

```sql
-- This does NOT work yet:
SELECT apply_with('substr',
    args := ['hello world'],
    kwargs := {start: 7, length: 5}
);  -- Error!

-- Workaround: Use apply() with named params
SELECT apply('substr', 'hello world', start := 7, length := 5);  -- Works!
```

## Operators vs Functions

SQL operators like `NOT`, `AND`, `OR`, `+`, `-` are not functions and cannot be called with `apply()`.

```sql
-- This does NOT work:
SELECT apply('not', true);  -- Error: 'not' is not a function
SELECT apply('+', 1, 2);    -- Error: '+' is not a function

-- Workaround: Use the operator directly
SELECT NOT true;
SELECT 1 + 2;

-- Or use equivalent functions if available:
SELECT apply('negate', -1);  -- If such a function exists
```

## SQL Keywords

Some SQL constructs that look like functions are actually keywords:

```sql
-- These do NOT work:
SELECT apply('current_date');      -- Error: not a function
SELECT apply('current_timestamp'); -- Error: not a function

-- Workaround: Use the function versions
SELECT apply('now');           -- Works!
SELECT apply('today');         -- Works! (if available)
```

## Performance Considerations

### Per-Row Execution

Dynamic function calls have overhead compared to native function calls. For maximum performance with large datasets:

1. Use native SQL when the function is known at query time
2. Batch similar operations when possible
3. Consider materializing results for repeated access

### Function Lookup Caching

Function lookups are cached per-query, so calling the same function name many times is efficient. However, calling many different function names may have higher overhead.

## Security Notes

### Function Name Validation

Function names are validated to prevent SQL injection:

- Must start with a letter or underscore
- Can only contain letters, numbers, and underscores
- Special characters are rejected

```sql
-- These are rejected:
SELECT apply('func; DROP TABLE users', 'x');  -- Error: invalid name
SELECT apply('func--comment', 'x');           -- Error: invalid name
```

### Blacklisted Functions

By default, certain dangerous functions may be blacklisted (see [Security](internal/SECURITY_CONFIG_BASED.md) for details):

- `system` - Shell command execution
- `install` - Extension installation
- `load` - Extension loading
- `export_database` - Data export

## Future Improvements

Planned features for future releases:

1. **JSON args support** - Allow heterogeneous arguments via JSON
2. **kwargs support** - Named parameters via struct
3. **Aggregate function support** - Call aggregates dynamically
4. **Table function support** - `apply_table()` for table functions
5. **Partial application** - `partial()` for currying functions
