# API Reference

Complete reference for FuncApply extension functions.

## apply

Dynamically calls a function by name.

### Signature

```sql
apply(func_name VARCHAR, ...args ANY) -> ANY
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `func_name` | `VARCHAR` | Name of the function to call |
| `...args` | `ANY` | Arguments to pass to the function |

### Returns

The return type matches the called function's return type. When the function name is a constant, the return type is inferred at compile time.

### Description

`apply()` invokes any scalar function or macro by name. Arguments are passed through directly, including support for named parameters.

### Examples

**Basic usage:**

```sql
SELECT apply('upper', 'hello');
-- Result: HELLO

SELECT apply('abs', -42);
-- Result: 42
```

**Multiple arguments:**

```sql
SELECT apply('substr', 'hello world', 7, 5);
-- Result: world

SELECT apply('concat', 'a', 'b', 'c');
-- Result: abc
```

**Named parameters:**

```sql
SELECT apply('substr', 'hello world', start := 7, length := 5);
-- Result: world

SELECT apply('lpad', 'hi', 5, padchar := '*');
-- Result: ***hi
```

**Calling macros:**

```sql
SELECT apply('list_sum', [1, 2, 3, 4, 5]);
-- Result: 15

SELECT apply('list_reverse', [1, 2, 3]);
-- Result: [3, 2, 1]
```

**NULL handling:**

```sql
SELECT apply(NULL, 'hello');
-- Result: NULL

SELECT apply('upper', NULL);
-- Result: NULL
```

**Dynamic function names:**

```sql
SELECT apply(func_name, value)
FROM (VALUES
    ('upper', 'hello'),
    ('lower', 'WORLD'),
    ('reverse', 'abc')
) AS t(func_name, value);
-- Results: HELLO, world, cba
```

### Errors

| Error | Cause |
|-------|-------|
| `invalid function name` | Function name contains invalid characters |
| `Function does not exist` | No function with that name found |
| `No function matches` | Arguments don't match any overload |

---

## apply_with

Calls a function with arguments provided as a list.

### Signature

```sql
apply_with(func_name VARCHAR, args LIST, kwargs STRUCT) -> ANY
apply_with(func_name VARCHAR, args := LIST) -> ANY
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `func_name` | `VARCHAR` | Name of the function to call |
| `args` | `LIST` | Positional arguments as a list |
| `kwargs` | `STRUCT` | Named arguments (not yet supported) |

### Returns

The return type matches the called function's return type.

### Description

`apply_with()` provides an alternative way to call functions where arguments are passed as a list. This is useful when arguments are stored in a column or computed dynamically.

**Important:** DuckDB lists must be homogeneous (all elements same type). For mixed-type arguments, use `apply()` directly.

### Examples

**Basic usage:**

```sql
SELECT apply_with('upper', args := ['hello']);
-- Result: HELLO

SELECT apply_with('concat', args := ['a', 'b', 'c']);
-- Result: abc
```

**Positional syntax:**

```sql
SELECT apply_with('lower', ['HELLO'], NULL);
-- Result: hello
```

**No arguments:**

```sql
SELECT apply_with('random', args := []) IS NOT NULL;
-- Result: true
```

**NULL handling:**

```sql
SELECT apply_with(NULL, args := ['hello']);
-- Result: NULL

SELECT apply_with('random', args := NULL) IS NOT NULL;
-- Result: true
```

### Limitations

- `kwargs` (named parameters via struct) not yet supported
- List elements must be the same type
- For mixed types, use `apply()` instead

---

## function_exists

Checks if a function with the given name exists.

### Signature

```sql
function_exists(name VARCHAR) -> BOOLEAN
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | `VARCHAR` | Name of the function to check |

### Returns

`BOOLEAN` - `true` if the function exists, `false` otherwise.

### Description

`function_exists()` checks whether a function is available in the current context. It searches:

- Scalar functions
- Macros
- Aggregate functions
- Table functions

### Examples

**Basic checks:**

```sql
SELECT function_exists('upper');
-- Result: true

SELECT function_exists('nonexistent_func');
-- Result: false
```

**Case insensitive:**

```sql
SELECT function_exists('UPPER');
-- Result: true

SELECT function_exists('Upper');
-- Result: true
```

**Check custom functions:**

```sql
CREATE MACRO double_it(x) AS x * 2;

SELECT function_exists('double_it');
-- Result: true
```

**Conditional execution:**

```sql
SELECT CASE
    WHEN function_exists(func_name)
    THEN apply(func_name, 'test')
    ELSE 'Function not available'
END as result
FROM function_configs;
```

**Batch validation:**

```sql
SELECT
    name,
    function_exists(name) as valid
FROM (VALUES
    ('upper'),
    ('fake_func'),
    ('lower')
) AS t(name);
-- Results: true, false, true
```

### Edge Cases

```sql
SELECT function_exists('');
-- Result: false

SELECT function_exists(NULL);
-- Result: NULL
```

---

## Type Inference

When the function name is a compile-time constant, FuncApply infers the return type from the target function:

```sql
SELECT typeof(apply('length', 'hello'));
-- Result: BIGINT

SELECT typeof(apply('upper', 'hello'));
-- Result: VARCHAR

SELECT typeof(apply('abs', -3.14));
-- Result: DECIMAL(3,2)
```

When the function name is dynamic (from a column), the return type defaults to `VARCHAR`:

```sql
SELECT typeof(apply(func_col, 'hello')) FROM ...;
-- Result: VARCHAR (default)
```
