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

## apply_table

Dynamically calls a table function by name.

### Signature

```sql
apply_table(func_name VARCHAR, ...args ANY) -> TABLE
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `func_name` | `VARCHAR` | Name of the table function to call |
| `...args` | `ANY` | Arguments to pass to the function |

### Returns

A table with columns determined by the called table function.

### Description

`apply_table()` invokes any table function by name, returning its results as a table. This is useful for dynamic data generation and parameterized table access.

### Examples

**Basic usage:**

```sql
SELECT * FROM apply_table('range', 5);
-- Returns: 0, 1, 2, 3, 4

SELECT * FROM apply_table('generate_series', 1, 10, 2);
-- Returns: 1, 3, 5, 7, 9
```

**With joins:**

```sql
SELECT d.*, r.range as idx
FROM my_data d
CROSS JOIN apply_table('range', 3) r;
```

**In subqueries:**

```sql
SELECT * FROM my_table
WHERE id IN (SELECT range FROM apply_table('range', 100));
```

**Dynamic row generation:**

```sql
-- Generate rows based on a column value
SELECT d.id, r.range as position
FROM data d
CROSS JOIN LATERAL apply_table('range', d.count) r;
```

### Errors

| Error | Cause |
|-------|-------|
| `invalid function name` | Function name contains invalid characters |
| `function does not exist` | No table function with that name found |
| `is a scalar function` | Function exists but is scalar (use `apply()` instead) |

---

## apply_table_with

Calls a table function with arguments provided as a list.

### Signature

```sql
apply_table_with(func_name VARCHAR, args LIST, kwargs STRUCT) -> TABLE
apply_table_with(func_name VARCHAR, args := LIST) -> TABLE
apply_table_with(func_name VARCHAR, args := LIST, kwargs := STRUCT) -> TABLE
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `func_name` | `VARCHAR` | Name of the table function to call |
| `args` | `LIST` | Positional arguments as a list |
| `kwargs` | `STRUCT` | Named arguments as a struct |

### Returns

A table with columns determined by the called table function.

### Description

`apply_table_with()` provides an alternative way to call table functions where arguments are passed as a list or struct. Unlike `apply_with()` for scalar functions, `apply_table_with()` fully supports `kwargs` for named parameters.

### Examples

**Basic usage:**

```sql
SELECT * FROM apply_table_with('range', args := [5]);
-- Returns: 0, 1, 2, 3, 4
```

**With kwargs:**

```sql
SELECT * FROM apply_table_with('generate_series',
    args := [1],
    kwargs := {stop: 10, step: 2}
);
-- Returns: 1, 3, 5, 7, 9
```

**Positional syntax:**

```sql
SELECT * FROM apply_table_with('range', [10], NULL);
-- Returns: 0, 1, 2, ..., 9
```

### Errors

| Error | Cause |
|-------|-------|
| `invalid function name` | Function name contains invalid characters |
| `function does not exist` | No table function with that name found |
| `is a scalar function` | Function exists but is scalar (use `apply_with()` instead) |

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

---

## Security Configuration

FuncApply includes a configurable security model to control which functions can be called dynamically. This is essential for multi-tenant environments or when allowing user-provided function names.

### Security Modes

| Mode | Description |
|------|-------------|
| `none` | No restrictions (default) |
| `blacklist` | Block specific functions (allow all others) |
| `whitelist` | Only allow specific functions (block all others) |
| `validator` | Call a custom macro to validate each call |

### func_apply_set_security_mode

Sets the security mode for dynamic function calls.

```sql
func_apply_set_security_mode(mode VARCHAR) -> VARCHAR
```

**Example:**

```sql
SELECT func_apply_set_security_mode('blacklist');
-- Result: Security mode set to: blacklist
```

### func_apply_set_blacklist

Sets the list of functions to block (used in blacklist mode).

```sql
func_apply_set_blacklist(functions LIST) -> VARCHAR
```

**Example:**

```sql
SELECT func_apply_set_blacklist(['system', 'load', 'eval']);
-- Result: Blacklist set with 3 functions
```

**Default blacklist:** `system`, `getenv`, `load`, `install`, `uninstall`, `force_install`, `export_database`, `import_database`, `create_secret`, `drop_secret`

### func_apply_set_whitelist

Sets the list of functions to allow (used in whitelist mode).

```sql
func_apply_set_whitelist(functions LIST) -> VARCHAR
```

**Example:**

```sql
SELECT func_apply_set_whitelist(['upper', 'lower', 'concat', 'substr']);
-- Result: Whitelist set with 4 functions
```

### func_apply_set_validator

Sets a custom validator function (used in validator mode).

```sql
func_apply_set_validator(func_name VARCHAR) -> VARCHAR
```

The validator function/macro must have the signature:

```sql
validator(func_name VARCHAR, params STRUCT) -> BOOLEAN
```

Where `params` has the structure:

```sql
{
    total_args: INTEGER,                    -- Total number of arguments
    positional: {
        arg_indexes: VARCHAR[],             -- ['1', '2', '3', ...]
        arg_types: VARCHAR[],               -- ['VARCHAR', 'INTEGER', ...]
        arg_values: STRUCT                  -- {'1': val1, '2': val2, ...}
    },
    named: {
        arg_names: VARCHAR[],               -- ['start', 'length', ...]
        arg_types: VARCHAR[],               -- ['INTEGER', 'INTEGER', ...]
        arg_values: STRUCT                  -- {'start': 7, 'length': 5, ...}
    }
}
```

**Example:**

```sql
-- Create a validator that allows specific functions and limits arg count
CREATE MACRO my_validator(func_name, params) AS (
    func_name IN ('upper', 'lower', 'concat')
    AND params.total_args <= 5
);

SELECT func_apply_set_security_mode('validator');
SELECT func_apply_set_validator('my_validator');

SELECT apply('upper', 'hello');  -- Works
SELECT apply('system', 'ls');     -- Blocked
```

### func_apply_set_on_block

Configures what happens when a function call is blocked.

```sql
func_apply_set_on_block(behavior VARCHAR) -> VARCHAR
```

| Behavior | Description |
|----------|-------------|
| `error` | Throw an error (default) |
| `null` | Return NULL |
| `default` | Return a configured default value |

**Example:**

```sql
SELECT func_apply_set_on_block('null');
-- Blocked calls now return NULL instead of throwing

SELECT func_apply_set_on_block('default');
SELECT func_apply_set_block_default('BLOCKED');
-- Blocked calls now return 'BLOCKED'
```

### func_apply_set_block_default

Sets the default value returned when `on_block` is `'default'`.

```sql
func_apply_set_block_default(value ANY) -> VARCHAR
```

### func_apply_lock_security

Locks the security configuration, preventing any further changes. **This is irreversible for the session.**

```sql
func_apply_lock_security() -> VARCHAR
```

**Example:**

```sql
-- Configure security
SELECT func_apply_set_security_mode('whitelist');
SELECT func_apply_set_whitelist(['upper', 'lower']);

-- Lock it
SELECT func_apply_lock_security();
-- Result: Security settings locked (cannot be unlocked)

-- This will now fail
SELECT func_apply_set_security_mode('none');
-- Error: Security settings are locked
```

### func_apply_get_security_config

Returns the current security configuration as JSON.

```sql
func_apply_get_security_config() -> VARCHAR
```

**Example:**

```sql
SELECT func_apply_get_security_config();
-- Result:
-- {
--   "mode": "whitelist",
--   "on_block": "error",
--   "locked": false,
--   "validator": "",
--   "blacklist": [...],
--   "whitelist": ["upper", "lower"]
-- }
```

### Complete Security Example

```sql
-- Load extension
LOAD 'func_apply';

-- Set up whitelist mode with safe functions
SELECT func_apply_set_security_mode('whitelist');
SELECT func_apply_set_whitelist([
    'upper', 'lower', 'concat', 'substr', 'length',
    'abs', 'round', 'floor', 'ceil',
    'date_part', 'current_date'
]);

-- Configure graceful handling of blocked calls
SELECT func_apply_set_on_block('default');
SELECT func_apply_set_block_default('ACCESS_DENIED');

-- Lock the configuration
SELECT func_apply_lock_security();

-- Now dynamic calls are safe
SELECT apply(user_func, user_data) FROM user_requests;
-- Only whitelisted functions work; others return 'ACCESS_DENIED'
```
