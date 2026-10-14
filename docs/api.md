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

## func_dispatch

Dispatches to the function named by `routes[key]`, forwarding the remaining
arguments, with its real return type resolved at bind time.

### Signature

```sql
func_dispatch(routes MAP(VARCHAR, VARCHAR), key VARCHAR, ...args ANY) -> ANY
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `routes` | `MAP(VARCHAR, VARCHAR)` | Maps a key to the name of the function or macro to call. `'*'` is used as a fallback when `key` is absent. |
| `key` | `VARCHAR` | Looked up in `routes` to choose which function to call. |
| `...args` | `ANY` | Forwarded positionally to the resolved function. |

### Returns

When `routes` is a compile-time constant, every candidate route is bound at
plan time against the forwarded arguments, and the candidates' return types
are unified with DuckDB's normal type promotion (e.g. `INTEGER` and `BIGINT`
unify to `BIGINT`); that unified type becomes the function's real return
type — unlike `apply()` with a dynamic function name, which always collapses
to `VARCHAR`. When `routes` is not a constant, the return type is `VARCHAR`,
the same as `apply()`.

### Description

`func_dispatch()` resolves `routes[key]` to a scalar function or macro name
and calls it with `...args`. An exact match on `key` always wins; `'*'` is
used only when `key` is absent from `routes`. It returns `NULL` when neither
matches, when `key` is `NULL`, or when `routes` is `NULL` or empty.

This exists to fix the type collapse `apply()` suffers with a dynamic
function name: because `func_dispatch` can see every candidate name when
`routes` is constant, it binds all of them against the real argument types
and reports the real unified return type instead of defaulting to `VARCHAR`.

**Every candidate must accept the forwarded arguments.** With a constant
routing map, each candidate is bound against `...args`. If any candidate's
declared parameters don't match — wrong arity, or an argument with no cast to
the declared parameter type — the call is a bind error naming that candidate
(see Errors below), even if every other candidate agrees on a return type. A
routing map should dispatch only to functions that all accept the arguments
the call forwards and agree on a return type.

This check requires the forwarded argument types to be known. A macro body's own
parameter, a prepared-statement `?`, and a bare `NULL` have no concrete type at
bind time; a route cannot be judged against an argument it knows nothing about,
so no candidate is checked and the return type falls back to `VARCHAR`. That is
what lets `CREATE MACRO normalize_name(filename) AS func_dispatch(...,
filename)` be created and called.

Arguments are forwarded **positionally only**. A named argument like
`b := 'y'` is accepted, but its alias is ignored — the value is still
matched by its position among `...args`, not by the name `b`.

### Examples

**Basic usage:**

```sql
CREATE MACRO route_git(p) AS 'GIT:' || p;
CREATE MACRO route_http(p) AS 'HTTP:' || p;

SELECT func_dispatch(MAP{'git': 'route_git', 'https': 'route_http'}, 'git', 'a.git');
-- Result: GIT:a.git
```

**`'*'` wildcard fallback:**

```sql
CREATE MACRO route_any(p) AS 'ANY:' || p;

SELECT func_dispatch(MAP{'git': 'route_git', '*': 'route_any'}, 's3', 'x');
-- Result: ANY:x
```

**Real return type, unlike `apply()` with a dynamic name:**

```sql
CREATE MACRO pair(x) AS [x, x];

-- apply() with a dynamic name returns the STRING '[7, 7]':
SELECT typeof(apply(MAP{'a': 'pair'}[k], 7)) FROM (VALUES ('a')) t(k);
-- Result: VARCHAR

-- func_dispatch returns a real list:
SELECT typeof(func_dispatch(MAP{'a': 'pair'}, 'a', 7));
-- Result: INTEGER[]
SELECT len(func_dispatch(MAP{'a': 'pair'}, 'a', 7));
-- Result: 2
```

**Miss and NULL handling:**

```sql
SELECT func_dispatch(MAP{'git': 'route_git'}, 's3', 'x') IS NULL;
-- Result: true

SELECT func_dispatch(MAP{'git': 'route_git'}, NULL, 'x') IS NULL;
-- Result: true
```

**Positional forwarding only:**

```sql
CREATE MACRO route_two(a, b) AS 'TWO:' || a || '/' || b;

-- Alias names are ignored; 'y' is forwarded to a (1st position) and
-- 'x' to b (2nd position), REGARDLESS of the b := / a := labels:
SELECT func_dispatch(MAP{'k': 'route_two'}, 'k', b := 'y', a := 'x');
-- Result: TWO:y/x
```

### Errors

| Error | Cause |
|-------|-------|
| `routing map has a NULL function name` | A value in `routes` is `NULL` |
| `invalid function name` | A route name contains invalid characters |
| `function '...' does not exist` | A route name does not resolve to any function |
| `is a table function` | A route name resolves to a table function (use `apply_table()` instead) |
| `candidate return types ... cannot be unified` | Two candidates from a constant routing map resolve to incompatible return types |
| `candidate '...' cannot be called with the forwarded arguments` | A candidate's declared parameters don't accept `...args` (wrong arity, or no cast to the declared type) |

**Run-time errors:** whatever the resolved function raises, plus the session
security policy, which applies to every candidate including `'*'`.

### Limitations

- `'*'` participates in exact-key matching like any other route, but it also
  serves as the fallback, so it cannot be routed anywhere *different* from
  the fallback. See [Limitations](limitations.md).
- A constant routing map requires every candidate to accept the forwarded
  arguments; a route that can't is a bind error, not a silent fallback. See
  [Limitations](limitations.md).
- Wrapping `func_dispatch` in a macro works, but the wrapper's return type is
  `VARCHAR`: the wrapping macro's own parameter has no concrete type when the
  body is validated, so the candidates cannot be bound there. See
  [Limitations](limitations.md).

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

[`func_dispatch()`](#func_dispatch) removes this dynamic-name restriction when
its routing map is a compile-time constant: it binds every candidate function
against the forwarded arguments and reports their unified return type, instead
of defaulting to `VARCHAR`. That only holds when every candidate accepts the
forwarded arguments — see [`func_dispatch`](#func_dispatch) and
[Limitations](limitations.md) for the bind-error case where one doesn't.

```sql
SELECT typeof(func_dispatch(MAP{'a': 'length'}, 'a', 'hello'));
-- Result: BIGINT
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

**Non-VARCHAR return types.** `func_dispatch()` makes non-`VARCHAR` return
types normal (see [`func_dispatch`](#func_dispatch)), and `on_block = 'default'`
casts `block_default` to whatever return type the call was bound to. If the
configured default has no cast to that type, a blocked call fails instead of
returning the default — measured: with `block_default = 'FALLBACK'` and a
route bound to `INTEGER`, a blocked call raises
`Could not convert string 'FALLBACK' to INT32` rather than returning
`'FALLBACK'`. Set a `block_default` that casts to every bound return type the
routing map can produce, or use `on_block = 'null'` instead.

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
