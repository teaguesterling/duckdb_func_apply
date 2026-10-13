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

`func_dispatch()` removes this restriction when the routing map is a constant,
because it can bind every candidate at plan time:

```sql
SELECT typeof(func_dispatch(MAP{'a': 'length'}, 'a', 'hello'));  -- BIGINT
```

A routing map that is itself non-constant still falls back to `VARCHAR`.

**This only works when every route accepts the forwarded arguments.** With a
constant routing map, each candidate is bound against the arguments the call
forwards. If any candidate's declared parameters don't match — wrong arity, or
an argument with no cast to the declared parameter type — the call is a
**bind error**, naming the offending candidate:

```sql
-- m_one_arg takes one parameter but this call forwards zero:
SELECT func_dispatch(MAP{'a': 'm_zero_arg', 'b': 'm_one_arg'}, 'a');
-- Error: func_dispatch: candidate 'm_one_arg' cannot be called with the
-- forwarded arguments; every route in a constant routing map must accept
-- them (or make the routing map non-constant to fall back to VARCHAR)
```

A routing map should dispatch only to functions that all accept the arguments
the call site forwards and agree on a return type.

This check only runs when the forwarded argument types are actually known. A
macro body's own parameter, a prepared-statement `?`, and a bare `NULL` have no
concrete type at bind time, and a route cannot meaningfully be judged against an
argument it knows nothing about — so in those cases no candidate is checked and
the return type falls back to `VARCHAR`.

A candidate that *is* callable but whose return type cannot be determined — its
body folds away to `NULL` for any probe value, as `nullif(x, '')` does — is not
an error either; the whole call falls back to `VARCHAR`.

### Wrapping `func_dispatch()` in a macro returns `VARCHAR`, not the unified type

A macro's body is bound once, eagerly, at `CREATE MACRO` time, using its own
*not-yet-resolved* parameters. A wrapper like

```sql
CREATE MACRO clean_git(p) AS regexp_replace(p, '^git://', '');

CREATE MACRO normalize_name(filename) AS
  func_dispatch(MAP{'git': 'clean_git'}, 'git', filename);
```

works, and `normalize_name('git://host/a.git')` returns the right value. But
because `filename` has no concrete type when the body is validated,
`func_dispatch` declines to bind the candidates there, so the wrapper's return
type is `VARCHAR` — the same fallback a non-constant routing map gets. Call
`func_dispatch` inline in the query when you need the real unified return type.

### `'*'` can be a dispatch key, but cannot route anywhere different from the fallback

`func_dispatch()`'s lookup checks for an *exact* match on `key` first, and `'*'`
participates in that exact-match pass like any other map key — so a literal
`'*'` key does resolve:

```sql
SELECT func_dispatch(MAP{'*': 'upper'}, '*', 'x');  -- 'X'
```

The real limitation is narrower: `'*'` also serves as the fallback route used
when `key` is absent, and both lookups read the same map entry, so `'*'`
cannot be routed anywhere *different* from the fallback. If you need an exact
match on the literal string `'*'` that differs from the fallback behavior, use
a sentinel the key domain cannot otherwise produce.

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
2. **kwargs support for apply_with()** - Named parameters via struct for scalar functions
3. **Aggregate function support** - Call aggregates dynamically
4. **Partial application** - `partial()` for currying functions
