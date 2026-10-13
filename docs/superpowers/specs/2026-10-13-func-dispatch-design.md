# func_dispatch — map-driven dynamic dispatch with real return types

Status: approved design, not yet implemented
Date: 2026-10-13

## Problem

Routing a value to one of several cleaning functions based on a computed key
already works today:

```sql
SELECT apply(routes[urlpattern_protocol(u)], u) FROM t;
-- git://h/a.git -> GIT:h/a.git
-- https://h/b   -> HTTP:https://h/b
```

So the gap is not dispatch. The gap is **typing**. When the function name is not
a bind-time constant, `apply` has nothing to bind against and falls back to
VARCHAR. Measured on v1.5.6:

| dispatch        | `plus1(41)`      | `mk(7)` (returns a list) |
|-----------------|------------------|--------------------------|
| constant name   | `42` INTEGER     | `[7,7]` INTEGER[]        |
| dynamic name    | `42` VARCHAR     | `[7, 7]` **VARCHAR**     |

A list-returning macro comes back as the *string* `'[7, 7]'`, so every
downstream list operation breaks. `docs/limitations.md` already documents this
under "Return Type Inference with Dynamic Names"; this feature closes it for the
map-dispatch case.

`apply` cannot be fixed here: it receives an opaque string and cannot know the
candidate set. `func_dispatch` receives the whole routing table, so when that
table is foldable it knows every function that could be called and can bind them
all at plan time.

## Non-goals, with evidence

Lambdas are out of scope. This is a feasibility finding, not a preference:

- **A MAP cannot hold lambdas.** `MAP{'git': lambda p: upper(p)}` fails with
  `Binder Error: This scalar function does not support lambdas!`. Lambdas are
  not first-class values, so no map can carry them.
- **Only one lambda per call, fixed at argument index 1.**
  `bind_function_expression.cpp:225` reads `constexpr idx_t lambda_expr_idx = 1;`
  with a `D_ASSERT` that the child at that index is a lambda. Confirmed:
  a lambda at index 0 is rejected, and two lambdas in one call are rejected.
  A kwargs-of-lambdas routing table is therefore impossible without patching
  DuckDB.
- **A `key_fn` lambda would buy almost nothing.** Inside a macro the parameter is
  already in scope, so the caller writes the key expression directly. The lambda
  would only save repeating one identifier.

A **kwargs** routing table (`func_dispatch(key, git := 'fn', ...)`) IS feasible —
`struct_pack` reads kwarg names from child aliases at bind time, and keys survive
intact including `"git://"` and `"weird quoted"`. It is deferred, not rejected:
this use case says the names come from a MAP variable, and MAP keeps the routing
table a first-class value.

## Signature

```
func_dispatch(routes, key, args...) -> <unified candidate return type>
```

- `routes` : `MAP(VARCHAR, VARCHAR)` — dispatch key -> function or macro name.
- `key`    : `VARCHAR` — the already-computed dispatch key.
- `args...`: forwarded verbatim to the resolved function, positional and aliased
  alike, exactly as `apply` forwards them.

```sql
CREATE MACRO normalize_name(filename) AS
  func_dispatch(MAP{'git': 'get_git_object_path', 'https': 'get_http_path'},
                urlpattern_protocol(filename),
                filename);
```

Note on keys: `url_parse(p).protocol` yields `'git:'` (WHATWG, trailing colon),
`urlpattern_protocol(p)` yields `'git'`, and neither yields `'git://'`. Map keys
must match whichever extractor is used; the docs will state this explicitly
because it is an easy silent-miss.

## Return type unification

At bind time, if `routes` is foldable:

1. Evaluate `routes` and collect its values as the candidate name set.
2. Reject a candidate set containing a NULL or a non-identifier name
   (`BinderException`).
3. For each candidate, resolve it and bind it against the actual `args...` types.
   A name that does not exist, or is a table function, is a `BinderException` at
   plan time (`apply` only discovers this per row).
4. Unify the candidate return types left to right with
   `LogicalType::TryGetMaxLogicalType(context, left, right, out)`, which returns
   false rather than throwing when no common type exists.
   `INTEGER + BIGINT -> BIGINT`. Pairs with no implicit cast raise a
   `BinderException` naming both candidates and both types; measured, this
   includes `INTEGER`/`VARCHAR` and `LIST`/`STRUCT`.
5. Set the unified type as the function's return type.

If `routes` is **not** foldable, fall back to VARCHAR with the same behavior as
`apply` today. This is a documented limitation, not a silent degradation.

Execution re-resolves the name per row through the existing
`ExecuteFunctionInternal`, so a routing table that changes per row still works;
only the *typing* benefit requires foldability.

## Miss and NULL semantics

| condition                        | result |
|----------------------------------|--------|
| `key` is NULL                    | NULL   |
| `key` absent from `routes`       | NULL   |
| `key` absent, `'*'` present      | dispatch to the `'*'` value |
| `routes` is NULL or empty        | NULL   |

The fallback is a `'*'` **key in the routing table**, not a named parameter.
Rationale: `apply` forwards aliased arguments to the target
(`apply('substr','hello world', start:=7, length:=5)` returns `world`), and a
macro may legitimately have a parameter named `default` — verified. Consuming a
`default :=` kwarg as configuration would break forwarding symmetry and shadow
real target parameters. Putting the fallback in the map keeps the argument space
untouched and keeps the routing table self-describing.

Cost: a literal `'*'` cannot be a dispatch key. Acceptable for the intended key
domains (URL schemes, file kinds, type tags); documented.

The `'*'` candidate participates in return-type unification like any other.

## Security

Security policy is enforced at **run time only**, through the existing
`ExecuteFunction` -> `ValidateFunctionCall` / `GetBlockedValue` path, exactly as
`apply` does.

Bind time deliberately does NOT consult the policy. Checking it there would turn
`func_apply_set_on_block('null')` and `('default')` into hard `BinderException`s,
contradicting the behavior the user configured; and the policy can change between
plan and execution, so a bind-time verdict is not authoritative anyway. Bind-time
errors are therefore limited to: name validity, name existence, wrong function
kind, and return-type unification.

`func_dispatch` introduces no new identifier interpolation. Unlike
`apply_table_with`, it builds no SQL text, so the injection class behind
GHSA-55g5-vp25-phpg does not apply. It must not acquire a reparse path.

Like `apply`, this is dynamic dispatch on the caller's own context, not a
sandbox, and adds no privilege.

## Errors

| situation | behavior |
|-----------|----------|
| candidate name does not exist | `BinderException` if foldable; otherwise run-time `InvalidInputException` as today |
| candidate is a table function | error naming `apply_table` |
| candidate is an aggregate/window function | error; unchanged from `apply` |
| candidate return types cannot unify | `BinderException` naming both candidates and types |
| `routes` has a NULL or invalid name | `BinderException` when foldable |
| blocked by security policy | honors `func_apply_set_on_block` (`error`/`null`/`default`) |

## Testing

New file `test/sql/func_dispatch.test`:

- happy path: two-scheme routing table, both branches
- **typing (the point of the feature):** `typeof()` on INTEGER-, LIST- and
  STRUCT-returning candidates must be the real type, not VARCHAR. Each of these
  must be shown to fail against `apply` in the same file, so the test
  discriminates rather than merely passing.
- unification: INTEGER+BIGINT -> BIGINT; INTEGER+VARCHAR -> VARCHAR;
  STRUCT vs LIST -> error
- miss -> NULL; NULL key -> NULL; `'*'` fallback hit; `'*'` plus a real hit
- non-foldable routes: still dispatches, documented VARCHAR
- arg forwarding: positional and aliased (`start := 7`)
- errors: missing name, table function, non-identifier, NULL name
- security: blacklisted candidate under each `on_block` mode

Because sqllogictest halts a file at the first failure, the
negative/discriminating assertions go in their own file or early, so a single
flipped expectation cannot mask the rest.

## Documentation

- `README.md`: `func_dispatch` section; the key-extractor mismatch warning
- `docs/api.md`: full signature and semantics table
- `docs/limitations.md`: narrow the "Return Type Inference with Dynamic Names"
  entry to say `func_dispatch` fixes it for foldable routing tables, and add the
  `'*'`-cannot-be-a-key note
- `docs/examples.md`: the URL-routing example, using a real extractor

## Risks

1. **~~Type promotion may surprise~~ — MEASURED FALSE, 2026-10-13.** The spec
   predicted `INTEGER + VARCHAR -> VARCHAR`, silently stringifying the integer
   branch. DuckDB does not do this: there is no implicit cast between a real
   (non-literal) INTEGER and VARCHAR, so such a routing map raises a
   `BinderException` instead. Measured via CASE, which unifies through the same
   path: `CASE WHEN true THEN i ELSE s END` over an INTEGER and a VARCHAR column
   gives `Cannot mix values of type VARCHAR and INTEGER`. Only widening pairs
   unify (`INTEGER + BIGINT -> BIGINT`, confirmed). The practical effect is the
   stricter "require agreement" behavior, obtained for free from DuckDB's cast
   rules rather than enforced by us. An INTEGER literal DOES absorb into
   VARCHAR's side, so a literal-only map can still promote.
2. **Bind-time binding of every candidate costs plan time** proportional to map
   size. Fine for routing tables of a handful of entries; a thousand-entry map
   would be slow to plan.
3. **DuckDB 2.0 compat.** The repo already builds against both lines via
   `src/include/duckdb_compat.hpp`; `MaxLogicalType` and the bind-return-type
   setter must be checked on both.
