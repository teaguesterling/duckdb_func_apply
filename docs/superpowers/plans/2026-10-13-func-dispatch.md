# func_dispatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `func_dispatch(routes, key, args...)` to the func_apply extension: map-driven dynamic dispatch that resolves the target function's *real* return type at bind time instead of collapsing to VARCHAR.

**Architecture:** One new bind function and one new executor in the existing single-file extension, plus a helper factored out of `BindApply` so both share return-type inference. Bind evaluates the routing MAP when foldable, binds every candidate against the actual argument types, and unifies the results with `LogicalType::TryGetMaxLogicalType`. Execution reuses the existing per-row `ExecuteFunction`, so security and macro/scalar handling come for free.

**Tech Stack:** C++17, DuckDB extension API (builds against both v1.5 and v2.0 via `src/include/duckdb_compat.hpp`), sqllogictest.

**Spec:** `docs/superpowers/specs/2026-10-13-func-dispatch-design.md`

## Global Constraints

- Must compile against **both** DuckDB lines. Never call a v2.0-only API directly; use the shims in `src/include/duckdb_compat.hpp` (`CompatBindSetReturnType`, `CompatExprReturnType`, `CompatMakeName`, `CompatSetScalarVarArgs`, `CompatSetScalarNullHandling`, `CompatSetCaptureArgumentAliases`, `DUCKDB_SCALAR_BIND_PARAMS`).
- C++17 only. `std::map::contains` and other C++20 members are forbidden — they compile on the v2.0 line and break the pinned line.
- Never use braced-init for aggregates that narrow an integer (e.g. `int64_t` into `idx_t`). gcc warns; clang and MSVC **error**. Use an explicit `static_cast`.
- Wildcard key is the literal string `*`, defined once as `FUNC_DISPATCH_WILDCARD`.
- Security policy is enforced at **run time only** (via `ExecuteFunction`). Bind must not consult it.
- Build with bounded parallelism: `make -j4` at most. This machine has OOM-killed `-j` builds of this project before.
- Every new test file begins with `require func_apply`.

---

### Task 1: Runtime dispatch, registration, and miss/NULL/wildcard semantics

Deliverable: a working `func_dispatch` that dispatches correctly and returns VARCHAR (typing comes in Task 2).

**Files:**
- Modify: `src/func_apply_extension.cpp` (add section after the `apply_with` section; register in `LoadInternal` near the existing `apply` registration at ~line 1357)
- Test: `test/sql/func_dispatch.test` (create)

**Interfaces:**
- Consumes: existing `ExecuteFunction(ClientContext&, const string&, const vector<Value>&)`, `IsValidIdentifier(const string&)`.
- Produces: `FUNC_DISPATCH_WILDCARD` (constant), `LookupRoute(const Value &routes, const string &key, string &out_name) -> bool`, `FuncDispatchScalarFun(DataChunk&, ExpressionState&, Vector&)`.

- [ ] **Step 1: Write the failing test**

Create `test/sql/func_dispatch.test`:

```
# name: test/sql/func_dispatch.test
# description: func_dispatch map-driven dynamic dispatch
# group: [sql]

require func_apply

statement ok
CREATE MACRO route_git(p) AS 'GIT:' || p;

statement ok
CREATE MACRO route_http(p) AS 'HTTP:' || p;

statement ok
CREATE MACRO route_any(p) AS 'ANY:' || p;

# --- happy path: both branches of a two-entry table ---
query I
SELECT func_dispatch(MAP{'git': 'route_git', 'https': 'route_http'}, 'git', 'a.git');
----
GIT:a.git

query I
SELECT func_dispatch(MAP{'git': 'route_git', 'https': 'route_http'}, 'https', 'b');
----
HTTP:b

# --- miss -> NULL ---
query I
SELECT func_dispatch(MAP{'git': 'route_git'}, 's3', 'x') IS NULL;
----
true

# --- NULL key -> NULL ---
query I
SELECT func_dispatch(MAP{'git': 'route_git'}, NULL, 'x') IS NULL;
----
true

# --- empty map -> NULL ---
query I
SELECT func_dispatch(MAP{}::MAP(VARCHAR,VARCHAR), 'git', 'x') IS NULL;
----
true

# --- '*' wildcard fallback on a miss ---
query I
SELECT func_dispatch(MAP{'git': 'route_git', '*': 'route_any'}, 's3', 'x');
----
ANY:x

# --- an exact hit beats the wildcard ---
query I
SELECT func_dispatch(MAP{'git': 'route_git', '*': 'route_any'}, 'git', 'x');
----
GIT:x

# --- per-row dispatch from a column ---
query II
SELECT k, func_dispatch(MAP{'git': 'route_git', 'https': 'route_http'}, k, 'v')
FROM (VALUES ('git'), ('https')) t(k) ORDER BY k;
----
git	GIT:v
https	HTTP:v

# --- positional arg forwarding (multi-arg target) ---
statement ok
CREATE MACRO route_two(a, b) AS 'TWO:' || a || '/' || b;

query I
SELECT func_dispatch(MAP{'k': 'route_two'}, 'k', 'x', 'y');
----
TWO:x/y

# --- aliased arg forwarding, as apply() does ---
query I
SELECT func_dispatch(MAP{'k': 'route_two'}, 'k', 'x', b := 'y');
----
TWO:x/y

# --- errors ---
statement error
SELECT func_dispatch(MAP{'git': 'no_such_function_xyz'}, 'git', 'x');
----
does not exist

statement error
SELECT func_dispatch(MAP{'git': 'range'}, 'git', 1);
----
apply_table

statement error
SELECT func_dispatch(MAP{'git': 'route_git'});
----
requires
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
make -j4 && build/release/test/unittest "test/sql/func_dispatch.test"
```

Expected: FAIL. First failure should be the very first query, reporting that `func_dispatch` does not exist.

- [ ] **Step 3: Implement the runtime path**

In `src/func_apply_extension.cpp`, add this section immediately after the `apply_with` section (before the `apply_table` section):

```cpp
//===--------------------------------------------------------------------===//
// func_dispatch(routes MAP(VARCHAR,VARCHAR), key VARCHAR, ...args ANY) -> ANY
//===--------------------------------------------------------------------===//

//! Routing-table key whose value is used when the dispatch key is absent.
//! A literal '*' therefore cannot be a dispatch key; documented in docs/limitations.md.
static constexpr const char *FUNC_DISPATCH_WILDCARD = "*";

//! Resolve `key` against a MAP value, falling back to FUNC_DISPATCH_WILDCARD.
//! Returns false when nothing matched (caller yields NULL).
static bool LookupRoute(const Value &routes, const string &key, string &out_name) {
	if (routes.IsNull()) {
		return false;
	}
	auto &entries = MapValue::GetChildren(routes);
	string wildcard_name;
	bool have_wildcard = false;
	for (auto &entry : entries) {
		auto &kv = StructValue::GetChildren(entry);
		if (kv[0].IsNull() || kv[1].IsNull()) {
			continue;
		}
		auto entry_key = StringValue::Get(kv[0]);
		if (entry_key == key) {
			out_name = StringValue::Get(kv[1]);
			return true;
		}
		if (entry_key == FUNC_DISPATCH_WILDCARD) {
			wildcard_name = StringValue::Get(kv[1]);
			have_wildcard = true;
		}
	}
	if (have_wildcard) {
		out_name = wildcard_name;
		return true;
	}
	return false;
}

static void FuncDispatchScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	idx_t count = args.size();

	for (idx_t i = 0; i < count; i++) {
		auto routes = args.data[0].GetValue(i);
		auto key_val = args.data[1].GetValue(i);
		if (routes.IsNull() || key_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		string func_name;
		if (!LookupRoute(routes, StringValue::Get(key_val), func_name)) {
			result.SetValue(i, Value());
			continue;
		}

		if (!IsValidIdentifier(func_name)) {
			throw InvalidInputException("func_dispatch: invalid function name '%s'", func_name);
		}

		vector<Value> func_args;
		for (idx_t j = 2; j < args.ColumnCount(); j++) {
			func_args.push_back(args.data[j].GetValue(i));
		}

		try {
			result.SetValue(i, ExecuteFunction(context, func_name, func_args));
		} catch (const Exception &e) {
			throw InvalidInputException("func_dispatch('%s'): %s", func_name, e.what());
		}
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
}
```

Register it in `LoadInternal`, immediately after the `apply_with` registration:

```cpp
	// Register func_dispatch (map-driven dispatch, variadic forwarded args)
	auto func_dispatch_func = ScalarFunction("func_dispatch",
	                                         {LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
	                                          LogicalType::VARCHAR},
	                                         LogicalType::ANY, FuncDispatchScalarFun, BindFuncDispatch);
	CompatSetScalarVarArgs(func_dispatch_func, LogicalType::ANY);
	CompatSetScalarNullHandling(func_dispatch_func, FunctionNullHandling::SPECIAL_HANDLING);
	// Forwarded arguments may be aliased (func_dispatch(m, k, x, b := 'y')). v2.0
	// defaults alias capture OFF, and without this the aliases arrive empty and every
	// named argument is silently treated as positional -- runtime breakage, green build.
	CompatSetCaptureArgumentAliases(func_dispatch_func);
	func_dispatch_func.SetFallible();
	loader.RegisterFunction(func_dispatch_func);
```

For this task, add a minimal bind that only validates arity and sets VARCHAR. Place it directly above `FuncDispatchScalarFun`:

```cpp
static unique_ptr<FunctionData> BindFuncDispatch(DUCKDB_SCALAR_BIND_PARAMS) {
#ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS
	auto &context = bind_input.GetClientContext();
	auto &arguments = bind_input.GetArguments();
	auto &bound_function = bind_input.GetBoundFunction();
#endif
	(void)context;
	CompatBindSetReturnType(bound_function, LogicalType::VARCHAR);
	if (arguments.size() < 2) {
		throw BinderException("func_dispatch requires a routing map and a key");
	}
	return nullptr;
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
make -j4 && build/release/test/unittest "test/sql/func_dispatch.test"
```

Expected: PASS, all assertions.

Note: the two `statement error` cases for a missing name and for `range` currently fail at **run time** from `ExecuteFunction`, not bind time. Both error texts still match (`does not exist`, `apply_table`). Task 2 moves them to bind time; the assertions are written to match either.

- [ ] **Step 5: Commit**

```bash
git add src/func_apply_extension.cpp test/sql/func_dispatch.test
git commit -m "feat: add func_dispatch for map-driven dynamic dispatch

Dispatches to a function named by a routing MAP, with a '*' wildcard
fallback and NULL on a miss. Return typing follows in the next commit.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XoHU19qnngy1CJnXyX1csz"
```

---

### Task 2: Bind-time return-type unification

Deliverable: the actual point of the feature — real return types instead of VARCHAR.

**Files:**
- Modify: `src/func_apply_extension.cpp` (factor a helper out of `BindApply` ~lines 686-742; replace the Task 1 stub `BindFuncDispatch`)
- Test: `test/sql/func_dispatch_typing.test` (create)

**Interfaces:**
- Consumes: `GetCallableFunctionType`, `TableFunctionExists`, `IsValidIdentifier`, `CompatExprReturnType`, `CompatMakeName`.
- Produces: `TryInferCandidateReturnType(ClientContext &context, const string &func_name, const vector<unique_ptr<Expression>> &arguments, idx_t first_arg_idx, LogicalType &out) -> bool`.

Why a separate test file: sqllogictest aborts a file at the first failing assertion, so the discriminating typing assertions live apart from Task 1's behavioral file. A single flipped expectation there cannot mask the rest.

- [ ] **Step 1: Write the failing test**

Create `test/sql/func_dispatch_typing.test`:

```
# name: test/sql/func_dispatch_typing.test
# description: func_dispatch resolves real return types; apply() cannot
# group: [sql]

require func_apply

statement ok
CREATE MACRO m_int(x) AS x + 1;

statement ok
CREATE MACRO m_list(x) AS [x, x];

statement ok
CREATE MACRO m_struct(x) AS {'v': x};

statement ok
CREATE MACRO m_big(x) AS x::BIGINT;

statement ok
CREATE MACRO m_str(x) AS x::VARCHAR;

# --- CONTROL: apply() with a dynamic name collapses to VARCHAR.
# These assertions document the defect func_dispatch fixes. If they ever
# report something other than VARCHAR, the premise of this feature changed.
query I
SELECT typeof(apply(MAP{'a': 'm_int'}[k], 41)) FROM (VALUES ('a')) t(k);
----
VARCHAR

query I
SELECT typeof(apply(MAP{'a': 'm_list'}[k], 7)) FROM (VALUES ('a')) t(k);
----
VARCHAR

# --- func_dispatch keeps the real type ---
query I
SELECT typeof(func_dispatch(MAP{'a': 'm_int'}, 'a', 41));
----
INTEGER

query I
SELECT func_dispatch(MAP{'a': 'm_int'}, 'a', 41);
----
42

query I
SELECT typeof(func_dispatch(MAP{'a': 'm_list'}, 'a', 7));
----
INTEGER[]

# a real list, not the string '[7, 7]' -- this is the user-visible payoff
query I
SELECT len(func_dispatch(MAP{'a': 'm_list'}, 'a', 7));
----
2

query I
SELECT typeof(func_dispatch(MAP{'a': 'm_struct'}, 'a', 7));
----
STRUCT(v INTEGER)

query I
SELECT func_dispatch(MAP{'a': 'm_struct'}, 'a', 7).v;
----
7

# --- unification across candidates ---
query I
SELECT typeof(func_dispatch(MAP{'a': 'm_int', 'b': 'm_big'}, 'a', 41));
----
BIGINT

query I
SELECT typeof(func_dispatch(MAP{'a': 'm_int', 'b': 'm_str'}, 'a', 41));
----
VARCHAR

# the '*' candidate participates in unification like any other
query I
SELECT typeof(func_dispatch(MAP{'a': 'm_int', '*': 'm_big'}, 'a', 41));
----
BIGINT

# --- unification failure is a bind error naming both types ---
statement error
SELECT func_dispatch(MAP{'a': 'm_list', 'b': 'm_struct'}, 'a', 7);
----
cannot be unified

# --- bind-time validation ---
statement error
SELECT func_dispatch(MAP{'a': 'no_such_function_xyz'}, 'a', 1);
----
does not exist

statement error
SELECT func_dispatch(MAP{'a': 'range'}, 'a', 1);
----
apply_table

# --- non-foldable routing table: still dispatches, documented VARCHAR ---
query I
SELECT typeof(func_dispatch(r, 'a', 41)) FROM (SELECT MAP{'a': 'm_int'} AS r) s;
----
INTEGER

query II
SELECT func_dispatch(r, k, 41), typeof(func_dispatch(r, k, 41))
FROM (VALUES (MAP{'a': 'm_int'}, 'a')) t(r, k);
----
42	VARCHAR
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
make -j4 && build/release/test/unittest "test/sql/func_dispatch_typing.test"
```

Expected: FAIL at the first `typeof(func_dispatch(...))` assertion — got `VARCHAR`, expected `INTEGER`. The two `apply()` control assertions above it must PASS; if they fail, stop and re-read the spec's premise.

- [ ] **Step 3: Factor the inference helper out of BindApply**

In `src/func_apply_extension.cpp`, add above `BindApply`:

```cpp
//! Infer what `func_name` returns when called with `arguments[first_arg_idx..]`.
//! Handles both scalar functions (FunctionBinder) and macros (full expression
//! binding against typed placeholders). Returns false when the return type
//! cannot be determined, in which case the caller keeps its default.
static bool TryInferCandidateReturnType(ClientContext &context, const string &func_name,
                                        const vector<unique_ptr<Expression>> &arguments, idx_t first_arg_idx,
                                        LogicalType &out) {
	auto func_type = GetCallableFunctionType(context, func_name);

	if (func_type == CatalogType::SCALAR_FUNCTION_ENTRY) {
		vector<unique_ptr<Expression>> target_args;
		for (idx_t i = first_arg_idx; i < arguments.size(); i++) {
			target_args.push_back(arguments[i]->Copy());
		}
		ErrorData error;
		FunctionBinder binder(context);
		auto bound_expr =
		    binder.BindScalarFunction(DEFAULT_SCHEMA, CompatMakeName(func_name), std::move(target_args), error);
		if (error.HasError() || !bound_expr) {
			return false;
		}
		out = CompatExprReturnType(*bound_expr);
		return true;
	}

	if (func_type == CatalogType::MACRO_ENTRY) {
		vector<unique_ptr<ParsedExpression>> parsed_args;
		for (idx_t i = first_arg_idx; i < arguments.size(); i++) {
			if (arguments[i]->IsFoldable()) {
				auto val = ExpressionExecutor::EvaluateScalar(context, *arguments[i]);
				parsed_args.push_back(make_uniq<ConstantExpression>(val));
			} else {
				// A typed NULL placeholder: Value(LogicalType) is a NULL of that type.
				parsed_args.push_back(make_uniq<ConstantExpression>(Value(CompatExprReturnType(*arguments[i]))));
			}
		}
		auto func_expr = make_uniq<FunctionExpression>(CompatMakeName(func_name), std::move(parsed_args));
		try {
			auto binder = Binder::CreateBinder(context);
			ConstantBinder constant_binder(*binder, context, "func_dispatch");
			auto bound_expr = constant_binder.Bind(reinterpret_cast<unique_ptr<ParsedExpression> &>(func_expr));
			out = CompatExprReturnType(*bound_expr);
			return true;
		} catch (...) {
			return false;
		}
	}

	return false;
}
```

Note on the `reinterpret_cast`: `ConstantBinder::Bind` takes `unique_ptr<ParsedExpression>&`. If `func_expr` is already declared as `unique_ptr<ParsedExpression>` the cast is unnecessary — prefer declaring it that way and dropping the cast:

```cpp
		unique_ptr<ParsedExpression> func_expr =
		    make_uniq<FunctionExpression>(CompatMakeName(func_name), std::move(parsed_args));
		...
		auto bound_expr = constant_binder.Bind(func_expr);
```

Use this second form. It matches the existing code at `BindApply`.

- [ ] **Step 4: Replace BindFuncDispatch with the unifying version**

```cpp
static unique_ptr<FunctionData> BindFuncDispatch(DUCKDB_SCALAR_BIND_PARAMS) {
#ifdef DUCKDB_HAS_NEW_VECTOR_HEADERS
	auto &context = bind_input.GetClientContext();
	auto &arguments = bind_input.GetArguments();
	auto &bound_function = bind_input.GetBoundFunction();
#endif
	CompatBindSetReturnType(bound_function, LogicalType::VARCHAR);

	if (arguments.size() < 2) {
		throw BinderException("func_dispatch requires a routing map and a key");
	}

	// A non-foldable routing table is unknowable at plan time: keep VARCHAR.
	// Documented in docs/limitations.md.
	if (!arguments[0]->IsFoldable()) {
		return nullptr;
	}

	Value routes = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (routes.IsNull()) {
		return nullptr;
	}

	auto &entries = MapValue::GetChildren(routes);
	if (entries.empty()) {
		return nullptr;
	}

	LogicalType unified;
	bool have_unified = false;

	for (auto &entry : entries) {
		auto &kv = StructValue::GetChildren(entry);
		if (kv[1].IsNull()) {
			throw BinderException("func_dispatch: routing map has a NULL function name");
		}
		auto candidate = StringValue::Get(kv[1]);
		if (!IsValidIdentifier(candidate)) {
			throw BinderException("func_dispatch: invalid function name '%s'", candidate);
		}

		auto func_type = GetCallableFunctionType(context, candidate);
		if (func_type == CatalogType::INVALID) {
			if (TableFunctionExists(context, candidate)) {
				throw BinderException("func_dispatch: '%s' is a table function. Use apply_table() instead.",
				                      candidate);
			}
			throw BinderException("func_dispatch: function '%s' does not exist", candidate);
		}

		LogicalType candidate_type;
		if (!TryInferCandidateReturnType(context, candidate, arguments, 2, candidate_type)) {
			// Could not type this candidate: fall back to VARCHAR for the whole call
			// rather than reporting a type only some branches satisfy.
			CompatBindSetReturnType(bound_function, LogicalType::VARCHAR);
			return nullptr;
		}

		if (!have_unified) {
			unified = candidate_type;
			have_unified = true;
			continue;
		}

		LogicalType merged;
		if (!LogicalType::TryGetMaxLogicalType(context, unified, candidate_type, merged)) {
			throw BinderException(
			    "func_dispatch: candidate return types %s and %s cannot be unified; the routing map must "
			    "dispatch to functions with a common return type",
			    unified.ToString(), candidate_type.ToString());
		}
		unified = merged;
	}

	if (have_unified) {
		CompatBindSetReturnType(bound_function, unified);
	}
	return nullptr;
}
```

Then simplify `BindApply`'s scalar and macro branches to call `TryInferCandidateReturnType(context, func_name, arguments, 1, t)` and set the type when it returns true. Behavior must be unchanged — the extracted code is the same code.

- [ ] **Step 5: Run both test files to verify they pass**

```bash
make -j4
build/release/test/unittest "test/sql/func_dispatch_typing.test"
build/release/test/unittest "test/sql/func_dispatch.test"
```

Expected: both PASS. Task 1's two `statement error` cases now fail at bind time instead of run time; their substrings still match.

- [ ] **Step 6: Run the whole suite for regressions**

```bash
build/release/test/unittest
```

Expected: PASS. `BindApply` was refactored, so `test/sql/func_apply.test` is the regression guard that matters.

- [ ] **Step 7: Commit**

```bash
git add src/func_apply_extension.cpp test/sql/func_dispatch_typing.test
git commit -m "feat: resolve func_dispatch return types at bind time

Binds every candidate in a foldable routing map and unifies the results with
TryGetMaxLogicalType, so dispatching to a list- or struct-returning macro no
longer stringifies the result. Return-type inference is factored out of
BindApply and shared.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XoHU19qnngy1CJnXyX1csz"
```

---

### Task 3: Security-policy coverage

Deliverable: proof that `func_dispatch` honors the existing session policy and is not a bypass.

**Files:**
- Test: `test/sql/func_dispatch_security.test` (create)
- Modify: `src/func_apply_extension.cpp` only if a test reveals a gap.

**Interfaces:**
- Consumes: `ExecuteFunction` (already routes through `ValidateFunctionCall` / `GetBlockedValue`), and the session setters `func_apply_set_security_mode`, `func_apply_set_blacklist`, `func_apply_set_whitelist`, `func_apply_set_on_block`, `func_apply_set_block_default`.
- Produces: nothing new. This task is a guard.

- [ ] **Step 1: Write the failing test**

Create `test/sql/func_dispatch_security.test`:

```
# name: test/sql/func_dispatch_security.test
# description: func_dispatch honors the session security policy
# group: [sql]

require func_apply

statement ok
CREATE MACRO s_ok(x) AS 'OK:' || x;

statement ok
CREATE MACRO s_blocked(x) AS 'BLOCKED:' || x;

# --- whitelist mode: an allowed candidate still works ---
statement ok
SELECT func_apply_set_security_mode('whitelist');

statement ok
SELECT func_apply_set_whitelist(['s_ok']);

query I
SELECT func_dispatch(MAP{'a': 's_ok'}, 'a', 'x');
----
OK:x

# --- a candidate outside the whitelist is blocked at run time ---
statement error
SELECT func_dispatch(MAP{'a': 's_blocked'}, 'a', 'x');
----
blocked

# --- on_block = 'null' yields NULL rather than an error ---
statement ok
SELECT func_apply_set_on_block('null');

query I
SELECT func_dispatch(MAP{'a': 's_blocked'}, 'a', 'x') IS NULL;
----
true

# --- on_block = 'default' yields the configured value ---
statement ok
SELECT func_apply_set_on_block('default');

statement ok
SELECT func_apply_set_block_default('FALLBACK');

query I
SELECT func_dispatch(MAP{'a': 's_blocked'}, 'a', 'x');
----
FALLBACK

# --- the wildcard route is policed too: it is not a side door ---
statement ok
SELECT func_apply_set_on_block('error');

statement error
SELECT func_dispatch(MAP{'*': 's_blocked'}, 'anything', 'x');
----
blocked

statement ok
SELECT func_apply_set_security_mode('none');
```

- [ ] **Step 2: Run the test**

```bash
make -j4 && build/release/test/unittest "test/sql/func_dispatch_security.test"
```

Expected: PASS without source changes, because `FuncDispatchScalarFun` calls `ExecuteFunction`, which validates. If any assertion fails, that is a real bypass — fix `FuncDispatchScalarFun` to route every call through `ExecuteFunction` (never `ExecuteFunctionInternal` with `skip_security_check = true`).

Note on the `on_block = 'default'` case: `func_apply_set_block_default('FALLBACK')` sets a VARCHAR, and Task 2 types this call from the candidate `s_blocked`, which also returns VARCHAR. Should a future test mix a non-VARCHAR default with a typed candidate, expect a cast error; that interaction is out of scope here.

- [ ] **Step 3: Commit**

```bash
git add test/sql/func_dispatch_security.test
git commit -m "test: pin func_dispatch to the session security policy

Covers whitelist enforcement, all three on_block behaviors, and the '*'
wildcard route, so the fallback cannot become a policy side door.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XoHU19qnngy1CJnXyX1csz"
```

---

### Task 4: Documentation

Deliverable: `func_dispatch` documented, and the limitation it closes narrowed rather than left stale.

**Files:**
- Modify: `README.md` (new section after the `apply_with` docs; add to the Overview bullet list)
- Modify: `docs/api.md`
- Modify: `docs/limitations.md` (the "Return Type Inference with Dynamic Names" entry, ~line 48; the kwargs entry stays as-is)
- Modify: `docs/examples.md`

**Interfaces:**
- Consumes: the finished behavior from Tasks 1-3.
- Produces: nothing code-facing.

- [ ] **Step 1: Add the README section**

Add to the Overview bullet list:

```markdown
- Route a value to one of several functions with `func_dispatch()`
```

Add this section after the `apply_with` documentation:

```markdown
### `func_dispatch(routes, key, ...args)`

Dispatches to the function named by `routes[key]`, forwarding `...args`.

```sql
CREATE MACRO get_git_object_path(p) AS 'GIT:' || regexp_replace(p, '^git://', '');
CREATE MACRO get_http_path(p) AS 'HTTP:' || p;

CREATE MACRO normalize_name(filename) AS
  func_dispatch(MAP{'git': 'get_git_object_path', 'https': 'get_http_path'},
                urlpattern_protocol(filename),
                filename);

SELECT normalize_name('git://host/a/b.git');  -- GIT:host/a/b.git
```

Unlike `apply()` with a dynamic name, `func_dispatch` binds every function in
the routing map at plan time, so the result keeps its **real type**:

```sql
CREATE MACRO pair(x) AS [x, x];

-- apply() with a dynamic name returns the STRING '[7, 7]':
SELECT typeof(apply(MAP{'a':'pair'}[k], 7)) FROM (VALUES ('a')) t(k);  -- VARCHAR

-- func_dispatch returns a real list:
SELECT typeof(func_dispatch(MAP{'a':'pair'}, 'a', 7));                 -- INTEGER[]
```

| behavior | result |
|----------|--------|
| `key` missing from `routes` | `NULL` |
| `key` is `NULL` | `NULL` |
| `routes` contains a `'*'` entry | used when `key` is missing |
| candidates return different types | unified (`INTEGER`+`BIGINT` -> `BIGINT`); a bind error if they have no common type |
| `routes` is not a constant | works, but the return type falls back to `VARCHAR` |

**Watch the key extractor.** The map key must match exactly what the extractor
produces. For a URL these differ:

| expression | value for `git://host/x` |
|------------|--------------------------|
| `urlpattern_protocol(p)` | `git` |
| `url_parse(p).protocol` | `git:` |

A key of `'git://'` matches neither and silently returns `NULL`.

The routing map is checked against the session [security policy](#security-model)
at run time, including the `'*'` entry.
```

- [ ] **Step 2: Add the api.md entry**

Append to `docs/api.md`, matching the file's existing heading style:

```markdown
## `func_dispatch(routes, key, ...args)`

**Signature:** `func_dispatch(MAP(VARCHAR, VARCHAR), VARCHAR, ...ANY) -> ANY`

Resolves `routes[key]` to a scalar function or macro name and calls it with
`...args`. Falls back to the `'*'` entry when `key` is absent; returns `NULL`
when neither matches, when `key` is `NULL`, or when `routes` is `NULL` or empty.

**Return type.** When `routes` is a constant, every candidate is bound at plan
time and the return types are unified with DuckDB's normal type promotion; that
unified type is the function's return type. When `routes` is not a constant the
return type is `VARCHAR`, as with `apply()`.

**Bind-time errors:** a candidate that does not exist, is a table function, has
a non-identifier name, or is `NULL`; and candidates whose return types cannot be
unified.

**Run-time errors:** whatever the target raises, plus the session security
policy, which applies to every candidate including `'*'`.

Arguments are forwarded positionally and by alias, the same way `apply()`
forwards them: `func_dispatch(m, k, 'hello world', start := 7)`.
```

- [ ] **Step 3: Narrow the stale limitation**

In `docs/limitations.md`, replace the "Return Type Inference with Dynamic Names" body (keep the heading) with:

```markdown
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

### `'*'` cannot be a dispatch key

`func_dispatch()` reserves the map key `'*'` for the fallback route, so a literal
`'*'` cannot be dispatched on. Use a sentinel the key domain cannot produce if you
need to route it explicitly.
```

- [ ] **Step 4: Add the worked example**

Append to `docs/examples.md`:

```markdown
## Routing URLs to per-scheme cleaners

```sql
LOAD func_apply;
LOAD urlpattern;

CREATE MACRO clean_git(p)   AS regexp_replace(p, '^git://', '');
CREATE MACRO clean_http(p)  AS regexp_replace(p, '^https?://', '');
CREATE MACRO clean_other(p) AS p;

CREATE MACRO normalize_name(filename) AS
  func_dispatch(MAP{'git':   'clean_git',
                    'http':  'clean_http',
                    'https': 'clean_http',
                    '*':     'clean_other'},
                urlpattern_protocol(filename),
                filename);

SELECT u, normalize_name(u) AS cleaned
FROM (VALUES ('git://host/a.git'), ('https://host/b'), ('s3://bucket/c')) t(u);
```

The `'*'` entry catches every scheme the table does not name, so an unknown
scheme passes through instead of becoming `NULL`.
```

- [ ] **Step 5: Verify every documented example actually runs**

Do not trust the prose. Execute each example against the built extension:

```bash
cat > /tmp/doc_examples.sql <<'SQL'
LOAD func_apply;
CREATE MACRO pair(x) AS [x, x];
SELECT typeof(apply(MAP{'a':'pair'}[k], 7)) FROM (VALUES ('a')) t(k);
SELECT typeof(func_dispatch(MAP{'a':'pair'}, 'a', 7));
SELECT typeof(func_dispatch(MAP{'a': 'length'}, 'a', 'hello'));
CREATE MACRO route_two(a, b) AS a || '/' || b;
SELECT func_dispatch(MAP{'k':'route_two'}, 'k', 'x', b := 'y');
SQL
build/release/duckdb -unsigned -init /dev/null < /tmp/doc_examples.sql
```

Expected: `VARCHAR`, `INTEGER[]`, `BIGINT`, `x/y`. Any mismatch means the docs are wrong — fix the docs, not the expectation.

The README's `normalize_name` example needs the `urlpattern` extension; run it only if `urlpattern` is installed, and otherwise verify the same shape with a plain key expression.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/api.md docs/limitations.md docs/examples.md
git commit -m "docs: document func_dispatch and narrow the dynamic-name limitation

Adds the function to README, api.md and examples.md, records that '*' is
reserved, and warns that url scheme extractors disagree ('git' vs 'git:'),
which is an easy silent miss.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01XoHU19qnngy1CJnXyX1csz"
```

---

## Self-Review

**Spec coverage.** Signature -> Task 1. Return-type unification incl. `TryGetMaxLogicalType` and the non-foldable fallback -> Task 2. Miss/NULL/`'*'` table -> Task 1. Security (run-time only, per the amended spec) -> Task 3. Error table -> Tasks 1 and 2. Testing list -> Tasks 1-3; the spec's requirement that discriminating assertions be isolated from first-failure masking is met by splitting typing into its own file. Documentation list -> Task 4, all four files. Non-goals need no task. Risk 3 (v2.0 compat) is covered by the Global Constraints plus the compat shims used in every snippet.

**Known gap, deliberate:** the spec's risk 1 (`INTEGER + VARCHAR -> VARCHAR` silently stringifying one branch) is documented in Task 4's README table, not defended against in code. That was the approved decision.

**Placeholder scan.** No TBD/TODO. Every code step carries real code; every test step carries real assertions; every run step names a command and its expected outcome.

**Type consistency.** `TryInferCandidateReturnType(context, name, arguments, first_arg_idx, out) -> bool` is defined in Task 2 and used in Task 2 only (by `BindFuncDispatch` and the refactored `BindApply`). `LookupRoute(routes, key, out_name) -> bool` and `FUNC_DISPATCH_WILDCARD` are defined and used in Task 1, and `FUNC_DISPATCH_WILDCARD`'s reservation is documented in Task 4. `BindFuncDispatch` is declared as a stub in Task 1 and replaced in Task 2 — the registration snippet in Task 1 references it, so it must exist before the registration compiles, which the Task 1 ordering satisfies.
