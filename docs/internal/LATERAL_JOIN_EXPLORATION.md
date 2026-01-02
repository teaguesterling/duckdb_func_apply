# Lateral Join Table Function Exploration

## Goal

Enable dynamic per-row table function invocation with lateral join semantics:

```sql
-- Desired pattern: call different table functions per row
WITH config AS (SELECT 'range' AS func, 3 AS arg)
SELECT * FROM config, apply_table_each(config.func, config.arg)
```

## Core Challenge: Schema Discovery at Bind Time

DuckDB requires concrete return types at bind time. Table functions must declare their column types before execution begins. This conflicts with dynamic invocation where the function name comes from a column value (unknown at bind time).

## Approaches Explored

### 1. `in_out_function` Pattern

**Concept:** Use DuckDB's `in_out_function` table function type, which receives input chunks and produces output chunks (like a streaming operator).

**Implementation:**
- Register table function with `in_out_function` callback
- Use `schema_query` parameter to discover schema at bind time
- Execute target function per-row during `in_out_function` execution

**Findings:**
- Named parameters (`schema_query := '...'`) are NOT accessible via `input.named_parameters` when the function has lateral join inputs (column references). All parameters become part of `input_table_types`/`input_table_names`.
- Workaround: Use session variable (`SET VARIABLE apply_table_each_schema = '...'`) - poor UX.
- Calling `context.Query()` from within bind or execution causes deadlock - must create new `Connection`.
- The `in_out_function` executor calls the function repeatedly with the same input when used standalone (no actual lateral join source). Required hash-based detection to return `FINISHED`.
- Result ordering was incorrect in lateral join mode.

**Verdict:** Partially functional but architecturally problematic.

### 2. `bind_replace` Pattern

**Concept:** At bind time, replace the table function call with generated SQL.

**Works for:** Static arguments (constants known at bind time).

**Does not work for:** Dynamic arguments from columns - the whole point of lateral joins.

### 3. Expression Sniffer / Bind-Time Evaluation

**Concept:** Evaluate column reference expressions at bind time by tracing CTE definitions.

**Implementation (bindvariables):**
- Parse struct argument to extract field names and expressions
- For column references, look up CTE via `binder.FindCTE()`
- Execute CTE query to get value, set as session variable
- Enable use with `pathvariable:` protocol for dynamic file paths

**Findings:**
- Works for simple cases where CTE returns single row
- Requires expressions to be evaluable at bind time
- Session variable approach has lifetime/scoping issues

## Key DuckDB Internals Learned

### TableFunctionBindInput Structure
```cpp
struct TableFunctionBindInput {
    vector<Value> inputs;                    // Evaluated constant arguments
    named_parameter_map_t named_parameters;  // Named params (NOT available with lateral join inputs!)
    vector<LogicalType> input_table_types;   // Types of lateral join columns
    vector<string> input_table_names;        // Names of lateral join columns (includes named params!)
    TableFunctionRef *ref;                   // Reference to original parse node
};
```

### in_out_function Signature
```cpp
typedef OperatorResultType (*in_out_function_t)(
    ExecutionContext &context,
    TableFunctionInput &data,
    DataChunk &input,    // Input rows from lateral join source
    DataChunk &output    // Output rows to produce
);
```

Return values:
- `NEED_MORE_INPUT`: Request next input chunk
- `HAVE_MORE_OUTPUT`: More output for current input
- `FINISHED`: Done producing output

### Avoiding Deadlock
Calling `context.Query()` or `context.Prepare()` during bind/execution holds locks that would need to be reacquired. Solution:
```cpp
auto &db = DatabaseInstance::GetDatabase(context);
Connection conn(db);  // New connection avoids deadlock
auto result = conn.Query(sql);
```

## Alternative Approaches (Not Implemented)

### Scalar Function Returning LIST<STRUCT>
```sql
SELECT t.*, r.*
FROM config t,
     LATERAL unnest(apply_table_list(t.func, t.arg, schema_query := '...')) AS r
```

Advantages:
- Scalar functions are simpler to implement
- UNNEST handles row expansion naturally
- Named parameters should work normally

Still requires schema discovery at bind time.

### Explicit Schema Declaration
```sql
SELECT * FROM apply_table_each(
    config.func, config.arg,
    schema := [('col1', 'INTEGER'), ('col2', 'VARCHAR')]
)
```

Shifts burden to user but avoids sniffing complexity.

## Recommendations

1. **For static table function calls:** Use `apply_table()` with `bind_replace` - clean and works well.

2. **For dynamic per-row calls:** Consider whether the use case truly requires it. Often, the set of functions to call is known, and a UNION approach works:
   ```sql
   SELECT * FROM range(3) WHERE func = 'range'
   UNION ALL
   SELECT * FROM generate_series(1,5) WHERE func = 'generate_series'
   ```

3. **If truly needed:** The scalar-returning-LIST approach is the most promising path forward. Would require:
   - Schema discovery via `schema_query` at bind time
   - Return type of `LIST(STRUCT(...))`
   - Runtime execution in new Connection
   - UNNEST for row expansion

## Files Created (Now Removed)

- `apply_table_each` table function with `in_out_function`
- `bindvariables` table function for bind-time variable setting
- Test file `test/sql/apply_table_each.test`
- Test file `test/sql/bindvariables_pathvariable.test`
