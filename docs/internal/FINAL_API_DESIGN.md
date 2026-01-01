# duckdb_functools - Final API Design

## Two Flavors of Apply

### 1. `apply()` - Direct Variadic Application

**Signature:**
```sql
apply(func VARCHAR, arg1 ANY, arg2 ANY, ..., argN ANY) -> ANY
-- Variadic: accepts any number of positional arguments
```

**Usage:**
```sql
-- Direct call
SELECT apply('substr', 'hello world', 7, 5);
-- Equivalent to: substr('hello world', 7, 5)

SELECT apply('concat', 'a', 'b', 'c', 'd');
-- Equivalent to: concat('a', 'b', 'c', 'd')

-- With function chaining (very DuckDB-like!)
SELECT ('substr').apply('hello world', 7, 5);
-- Equivalent to: substr('hello world', 7, 5)

SELECT ('upper').apply('hello');
-- Equivalent to: upper('hello')

-- Chain multiple applies
SELECT ('trim').apply('  hello  ').apply('upper').apply('reverse');
-- Wait, this doesn't work as intended...
-- Actually this would be: reverse(upper(trim('  hello  ')))
-- No, that's wrong too...

-- Let me rethink chaining...
-- ('trim').apply('  hello  ') becomes apply('trim', '  hello  ')
-- Then .apply('upper') chains on the result of the first apply
-- So it's like: apply('trim', '  hello  ').apply('upper')
-- Hmm, this doesn't make sense

-- Actually, chaining works like this:
SELECT 'hello'.upper();  -- upper('hello')
SELECT 'hello'.upper().reverse();  -- reverse(upper('hello'))

-- So for apply:
SELECT ('substr').apply('hello world', 7, 5);
-- Becomes: apply('substr', 'hello world', 7, 5)

-- But you can't really chain multiple applies meaningfully
-- Unless you're doing something like:
SELECT result.apply('upper')
FROM (SELECT apply('substr', 'hello world', 7, 5) as result);
```

**Implementation:**
```cpp
// Variadic function accepting any number of args
ScalarFunction apply_func("apply", {LogicalType::VARCHAR}, LogicalType::ANY, ApplyVariadic);
apply_func.varargs = LogicalType::ANY;  // Accept any additional arguments

void ApplyVariadic(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &func_names = args.data[0];
    
    for (idx_t i = 0; i < args.size(); i++) {
        // Get function name
        string func_name = GetString(func_names, i);
        
        // Collect all arguments (args.data[1..n])
        vector<Value> call_args;
        for (idx_t arg_idx = 1; arg_idx < args.ColumnCount(); arg_idx++) {
            call_args.push_back(args.data[arg_idx].GetValue(i));
        }
        
        // Look up function
        vector<LogicalType> arg_types;
        for (auto &arg : call_args) {
            arg_types.push_back(arg.type());
        }
        
        auto func = LookupFunction(context, func_name, arg_types);
        if (!func) {
            throw InvalidInputException("Function '%s' not found", func_name);
        }
        
        // Execute function
        result.SetValue(i, ExecuteFunction(func, call_args));
    }
}
```

**Limitations of apply():**
- ❌ Cannot pass named parameters through: `apply('substr', 'hello', start := 7)`
  - Named params are SQL syntax, parsed before function execution
  - Would be interpreted as named param TO apply(), not passthrough
- ✅ Can pass positional args
- ✅ Works with function chaining

### 2. `apply_with()` - Structured Application

**Signature:**
```sql
apply_with(
    func VARCHAR | STRUCT,                    -- Function name or partial descriptor
    args ANY[] | JSON DEFAULT NULL,           -- Positional arguments  
    kwargs STRUCT | MAP | JSON DEFAULT NULL   -- Named arguments
) -> ANY
```

**Usage:**
```sql
-- With args array
SELECT apply_with('substr', args := ['hello world', 7, 5]);

-- With kwargs struct
SELECT apply_with('substr', 
    args := ['hello world'],
    kwargs := {'start': 7, 'length': 5}
);

-- With JSON (flexible!)
SELECT apply_with('substr',
    args := '[\"hello world\"]'::JSON,
    kwargs := '{\"start\": 7, \"length\": 5}'::JSON
);

-- Just kwargs, no positional
SELECT apply_with('substr',
    kwargs := {'string': 'hello world', 'start': 7, 'length': 5}
);

-- Function chaining
SELECT ('substr').apply_with(
    args := ['hello world'],
    kwargs := {'start': 7, 'length': 5}
);

-- With partial descriptor (STRUCT)
SELECT apply_with(
    partial('substr', ['hello world']),
    kwargs := {'start': 7, 'length': 5}
);

-- Data-driven from table
SELECT apply_with(handler, args := input_data)
FROM config_table;
```

**Implementation:**
```cpp
ScalarFunction("apply_with",
    {LogicalType::ANY,        // func (VARCHAR or STRUCT)
     LogicalType::ANY,        // args (ANY[], JSON, or NULL)
     LogicalType::ANY},       // kwargs (STRUCT, MAP, JSON, or NULL)
    LogicalType::ANY,
    ApplyWith
);

void ApplyWith(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &func_input = args.data[0];
    auto &args_input = args.data[1];
    auto &kwargs_input = args.data[2];
    
    for (idx_t i = 0; i < args.size(); i++) {
        string func_name;
        vector<Value> call_args;
        map<string, Value> call_kwargs;
        
        // 1. Extract function name (handle VARCHAR or STRUCT partial)
        if (func_input.GetType().id() == LogicalTypeId::VARCHAR) {
            func_name = GetString(func_input, i);
        }
        else if (func_input.GetType().id() == LogicalTypeId::STRUCT) {
            // Partial descriptor
            auto partial = GetStruct(func_input, i);
            func_name = partial["func"].ToString();
            
            // Get fixed args/kwargs from partial
            auto fixed_args = partial["fixed_args"].GetListValue();
            auto fixed_kwargs = partial["fixed_kwargs"].GetStructValue();
            
            // Merge with provided args/kwargs later
            call_args.insert(call_args.end(), fixed_args.begin(), fixed_args.end());
            call_kwargs = fixed_kwargs;
        }
        
        // 2. Extract args (handle ANY[], JSON, or NULL)
        if (!args_input.GetValue(i).IsNull()) {
            auto args_type = args_input.GetType();
            
            if (args_type.id() == LogicalTypeId::LIST) {
                // ANY[] - direct list
                auto args_list = GetList(args_input, i);
                call_args.insert(call_args.end(), args_list.begin(), args_list.end());
            }
            else if (args_type.id() == LogicalTypeId::JSON) {
                // JSON - parse as array
                auto json_str = GetString(args_input, i);
                auto args_list = ParseJSONArray(json_str);
                call_args.insert(call_args.end(), args_list.begin(), args_list.end());
            }
        }
        
        // 3. Extract kwargs (handle STRUCT, MAP, JSON, or NULL)
        if (!kwargs_input.GetValue(i).IsNull()) {
            auto kwargs_type = kwargs_input.GetType();
            
            if (kwargs_type.id() == LogicalTypeId::STRUCT) {
                // STRUCT - direct conversion
                auto kwargs_struct = GetStructAsMap(kwargs_input, i);
                call_kwargs = MergeMaps(call_kwargs, kwargs_struct);
            }
            else if (kwargs_type.id() == LogicalTypeId::MAP) {
                // MAP - convert to kwargs
                auto kwargs_map = GetMapAsMap(kwargs_input, i);
                call_kwargs = MergeMaps(call_kwargs, kwargs_map);
            }
            else if (kwargs_type.id() == LogicalTypeId::JSON) {
                // JSON - parse as object
                auto json_str = GetString(kwargs_input, i);
                auto kwargs_obj = ParseJSONObject(json_str);
                call_kwargs = MergeMaps(call_kwargs, kwargs_obj);
            }
        }
        
        // 4. Build function call with named parameters
        string sql = BuildFunctionCallSQL(func_name, call_args, call_kwargs);
        // e.g., "substr('hello', start := 7, length := 5)"
        
        // 5. Execute and return
        auto func_result = ExecuteSQL(sql);
        result.SetValue(i, func_result);
    }
}
```

## Complete API

```sql
-- 1. Direct variadic apply (positional only)
apply(func VARCHAR, ...args ANY) -> ANY

-- 2. Structured apply (supports kwargs, JSON, partials)
apply_with(
    func VARCHAR | STRUCT,
    args ANY[] | JSON DEFAULT NULL,
    kwargs STRUCT | MAP | JSON DEFAULT NULL
) -> ANY

-- 3. Create partial descriptors
partial(
    func VARCHAR,
    args ANY[] DEFAULT [],
    kwargs STRUCT DEFAULT {}
) -> STRUCT

-- 4. Optional: check if function exists
function_exists(name VARCHAR) -> BOOLEAN
```

## Usage Patterns

### Pattern 1: Simple Dynamic Calls

```sql
-- Direct call (most natural)
SELECT apply('upper', 'hello');

-- With chaining
SELECT ('upper').apply('hello');

-- Multiple args
SELECT apply('concat', 'a', 'b', 'c');
```

### Pattern 2: Data-Driven with Kwargs

```sql
-- Configuration table
CREATE TABLE transforms (
    name VARCHAR,
    func VARCHAR,
    args JSON,
    kwargs JSON
);

INSERT INTO transforms VALUES (
    'clean_text',
    'regexp_replace',
    '["  hello world  "]'::JSON,
    '{"pattern": "\\s+", "replacement": " ", "modifiers": "g"}'::JSON
);

-- Apply from config
SELECT apply_with(func, args := args::JSON, kwargs := kwargs::JSON)
FROM transforms
WHERE name = 'clean_text';
```

### Pattern 3: Partial Application

```sql
-- Create reusable partials
CREATE TABLE url_builders (
    name VARCHAR,
    builder STRUCT(func VARCHAR, fixed_args ANY[], fixed_kwargs STRUCT)
);

INSERT INTO url_builders VALUES (
    'https',
    partial('concat', ['https://'])
);

-- Use with apply_with
SELECT apply_with(builder, args := ['example.com'])
FROM url_builders
WHERE name = 'https';
-- 'https://example.com'

-- Or with apply (if no kwargs needed)
SELECT apply(builder, 'example.com')
FROM url_builders
WHERE name = 'https';
```

### Pattern 4: URL Routing

```sql
CREATE TABLE routes (
    pattern URLPattern,
    handler STRUCT(func VARCHAR, fixed_args ANY[], fixed_kwargs STRUCT)
);

INSERT INTO routes VALUES (
    '/api/v1/*',
    partial('handle_request', [], {
        'api_version': 'v1',
        'rate_limit': 1000
    })
);

-- Route request
SELECT apply_with(
    handler,
    args := [url_pattern_extract(pattern, url)]
)
FROM routes
WHERE url_pattern_test(pattern, url)
LIMIT 1;
```

### Pattern 5: Middleware Chain

```sql
CREATE TABLE middleware (
    step INTEGER,
    mw STRUCT(func VARCHAR, fixed_args ANY[], fixed_kwargs STRUCT)
);

-- Apply chain
SELECT list_reduce(
    (SELECT list(mw ORDER BY step) FROM middleware),
    (req, mw) -> apply_with(mw, args := [req]),
    initial_request
);
```

## Function Chaining Examples

```sql
-- apply() with chaining
SELECT ('substr').apply('hello world', 7, 5);
-- 'world'

-- apply_with() with chaining
SELECT ('substr').apply_with(
    args := ['hello world'],
    kwargs := {'start': 7, 'length': 5}
);
-- 'world'

-- Chaining results
SELECT result.('upper').apply()
FROM (SELECT apply('substr', 'hello world', 7, 5) as result);
-- Hmm, this syntax doesn't work

-- Better: chain on string result
SELECT apply('upper', apply('substr', 'hello world', 7, 5));
-- 'WORLD'

-- Or use DuckDB's native chaining when possible
SELECT 'hello world'.substr(7, 5).upper();
-- 'WORLD'
```

## Comparison

| Feature | apply() | apply_with() |
|---------|---------|--------------|
| **Syntax** | Natural, variadic | Structured |
| **Positional args** | ✅ Direct | ✅ Via array/JSON |
| **Named params** | ❌ Cannot passthrough | ✅ Via struct/map/JSON |
| **Function chaining** | ✅ | ✅ |
| **Partial descriptors** | ❌ | ✅ |
| **Data-driven** | Limited | ✅ Full support |
| **JSON support** | ❌ | ✅ |
| **Use case** | Simple, direct calls | Config-driven, complex |

## Why Both?

**Use `apply()` when:**
- Simple, direct function calls
- Positional args only
- Want natural syntax close to direct call
- Using interactively

**Use `apply_with()` when:**
- Need named parameters
- Args/kwargs come from data (tables, JSON, config)
- Working with partial descriptors
- Building complex, data-driven systems

## Limitations & Notes

**Named Parameter Passthrough:**
This does NOT work:
```sql
SELECT apply('substr', 'hello', start := 7);  -- ❌
```
Because `start := 7` is SQL syntax parsed before `apply()` executes.

Instead, use `apply_with()`:
```sql
SELECT apply_with('substr', args := ['hello'], kwargs := {'start': 7});  -- ✅
```

**Function Chaining:**
Both support chaining:
```sql
SELECT ('func_name').apply(arg1, arg2);
SELECT ('func_name').apply_with(args := [arg1, arg2]);
```

But nested chaining of results is limited - use DuckDB's native `.method()` when possible.

## Implementation Summary

Total: ~500 lines of C++

1. `apply()` - variadic positional args (~100 lines)
2. `apply_with()` - structured with multiple input types (~200 lines)
3. `partial()` - create descriptors (~100 lines)
4. Utilities - lookups, SQL generation (~100 lines)

Clean, powerful, flexible. ✓
