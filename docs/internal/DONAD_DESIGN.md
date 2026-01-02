# Donad: Declarative Effect Chains for DuckDB

## Name

**donad** = Donald (Duck) + monad

A monadic execution model for declarative state threading and effect execution in DuckDB.

*Alternative considered: flowal (flow + fowl)*

## Core Concept

Donad separates *describing* operations from *executing* them:

```
┌────────────────────────────────────────────────────────────────┐
│  Pure Description Layer                                        │
│                                                                 │
│  Chain of steps, each declaring:                               │
│  - What operation to perform                                   │
│  - How to transform state                                      │
│  - What to pass to the next step                               │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│  Impure Execution Layer                                        │
│                                                                 │
│  Interpreter that:                                             │
│  - Executes each step                                          │
│  - Threads state through the chain                             │
│  - Handles errors and rollback                                 │
│  - Returns final state                                         │
└────────────────────────────────────────────────────────────────┘
```

## API Design

### Core Execution

```sql
-- Execute a chain of steps
do_run(steps LIST) -> STRUCT(state, result, error)

-- Execute with initial state
do_run(steps LIST, initial_state STRUCT) -> STRUCT(state, result, error)
```

### Chain Construction

```sql
-- Build a chain (returns a describtion, doesn't execute)
do_chain([
    step1,
    step2,
    step3
]) -> DONAD_CHAIN
```

## Standard Steps

### State Manipulation

```sql
-- Set state value
do_set(key VARCHAR, value ANY)
do_set({key1: val1, key2: val2})

-- Transform state
do_map(state -> new_state)

-- Get from state (for conditional logic)
do_get(key VARCHAR) -> value
```

### Database Operations

```sql
-- Insert and capture result
do_insert(
    table VARCHAR,
    values STRUCT,
    returning := ['id', 'created_at']  -- optional, adds to state
)

-- Update with conditions
do_update(
    table VARCHAR,
    set STRUCT,
    where VARCHAR,  -- or expression
    returning := [...]
)

-- Delete
do_delete(
    table VARCHAR,
    where VARCHAR,
    returning := [...]
)

-- Query (read into state)
do_query(
    sql VARCHAR,
    params := [...],
    as := 'result_key'  -- key in state
)
```

### Control Flow

```sql
-- Conditional execution
do_if(
    condition,  -- expression using state
    then_steps LIST,
    else_steps LIST  -- optional
)

-- Early return
do_return(value)

-- Fail with error
do_fail(message VARCHAR)

-- Assert condition
do_assert(condition, error_message)
```

### Transaction Control

```sql
-- Wrap steps in transaction
do_transaction([
    step1,
    step2,
    step3
])

-- Explicit savepoint
do_savepoint(name VARCHAR, steps LIST)
```

## Example: User Registration

```sql
CREATE MACRO register_user(email, password, name) AS (
    do_chain([
        -- Validate input
        do_assert(email IS NOT NULL, 'Email required'),
        do_assert(length(password) >= 8, 'Password too short'),

        -- Check for existing user
        do_query(
            'SELECT id FROM users WHERE email = $1',
            params := [email],
            as := 'existing'
        ),
        do_if(
            state -> state.existing IS NOT NULL,
            [do_fail('Email already registered')]
        ),

        -- Hash password (using some hash function)
        do_set('password_hash', hash_password(password)),

        -- Insert user
        do_insert(
            'users',
            {email: email, password_hash: state.password_hash, name: name},
            returning := ['id', 'created_at']
        ),

        -- Insert audit log
        do_insert(
            'audit_log',
            {action: 'user_registered', user_id: state.id, timestamp: now()}
        ),

        -- Send welcome email (side effect)
        do_effect('send_email', {
            to: email,
            template: 'welcome',
            vars: {name: name}
        }),

        -- Return result
        do_return({
            success: true,
            user_id: state.id,
            created_at: state.created_at
        })
    ])
);

-- Execute it
SELECT do_run(register_user('alice@example.com', 'secret123', 'Alice'));
```

## Example: HTTP Handler with Donad

```sql
CREATE MACRO create_user_handler(req) AS (
    do_chain([
        -- Parse request
        do_set('body', json_deserialize(req.body)),

        -- Validate
        do_assert(state.body.email IS NOT NULL, 'Email required'),

        -- Insert user
        do_insert('users', state.body, returning := ['id']),

        -- Build response
        do_return({
            status: 201,
            headers: {'Content-Type': 'application/json'},
            body: to_json({id: state.id, email: state.body.email})
        })
    ])
);

-- httpserver calls this and executes the chain
```

## State Threading Model

State flows through the chain, each step can read and modify it:

```
initial_state: {}
        ↓
┌─────────────────────────┐
│ do_set('x', 1)      │  state: {x: 1}
└─────────────────────────┘
        ↓
┌─────────────────────────┐
│ do_insert(...)      │  state: {x: 1, id: 123, created_at: ...}
│   returning := [id, ...]│
└─────────────────────────┘
        ↓
┌─────────────────────────┐
│ do_map(s ->         │  state: {x: 1, id: 123, doubled: 2}
│   {...s, doubled: s.x*2}│
└─────────────────────────┘
        ↓
final_state: {x: 1, id: 123, created_at: ..., doubled: 2}
```

## Error Handling

```sql
do_chain([
    do_try([
        -- steps that might fail
        do_insert(...),
        do_insert(...)
    ], catch := [
        -- error handling steps
        do_set('error', state.do_error),
        do_return({success: false, error: state.error.message})
    ]),

    -- continues here if try succeeded
    do_return({success: true})
])
```

## Custom Step Types

### C++ Extension API

Custom steps are C++ classes that:
1. Define what parameters they accept
2. Execute some logic (pure or side-effecting)
3. Return a new state (or the same state unchanged)

When registered, they become available as `do_<name>()` in SQL.

#### Basic Step Structure

```cpp
#include "donad/step.hpp"

namespace donad {

class SendEmailStep : public DonadStep {
public:
    // The SQL function will be do_send_email()
    static constexpr const char* NAME = "send_email";

    // Define accepted parameters
    static vector<LogicalType> GetParameterTypes() {
        return {LogicalType::STRUCT({
            {"to", LogicalType::VARCHAR},
            {"template", LogicalType::VARCHAR},
            {"vars", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::ANY)}
        })};
    }

    // Execute the step
    // - context: DuckDB client context
    // - params: The parameters passed to do_send_email(...)
    // - current_state: The state flowing through the chain
    // Returns: The new state (can be same as current_state)
    static Value Execute(ClientContext &context,
                         const Value &params,
                         const Value &current_state) {
        // Extract params
        auto to = StructValue::GetChildren(params)[0].ToString();
        auto tmpl = StructValue::GetChildren(params)[1].ToString();
        auto vars = StructValue::GetChildren(params)[2];

        // Do the side effect
        EmailService::Send(to, tmpl, vars);

        // Return state unchanged
        return current_state;
    }
};

} // namespace donad
```

#### Step That Modifies State

```cpp
class HttpGetStep : public DonadStep {
public:
    static constexpr const char* NAME = "http_get";

    static vector<LogicalType> GetParameterTypes() {
        return {
            LogicalType::VARCHAR,  // url
            LogicalType::VARCHAR   // state_key - where to store result
        };
    }

    static Value Execute(ClientContext &context,
                         const Value &params,
                         const Value &current_state) {
        auto url = params.GetChild(0).ToString();
        auto state_key = params.GetChild(1).ToString();

        // Perform HTTP request
        auto response = HttpClient::Get(url);

        // Add result to state
        return DonadState::Set(current_state, state_key, Value(response.body));
    }
};
```

#### Step With Error Handling

```cpp
class ValidateStep : public DonadStep {
public:
    static constexpr const char* NAME = "validate_json";

    static vector<LogicalType> GetParameterTypes() {
        return {
            LogicalType::VARCHAR,  // json_string
            LogicalType::VARCHAR   // schema_name
        };
    }

    static Value Execute(ClientContext &context,
                         const Value &params,
                         const Value &current_state) {
        auto json_str = params.GetChild(0).ToString();
        auto schema = params.GetChild(1).ToString();

        try {
            auto parsed = JsonValidator::Validate(json_str, schema);
            return DonadState::Set(current_state, "validated", parsed);
        } catch (const ValidationError &e) {
            // Throw DonadError to abort the chain
            throw DonadError("Validation failed: " + e.message());
        }
    }
};
```

#### Async/Deferred Step

```cpp
class QueueJobStep : public DonadStep {
public:
    static constexpr const char* NAME = "queue_job";

    static vector<LogicalType> GetParameterTypes() {
        return {
            LogicalType::VARCHAR,                          // job_type
            LogicalType::MAP(LogicalType::VARCHAR, LogicalType::ANY)  // job_params
        };
    }

    static Value Execute(ClientContext &context,
                         const Value &params,
                         const Value &current_state) {
        auto job_type = params.GetChild(0).ToString();
        auto job_params = params.GetChild(1);

        // Queue job, get job_id back
        auto job_id = JobQueue::Enqueue(job_type, job_params);

        // Add job_id to state
        return DonadState::Set(current_state, "job_id", Value(job_id));
    }
};
```

#### Registration

```cpp
// In your extension's Load function
void DonadExtension::Load(DatabaseInstance &db) {
    // Register built-in steps
    DonadRegistry::Register<SendEmailStep>(db);
    DonadRegistry::Register<HttpGetStep>(db);
    DonadRegistry::Register<ValidateStep>(db);
    DonadRegistry::Register<QueueJobStep>(db);
}
```

#### State Helper API

```cpp
namespace DonadState {
    // Get a value from state
    Value Get(const Value &state, const string &key);

    // Set a value in state (returns new state)
    Value Set(const Value &state, const string &key, const Value &value);

    // Merge struct into state
    Value Merge(const Value &state, const Value &to_merge);

    // Check if key exists
    bool Has(const Value &state, const string &key);

    // Remove key from state
    Value Remove(const Value &state, const string &key);
}
```

#### Usage in SQL

Once registered, your step is available:

```sql
-- Your custom step
SELECT do_run([
    do_set('url', 'https://api.example.com/users'),
    do_http_get(state.url, 'response'),     -- Your custom step!
    do_validate_json(state.response, 'user_schema'),
    do_queue_job('process_user', state.validated),
    do_send_email({
        to: state.validated.email,
        template: 'welcome',
        vars: {name: state.validated.name}
    }),
    do_return({job_id: state.job_id})
]);
```

### Rust Extension API

Rust steps use a procedural macro for ergonomic definition.

#### Basic Step

```rust
use donad::{do_step, State, StepResult, Value};

#[do_step(name = "send_email")]
fn send_email(
    to: String,
    template: String,
    vars: HashMap<String, Value>,
    state: &State,
) -> StepResult {
    // Do the side effect
    email_service::send(&to, &template, &vars)?;

    // Return unchanged state
    Ok(state.clone())
}
```

#### Step That Modifies State

```rust
#[do_step(name = "http_get")]
fn http_get(url: String, state_key: String, state: &State) -> StepResult {
    let response = reqwest::blocking::get(&url)?;
    let body = response.text()?;

    Ok(state.set(&state_key, Value::from(body)))
}
```

#### Step With Database Access

```rust
#[do_step(name = "fetch_user")]
fn fetch_user(user_id: i64, ctx: &ClientContext, state: &State) -> StepResult {
    // Execute query through DuckDB
    let result = ctx.execute_scalar(
        "SELECT * FROM users WHERE id = $1",
        &[Value::from(user_id)]
    )?;

    Ok(state.set("user", result))
}
```

#### Error Handling

```rust
#[do_step(name = "validate_schema")]
fn validate_schema(data: Value, schema: String, state: &State) -> StepResult {
    match json_schema::validate(&data, &schema) {
        Ok(validated) => Ok(state.set("validated", validated)),
        Err(e) => Err(DonadError::validation(format!("Schema validation failed: {}", e)))
    }
}
```

#### Registration

```rust
// lib.rs
use donad::DonadRegistry;

#[no_mangle]
pub extern "C" fn donad_register(registry: &mut DonadRegistry) {
    registry.register(send_email);
    registry.register(http_get);
    registry.register(fetch_user);
    registry.register(validate_schema);
}
```

#### Async Steps (Optional Feature)

```rust
#[do_step(name = "fetch_external", async)]
async fn fetch_external(url: String, state: &State) -> StepResult {
    let response = reqwest::get(&url).await?;
    let data = response.json::<serde_json::Value>().await?;

    Ok(state.set("external_data", Value::from(data)))
}
```

---

## Built-in Database Operations

These are the core steps for database interaction.

### do_insert

Insert rows and capture returning values.

```sql
-- Basic insert
do_insert('users', {name: 'Alice', email: 'alice@example.com'})

-- With RETURNING (adds to state)
do_insert('users',
    {name: 'Alice', email: 'alice@example.com'},
    returning := ['id', 'created_at']
)
-- State now has: {id: 123, created_at: '2024-01-15 10:30:00'}

-- Insert from state
do_insert('users', state.new_user, returning := ['id'])

-- Multiple rows
do_insert('audit_log', [
    {action: 'create', entity: 'user', entity_id: state.id},
    {action: 'notify', entity: 'user', entity_id: state.id}
])
```

### do_update

Update rows with conditions.

```sql
-- Basic update
do_update('users',
    set := {status: 'active', updated_at: now()},
    where := 'id = ' || state.user_id
)

-- With RETURNING
do_update('users',
    set := {login_count: 'login_count + 1'},
    where := {id: state.user_id},  -- struct = AND conditions
    returning := ['login_count']
)

-- Update with subquery condition
do_update('orders',
    set := {status: 'shipped'},
    where := 'user_id IN (SELECT id FROM users WHERE tier = ''premium'')',
    returning := ['id', 'user_id']
)

-- Bulk update with state array
do_update('inventory',
    set := {quantity: 0},
    where := 'product_id = ANY(' || state.sold_out_ids || ')'
)
```

### do_delete

Delete rows with conditions.

```sql
-- Basic delete
do_delete('sessions', where := {user_id: state.user_id})

-- Delete with RETURNING (for audit)
do_delete('temp_tokens',
    where := 'expires_at < now()',
    returning := ['id', 'user_id']
)
-- State: {deleted: [{id: 1, user_id: 10}, {id: 2, user_id: 11}]}

-- Soft delete pattern
do_update('users',
    set := {deleted_at: now(), deleted_by: state.admin_id},
    where := {id: state.target_user_id}
)
```

### do_query / do_select

Execute SELECT queries and store results.

```sql
-- Basic query
do_query(
    'SELECT * FROM users WHERE id = $1',
    params := [state.user_id],
    as := 'user'
)

-- Query returning multiple rows
do_query(
    'SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at DESC LIMIT 10',
    params := [state.user_id],
    as := 'recent_orders'
)

-- Aggregate query
do_query(
    'SELECT count(*) as total, sum(amount) as revenue FROM orders WHERE user_id = $1',
    params := [state.user_id],
    as := 'stats'
)

-- Dynamic query from state
do_query(
    state.custom_query,
    params := state.query_params,
    as := 'results'
)
```

### do_call

Call stored procedures or macros.

```sql
-- Call a macro
do_call('calculate_shipping', [state.order_id, state.destination], as := 'shipping')

-- Call with named params
do_call('process_payment',
    args := [state.amount],
    kwargs := {currency: 'USD', provider: state.payment_provider},
    as := 'payment_result'
)

-- Using func_apply integration
do_call(state.handler_name, [state.request], as := 'response')
```

### do_copy

Bulk data operations with COPY.

```sql
-- Export to file
do_copy(
    'SELECT * FROM orders WHERE created_at > $1',
    params := [state.export_since],
    to := '/tmp/orders_export.parquet',
    format := 'parquet'
)

-- Import from file
do_copy(
    from := state.import_file,
    to := 'staging_table',
    format := 'csv',
    header := true
)

-- Copy between tables
do_copy(
    'SELECT id, name, email FROM users WHERE tier = ''premium''',
    to := 'premium_users_backup'
)
```

### do_cte / do_with

Build and execute dynamic CTEs.

```sql
-- Simple CTE
do_with(
    ctes := [
        {name: 'active_users', query: 'SELECT * FROM users WHERE status = ''active'''},
        {name: 'recent_orders', query: 'SELECT * FROM orders WHERE created_at > now() - interval ''7 days'''}
    ],
    select := 'SELECT u.*, count(o.id) as order_count
               FROM active_users u
               LEFT JOIN recent_orders o ON o.user_id = u.id
               GROUP BY u.id',
    as := 'user_activity'
)

-- Dynamic CTE based on state
do_with(
    ctes := state.report_ctes,  -- [{name, query}, ...]
    select := state.final_select,
    as := 'report_data'
)

-- Recursive CTE
do_with(
    ctes := [{
        name: 'org_tree',
        query: '
            SELECT id, name, parent_id, 1 as level FROM orgs WHERE id = ' || state.root_id || '
            UNION ALL
            SELECT o.id, o.name, o.parent_id, t.level + 1
            FROM orgs o JOIN org_tree t ON o.parent_id = t.id
        ',
        recursive := true
    }],
    select := 'SELECT * FROM org_tree ORDER BY level, name',
    as := 'hierarchy'
)
```

### do_exec

Execute arbitrary SQL (escape hatch).

```sql
-- DDL operations
do_exec('CREATE TEMP TABLE processing AS SELECT * FROM staging WHERE batch_id = $1',
    params := [state.batch_id]
)

-- Complex multi-statement
do_exec('
    DROP TABLE IF EXISTS temp_results;
    CREATE TABLE temp_results AS SELECT * FROM compute_heavy_query($1);
    ANALYZE temp_results;
', params := [state.input_param])

-- Pragma/settings
do_exec('SET memory_limit = ''4GB''')
```

---

## Built-in Effect Registry

Donad maintains a registry of known effect types:

| Effect | Description |
|--------|-------------|
| `do_insert` | Insert row(s), capture RETURNING |
| `do_update` | Update rows, capture RETURNING |
| `do_delete` | Delete rows, capture RETURNING |
| `do_query` | Execute SELECT, store results |
| `do_call` | Call procedure/macro |
| `do_copy` | Bulk import/export |
| `do_with` | Dynamic CTE builder |
| `do_exec` | Raw SQL execution |
| `do_set` | Set state value |
| `do_map` | Transform state |
| `do_if` | Conditional execution |
| `do_assert` | Validate condition |
| `do_fail` | Abort with error |
| `do_return` | Early return with value |
| `do_effect` | Generic extensible effect |

## Transaction Semantics

By default, `do_run` wraps execution in a transaction:

```sql
-- Implicit transaction
SELECT do_run([...]);

-- Explicit control
SELECT do_run([...], transaction := false);

-- Nested transactions via savepoints
SELECT do_run([
    do_savepoint('sp1', [...]),
    do_savepoint('sp2', [...])
]);
```

## Composition

Chains can be composed:

```sql
-- Define reusable sub-chains
CREATE MACRO validate_user(email, password) AS (
    do_chain([
        do_assert(email IS NOT NULL, 'Email required'),
        do_assert(length(password) >= 8, 'Password too short')
    ])
);

CREATE MACRO audit_action(action) AS (
    do_chain([
        do_insert('audit_log', {
            action: action,
            user_id: state.user_id,
            timestamp: now()
        })
    ])
);

-- Compose them
CREATE MACRO register_user(email, password, name) AS (
    do_chain([
        do_embed(validate_user(email, password)),
        do_insert('users', {...}, returning := ['id']),
        do_set('user_id', state.id),
        do_embed(audit_action('user_registered')),
        do_return({success: true, id: state.id})
    ])
);
```

## Debugging & Tracing

```sql
-- Run with trace
SELECT do_run([...], trace := true);

-- Returns detailed execution trace
{
    "steps": [
        {"step": "do_set", "input_state": {}, "output_state": {"x": 1}, "duration_ms": 0.1},
        {"step": "do_insert", "input_state": {"x": 1}, "output_state": {"x": 1, "id": 123}, "duration_ms": 2.3},
        ...
    ],
    "final_state": {...},
    "total_duration_ms": 5.4
}
```

## Open Questions

1. **Async effects** - How to handle effects that don't complete immediately?
2. **Parallel steps** - `do_parallel([step1, step2])` for independent operations?
3. **Effect batching** - Combine multiple inserts into single statement?
4. **State schema** - Optional type definitions for state?
5. **Middleware** - Before/after hooks for all steps?

## Implementation Phases

1. **Phase 1:** Core execution (`do_run`), basic state ops (`set`, `map`, `get`)
2. **Phase 2:** Database ops (`insert`, `update`, `delete`, `query`)
3. **Phase 3:** Control flow (`if`, `assert`, `fail`, `return`)
4. **Phase 4:** Transactions, error handling (`try`/`catch`)
5. **Phase 5:** Custom step API (C++/Rust)
6. **Phase 6:** Tracing, debugging, optimization

## Related Work

- Haskell IO Monad / Effect systems
- Clojure's `doseq` and transducers
- Redux actions and reducers
- Event sourcing / Command pattern
- Elixir's Ecto.Multi
