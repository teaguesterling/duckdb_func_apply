# Table Functions and Security for duckdb_functools

## Part 1: Table-Valued Function Application

### Naming Convention

**Recommendation: `apply_table` and `apply_table_with`**

Why?
- Consistent with `apply` / `apply_with` naming
- DuckDB precedent: `list_value`, `list_filter` (prefix pattern)
- Reads naturally: "apply this table function"

### Function Signatures

```sql
-- 1. Variadic table function application
apply_table(func VARCHAR, ...args ANY) -> TABLE

-- 2. Structured table function application  
apply_table_with(
    func VARCHAR,
    args ANY[] | JSON DEFAULT NULL,
    kwargs STRUCT | MAP | JSON DEFAULT NULL
) -> TABLE
```

### Usage Examples

```sql
-- Basic table function
SELECT * FROM apply_table('read_csv', 'data.csv');

-- With options
SELECT * FROM apply_table('read_parquet', 'data.parquet');

-- Using apply_table_with with kwargs
SELECT * FROM apply_table_with('read_csv',
    args := ['data.csv'],
    kwargs := {'header': true, 'delimiter': ','}
);

-- Dynamic data source loading
CREATE TABLE data_sources (
    name VARCHAR,
    loader VARCHAR,
    path VARCHAR,
    options JSON
);

INSERT INTO data_sources VALUES
    ('users', 'read_csv', 'users.csv', '{"header": true}'),
    ('posts', 'read_parquet', 'posts.parquet', NULL);

-- Load dynamically with LATERAL
SELECT ds.name, t.*
FROM data_sources ds
LATERAL (
    SELECT * FROM apply_table_with(
        ds.loader, 
        args := [ds.path], 
        kwargs := ds.options::JSON
    )
) t;
```

---

## Part 2: Security Model

### Three-Layer Security

#### Layer 1: Built-in Blacklist (Always Active)

Hard-coded dangerous functions never allowed:

```sql
-- These always fail, regardless of settings
SELECT apply('install', 'extension');  -- ❌ Blacklisted
SELECT apply('export_database', 'path');  -- ❌ Blacklisted
```

**Default blacklist:**
- `install`, `load` - Extension loading (code execution)
- `export_database`, `copy` (for COPY TO) - Data exfiltration
- Network functions if HTTP extension loaded

#### Layer 2: Whitelist Mode (Optional)

```sql
-- Enable whitelist mode
SET functools_security_mode = 'whitelist';

-- Create whitelist table
CREATE TABLE functools_whitelist (
    function_name VARCHAR PRIMARY KEY
);

INSERT INTO functools_whitelist VALUES
    ('upper'), ('lower'), ('concat'), ('substr'),
    ('list_transform'), ('list_filter'),
    ('json_extract');

-- Only whitelisted functions work
SELECT apply('upper', 'hello');  -- ✅ Allowed
SELECT apply('custom_func', 'test');  -- ❌ Not whitelisted
```

#### Layer 3: Custom Validator (Advanced)

```sql
-- Set custom validation function
SET functools_validator = 'my_security_check';

CREATE FUNCTION my_security_check(
    func_name VARCHAR,
    args ANY[]
) RETURNS BOOLEAN AS (
    CASE
        WHEN func_name = 'read_csv' 
            THEN args[1]::VARCHAR NOT LIKE '/etc/%'  -- Block system files
        WHEN func_name LIKE 'list_%'
            THEN true
        ELSE false
    END
);
```

### Security Settings

```sql
-- Security modes
SET functools_security_mode = 'permissive';  -- Default: allow all except blacklist
SET functools_security_mode = 'whitelist';   -- Only whitelisted functions
SET functools_security_mode = 'strict';      -- Whitelist + validator required

-- Custom validator
SET functools_validator = 'my_validator';

-- Audit logging
SET functools_log_calls = true;
SET functools_log_denied = true;
```

### Security Tables

```sql
-- Whitelist (user-managed)
functools_whitelist (
    function_name VARCHAR PRIMARY KEY,
    notes VARCHAR
)

-- Blacklist (read-only, built-in)
functools_blacklist (
    function_name VARCHAR PRIMARY KEY,
    reason VARCHAR,
    severity VARCHAR  -- 'HIGH', 'MEDIUM', 'LOW'
)

-- Audit log
functools_audit_log (
    timestamp TIMESTAMP,
    user_id VARCHAR,
    function_name VARCHAR,
    args JSON,
    success BOOLEAN,
    error_message VARCHAR,
    execution_time_ms DOUBLE
)
```

### Example Configurations

**Development (Permissive):**
```sql
SET functools_security_mode = 'permissive';
-- Allows everything except blacklisted functions
```

**Production (Whitelist):**
```sql
SET functools_security_mode = 'whitelist';

CREATE TABLE functools_whitelist AS
SELECT unnest([
    'upper', 'lower', 'trim', 'concat',
    'list_transform', 'list_reduce'
]) as function_name;
```

**Multi-tenant (Strict):**
```sql
SET functools_security_mode = 'strict';
SET functools_validator = 'check_tenant_permissions';

CREATE FUNCTION check_tenant_permissions(
    func_name VARCHAR,
    args ANY[]
) RETURNS BOOLEAN AS (
    -- Custom tenant isolation logic
    true  -- Or check permissions table
);
```

### Security Introspection Functions

```sql
-- Check if function is allowed
function_is_allowed(name VARCHAR) -> BOOLEAN

-- Check if function is blacklisted
function_is_blacklisted(name VARCHAR) -> BOOLEAN

-- Get security info
function_security_info(name VARCHAR) -> STRUCT(
    allowed BOOLEAN,
    blacklisted BOOLEAN,
    whitelisted BOOLEAN,
    reason VARCHAR
)
```

### Usage Examples

```sql
-- Check before calling
SELECT CASE
    WHEN function_is_allowed('read_csv') 
        THEN apply('read_csv', 'data.csv')
    ELSE 'Function not allowed'
END;

-- Get security details
SELECT function_security_info('install');
-- {allowed: false, blacklisted: true, reason: 'Code execution risk'}

-- Query whitelist
SELECT * FROM functools_whitelist ORDER BY function_name;

-- Query blacklist (read-only)
SELECT * FROM functools_blacklist;
```

### Audit Log Queries

```sql
-- Recent calls
SELECT * FROM functools_audit_log 
ORDER BY timestamp DESC 
LIMIT 100;

-- Most used functions
SELECT 
    function_name,
    COUNT(*) as call_count,
    AVG(execution_time_ms) as avg_time_ms
FROM functools_audit_log
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY function_name
ORDER BY call_count DESC;

-- Failed calls (security violations)
SELECT * FROM functools_audit_log
WHERE NOT success
  AND error_message LIKE '%not in whitelist%'
ORDER BY timestamp DESC;
```

## Complete API Summary

### Core Functions (6)
```sql
1. apply(func VARCHAR, ...args ANY) -> ANY
2. apply_with(func VARCHAR|STRUCT, args, kwargs) -> ANY
3. apply_table(func VARCHAR, ...args ANY) -> TABLE
4. apply_table_with(func VARCHAR, args, kwargs) -> TABLE
5. partial(func VARCHAR, args, kwargs) -> STRUCT
6. function_exists(name VARCHAR) -> BOOLEAN
```

### Security Functions (3)
```sql
7. function_is_allowed(name VARCHAR) -> BOOLEAN
8. function_is_blacklisted(name VARCHAR) -> BOOLEAN
9. function_security_info(name VARCHAR) -> STRUCT
```

### Settings (5)
```sql
SET functools_security_mode = 'permissive'|'whitelist'|'strict';
SET functools_validator = 'function_name';
SET functools_log_calls = true|false;
SET functools_log_denied = true|false;
SET functools_max_call_depth = 10;  -- Prevent infinite recursion
```

### Security Tables (3)
```sql
functools_whitelist     -- User-managed allowed functions
functools_blacklist     -- Read-only dangerous functions
functools_audit_log     -- Call history and violations
```

## Implementation Priority

### Phase 1 (MVP): Core + Basic Security
- ✅ `apply`, `apply_with`
- ✅ `partial`
- ✅ `function_exists`
- ✅ Built-in blacklist (Layer 1)
- ✅ Permissive mode

### Phase 2: Table Functions
- ✅ `apply_table`, `apply_table_with`
- ✅ STRUCT return type for schema flexibility

### Phase 3: Advanced Security
- ✅ Whitelist mode (Layer 2)
- ✅ `function_is_allowed`, `function_is_blacklisted`
- ✅ Audit logging

### Phase 4: Custom Validation
- ✅ Custom validator support (Layer 3)
- ✅ `function_security_info`
- ✅ Advanced audit queries

**Total: ~800 lines of C++ across all phases**
