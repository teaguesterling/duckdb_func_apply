# duckdb_functools - Configuration-Based Security

## Simplified Security Model

### Security via Settings (Not Tables!)

Use DuckDB settings like `search_path` - simple comma-delimited strings.

```sql
-- Blacklist (default has dangerous functions)
SET functools_blacklist = 'install,load,system,export_database';

-- Whitelist (default empty - only used in whitelist mode)
SET functools_whitelist = 'upper,lower,concat,substr,list_transform';

-- Security mode
SET functools_security_mode = 'permissive';  -- Default
```

## Default Blacklist

**Default value:** `'install,load,system,export_database,copy'`

These are dangerous operations that should be **opt-in**:

| Function | Risk | Why Blacklisted |
|----------|------|-----------------|
| `system` | **CRITICAL** | Shell command execution |
| `install` | **CRITICAL** | Extension installation (code execution) |
| `load` | **CRITICAL** | Extension loading (code execution) |
| `export_database` | **HIGH** | Data exfiltration |
| `copy` | **HIGH** | File writes via COPY TO |

### Customizing the Blacklist

```sql
-- View current blacklist
SELECT current_setting('functools_blacklist');
-- 'install,load,system,export_database,copy'

-- Remove 'system' if you need it (opt-in)
SET functools_blacklist = 'install,load,export_database,copy';

-- Now system works
SELECT apply('system', 'echo hello');  -- ✅ Allowed

-- Add custom dangerous functions
SET functools_blacklist = 'install,load,system,export_database,copy,my_dangerous_func';

-- Empty blacklist (allow everything - use with caution!)
SET functools_blacklist = '';
```

## Security Modes

### Mode 1: Permissive (Default)

Allow everything **except blacklist**.

```sql
SET functools_security_mode = 'permissive';

-- Works (not blacklisted)
SELECT apply('upper', 'hello');  -- ✅
SELECT apply('read_csv', 'data.csv');  -- ✅
SELECT apply('custom_func', 'arg');  -- ✅

-- Blocked (in blacklist)
SELECT apply('system', 'rm -rf /');  -- ❌ Blacklisted
SELECT apply('install', 'httpfs');  -- ❌ Blacklisted
```

### Mode 2: Whitelist

Allow **only whitelisted** functions (blacklist still applies).

```sql
SET functools_security_mode = 'whitelist';
SET functools_whitelist = 'upper,lower,concat,substr,list_transform,list_filter';

-- Works (whitelisted)
SELECT apply('upper', 'hello');  -- ✅

-- Blocked (not whitelisted)
SELECT apply('read_csv', 'data.csv');  -- ❌ Not in whitelist
SELECT apply('custom_func', 'arg');  -- ❌ Not in whitelist

-- Blocked (blacklisted even if you add to whitelist)
SET functools_whitelist = 'upper,system';  -- Adding system to whitelist
SELECT apply('system', 'echo hi');  -- ❌ Still blocked by blacklist!
```

### Mode 3: Strict

Whitelist **AND** custom validator.

```sql
SET functools_security_mode = 'strict';
SET functools_whitelist = 'upper,lower,read_csv';
SET functools_validator = 'my_validator';

CREATE FUNCTION my_validator(func VARCHAR, args ANY[]) AS (
    -- Custom validation logic
    CASE WHEN func = 'read_csv'
         THEN args[1]::VARCHAR NOT LIKE '/etc/%'  -- No system files
         ELSE true
    END
)::BOOLEAN;

-- Must pass: whitelist + validator + not blacklisted
SELECT apply('upper', 'hello');  -- ✅ All checks pass
SELECT apply('read_csv', 'data.csv');  -- ✅ All checks pass
SELECT apply('read_csv', '/etc/passwd');  -- ❌ Fails validator
SELECT apply('substr', 'test');  -- ❌ Not whitelisted
```

## All Security Settings

```sql
-- Security mode
SET functools_security_mode = 'permissive'|'whitelist'|'strict';
-- Default: 'permissive'

-- Blacklist (comma-delimited)
SET functools_blacklist = 'install,load,system,export_database,copy';
-- Default: 'install,load,system,export_database,copy'

-- Whitelist (comma-delimited, only used in whitelist/strict modes)
SET functools_whitelist = 'upper,lower,concat,substr';
-- Default: '' (empty)

-- Custom validator function
SET functools_validator = 'my_validator_function';
-- Default: NULL (none)

-- Audit logging
SET functools_log_calls = true|false;
-- Default: false

SET functools_log_denied = true|false;
-- Default: false

-- Max recursion depth (prevent infinite loops)
SET functools_max_call_depth = 10;
-- Default: 10
```

## Security Functions

```sql
-- Check if function is allowed (considering all layers)
function_is_allowed(name VARCHAR) -> BOOLEAN

-- Check if function is blacklisted
function_is_blacklisted(name VARCHAR) -> BOOLEAN

-- Check if function is whitelisted (only relevant in whitelist mode)
function_is_whitelisted(name VARCHAR) -> BOOLEAN

-- Get complete security info
function_security_info(name VARCHAR) -> STRUCT(
    blacklisted BOOLEAN,
    whitelisted BOOLEAN,
    allowed BOOLEAN,
    reason VARCHAR
)
```

## Implementation

### Parse Comma-Delimited Settings

```cpp
static unordered_set<string> ParseCommaSeparated(const string &setting_value) {
    unordered_set<string> result;
    if (setting_value.empty()) {
        return result;
    }
    
    vector<string> parts = StringUtil::Split(setting_value, ',');
    for (auto &part : parts) {
        auto trimmed = StringUtil::Trim(part);
        if (!trimmed.empty()) {
            result.insert(StringUtil::Lower(trimmed));
        }
    }
    return result;
}
```

### Check Function Security

```cpp
bool CheckFunctionSecurity(
    ClientContext &context,
    const string &func_name,
    const vector<Value> &args
) {
    auto func_lower = StringUtil::Lower(func_name);
    
    // Get settings
    auto blacklist_str = GetSetting(context, "functools_blacklist");
    auto whitelist_str = GetSetting(context, "functools_whitelist");
    auto security_mode = GetSetting(context, "functools_security_mode");
    auto validator_name = GetSetting(context, "functools_validator");
    
    // Parse blacklist
    auto blacklist = ParseCommaSeparated(blacklist_str);
    
    // Layer 1: Blacklist check (always enforced)
    if (blacklist.count(func_lower)) {
        throw PermissionException(
            "Function '%s' is blacklisted. "
            "To use this function, remove it from functools_blacklist setting.",
            func_name
        );
    }
    
    // Layer 2: Whitelist check (if in whitelist or strict mode)
    if (security_mode == "whitelist" || security_mode == "strict") {
        auto whitelist = ParseCommaSeparated(whitelist_str);
        
        if (!whitelist.count(func_lower)) {
            throw PermissionException(
                "Function '%s' is not in whitelist. "
                "Add to functools_whitelist setting or use permissive mode.",
                func_name
            );
        }
    }
    
    // Layer 3: Custom validator (if in strict mode)
    if (security_mode == "strict" && !validator_name.empty()) {
        bool is_valid = CallValidatorFunction(context, validator_name, func_name, args);
        
        if (!is_valid) {
            throw PermissionException(
                "Function '%s' failed custom validation.",
                func_name
            );
        }
    }
    
    return true;
}
```

### Default Settings Initialization

```cpp
void FuncToolsExtension::Load(DuckDB &db) {
    // Set default security settings
    db.instance->config.SetDefault(
        "functools_blacklist",
        "install,load,system,export_database,copy"
    );
    
    db.instance->config.SetDefault(
        "functools_whitelist",
        ""  // Empty by default
    );
    
    db.instance->config.SetDefault(
        "functools_security_mode",
        "permissive"
    );
    
    db.instance->config.SetDefault(
        "functools_validator",
        ""  // No validator by default
    );
    
    // ... register functions
}
```

## Usage Examples

### Example 1: Development (Allow Everything)

```sql
-- Disable blacklist for testing
SET functools_blacklist = '';

-- Now even dangerous functions work
SELECT apply('system', 'ls -la');  -- ✅ Allowed
SELECT apply('install', 'httpfs');  -- ✅ Allowed
```

### Example 2: Production (Strict Whitelist)

```sql
-- Only allow safe string and list functions
SET functools_security_mode = 'whitelist';
SET functools_whitelist = 'upper,lower,trim,concat,substr,list_transform,list_filter,list_reduce';

-- Safe functions work
SELECT apply('upper', 'hello');  -- ✅

-- Everything else blocked
SELECT apply('system', 'echo hi');  -- ❌ Blacklisted
SELECT apply('read_csv', 'data.csv');  -- ❌ Not whitelisted
SELECT apply('my_custom_func', 'arg');  -- ❌ Not whitelisted
```

### Example 3: Allow read_csv but not system

```sql
-- Default blacklist (includes system)
-- Default mode (permissive)

SELECT apply('read_csv', 'data.csv');  -- ✅ Not blacklisted
SELECT apply('system', 'echo hi');  -- ❌ Blacklisted
```

### Example 4: Need system for legitimate use

```sql
-- Remove system from blacklist (opt-in)
SET functools_blacklist = 'install,load,export_database,copy';

-- Now system works
SELECT apply('system', 'echo "Processing complete"');  -- ✅
```

### Example 5: Multi-tenant with custom validation

```sql
SET functools_security_mode = 'strict';
SET functools_whitelist = 'upper,lower,read_csv,write_csv';
SET functools_validator = 'validate_tenant_access';

CREATE FUNCTION validate_tenant_access(func VARCHAR, args ANY[]) AS (
    CASE 
        WHEN func IN ('read_csv', 'write_csv')
            THEN args[1]::VARCHAR LIKE '/tenant_' || current_setting('app.tenant_id') || '/%'
        ELSE true
    END
)::BOOLEAN;

-- Set tenant context
SET app.tenant_id = 'acme';

-- Allowed (path matches tenant)
SELECT apply('read_csv', '/tenant_acme/data.csv');  -- ✅

-- Blocked (wrong tenant path)
SELECT apply('read_csv', '/tenant_other/data.csv');  -- ❌ Fails validator
```

## Introspection

```sql
-- View current security settings
SELECT 
    current_setting('functools_security_mode') as mode,
    current_setting('functools_blacklist') as blacklist,
    current_setting('functools_whitelist') as whitelist,
    current_setting('functools_validator') as validator;

-- Check specific function
SELECT function_security_info('system');
-- {blacklisted: true, whitelisted: false, allowed: false, 
--  reason: 'Function is blacklisted'}

SELECT function_security_info('upper');
-- {blacklisted: false, whitelisted: false, allowed: true,
--  reason: 'Allowed in permissive mode'}

-- List all blacklisted functions
SELECT unnest(string_split(current_setting('functools_blacklist'), ',')) as func
ORDER BY func;

-- List all whitelisted functions
SELECT unnest(string_split(current_setting('functools_whitelist'), ',')) as func
ORDER BY func;
```

## Migration Guide

### From Table-Based to Setting-Based

**Old approach (don't use):**
```sql
CREATE TABLE functools_whitelist (function_name VARCHAR);
INSERT INTO functools_whitelist VALUES ('upper'), ('lower');
```

**New approach:**
```sql
SET functools_whitelist = 'upper,lower';
```

**Benefits:**
- ✅ Simpler - no tables to manage
- ✅ Faster - settings are cached
- ✅ Standard DuckDB pattern (like search_path)
- ✅ Easy to export/import configurations
- ✅ Works across sessions with config file

## Configuration File

Save settings in `.duckdbrc` or pass via CLI:

```sql
-- .duckdbrc
SET functools_security_mode = 'whitelist';
SET functools_whitelist = 'upper,lower,concat,substr,list_transform';
SET functools_blacklist = 'install,load,system,export_database,copy';
```

Or via connection string:
```python
con = duckdb.connect(config={
    'functools_security_mode': 'whitelist',
    'functools_whitelist': 'upper,lower,concat',
    'functools_blacklist': 'install,load,system'
})
```

## Audit Log (Still a Table)

```sql
-- Audit log remains a table (makes sense for historical data)
CREATE TABLE functools_audit_log (
    timestamp TIMESTAMP DEFAULT NOW(),
    user_id VARCHAR,
    function_name VARCHAR,
    args JSON,
    success BOOLEAN,
    error_message VARCHAR,
    execution_time_ms DOUBLE
);

-- Enable logging
SET functools_log_calls = true;

-- Query recent activity
SELECT * FROM functools_audit_log 
ORDER BY timestamp DESC 
LIMIT 100;
```

## Summary

**Settings-based security:**
- ✅ `functools_blacklist` - Comma-delimited dangerous functions
- ✅ `functools_whitelist` - Comma-delimited allowed functions (whitelist mode)
- ✅ `functools_security_mode` - permissive|whitelist|strict
- ✅ `functools_validator` - Custom validation function
- ✅ Simple, standard DuckDB pattern
- ✅ Easy to configure and introspect

**Default blacklist includes:**
- ✅ `system` - Shell execution (CRITICAL)
- ✅ `install` - Extension installation
- ✅ `load` - Extension loading
- ✅ `export_database` - Data export
- ✅ `copy` - File writes

**Dangerous functions are opt-in** - remove from blacklist to use.

This is much cleaner! 🎉
