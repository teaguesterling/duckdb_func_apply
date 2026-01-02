# Examples

Common usage patterns and recipes for the FuncApply extension.

## Data-Driven Transformations

### Column Transformations from Config

Store transformation rules in a table and apply them dynamically:

```sql
-- Configuration table
CREATE TABLE column_transforms (
    table_name VARCHAR,
    column_name VARCHAR,
    transform_func VARCHAR
);

INSERT INTO column_transforms VALUES
    ('users', 'email', 'lower'),
    ('users', 'name', 'trim'),
    ('users', 'phone', 'regexp_replace');

-- Apply transformations
SELECT
    column_name,
    apply(transform_func, raw_value) as clean_value
FROM raw_data r
JOIN column_transforms t
    ON r.source_table = t.table_name
    AND r.source_column = t.column_name;
```

### Dynamic Data Cleaning Pipeline

```sql
-- Define cleaning steps
CREATE TABLE cleaning_steps (
    step_order INT,
    func_name VARCHAR,
    description VARCHAR
);

INSERT INTO cleaning_steps VALUES
    (1, 'trim', 'Remove whitespace'),
    (2, 'lower', 'Lowercase'),
    (3, 'regexp_replace', 'Remove special chars');

-- Apply in sequence (using a recursive approach or multiple joins)
WITH step1 AS (
    SELECT id, apply('trim', value) as value FROM raw_data
),
step2 AS (
    SELECT id, apply('lower', value) as value FROM step1
)
SELECT * FROM step2;
```

## Dynamic Function Selection

### Type-Based Function Selection

Choose functions based on data type:

```sql
SELECT
    column_name,
    typeof(value) as data_type,
    apply(
        CASE typeof(value)
            WHEN 'VARCHAR' THEN 'length'
            WHEN 'INTEGER' THEN 'abs'
            WHEN 'DOUBLE' THEN 'round'
            ELSE 'typeof'
        END,
        value
    ) as result
FROM mixed_data;
```

### Conditional Transformations

```sql
SELECT
    id,
    apply(
        CASE
            WHEN needs_uppercase THEN 'upper'
            WHEN needs_lowercase THEN 'lower'
            ELSE 'trim'
        END,
        text_value
    ) as processed
FROM documents;
```

## Validation Patterns

### Pre-Flight Function Check

Validate functions before batch processing:

```sql
-- Check all required functions exist
SELECT
    func_name,
    function_exists(func_name) as available
FROM required_functions
WHERE NOT function_exists(func_name);

-- If any rows returned, abort the process
```

### Safe Function Execution

```sql
SELECT
    id,
    CASE
        WHEN function_exists(transform_func)
        THEN apply(transform_func, value)
        ELSE value  -- Keep original if function missing
    END as result
FROM data_with_transforms;
```

### Function Availability Report

```sql
SELECT
    category,
    func_name,
    function_exists(func_name) as supported,
    CASE
        WHEN function_exists(func_name)
        THEN apply(func_name, 'test')
        ELSE 'N/A'
    END as test_result
FROM (VALUES
    ('string', 'upper'),
    ('string', 'lower'),
    ('string', 'reverse'),
    ('math', 'abs'),
    ('math', 'sqrt'),
    ('custom', 'my_transform')
) AS t(category, func_name);
```

## Working with Macros

### Using Built-in List Macros

```sql
-- List aggregation
SELECT apply('list_sum', [1, 2, 3, 4, 5]);
-- Result: 15

SELECT apply('list_avg', [10, 20, 30]);
-- Result: 20.0

-- List manipulation
SELECT apply('list_reverse', ['a', 'b', 'c']);
-- Result: [c, b, a]

SELECT apply('list_sort', [3, 1, 4, 1, 5]);
-- Result: [1, 1, 3, 4, 5]
```

### Custom Macros

```sql
-- Define custom macros
CREATE MACRO double_it(x) AS x * 2;
CREATE MACRO greet(name) AS 'Hello, ' || name || '!';

-- Use with apply
SELECT apply('double_it', 21);
-- Result: 42

SELECT apply('greet', 'World');
-- Result: Hello, World!
```

## Vectorized Processing

### Batch Transformations

```sql
-- Process entire columns efficiently
SELECT
    original,
    apply('upper', original) as upper_case,
    apply('length', original) as len,
    apply('reverse', original) as reversed
FROM (VALUES
    ('hello'),
    ('world'),
    ('duckdb')
) AS t(original);
```

### Dynamic Column Processing

```sql
-- Different function per row
SELECT
    func_name,
    input_value,
    apply(func_name, input_value) as output
FROM (VALUES
    ('upper', 'hello'),
    ('lower', 'WORLD'),
    ('reverse', 'abc'),
    ('trim', '  spaces  ')
) AS t(func_name, input_value);
```

## Named Parameters

### Using Named Parameters with apply()

```sql
-- Substring with named params
SELECT apply('substr', 'hello world', start := 7, length := 5);
-- Result: world

-- Padding with named params
SELECT apply('lpad', 'hi', 10, padchar := '-');
-- Result: --------hi

-- Mixed positional and named
SELECT apply('substr', 'abcdefgh', 3, length := 4);
-- Result: cdef
```

## Integration Patterns

### With CTEs

```sql
WITH
    transformations AS (
        SELECT 'upper' as func UNION ALL
        SELECT 'lower' UNION ALL
        SELECT 'reverse'
    ),
    data AS (
        SELECT 'Hello World' as text
    )
SELECT
    t.func,
    apply(t.func, d.text) as result
FROM data d
CROSS JOIN transformations t;
```

### With Window Functions

```sql
SELECT
    id,
    value,
    apply('upper', value) as upper_value,
    apply('length', value) as len,
    SUM(apply('length', value)) OVER (ORDER BY id) as cumulative_len
FROM strings_table;
```

### With Aggregations

```sql
SELECT
    category,
    COUNT(*) as count,
    MAX(apply('length', name)) as max_name_length
FROM products
GROUP BY category;
```

## Table Functions

### Dynamic Row Generation

```sql
-- Generate a sequence dynamically
SELECT * FROM apply_table('range', 10);
-- Returns: 0, 1, 2, ..., 9

-- Generate series with step
SELECT * FROM apply_table('generate_series', 0, 100, 10);
-- Returns: 0, 10, 20, ..., 100
```

### Data Expansion with Cross Join

```sql
-- Repeat each row a variable number of times
CREATE TABLE items (name VARCHAR, repeat_count INT);
INSERT INTO items VALUES ('A', 2), ('B', 3), ('C', 1);

SELECT i.name, r.range as instance
FROM items i
CROSS JOIN apply_table('range', i.repeat_count) r;
-- Returns:
-- A, 0
-- A, 1
-- B, 0
-- B, 1
-- B, 2
-- C, 0
```

### Using apply_table_with for Structured Calls

```sql
-- With args list
SELECT * FROM apply_table_with('range', args := [5]);

-- With kwargs struct
SELECT * FROM apply_table_with('generate_series',
    args := [1],
    kwargs := {stop: 10, step: 2}
);
```

### Dynamic Table Function Selection

```sql
-- Choose table function based on condition
SELECT *
FROM (
    SELECT CASE
        WHEN use_range THEN 'range'
        ELSE 'generate_series'
    END as func_name,
    10 as arg1
    FROM config
) params
CROSS JOIN LATERAL apply_table(params.func_name, params.arg1);
```

## Error Handling

### Graceful Degradation

```sql
SELECT
    id,
    func_name,
    CASE
        WHEN func_name IS NULL THEN 'No function specified'
        WHEN NOT function_exists(func_name) THEN 'Function not found: ' || func_name
        ELSE TRY_CAST(apply(func_name, value) AS VARCHAR)
    END as result
FROM operations;
```

### Logging Invalid Operations

```sql
-- Find invalid function references
SELECT DISTINCT func_name
FROM config_table
WHERE NOT function_exists(func_name);
```
