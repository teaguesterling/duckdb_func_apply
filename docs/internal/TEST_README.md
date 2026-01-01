# duckdb_functools Test Suite

Comprehensive test suite for the duckdb_functools extension.

## Test Files

### Core Functions

1. **test_apply.test** - `apply()` variadic function
   - Basic single and multiple argument calls
   - String, numeric, list, and date functions
   - Null handling
   - Batch processing (vectorization)
   - Error cases (function not found, wrong args, wrong types)
   - Coverage: ~50 test cases

2. **test_apply_with.test** - `apply_with()` structured function
   - Args as arrays
   - Kwargs as structs, maps, and JSON
   - Combined positional + keyword arguments
   - Partial descriptor handling
   - JSON input/output
   - Batch processing with configuration
   - Error cases (invalid JSON, wrong types)
   - Coverage: ~60 test cases

3. **test_partial.test** - `partial()` descriptor creation
   - Basic partial with fixed args
   - Partial with fixed kwargs
   - Partial with both args and kwargs
   - Storing partials in tables
   - Composing partials
   - Middleware patterns
   - Data transformation patterns
   - Error cases (invalid inputs)
   - Coverage: ~40 test cases

4. **test_function_exists.test** - `function_exists()` introspection
   - Built-in functions (scalar, aggregate, list, date, math)
   - User-defined functions and macros
   - Case sensitivity
   - Validation before dynamic calls
   - Batch validation
   - Error cases (null, empty string)
   - Coverage: ~35 test cases

### Advanced Features

5. **test_chaining.test** - Function chaining
   - Basic chaining with `apply()`
   - Chaining with `apply_with()`
   - Nested function calls
   - Chaining with partial descriptors
   - Chaining in queries (WHERE, CTEs, subqueries)
   - Mixed native DuckDB + apply chaining
   - Coverage: ~45 test cases

6. **test_patterns.test** - Integration with DuckDB features
   - Map using `list_transform` + lambda + apply
   - Reduce using `list_reduce` + lambda + apply
   - Filter using `list_filter` + lambda + apply
   - List comprehensions with apply
   - Partial application in pipelines
   - Data-driven transformations
   - Middleware composition
   - Function composition
   - Map-reduce patterns
   - Coverage: ~50 test cases

### Real-World Use Cases

7. **test_url_routing.test** - URL routing system
   - Basic routing with pattern matching
   - Routing with partial descriptors
   - Middleware chains
   - Batch routing (multiple URLs)
   - Dynamic routing configuration
   - Method-based routing (GET, POST, DELETE)
   - Coverage: ~30 test cases

### Edge Cases & Robustness

8. **test_edge_cases.test** - Edge cases and error handling
   - Null handling (function names, arguments)
   - Empty inputs (empty strings, arrays)
   - Type mismatches
   - Special characters (Unicode, newlines, quotes)
   - Large inputs (long strings, many arguments)
   - Function name variations (case, operators)
   - Overloaded functions
   - Recursive/nested calls
   - Invalid JSON
   - Partial edge cases
   - Batch edge cases
   - Transaction isolation
   - Coverage: ~70 test cases

## Running Tests

### Run All Tests

```bash
# Using DuckDB test runner
duckdb_test test/sql/functools/*.test

# Or individually
duckdb_test test/sql/functools/apply.test
duckdb_test test/sql/functools/apply_with.test
duckdb_test test/sql/functools/partial.test
# ... etc
```

### Run Specific Test Category

```bash
# Core functions only
duckdb_test test/sql/functools/test_apply*.test test/sql/functools/test_partial.test

# Patterns and integration
duckdb_test test/sql/functools/test_patterns.test test/sql/functools/test_chaining.test

# Real-world use cases
duckdb_test test/sql/functools/test_url_routing.test

# Edge cases
duckdb_test test/sql/functools/test_edge_cases.test
```

### Interactive Testing

```sql
-- Load extension
LOAD 'functools';

-- Quick smoke test
SELECT apply('upper', 'hello');
-- Expected: HELLO

SELECT apply_with('substr', 
    args := ['hello world'],
    kwargs := {'start': 7, 'length': 5}
);
-- Expected: world

SELECT apply_with(
    partial('concat', ['https://']),
    args := ['example.com']
);
-- Expected: https://example.com

SELECT function_exists('upper');
-- Expected: true
```

## Test Coverage Summary

| Component | Test File | Test Cases | Coverage |
|-----------|-----------|------------|----------|
| apply() | test_apply.test | ~50 | Core functionality, errors |
| apply_with() | test_apply_with.test | ~60 | Args/kwargs, JSON, partials |
| partial() | test_partial.test | ~40 | Descriptors, composition |
| function_exists() | test_function_exists.test | ~35 | Validation, introspection |
| Chaining | test_chaining.test | ~45 | Method chaining, nesting |
| Patterns | test_patterns.test | ~50 | DuckDB integration |
| URL Routing | test_url_routing.test | ~30 | Real-world use case |
| Edge Cases | test_edge_cases.test | ~70 | Robustness, errors |
| **Total** | **8 files** | **~380** | **Comprehensive** |

## Test Organization

```
test/sql/functools/
├── test_apply.test              # Core: apply() function
├── test_apply_with.test         # Core: apply_with() function
├── test_partial.test            # Core: partial() function
├── test_function_exists.test    # Core: function_exists() helper
├── test_chaining.test           # Advanced: function chaining
├── test_patterns.test           # Advanced: DuckDB integration patterns
├── test_url_routing.test        # Use case: URL routing system
└── test_edge_cases.test         # Robustness: edge cases and errors
```

## Expected Behavior

### Success Cases
- All valid function calls execute correctly
- Vectorization works (batch processing)
- Null values are handled gracefully
- Type coercion works as expected
- Partial descriptors compose properly
- Function chaining works with dot notation

### Error Cases
- Invalid function names → clear error messages
- Wrong argument count → "No function matches"
- Type mismatches → "Conversion Error"
- Null function name → "function name cannot be NULL"
- Invalid JSON → "Invalid JSON"
- Malformed partials → "Invalid partial descriptor"

## Performance Expectations

- **Vectorization**: All functions should process batches efficiently
- **No O(n²)**: List operations should be linear
- **Caching**: Function lookups should be cached per batch
- **Memory**: Large inputs (10K+ rows) should not cause issues

## Continuous Integration

### Required Checks
- [ ] All tests pass
- [ ] No memory leaks (valgrind)
- [ ] No undefined behavior (sanitizers)
- [ ] Coverage > 90%

### Optional Checks
- [ ] Performance benchmarks
- [ ] Fuzzing tests
- [ ] Stress tests (1M+ rows)

## Adding New Tests

When adding functionality, create tests for:

1. **Happy path** - Basic usage works
2. **Edge cases** - Empty, null, large inputs
3. **Error cases** - Invalid inputs, wrong types
4. **Integration** - Works with DuckDB features
5. **Performance** - Handles large batches

### Test Template

```sql
# test/sql/functools/test_new_feature.test
# Tests for new_feature

require functools

statement ok
LOAD 'functools';

# Happy path
query T
SELECT new_feature('input');
----
expected_output

# Edge case: null
query T
SELECT new_feature(NULL);
----
NULL

# Error case
statement error
SELECT new_feature('invalid');
----
Error message pattern
```

## Known Limitations

1. **Named parameters** cannot be passed through `apply()` directly
   - Use `apply_with()` with kwargs instead
2. **Table functions** not yet supported
   - Only scalar functions work currently
3. **Aggregate functions** work but context is limited
   - Use `list_aggregate` for list-based aggregation

## Future Test Coverage

- [ ] Table function support (apply_table)
- [ ] Aggregate function edge cases
- [ ] Window function integration
- [ ] Parallel execution tests
- [ ] Memory profiling
- [ ] Fuzzing with random inputs
- [ ] Integration with other extensions

## Contributing

When contributing tests:

1. Follow existing test file structure
2. Use descriptive test names
3. Include both positive and negative cases
4. Add comments explaining non-obvious tests
5. Update this README with new test files

## Questions?

- Check existing tests for examples
- See `PATTERNS.md` for usage patterns
- Read `IMPLEMENTATION_SPEC.md` for design details
