# Proposal: Pure Function Handlers for DuckDB httpserver

## Summary

Extend the httpserver extension with two new capabilities:
1. **Thread pool** - Handle concurrent requests using DuckDB's threading model
2. **Function handlers** - Route requests through user-defined pure functions instead of raw SQL execution

## Motivation

The current httpserver executes raw SQL from request bodies. This is powerful but limited:
- No structured request/response handling
- No routing capabilities
- No way to build REST APIs or web applications
- Security concerns with arbitrary SQL execution

By allowing a user-defined function as the request handler, we enable:
- **Pure functional architecture** - handlers are stateless transformations
- **Testability** - handlers can be unit tested without starting the server
- **Composability** - combine with other extensions (urlpattern, func_apply, etc.)
- **Security** - controlled execution through defined functions

## Proposed API

### Enhanced httpserve_start

```sql
SELECT httpserve_start(
    host := '0.0.0.0',
    port := 9999,
    auth := 'user:pass',
    handler := 'my_request_handler',  -- NEW: function/macro name (optional)
    threads := 4                       -- NEW: thread pool size (optional, default 1)
);
```

**Backwards Compatibility:** When `handler` is not specified, behavior remains unchanged (execute raw SQL from request body).

### Request Struct

The handler receives a struct containing all request data:

```sql
{
    method: VARCHAR,           -- 'GET', 'POST', 'PUT', 'DELETE', etc.
    path: VARCHAR,             -- '/users/123'
    query_string: VARCHAR,     -- 'format=json&limit=10'
    headers: MAP(VARCHAR, VARCHAR),  -- {'Content-Type': 'application/json', ...}
    body: VARCHAR              -- Raw request body
}
```

### Response Struct

The handler returns a struct defining the response:

```sql
{
    status: INTEGER,           -- HTTP status code (200, 404, 500, etc.)
    headers: MAP(VARCHAR, VARCHAR),  -- Response headers
    body: VARCHAR              -- Response body
}
```

### Handler Signature

```sql
handler(request STRUCT(...)) -> STRUCT(status INTEGER, headers MAP, body VARCHAR)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        httpserver                                │
│                                                                  │
│  ┌───────────┐      ┌────────────────────┐      ┌───────────┐   │
│  │  Receive  │      │      Handler       │      │   Send    │   │
│  │  (impure) │ ──▶  │      (PURE)        │ ──▶  │  (impure) │   │
│  │           │      │                    │      │           │   │
│  │ Parse HTTP│      │ request STRUCT     │      │ Format    │   │
│  │ into      │      │       ↓            │      │ STRUCT    │   │
│  │ STRUCT    │      │ response STRUCT    │      │ to HTTP   │   │
│  └───────────┘      └────────────────────┘      └───────────┘   │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Thread Pool                            │   │
│  │  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐          │   │
│  │  │Worker 1│  │Worker 2│  │Worker 3│  │Worker N│          │   │
│  │  │Context │  │Context │  │Context │  │Context │          │   │
│  │  └────────┘  └────────┘  └────────┘  └────────┘          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Benefits of Pure Handlers

| Aspect | Benefit |
|--------|---------|
| **Testing** | Call handler directly: `SELECT handler({...})` |
| **Debugging** | Replay any request, get deterministic result |
| **Caching** | Memoize responses based on request hash |
| **Composability** | Chain handlers, add middleware as function composition |
| **Parallelism** | No shared mutable state, trivially thread-safe |
| **Security** | No arbitrary SQL execution, controlled code paths |

## Example Usage

### Basic Handler

```sql
CREATE MACRO hello_handler(request) AS (
    {
        'status': 200,
        'headers': map(['Content-Type'], ['text/plain']),
        'body': 'Hello, ' || request.path
    }
);

SELECT httpserve_start('0.0.0.0', 9999, '', handler := 'hello_handler');
```

### REST API with Routing (using urlpattern + func_apply)

```sql
-- Route definitions
CREATE TABLE routes (
    method VARCHAR,
    pattern VARCHAR,
    handler VARCHAR
);

INSERT INTO routes VALUES
    ('GET',  '/users/:id', 'get_user'),
    ('GET',  '/users',     'list_users'),
    ('POST', '/users',     'create_user');

-- Individual handlers
CREATE MACRO get_user(params, query, body) AS (
    SELECT to_json(u) FROM users u WHERE u.id = CAST(params['id'] AS INTEGER)
);

CREATE MACRO list_users(params, query, body) AS (
    SELECT to_json(list(u)) FROM users u LIMIT COALESCE(CAST(query['limit'] AS INTEGER), 10)
);

-- Main router using func_apply for dynamic dispatch
CREATE MACRO api_handler(req) AS (
    WITH matched AS (
        SELECT handler, urlpattern_params(pattern, req.path) as params
        FROM routes
        WHERE method = req.method
          AND urlpattern_match(pattern, req.path)
        LIMIT 1
    )
    SELECT CASE
        WHEN handler IS NOT NULL THEN {
            'status': 200,
            'headers': map(['Content-Type'], ['application/json']),
            'body': apply(handler, params, parse_query_string(req.query_string), req.body)
        }
        ELSE {
            'status': 404,
            'headers': map(['Content-Type'], ['application/json']),
            'body': '{"error": "Not found"}'
        }
    END
    FROM matched
);

-- Start server
SELECT httpserve_start('0.0.0.0', 9999, '',
    handler := 'api_handler',
    threads := 8
);
```

### Testing Without Server

```sql
-- Unit test individual handlers
SELECT get_user({'id': '123'}, {}, '');

-- Integration test the router
SELECT api_handler({
    'method': 'GET',
    'path': '/users/123',
    'query_string': '',
    'headers': map([], []),
    'body': ''
});

-- Batch test from saved requests
SELECT request, api_handler(request) as response
FROM test_requests;
```

## Threading Model

### Option A: Connection Per Worker
Each worker thread maintains its own DuckDB connection/context.
- **Pros:** Complete isolation, simple model
- **Cons:** Memory overhead, connection setup time

### Option B: Shared Connection with Task Scheduler
Use DuckDB's existing TaskScheduler for parallel query execution.
- **Pros:** Efficient resource usage, already battle-tested
- **Cons:** May need careful handling for writes

### Option C: Connection Pool
Maintain a pool of connections, workers borrow as needed.
- **Pros:** Balance of isolation and efficiency
- **Cons:** More complex implementation

**Recommendation:** Start with Option A for simplicity, optimize later if needed.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Handler throws exception | Return 500 with error message in body |
| Handler returns NULL | Return 500 with "Handler returned NULL" |
| Handler not found | Return 500 at startup, fail to start server |
| Invalid response struct | Return 500 with validation error |

```sql
-- Error response format
{
    'status': 500,
    'headers': map(['Content-Type'], ['application/json']),
    'body': '{"error": "Handler exception", "message": "..."}'
}
```

## Configuration

### Environment Variables (existing)
- `DUCKDB_HTTPSERVER_FOREGROUND=1` - Run in foreground
- `DUCKDB_HTTPSERVER_DEBUG=1` - Enable debug logging

### New Environment Variables
- `DUCKDB_HTTPSERVER_THREADS=N` - Default thread count (override with parameter)
- `DUCKDB_HTTPSERVER_TIMEOUT=MS` - Request timeout in milliseconds

## Migration Path

1. **Phase 1:** Add `handler` parameter (optional), backwards compatible
2. **Phase 2:** Add `threads` parameter with default of 1
3. **Phase 3:** Optimize threading model based on real-world usage

## Open Questions

1. **Streaming responses** - Support for large responses without buffering entire body?
2. **WebSocket support** - Future consideration for real-time applications?
3. **Middleware pattern** - Built-in support, or leave to function composition?
4. **Request size limits** - Configurable max body size?
5. **Keep-alive connections** - Connection pooling on the HTTP side?

## Related Extensions

This proposal enables powerful combinations with:
- **[func_apply](../../../README.md)** - Dynamic function dispatch for routing
- **urlpattern** - URL pattern matching and parameter extraction
- **webbed** - HTML/XML parsing and rendering
- **duck_blocks** - Declarative document structure creation

Together, these form a complete web application framework in pure SQL.

## References

- [Current httpserver extension](https://github.com/Query-farm/duckdb-extension-httpserver)
- [DuckDB Community Extensions](https://duckdb.org/community_extensions/)
- [DuckDB Threading Model](https://duckdb.org/docs/guides/performance/how_to_tune_workloads)
