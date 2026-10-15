# WHERE queries

`resolveConfiguredShape()` keeps the configured root table, selected columns and
primary-key columns, then passes the original WHERE text and positional parameters
to Neon Realtime. The server does not parse, rewrite or evaluate the expression.
Realtime decides which SQL it supports. No PostgreSQL parser dependency is needed.

Both JSON `params` and Electric's existing `params[1]` syntax are supported. Values
remain separate from SQL, are placed in positional order and preserve exact numeric
strings. Repeated positions, mixed encodings, non-scalar values and imprecise JSON
numbers fail explicitly. Query paths and query-string shape handles are unchanged.

The application's `authorize` callback still runs on every request. It must decide
which predicates and referenced tables the caller may access. A configured root
table and parameter binding do not authorize an arbitrary caller-supplied WHERE.
Trusted proxies can continue supplying the authorized definition.

## Live evidence

The credentialed suite tests OR with an IN-subquery, IN, correlated EXISTS and NOT
EXISTS against actual Postgres and Realtime. Each case checks:

- The initial baseline.
- Inserts, updates and deletes confined to a referenced membership table, including
  tenant/filter entry and exit without a root-row write.
- Overlapping root/dependency transactions with both writers active before commit.
- Independent Postgres results after the client's installed MVCC snapshot proves
  every test transaction visible. Test writers are quiescent during comparison.
- Same-path GET recovery through the production handler and real Durable Streams
  reader; every replacement shape goes through authorization again.

In the observed backend, each tested write invalidates these subquery subscriptions
and requests a replacement baseline. The adapter invalidates the old shape; the
client receives 409, creates a fresh query-string handle and installs the new
baseline. The checks prove correct materialized results through refetch. They do
not prove incremental dependency updates, identical historical operations, Electric
move events or tag metadata. Those remain separate phase 7 gates.

A permanent `upstream_rejected` SQL error is different from continuity loss. It
returns HTTP 400 without creating another owner, including after SSE closes. The
response omits backend diagnostics and SQL text. A real invalid-column query and
controlled SDK/handler tests cover this boundary.

Run the suite with the private environment loaded:

```sh
node --env-file=.env.local node_modules/vitest/vitest.mjs run --config vitest.staging.config.ts
```

Generated query events, wire evidence and checkpoints remain under ignored
`.artifacts/staging`. Staging endpoints, identifiers and credentials stay private.
