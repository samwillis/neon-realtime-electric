# Neon Realtime Electric client

A TypeScript monorepo implementing the Electric client API over Durable Streams
and Neon Realtime. Implementation is in progress; see
[compatibility status](./docs/compatibility.md) for verified phase gates.

The [implementation plan](./implementation-and-testing-plan.md) defines phases
0–7. The [design](./durable-streams-shape-design.md) preserves application shape
URLs and query-string handles. The Function runs locally with `neon dev` against
Sam's staging Postgres/Realtime; deployed affinity tests remain deferred.

Development uses pinned pnpm 10.30.3. Client checks run on Node 22.12 or later;
the Function targets Node 24.

```sh
pnpm install --frozen-lockfile
pnpm check:upstream
pnpm build
pnpm test:client
pnpm test:types
pnpm test:package
pnpm check:api
```

The copied Electric implementation and its upstream license live in
`packages/client`. Reproduction details and local modifications are recorded in
`packages/client/UPSTREAM.md`.
