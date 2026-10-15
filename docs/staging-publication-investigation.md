# Staging publication gate: resolved setup issue

On 15 October 2026 the dedicated PG18 staging project
`neon-realtime-electric` (`<test-project-id>`) returned snapshots and MVCC
progress but no live insert publications. The captured server frames showed the
absence before the SDK adapter boundary. No Function was deployed.

The project's logical replication setting was false, and `SHOW wal_level`
returned `replica`. Provisioning Realtime alone had not enabled it. Enabling
logical replication on this dedicated project restarted its endpoints;
`SHOW wal_level` then returned `logical`.

The original integration test passed unchanged after that setup correction:

```sh
node --env-file=.env.local node_modules/vitest/vitest.mjs run --config vitest.staging.config.ts
```

The test proves raw baseline values and MVCC, live inserts, filtered removals,
full source transaction IDs, actual publication frontier LSNs, unrelated-write
progress, empty-shape progress and final rows matching SQL. Generated wire evidence
is saved in ignored `.artifacts/staging/source-evidence.json`. It records only
server evidence and generated test data, never client capabilities.

This resolves the live-publication setup failure. It does not establish every
phase 3 query/concurrency scenario, full client compatibility or platform affinity.
The earlier absence of changes was not evidence that progress could replace
publications; the assertions were retained throughout the investigation.
