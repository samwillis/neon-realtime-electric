# Staging publication gate: unresolved

Observed on 15 October 2026 using a dedicated PG18 staging project
`neon-realtime-electric` (`<test-project-id>`) in Sam's staging organization.
No Function was deployed. The published SDK is pinned to `0.1.0` with the reviewed
source-evidence callback patch.

The credentialed integration test currently fails waiting for the first inserted
row's publication. This is a required gate and has not been skipped or weakened.

## Reproduction

Run with the ignored staging environment loaded:

```sh
node --env-file=.env.local node_modules/vitest/vitest.mjs run --config vitest.staging.config.ts
```

The checked-in test creates a unique schema, creates primary-keyed tables, seeds
one row, then subscribes to a filtered query and an empty query. Baselines contain
the expected raw values and a real MVCC snapshot. It inserts a matching row after
both baselines complete and waits for the transaction's publication. Test-owned
schemas are dropped in `finally`.

A transparent test-only WebSocket relay records server frames, never client
capabilities. The frame log and adapter events are saved to ignored
`.artifacts/staging/source-evidence.json`. That file contains only generated test
rows and server evidence. Do not add credentials or capabilities to this recorder.

The captured wire contained `ready`, admissions, baseline start/batches/end and
MVCC progress. It contained no `open`, `keyed_results` or `commit` for the insert.
The SQL connection confirmed the inserted row existed. The progress snapshot
advanced beyond the insert's transaction ID. The adapter received the same
progress and no publication, so the missing change was already absent before the
SDK callback/shape log boundary.

An independent diagnostic with a unique table in `public`, integer primary key,
text value and an unfiltered `SELECT id, val` reproduced the missing publication.
That rules out this test's filter, numeric type and custom schema as sufficient
explanations. It does not establish the engine's root cause.

## Consequence

The real staging snapshot/read path is verified; live data correctness and
transaction acknowledgement acceptance remain unproven. Do not claim phase 3,
the early no-reader write/replay experiment, or full end-to-end compatibility has
passed. In particular, a progress snapshot cannot make absent changes correct.
Do not compensate with polling, synthetic publications, invented LSN/MVCC evidence
or a weaker integration assertion.

Next: diagnose the dedicated project's upstream ingestion/publication path, then
rerun the unchanged integration gate. The remaining local handler, storage,
package, advanced API and controlled protocol work can continue independently.
