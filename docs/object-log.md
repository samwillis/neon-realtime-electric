# Object-backed shape log

Both backends use one immutable byte-offset/logical-sequence index. Moving a
record into Object Storage changes neither its offset nor the read-boundary proof.
The index, row state and source subscription still belong to one local owner.
Objects do not make an expired or lost handle resumable on another owner.

`ObjectShapeLog` appends encoded records to its resident tail and asynchronously
seals ranges as newline-delimited JSON. Object keys contain the handle prefix,
first/last sequence and SHA-256 of the exact bytes. A successful upload publishes
the range descriptor before releasing resident bytes. Replay can cross multiple
sealed objects and the active tail in a single response. Each fetched object is
checked against its digest, record count and indexed sequences.

The default sealing threshold is 256 KiB, with an 8 MiB resident cap. A failed
upload leaves all promised bytes readable locally; a subsequent append or explicit
`flush()` retries. If retained bytes would exceed the cap, the owner invalidates
its handle. A single oversized source event can also exceed that cap. These are
configurable limits to measure against workload size, not throughput claims.

Missing, corrupt or unreadable sealed ranges invalidate the handle through the
normal 409/refetch boundary. They are not treated as caller offset errors. The
handler checks expiry again after asynchronous reads, and owner closure cancels
in-flight storage reads. Long-poll waiters still use the same local log tail.

Closing a log aborts uploads and deletes all attempted object keys, including
uploads whose response may have been lost. Await `log.disposed` to observe cleanup
failures. The handler tracks disposals and provides `drain()` after `close()`;
background deletion failures remain observable there. Network interruption can
leave an uncertain late write, and a killed owner cannot run cleanup, so deletion
also needs the expired-prefix sweep.

`createS3LogStore()` uses the pinned real AWS SDK, standard `AWS_*` environment
variables and mandatory path-style addressing. Keep the bucket private. With
keys under `<deployment-prefix>/<expiresAt-ms>/<handle>/<range>`, its paginated
`sweepExpired()` removes only prefixes whose absolute expiry has passed. It does
not delete other deployment prefixes or live handles. Run this on startup or a
separate janitor; a platform timer is not an execution guarantee.

## Evidence

`pnpm test:server` runs shared memory/object append/read contracts and the actual
Fetch handler with both backends and the real Durable Streams reader. Fault
fixtures cover pending/failed upload, bounded retention, corrupt/unavailable read,
read cancellation, expiry during read and deletion failure. A local HTTP S3 fixture
checks real SDK SigV4/path-style requests and paginated cleanup.

For live storage, select your dedicated test project and private bucket. Pull
only Object Storage variables into the ignored storage env file, preserving the
Realtime/database file. Configure any custom CLI hosts and profile privately:

```sh
neon env pull --service object-storage --file .env.storage.local

node --env-file=.env.storage.local node_modules/vitest/vitest.mjs run --config vitest.storage.config.ts
```

The live test passes for sealing, actual object-backed replay plus resident tail,
expired orphan cleanup and test-owned object deletion. Its generated envelopes
are controlled input, so this test proves storage behavior rather than live
Realtime publication correctness. It does not deploy a Function.

The separate two-process routing test proves controlled local affinity by query
handle and refetch on worker death. Actual platform routing, background ingestion
and eviction remain deferred as described in `AGENTS.md`.
