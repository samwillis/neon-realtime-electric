# Type and runtime lifecycle evidence

These gates exercise the production handler and copied client with the real
Durable Streams reader. They expand phase 7 coverage; they do not complete it.

## Real Postgres types

`tests/integration/type-matrix.test.ts` has eight credentialed scenarios: two
projection profiles, polling/SSE, and default/full replica. Each creates an
isolated schema, checks a populated and a NULL row, changes values to NULL,
restores values from NULL, changes custom arrays to different values, and deletes a row. Every comparison waits for an
installed MVCC snapshot covering the write; writers are quiescent during the
independent SQL comparison.

| Profile                                  | Coverage                                                                                                                                                                                                                                                                                                                                                                                           | Observed delivery                                                                                                                 |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Incremental, 42 columns                  | int2/int4/int8, bool, float4/float8, NaN/infinities, exact numeric, quoted/Unicode text, varchar/bpchar, UUID, JSON/JSONB, bytea; integer/bigint/text/bool/JSON arrays, empty and multidimensional arrays, NULL elements; date/infinity, timestamp/timestamptz, time/timetz, interval, inet/cidr/macaddr, bit/varbit, int4range and point; enum/multidimensional enum, domain and composite arrays | One baseline epoch, actual update/update/update/delete messages; full replica old values checked                                  |
| Extended, 43 columns including the above | date/infinity, timestamp/timestamptz, time/timetz, interval, inet/cidr/macaddr, bit/varbit, int4range, point and XML                                                                                                                                                                                                                                                                               | Correct materialized values through replacement baselines; four `upstream_cancelled` events and five baseline epochs per scenario |

The extended projection currently does not pass incremental event compatibility.
The strict profile now covers 42 columns, preserving one handle and baseline
epoch at every baseline/write/delete barrier. Its evidence includes real ordered
publications and old values in both directions of the NULL transition. The
remaining full-profile event gate still requires a backend change and strict rerun.
The extended test deliberately reports convergence
separately; it is not a replacement for the incremental gate.

Golden parsed values are independent of the candidate parser. String-only types
also compare with Postgres text output under documented wire display settings.
`bpchar::text` strips padding, so padded character data has an explicit golden
value. Int8 precision, microseconds, bytea hex, JSON nulls, array escaping and
non-finite floats are retained. Generated evidence includes original wire frames,
baseline counts, operations and checkpoints under ignored `.artifacts/staging`.

All eight type scenarios now start the production Fetch handler in a separate
Function process with `neon dev`, using an isolated configured-table allowlist.
The runner records original Realtime frames through a transparent local relay,
without altering server frames. Configured origin/path and GET methods are
asserted, and strict-profile handles remain stable after every write. Teardown
stops the Function and drops its isolated schema. This is local Function evidence;
it does not establish deployed affinity, device runtimes, every PostgreSQL
extension/domain, custom parser or array edge case.

## AppState and wake recovery

`tests/server/mobile-lifecycle.test.ts` adds 16 service-free behavioral gates:

- Simulated React Native AppState background/inactive cancels the active read.
  The owner continues ingesting without readers. Foreground resumes through a
  non-live catch-up at the same path and query-string handle, retaining writes
  made while hidden.
- Owner loss while backgrounded refetches through authorization and a fresh
  query-string handle; the replacement baseline preserves materialized results.
- A system clock gap aborts a hung poll and starts non-live catch-up, including
  when AbortController drops abort reasons. Interval/clock simulation leaves log
  persistence, body consumption and poll deadlines real.
- All scenarios use memory/object logs and native/Electric-only headers.
  AppState scenarios additionally run polling/SSE. Cleanup removes subscriptions,
  listeners, wake timers and fixture objects.

This proves the runtime adapter and native reader lifecycle in controlled Node
tests. It does not claim execution on an iOS/Android device. Existing packed
React Native entry-point resolution and inherited HTTP lifecycle tests remain
separate evidence. Subset pause/reentrant behavior still needs its own gates.

Run `pnpm test:server` for lifecycle checks. Run the credentialed type suite with:

```sh
node --env-file=.env.local node_modules/vitest/vitest.mjs run --config vitest.staging.config.ts tests/integration/type-matrix.test.ts
```

Missing credentials fail explicitly. Diagnostic filtered runs are not counted as
acceptance; the committed type suite has no skipped scenarios.
