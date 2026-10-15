# Implementation status and compatibility gates

Target: the full implementation plan through phase 7. No phase is complete until
its exit evidence is recorded. Deferred platform affinity is outside the current
local/staging test target, as agreed in `AGENTS.md`.

| Phase                                         | Status                                 | Evidence / remaining gate                                                                                                                                                                                                               |
| --------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0: pristine import and compatibility baseline | Passed                                 | 80-file pinned import; 78 unchanged-file checks; build/typecheck; 553 runtime/declaration checks; identical packed consumers on TS 5.5.4/5.9.3; 62 exports across four declaration entry points                                         |
| 1: internal transport boundary                | Passed                                 | HTTP adapter extracted; all 553 baseline checks, build/typecheck, packed consumers and 62-export declaration comparison pass                                                                                                            |
| 2: real Durable Streams delivery              | Implemented; final gate audit pending  | Real reader/reference server; 59 native/protocol tests, 18 packed proxy tests, consumer/lifecycle conformance and clean patch install; early real staging/Function no-reader lifecycle now passes                                       |
| 3: real staging Realtime                      | Implemented in part; live gate passed  | Pinned SDK evidence patch, source adapter, serialized owner and WHERE passthrough; real baseline/live publications and transaction evidence pass after enabling logical replication; see docs/staging-publication-investigation.md |
| 4: Function handler and lifecycle             | Implemented in part; gates outstanding | Fetch read/HEAD/DELETE/long-poll/SSE handler, 112 server checks including real native reader; staging snapshot through `neon dev` passes; conformance audit outstanding; controlled two-process routing and real local no-reader replay pass |
| 5: Object Storage                             | Implemented in part; gates outstanding | Shared memory/object index and replay tests, real S3 SDK/HTTP fixture, live staging Object Storage replay/cleanup pass; local object-backed baseline/lifecycle pass; real local 15-minute expiry/refetch run passes                        |
| 6: replacement package/integrations           | Implemented; gate audit pending        | Fresh packed original/candidate React/TanStack builds and TS 5.5/5.9 compilation; Chrome baseline/live/auth/refetch and optimistic real tx acknowledgement pass                                                                         |
| 7: complete API expansion                     | Implemented in part                                | SSE, replica modes, changes-only/now, WHERE passthrough, expanded real type matrix and native-reader mobile lifecycle gates implemented; complete types/events, subsets and exact MVCC/cut reconciliation remain                                                                                                                   |

## Upstream test disposition

All upstream files are retained in their original relative layout. The baseline
configuration discovers all `test/**/*.test.ts` except these service-bound suites:

| Suite                                   | Current disposition                          | Required subsequent evidence                                                                             |
| --------------------------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `client.test.ts`                        | Original Electric service reference          | Preserve application assertions through the transport-fixture adaptation and real Neon integration suite |
| `integration.test.ts`                   | Original Electric service/Postgres reference | Port relevant behavior to staging Postgres/Realtime and real Durable Streams                             |
| `cache.test.ts`                         | Original Electric/nginx/Docker reference     | Replace protocol-specific assertions with native read/cache conformance and unchanged proxy fixtures     |
| All other runtime tests                 | Unchanged, service-free baseline             | Run with the initial source import; preserve or explicitly map when transport changes                    |
| `client.test-d.ts`, `helpers.test-d.ts` | Unchanged declaration tests                  | Keep passing alongside public API reports and packed consumer compilation                                |

These dispositions describe test execution, not feature support. An imported test
that has not been executed is not passing evidence. The baseline used Electric HTTP at phase 0. Production now selects Durable
Streams; the historical baseline configuration explicitly selects the reference
HTTP adapter. Real Neon source and local lifecycle evidence is recorded below.

## Phase 0 evidence

Verified locally on Node 22.23.2 and pnpm 10.30.3:

- `pnpm install --frozen-lockfile`: succeeds with the committed dependency graph.
- `pnpm check:upstream`: all 78 non-adapted upstream files match their pinned Git
  blob hashes; only package/workspace configuration has been adapted.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`: pass.
- `pnpm test:client`: 19 test files, 553 passing tests including eight declaration
  tests; no type errors. Includes the unchanged model/property tests.
- `pnpm test:types`: both unchanged declaration suites, eight passing tests.
- `pnpm test:package`: fresh original/candidate installs, identical positive and
  negative consumer fixtures on TypeScript 5.5.4 and 5.9.3; ESM/CJS runtime export
  and concrete prototype checks; browser/React Native entry-point resolution.
- `pnpm check:api --pristine`: 62 public exports and reachable declarations match
  the original npm package across all four declaration entry points. Every
  published source file matches the initial fork byte for byte. Conditional
  exports, package type paths and side-effect markers match.

Generated evidence is in `.artifacts/public-api.json` and
`.artifacts/package-compatibility/report.json`. The committed scripts regenerate
it; `.github/workflows/compatibility.yml` defines Node 22/24 CI checks. The first published sanitized revision also passed remote Compatibility CI.
Local evidence for later revisions is recorded separately below. The original service-dependent suites are retained but
have not been run against a real Electric service; their disposition above stays
explicit rather than being counted as passed.

## Phase 1 evidence

The private transport boundary now carries normalized response metadata, reads
and raw snapshot data. The original URL builder, message parser/materializer,
state machine, retry/reset, pause/refresh and subscriber serialization remain in
the shared client. The reference HTTP adapter is still selected.

After extraction, `pnpm typecheck`, `pnpm build`, `pnpm test:client` (553 tests),
`pnpm test:package`, `pnpm check:api` and `pnpm lint` pass. All upstream tests remain
unchanged. `pnpm check:upstream` verifies 77 untouched files and explicitly reports
`src/client.ts` as a reviewed local source modification. Neither phase 1 nor its
HTTP fixtures establish Durable Streams delivery; phase 2 must supply that proof.

## Phase 2 evidence and outstanding gates

The production factory now selects the real Durable Streams reader. The shared
client still constructs the outward URL, parses rows and owns public state and
subscriber delivery. Historical HTTP tests select the reference adapter only
through a test configuration alias, and all 553 baseline checks remain passing.

Verified locally on Node 22.23.2:

- `pnpm test:transport`: 55 tests covering raw envelope validation, precision,
  ordered baseline barriers, partial-batch replay, a logical cut inside a real
  multi-publication response, real HTTP one-shot/live reads, late schema, empty
  and multiple baseline chunks, initialization cancellation and expiry, early-handle
  body-error recovery, saved-offset replay, fresh-tail resume, dynamic auth/refresh
  and owner-loss refetch. Forced native multi-response catch-up/retries reevaluate
  auth and params on each HTTP call; resolver failures stop network retries. Native
  and Electric-only response headers are both exercised.
- `pnpm test:proxy`: 18 tests run the isolated original npm package and packed
  replacement through the same GET-only transparent, Electric query/header
  allowlist and trusted-predicate routes. Includes a slow baseline exceeding the
  proxy response timeout, ordinary live delivery, saved-offset resume, refresh and handle rotation.
  No body-transforming proxy compatibility is claimed.
- `pnpm test:durable-reader`: the upstream consumer/lifecycle selection reports
  182 passes, zero failures and 10 upstream skips (two unconditional Swift-tagged
  cases and eight feature-gated validation cases). This is reader evidence, not
  conformance of our future server handler or the facade's SSE/subset support.
- `pnpm test:durable-reader-install`: an isolated frozen install applies the
  committed reader patch and verifies ESM/CJS consumption completion and JSON
  errors. The built client bundles that artifact so consumer patch configuration
  is unnecessary. See `wire-protocol.md` for the reproduced upstream defect.
- Build, strict typecheck, packed consumer compilation/resolution and the
  62-export/four-entry-point API report remain passing. The import verifier now
  checks 76 untouched files and names both reviewed source/build modifications.

The controlled reader/proxy gates are verified. The early `neon dev`/real staging lifecycle experiment now passes separately;
controlled protocol tests alone do not establish source or Function behavior. The reference-server fixture serializes
append/index updates with reads: a stress run exposed a race in the initial
fixture where bytes became readable before its offset-to-sequence index was
updated. A reader could then receive a false invalid-offset error on its next
poll. The fixture now publishes those boundaries together. Absolute expiry here
is a controlled-clock fixture, not the required 15-minute real Function run.

Real Realtime ingestion/MVCC provenance, local Function execution and Object
Storage are implemented with the evidence below. Phases 3–7 retain outstanding
exit gates. At the phase 2 checkpoint the facade rejected SSE, changes-only/now
and subsets. The later phase 7 evidence below supersedes that status for SSE
and changes-only/now; subsets remain unsupported.

## First staging and local Function evidence

The copied facade and real Durable Streams reader completed a one-shot staging
snapshot through `neon dev`, then resumed the saved offset at the same endpoint.
The smoke script checks GET-only requests and preserves origin/path.

`pnpm test:sdk-patch` passed in a fresh frozen workspace install, verifying both
dependency patches, real WebSocket SDK delivery and exported SDK declarations.
`pnpm test:server` passes 35 source/owner/log/definition/handler tests. Source tests
exercise raw values, early schema, buffered publication frontiers, ordered
progress, cancellation and reset. Owner and handler tests use controlled source
events; they do not claim staging live correctness.

The required real Realtime integration test passes unchanged after enabling
logical replication on the dedicated staging project. See
[the investigation](./staging-publication-investigation.md). At this checkpoint the real 15-minute expiry gate was still pending; the later
passing run is recorded below. Phases 3–7 remain
incomplete.

## Object-backed replay and routing evidence

The server now has an injectable `ShapeLog`, one shared immutable index, and
`ObjectShapeLog` plus a real S3 adapter. Both backend variants run the same handler
and native-reader tests. Live Neon Object Storage tests pass against the private
bucket in the dedicated staging project. The controlled two-process routing test
passes for sticky query handles, no-reader ingestion/replay and a fresh handle
after worker death. Real Realtime publication acceptance passes separately; the routing fixture
uses controlled sources and does not establish platform routing. See [Object Storage details](./object-log.md).

## Local object-backed Function lifecycle

The Function running under `neon dev` against real staging Postgres/Realtime and
Neon Object Storage passes `test:local-baseline` and `test:local-lifecycle`.
The latter disconnects readers, commits a write, resumes the same handle/offset,
and checks replay merged with the baseline against SQL. It then verifies live
updates, filter exit/reentry, deletion and wrapped public transaction IDs against
actual database transaction IDs. Reports are ignored under `.artifacts/staging`.

An earlier real TTL run failed its pre-expiry assertion before the replication
setup correction; its cause was not established. It is not passing evidence.
The improved runner records response timing/status and probes the current tail
before expiry; the subsequent instrumented passing run is recorded under Real expiry evidence.

## Packed React/TanStack and browser evidence

`pnpm test:integration-packages` installs the packed candidate and original
1.5.28 in independent projects. Identical basic/custom, React and eager TanStack
sources compile with strict declarations on TypeScript 5.5.4 and 5.9.3, and build
with Vite. The application and published integration packages resolve the same
client artifact and CJS error class. Integration versions are pinned in the
fixture generator; no workspace links or consumer pnpm patches are used.

`pnpm test:browser` runs the built candidate in Chrome against local `neon dev`
and real staging data. All four example modes pass baseline/live data, dynamic
auth and 401 recovery using the public `FetchError`, and owner loss/refetch at the
same proxy URL. The custom example proves a text parser and column mapper.
TanStack's optimistic update is observed before SQL changes, then remains pending
until its actual Postgres transaction is acknowledged through the shape stream.
Only generated test rows are modified and removed. The browser report is in
ignored `.artifacts/staging/browser-integrations.json`. CI runs package
compilation/build on Node 22/24; credentialed browser runs remain local.

The auth test exposed stale metadata after response normalization failed and
native fetch retried. The transport now queues metadata only after successful
normalization and omits bodies for HTTP statuses that forbid them. The added
regression fails on the previous implementation and passes on the fix under
both native and Electric-header-only forwarding. Transport coverage is now 59
passing checks. This is not full phase 6/7 acceptance; the remaining gate audit,
advanced capabilities and platform limitations still apply.

## Baseline concurrency evidence

The real staging source suite now has two passing tests. A transparent relay
holds the baseline-end frame and all later frames while SQL commits an update to
NULL, filter exit, primary-key change and insert in one transaction. After release,
the adapter delivers baseline before publications; the owner emits three baseline
chunks, and replayed rows match independent SQL. The real baseline MVCC excludes
the later transaction, which is present in the real publication evidence. No
source frame or provenance is synthesized for this test. This tests writes during
delayed baseline delivery, not every possible engine snapshot-construction race.

## Production handler proxy evidence

The unchanged transparent, Electric query/header allowlist and trusted-predicate
routes now also run against the actual production handler, with both memory and
object log backends. Six scenarios prove early initialization, eager/live data,
owner-loss refetch and the same outward path/query contract. Server checks total 64. Trusted proxies use Electric's bracketed `params[1]` syntax; the configured
query compiler now binds that form alongside the JSON form, rejecting repeated
positions or mixed encodings as ambiguous. No proxy route was edited to make
these tests pass. SSE/subset proxy flows remain required when those capabilities
are enabled.

MVCC progress is also forwarded as a public `snapshot-end` control message after
its covered changes have been installed. Controlled tests prove that ordering
under both header profiles. The TanStack browser scenario now additionally
commits an UPDATE that leaves the row unchanged, then resolves `awaitTxId` using
the subsequent real source evidence. This covers an eager acknowledgement with
no visible row change, beyond the ordinary optimistic mutation path.

## Real expiry evidence

The object-backed run started at 21:08 UTC failed at its pre-expiry HEAD probe
at 21:22:53 UTC, ten seconds before the advertised 21:23:03 deadline. HTTP 409
means that handle was no longer usable; the run does not pass expiry acceptance.
The available logs do not establish whether upstream continuity, local hot reload
or the lifetime logic removed it. A subsequent instrumented object-backed run
completed the full 15-minute interval under `neon dev`: all 30 periodic HEAD
probes retained the original handle, a read ten seconds before expiry succeeded,
and the post-deadline read returned HTTP 409 with a replacement query-string
handle and no redirect. The ignored report records the exact deadline and probe
times. This passes the local expiry gate; platform execution guarantees remain
unverified.


## Phase 7 incremental evidence

The server now serves SSE with the real Durable Streams client while preserving
the configured path and Electric query/header forwarding. Controlled tests cover
streaming, reconnect, refresh and owner-loss refetch across both log backends and
header profiles. The packed Chrome suite passes all eight basic/custom/React/
TanStack modes across long polling and SSE, including auth recovery and owner
loss. Reconnect uses the last consumed checkpoint.

`changes_only`, `offset=now` and both replica payload modes are implemented.
The initial baseline is retained internally for classification but not exposed as
history for changes-only/now readers. Readiness still requires real baseline MVCC.
Full replica old values contain changed columns; default updates contain keys and
changed projected fields. Twelve controlled mode tests and a real local Function
run exercise cold readiness, future mutations and same-handle now resume. The
current service-free server suite has 81 passing checks; transport checks remain
59. Typecheck, pristine-file checks, public API comparison and packed integration
compilation/build pass after these changes.

WHERE expressions and positional parameters now pass directly to Realtime; no
PostgreSQL parser or local expression evaluator is installed. Four real query
scenarios cover OR/IN, IN, EXISTS and NOT EXISTS with dependency-only changes and
overlapping transactions. Realtime resets these subscriptions after writes, so
the real client refetches through the same authorized URL and verifies its new
baseline against SQL after an installed MVCC boundary. This proves convergence
through refetch, not incremental Electric tags/move metadata. See
[WHERE queries](./where-queries.md). Subsets, full type/mobile behavior and the
remaining conformance/performance gates still prevent full phase 7 acceptance.


## Expanded type and mobile lifecycle gates

The server suite now passes 112 checks, including 16 native-reader AppState/wake
scenarios across memory/object logs and native/Electric-only headers. Controlled
background/foreground and reason-less abort recovery preserve the handle when
continuity exists, and refetch after owner loss. These are simulated runtime
tests, not device execution.

Eight real type scenarios check 39 columns across polling/SSE and both replica
modes. A strict 24-column profile proves actual incremental updates and full
replica old values. The extended profile preserves values through refetch but
currently receives backend `upstream_cancelled` after writes; it does not pass
full incremental event compatibility. See [type and runtime evidence](./type-and-mobile-evidence.md)
for the precise matrix, synchronization boundaries and remaining limitations.
Subsets, complete type/event equivalence, native server conformance, local
performance and the comprehensive phase exit audit remain open.
