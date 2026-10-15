# Implementation status and compatibility gates

Target: the full implementation plan through phase 7. No phase is complete until
its exit evidence is recorded. Deferred platform affinity is outside the current
local/staging test target, as agreed in `AGENTS.md`.

| Phase                                         | Status                                 | Evidence / remaining gate                                                                                                                                                                                                               |
| --------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0: pristine import and compatibility baseline | Passed                                 | 80-file pinned import; 78 unchanged-file checks; build/typecheck; 553 runtime/declaration checks; identical packed consumers on TS 5.5.4/5.9.3; 62 exports across four declaration entry points                                         |
| 1: internal transport boundary                | Passed                                 | HTTP adapter extracted; all 553 baseline checks, build/typecheck, packed consumers and 62-export declaration comparison pass                                                                                                            |
| 2: real Durable Streams delivery              | Implemented; final gate audit pending  | Real reader/reference server; 55 native/protocol tests, 18 packed proxy tests, consumer/lifecycle conformance and clean patch install; early real staging/Function no-reader lifecycle now passes                                       |
| 3: real staging Realtime                      | Implemented in part; live gate passed  | Pinned SDK evidence patch, source adapter, serialized owner and bounded query compiler; real baseline/live publications and transaction evidence pass after enabling logical replication; see docs/staging-publication-investigation.md |
| 4: Function handler and lifecycle             | Implemented in part; gates outstanding | Fetch read/HEAD/DELETE/long-poll handler, 55 server checks including real native reader; staging snapshot through `neon dev` passes; conformance audit outstanding; controlled two-process routing and real local no-reader replay pass |
| 5: Object Storage                             | Implemented in part; gates outstanding | Shared memory/object index and replay tests, real S3 SDK/HTTP fixture, live staging Object Storage replay/cleanup pass; local object-backed baseline/lifecycle pass; real 15-minute TTL gate remains outstanding                        |
| 6: replacement package/integrations           | Pending                                | Packed aliases, React and eager TanStack browser/type behavior                                                                                                                                                                          |
| 7: complete API expansion                     | Pending                                | SSE, replica modes, changes-only/now, types, mobile lifecycle, broader SQL, subsets and exact MVCC/cut reconciliation                                                                                                                   |

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
it; `.github/workflows/compatibility.yml` defines Node 22/24 CI checks. CI itself
has not run remotely. The original service-dependent suites are retained but
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
exit gates; React/TanStack integration is pending. The facade
explicitly rejects SSE, changes-only/now and subset execution for now; copied
public types and historical HTTP tests do not imply those native capabilities.

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
[the investigation](./staging-publication-investigation.md). The real 15-minute
expiry test is available but has no completed passing run yet. Phases 3–7 remain
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
before expiry; a fresh passing full-duration run remains required.
