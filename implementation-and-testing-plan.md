# Implementation and testing plan

Draft, 15 October 2026. Builds on [Neon shapes over Durable Streams](./durable-streams-shape-design.md).

## Decisions

Use a TypeScript monorepo containing the client fork, server library, deployable Neon Function, shared wire contract, examples and compatibility tests.

**Copy the existing Electric client and its tests, then edit that copy to replace the HTTP transport with the real `@durable-streams/client`.** Preserve the public `ShapeStream` and `Shape` classes, types, helpers, parsers, column mappers and behavior wherever possible. Keep the fork independent: the published client must not depend on the original package at runtime, which would recurse when users alias it.

Use the real Durable Streams reference server for the first local integrations. Evaluate adapting its handler/storage boundary for the Function before implementing a separate read handler. Keep the Durable Streams reader as an ordinary pinned dependency. Do not fork it unless a demonstrated gap requires a change.

If the published Neon Realtime SDK hides metadata we need, a committed pnpm patch of its pinned release is an acceptable POC bridge. Keep those changes in the server's Realtime adapter, prove them against the real service and upstream them for a supported release. A patch cannot supply evidence or guarantees absent from the engine.

Users continue importing `ShapeStream` and `Shape` from `@electric-sql/client`; a package-manager alias/override resolves that name to our package, including transitive imports. `@neon/realtime-electric-client` is the proposed package name in this plan, not an existing published package.

One shape handle identifies one Durable Stream through the existing `handle` query parameter. Preserve the configured shape URL's path/origin and existing Electric query/method conventions so application proxies keep working. The POC assumes affinity to one owner, absolute expiry no later than 15 minutes, and refetch after owner or upstream continuity loss. Durable handoff and handles that survive workers remain separate future work.

## Current testing environment

Use Sam's Neon staging workspace for real Postgres and Realtime, and run the Function locally with `neon dev`. Platform request affinity is not available yet, so deployed Function testing is deferred. The current delivery target is a local end-to-end POC using real staging data, plus a Function-compatible artifact for later deployment. Simulate affinity and owner loss locally; do not claim platform guarantees from those tests. See [testing environment](./docs/testing-environment.md) for the recorded workflow and staging setup reference.

## URL and proxy compatibility contract

Every browser request stays on the configured `ShapeStreamOptions.url`. For example, an existing proxy keeps receiving `/api/shape?table=items&handle=abc&offset=...`; the handle never becomes a path segment. Initial sync remains a GET without a handle, and subset GET/POST requests remain on that same URL. Preserve existing query names and serialization, including live/SSE flags, custom/auth params and `ELECTRIC_PROTOCOL_QUERY_PARAMS`. Preserve the mode-specific method behavior of the original client; do not add mandatory bootstrap/descriptor paths, HEAD requests, transport-selection parameters, custom negotiation headers or cross-origin redirects.

Use the real Durable Streams client through its custom-fetch hook, with a small URL/header compatibility adapter. The existing URL builder determines outward requests; translate native Durable Streams live values to the original Electric live/SSE conventions. The Function accepts these aliases, emits both native `Stream-*` headers and existing Electric metadata-header aliases, and supplies handle metadata plus schema for row reads. Initialization-only responses may precede schema availability. The adapter normalizes surviving Electric aliases when a proxy strips unknown headers. This leaves the Durable Streams reader responsible for delivery and parsing.

Shape identity and affinity are keyed by the authorized shape context and query-string handle, excluding read-position/cursor changes. Keep shape-definition parameters on later requests because existing proxies may authorize or replace them on every request. Reuse the same path on expiry; only the handle query value rotates. Preserve compatible `409 must-refetch` behavior at the application boundary, with translation for the underlying reader where needed.

Unchanged transparent and Electric-allowlist proxy fixtures are mandatory acceptance tests for each milestone's supported flows, according to the capability matrix below. A proxy that transforms Electric payload bodies needs its own behavioral proof; URL compatibility alone does not establish that case. Backend configuration must still point at the new service, or the service must replace the old upstream at its configured origin.

## Repository layout

```text
packages/
  client/                    working fork of the Electric client
    src/                     existing layout, plus Durable Streams transport
    test/                    imported tests in their original relative layout
      neon/                  additional transport and compatibility scenarios
    SPEC.md                  imported client behavior specification
    UPSTREAM.md              origin, release, SHA, imported paths, local changes
  protocol/                  versioned raw shape envelopes and validation
  server/                    shape owner, Realtime adapter, log and HTTP handler
apps/
  function/                  thin default-export fetch handler and neon.ts
examples/
  basic/                     ShapeStream + Shape consumer
  react/                     existing Electric React hook with package alias
  tanstack/                  existing Electric collection with package alias
tests/
  compatibility/             declarations, consumer compilation, package installs
    proxies/                 unchanged existing proxy routes and allowlists
  transport/                 Durable Streams conformance harness
  integration/               real Realtime/Postgres and failure scenarios
  browser/                   Playwright consumers
scripts/
  import-electric-client.*   reproducible upstream import/check
  compare-public-api.*       public declaration/export comparison
docs/
  compatibility.md           capability and upstream-test disposition matrix
  upstream-changes.md        patch rationale and update procedure
  testing-environment.md     staging services and local Function workflow
patches/                     committed Neon Realtime SDK patches, if needed
pnpm-workspace.yaml          workspace and version-appropriate patch settings
pnpm-lock.yaml               exact dependency and patch resolution
```

Use pnpm workspaces, a pinned pnpm version in `packageManager`, a committed lockfile, TypeScript, Vitest and Playwright. Target Node.js 24 for the server/Function. Keep the client browser-compatible and preserve the upstream browser, Node, ESM/CJS and React Native entry-point contract. Start with workspace scripts; add a task runner only if measured build costs justify it.

`protocol` must be browser-safe and independent of client/server internals. Its raw-cell/event types describe the wire format; they must not create a circular dependency on the client's parsed public row types. The Function entry point wires dependencies into the server library so local and deployed tests exercise the same handler.

## Vendoring and upstream maintenance

Start with `@electric-sql/client@1.5.28`, the version already inspected. The inspected upstream revision is `3f9809fb0bd894cb74470d434749673492ad7acf`; its `client.ts`, `shape.ts`, `types.ts` and `index.ts` match the archived npm source byte for byte. Verify the complete imported source against that release before treating it as the baseline.

1. Import the package source, tests, fixtures, behavior spec and required build/test configuration. Include applicable license and attribution files. Make the pristine import independently reviewable before transport edits.
2. Record release, source SHA, path list and checksums in `UPSTREAM.md`/an import manifest. Commit the import script. A later refresh must be a reviewed change to the pinned baseline.
3. Adapt workspace dependencies and scripts without changing client behavior. Run pure tests, declaration tests and a build. Establish live integration behavior against a pinned real Electric service where needed.
4. Keep imported test layout intact so relative imports continue working. Add Neon tests separately and document changes to shared test helpers.
5. Record each test family's destination: unchanged, adapted fixture/transport, historical Electric HTTP reference, or deferred capability. Avoid silently dropping failing tests or weakening application assertions to accommodate the new implementation.

The original package is a **test oracle**, installed only in isolated reference fixtures. Keep it outside the candidate fixture's alias/override rules so the oracle cannot accidentally resolve to our fork.

Upstream updates: import the next release, run the pristine baseline, review API/spec changes, reapply the transport patch and run all compatibility gates. Do not make updates automatic merges.

## Implementation sequence and exit gates

### Capability gates

Keep every public declaration from the pinned Electric client from stage 0 onward. Runtime support advances separately. Publish an explicit required/deferred status for each scenario in `docs/compatibility.md`; unsupported runtime options fail clearly instead of silently changing their meaning.

| Capability | First required gate |
|---|---|
| Pristine import, public exports/types, packed consumer compilation | Stage 0; retained at every later stage |
| Original HTTP behavior through extracted transport boundary | Stage 1 reference gate |
| Real Durable Streams reader, eager full-log baseline, prompt initialization, `subscribe: false`, long polling, resume/refetch, unchanged GET/proxy flows | Stage 2 with controlled input; retained against Neon/Function at stages 3–6 |
| Configured primary-keyed tables, projection, bounded parameterized filters, `replica=default`; `int4`, `text`, `bool`, `uuid` and NULL values | Stages 3–4 with real Neon data |
| Ordered real MVCC/txid evidence, eager transaction acknowledgement | Stage 3 source gate; React/TanStack acceptance at stage 6 |
| Function-compatible read profile, owner-bound expiry/loss, ingestion between reader requests | Early local lifecycle check plus stage 4 `neon dev` handler tests |
| Object-backed replay, simulated affinity/fallback and 15-minute expiry | Stage 5 local gate; live storage integration when a bucket is available |
| Packed package aliases and unchanged basic/React/eager TanStack consumers | Stage 6; completes the local eager POC against staging Postgres/Realtime |
| Actual platform affinity/fallback, idle/eviction and bounded ingestion lifetime | Deferred until affinity is available; required before deployed service acceptance |
| SSE, `replica=full`, `changes_only`/`now`, expanded SQL/types, mobile lifecycle and remaining options | Stage 7, each with its own newly required behavior/proxy gates |
| `fetchSnapshot`/`requestSnapshot`, GET/POST subsets and exact cut reconciliation | Stage 7 subset gate, including unchanged subset proxies |

The copied parser's wider unit-test coverage does not imply wider server encoding support. SSE and subset fixtures may be inventoried/imported early, but their deferred candidate runs are not counted as passed. Once a capability is enabled, its required tests cannot be skipped to complete that milestone. Initial gates exercise only the applicable flow through unchanged fixtures; stage 7 adds the same fixtures' SSE/subset cases.

### Early local lifecycle check; deferred platform verification

Run a small local lifecycle experiment alongside stages 0–2 using `neon dev` and real staging Realtime. It needs only a minimal subscription, owner state and reconnect handling on the same shape endpoint. Make ingestion lifetime explicit, bound work by absolute shape expiry and implement cleanup. Record an intended platform mechanism separately; a `waitUntil` promise tied to expiry is a candidate to validate when deployed testing becomes possible.

Disconnect every reader, commit writes, then reconnect to the same handle. If the owner retained continuous ingestion, replay must include those writes. If execution or upstream continuity was lost, the old handle must invalidate and refetch; it must never silently resume a history with a gap. Exercise intentional expiry, local process restart/kill, cleanup and a simulated dead-owner fallback. Record local work lifetime and whether repeated creation can admit shapes with a useful remaining lifetime. Keep any registered work bounded by its own deadline and cleanup; actual platform eviction and invocation/concurrency accounting belong to the deferred deployed suite.

The documented 15-minute first-byte, heartbeat and `waitUntil` limits are distinct limits, not proof of a hard 15-minute process lifetime. Local `waitUntil` is not tracked, and timers/outbound subscriptions are not evidence of deployed execution lifetime. Request affinity is currently unavailable: do not deploy the Function to run this experiment now. Local lifecycle and simulated routing tests are sufficient for the current local milestone. Once affinity is available, verify actual routing/failover and an explicitly supported bounded ingestion mechanism, including no-reader writes/reconnect, idle shutdown, eviction and concurrency accounting, before deployed acceptance. That future gate does not block client, local Function or staging Postgres/Realtime work.

### 0. Establish the baseline and compatibility harness

Create the workspace and pristine client import. Build a public-export inventory, import the existing declaration tests and capture the initial public declaration report. Create isolated original-package and candidate-package consumer fixtures.

Inventory all concrete class members as well as `ShapeStreamInterface`: for example, `ShapeStream.options`, `ShapeStream.isPaused()` and `Shape.numSubscribers` are also public. Include constructor generics, options, helpers, static error methods, platform entry points and conditional exports.

**Exit:** the unchanged fork builds, baseline tests pass, and the same consumer sources compile against the original and the repackaged fork. No transport changes yet.

### 1. Extract the internal transport boundary

Keep public constructors and exports unchanged. Introduce an internal transport interface around shape creation/resume, ordered delivery, cancellation, refresh and subset fetching. The shared client logic remains responsible for public status, parsing, mapping, serialized callbacks and snapshot injection.

Use the existing HTTP implementation temporarily as a reference adapter during refactoring. Retain its URL construction in the candidate: it encodes the proxy compatibility contract. The published replacement will select Durable Streams internally; applications should not need a new constructor option. Existing HTTP response/reader code can remain test-only once the new path is proven.

Do not map the new protocol to synthetic Electric HTTP responses just to satisfy old mocks. Normalize transport events into the client’s existing message/lifecycle processing. Identify HTTP-coupled state transitions that need adaptation, including SSE's current LSN-to-offset assumptions.

**Exit:** the original transport still passes its behavior suite through the new boundary, and public type/consumer tests remain unchanged.

### 2. Connect the real Durable Streams client and reference server

Add the production dependency on `@durable-streams/client`. Use `stream()`/`subscribeJson()` for reading. Map existing fetch, headers, params, abort, backoff and live-mode options deliberately; avoid two independent retry loops owning the same failure. Translate transport errors to the fork's existing public error classes and handler contract.

Start a real reference server in the test harness. Use a controlled producer and a test adapter behind the existing shape endpoint: the first GET creates a stream, returns handle/schema aliases and reads its first data. Internally map the query-string handle to the reference server's path-keyed store; this mapping must not leak into browser-facing URLs. The application still constructs the copied `ShapeStream` and `Shape`; its reads now use the real Durable Streams library over HTTP through the compatibility adapter. No extra bootstrap endpoint is required by the browser.

Define version 1 envelopes for initialization, snapshot chunks, baseline completion, complete publications and MVCC progress. Each has a stable, strictly increasing per-handle `seq`, encoded as a decimal string and compared numerically without JavaScript number conversion. Assign sequences in the same serialized owner queue that updates row state and appends events. Append initialization before starting the source subscription. Use raw Postgres cells plus schema to preserve the existing parser. Initially put a complete publication in one JSON object. If publications are later fragmented, give their parts an explicit group/completion boundary; only complete publications establish state cuts or public commits. Baseline completion and progress identify the installed data boundary their evidence covers and cannot overtake that data.

Keep three positions separate: logical envelope/publication sequences, opaque Durable Streams resume offsets and Postgres LSNs. `subscribeJson()` reports one response-end offset for a batch that may contain several publications; that offset cannot locate a subset cut between them. Sequences survive replay, object sealing and changes to response grouping; only a fresh handle starts a new sequence epoch. Committed log bytes/transport offsets also remain immutable. Choose zero-padded numeric-pair transport offsets compatible with Electric's public `Offset` type and Durable Streams ordering.

Return the authorized handle and an initialization event promptly on the first GET, then finish that short response without waiting for the baseline. Include schema when available, and require it before any row-bearing response; resumed row reads carry it too. The custom-fetch boundary records the handle as soon as response metadata arrives, before body consumption can fail. Continue initialization through ordinary reads on the same URL. No descriptor request, new query parameter or long-held initial response is required. Test the schema acquisition path too, so waiting for buffered SDK rows cannot delay the first response past an existing proxy's timeout.

The facade treats physical transport tail and shape readiness separately. Even with `subscribe: false`, reopen catch-up reads after a temporary tail until the explicit baseline-complete barrier and all its covered data are installed; only then finish the one-shot sync. Bound this initialization loop with cancellation, expiry, retry/backoff and existing error handling. Never emit a public ready/up-to-date boundary for a partial baseline.

**Exit:** an unchanged basic consumer processes snapshot, insert, update, delete, disconnect/replay and explicit refetch through the real reader and reference server. Stage 2 long-poll/GET proxy gates pass, including a slow baseline behind an unchanged proxy with a short response timeout, one-shot initialization across temporary tails, early-handle resume and a response containing events on both sides of a synthetic subset cut. SSE and actual subset execution remain stage 7 gates.

### 3. Feed the stream from real Neon Realtime

Implement an injectable Realtime source adapter with a deterministic fake for isolated tests and a real SDK implementation. Pin the published SDK surface in use; the local checkout and published `0.1.0` callbacks differ.

Implement the shape owner: definition, authorization context, expiry, subscription, prior-row map, key mapping, serialized append queue, log head and waiters. Use configured primary-keyed tables and projections, and pass the authorized WHERE clause plus positional parameters directly to Realtime. Resolve root/projection identifiers against the configured tables. Realtime owns SQL validation and execution; do not introduce a PostgreSQL parser or a local WHERE evaluator. Backend query rejections return explicit errors.

Translate the initial baseline and ordered publications into the wire envelopes. Retain previous rows to classify inserts/updates/deletes, construct Electric keys and handle filter membership changes. Invalidate the handle on a replacement baseline/upstream reset rather than claiming continuity.

Expose baseline MVCC, progress, publication frontier and required schema/raw values through an explicit SDK adapter surface. Develop the small SDK additions in `neon-pkgs` and upstream them. During the POC, the reproducible pnpm patch procedure below can expose existing wire metadata before a release is available; isolate this temporary coupling inside the server adapter. Synthetic metadata exercises fixtures only. Real transaction-acknowledgement acceptance requires real ordered evidence from either the patched pinned SDK or a supported release, with no private runtime monkey-patching in the Function.

**Exit:** snapshot and live rows agree with Postgres after a synchronization barrier, including writes during baseline and filter transitions. Transaction-aware acceptance additionally proves acknowledgements for baseline-visible transactions, unrelated writes and empty shapes.

### 4. Serve the stream from the Function-compatible handler

Evaluate extracting/adapting the reference server's handler and store. Its current exported Node test server is not a ready Fetch handler with an injectable S3 backend. Time-box this evaluation to a small extraction experiment; either reuse a clean boundary or implement the read surface against the pinned protocol and its tests.

Implement initial creation, reads with inline metadata, existing GET/POST subsets when supported, and deletion on the same configured shape endpoint. Serve native Durable Streams read/HEAD semantics alongside the compatible query/header aliases, explicit errors and CORS. HEAD is optional for the compatible client; do not require new proxy methods for existing flows. Start with long polling and an in-memory log, behind an injectable storage interface. Share one local serialized log among readers of a handle. Preserve message boundaries and stable offset ranges independently of HTTP chunking.

Implement absolute expiry and cleanup, cancellation, waking waiting readers, missing-owner fallback and safe refetch. Exercise routing across two local owner processes using a controlled routing harness. Carry forward the early local lifecycle experiment's bounded ingestion/cleanup logic and test the actual Function entry point through `neon dev` against staging Postgres/Realtime. Platform affinity and execution lifetime remain unverified; affinity alone does not keep background work running.

**Exit:** the stage 4 JSON/read/HEAD/long-poll/expiry conformance profile passes against this handler, current-milestone proxy/consumer fixtures pass, and simulated owner death/expiry produces a fresh handle query value on the same endpoint and a correct new snapshot. This does not require stage 7 SSE/subset cases.

### 5. Add Object Storage and validate local owner routing

Add an Object Storage log backend for immutable sealed ranges while retaining the active tail and index on the owner. Reuse the same handler and logical append/read tests across memory and Object Storage backends. Keep tail bytes until safely uploaded; after an unrecoverable storage error retain the promised range or invalidate the handle.

Use a thin `apps/function` wrapper and `neon.ts`, running locally with `neon dev` against the user's staging Postgres/Realtime. Test the Object Storage adapter with a controlled storage fixture, and add real Neon Object Storage tests when an appropriate bucket is available. Live storage testing does not require deploying the Function. Keep actual platform affinity and dead-owner routing tests deferred until affinity exists; a local routing simulation is not evidence that the platform feature works.

**Exit:** replay spans sealed objects and the active tail without gaps or duplicates; upload failures and storage reads behave correctly; a shared handle stays on one owner under controlled local routing; owner loss and absolute expiry refetch safely. Run at least one real 15-minute expiry test under `neon dev` in addition to fast virtual-clock tests. Report fixture-backed versus live Object Storage coverage explicitly; neither proves deployed Function affinity.

### 6. Prove package replacement and eager integrations

Pack the client and install the tarball into fresh fixture projects with alias/override rules. Test the basic API, the existing Electric React hook and eager TanStack Electric collections. The integration libraries remain their published packages, including their own imports of `@electric-sql/client`.

Validate dynamic auth, exported error identity, custom parsers, column mapping, optimistic transaction acknowledgement and reset behavior. Add a tiny browser example for each integration. Keep test-only dependency overrides out of the distributed client.

**Exit:** fixtures compile, build and run with unchanged consumer imports and unchanged row/callback types; their dependency graphs resolve the intended replacement everywhere. The eager POC has no skipped required tests.

### 7. Expand toward the complete client contract

Add SSE, both replica modes, `changes_only`/`now`, the full parser/type matrix, mobile visibility/wake behavior and the remaining documented options. Expand root-table filters and subqueries using Realtime's supported SQL, with independent tests for row membership and Electric tag/move metadata.

Pass the original WHERE expression and bound parameters through to Realtime, aiming to support every WHERE expression its backend supports, including OR, EXISTS and IN (SELECT ...). Keep the configured root table, projection, primary-key handling and current URL/query contract. This does not allow arbitrary SELECT queries. Pass the WHERE text directly to Realtime and let its backend validate and execute the SQL. Do not add a PostgreSQL parser or a local expression evaluator. Parameter binding protects values but does not authorize predicates: retain backend/proxy authorization and trusted tenant restrictions.

Before advertising subquery support, commit writes only to referenced tables and prove correct root-row entry and exit. Compare with independent Postgres results at a proven synchronization boundary, including concurrent root and referenced-table writes. Verify actual Realtime behavior rather than assuming that accepted SQL implies correct dependency tracking.

Implement `fetchSnapshot` and `requestSnapshot` using a proven snapshot/stream cut. Preserve the distinction between returning data and injecting data into subscribers. Test ordered/limited/offset subsets, structured expressions, overlapping requests, replay suppression, cold changes-only subscriptions, reset and reentrant calls from subscriber callbacks.

A subset computed from retained owner state needs a correct evaluator and a stable captured cut. Return an exact logical `cutSeq` at a complete state boundary and, separately, any transport resume checkpoint. The checkpoint must be at or before the cut if replay is needed; it cannot be inferred from a response-end offset that covers later publications. Reconcile each publication by its sequence, including when one `subscribeJson()` batch straddles the cut. Serialize injection with normal delivery and prove that later changes already delivered before the subset response are retained/reapplied, while earlier replay cannot overwrite the subset. A subset computed in Postgres needs coordinated snapshot evidence. Return truthful public `SnapshotMetadata`; exposing the old types does not justify fabricated MVCC or LSN fields. Do not declare this milestone complete until that contract is demonstrated under concurrent writes.

**Exit:** every supported public API and integration scenario has passing behavioral tests and a documented capability status. Historical operation/transaction equivalence is assessed separately where upstream publications coalesce changes.

## Neon Realtime SDK patches

Use a pnpm patch if the current package already receives the necessary wire evidence but does not expose it through its public callbacks. Initially pin `@neon/realtime@0.1.0`; any move to a different version is a reviewed baseline change. Pin pnpm too, and use that version's patch configuration format.

The planned workflow is:

```sh
pnpm patch @neon/realtime@0.1.0
# Edit the generated package directory, then:
pnpm patch-commit <generated-directory>
```

Commit the generated patch, `patchedDependencies` configuration and lockfile. The inspected package exports built `dist/*.js` and `dist/*.d.ts`; patch the runtime artifacts and declarations actually selected by those export maps. Editing an unpublished source file alone has no effect. The local `neon-pkgs` checkout can develop/rebuild those artifacts, but acceptance and Function deployment use a clean reproducible install rather than a linked checkout or manually modified `node_modules`.

Keep changes small and reviewed: expose early schema, baseline MVCC, ordered progress and publication frontiers without changing their ordering or manufacturing missing provenance. Add adapter contract tests for schema availability, data-before-progress ordering, baseline-covered transactions, empty shapes and transactions with no visible row effect. Verify both the declarations and runtime behavior after `pnpm install --frozen-lockfile` in a clean environment, and run the real Realtime integration gates. Fail if the patch cannot apply or the tested SDK version differs.

This patch belongs to the server application's install/build boundary. Browser consumers who alias our Electric-compatible client do not install or patch the Neon SDK. pnpm patches in this monorepo are not automatically applied by consumers of a published server library: before distributing that library, use a supported SDK release or an explicitly tested artifact that contains the patched behavior with required attribution. Verify the deployed Function artifact also contains it. Remove the patch when an upstream release provides the same contract, then rerun the same runtime, type, clean-install and integration gates.

## Type compatibility strategy

Type compatibility should be an early, continuously enforced gate. Use several independent checks:

| Check | What it catches |
|---|---|
| Existing upstream `client.test-d.ts` and `helpers.test-d.ts` | Generic defaults, custom row/parser extensions, reserved params and narrowing regressions |
| Public API/declaration comparison | Missing exports/members, changed overloads, optionality, readonly properties, return types and conditional entry points |
| Bidirectional structural type assertions | Accidental narrowing/widening of options, interfaces, message unions, schemas, offsets and snapshot metadata |
| Positive and negative inference fixtures | Lost row inference, widened `any`, missing discriminated-union narrowing and invalid code becoming accepted |
| Identical consumer source compiled in isolated original/candidate installs | Practical source substitution, including actual React/TanStack declaration dependencies |
| Packed-package ESM/CJS/browser/React Native resolution checks | Differences hidden by workspace links or TypeScript path aliases |

Use `strict` compilation, `skipLibCheck: false`, upstream `expectTypeOf` tests and explicit `@ts-expect-error` cases. Check default rows and custom rows with `Date`/other parser extensions; synchronous and asynchronous callbacks; `Shape.rows`/`value`; mappers; snapshots; and error-handler return inference. Compare inferred declarations as well as successful compilation. Run the established supported TypeScript minimum and a pinned current compiler.

Do not use direct equality of two separately declared concrete classes with private fields as the sole compatibility test. The guarantee is source substitution when the package name resolves consistently to the replacement. Compare structural public surfaces explicitly, preserve every public member, and validate the installed application. Where interoperability is structural, also test cross-package use such as an original `Shape` consuming the candidate's `ShapeStreamInterface`.

Public declarations can match while advanced server capabilities are still unsupported. Record those runtime limitations explicitly during the POC; only passing behavior gates earns a claim of full compatibility.

## Alias and distribution tests

For a published release, a pnpm consumer could use the following template. Replace `VERSION` with an actual released version:

```json
{
  "dependencies": {
    "@electric-sql/client": "npm:@neon/realtime-electric-client@VERSION"
  },
  "pnpm": {
    "overrides": {
      "@electric-sql/client": "npm:@neon/realtime-electric-client@VERSION"
    }
  }
}
```

The direct alias preserves application imports; the override covers transitive dependencies such as TanStack's client import. Test this with real packed artifacts and inspect resolution from both the application and each integration. TypeScript `paths` alone is insufficient because runtime imports must resolve too. A bundler-only alias can also leave server builds or declaration resolution using the original package.

Gate the first release on pnpm fixtures. Add verified npm/Yarn recipes and install/build tests before documenting support for those package managers. Check peer dependency constraints, ESM/CJS resolution and duplicate error/client instances. Do not assume override syntax or peer resolution is identical across package managers.

## Test suites and acceptance criteria

### Imported tests

Keep parser, helper, expression, mapping, type and `Shape` behavior tests wherever their implementation is retained. Adapt client/model-based scenarios at the transport fixture boundary while retaining their application-level assertions. Preserve original HTTP-only tests as reference evidence, with explicit replacements in the transport suite.

The existing integration setup waits for Electric health and manages a Postgres schema. Replace that service setup for the Neon path while preserving the application's existing shape URL contract. The upstream unit config also omits some test files, so inventory the complete suite rather than assuming one existing command covers all tests.

### Existing proxy compatibility

Run the real client through checked-in proxy fixtures whose route code, query allowlists and header forwarding are unchanged between original and candidate runs. Only the configured upstream service changes. Initially include transparent forwarding, `ELECTRIC_PROTOCOL_QUERY_PARAMS` allowlisting, Electric response-header allowlisting, a route that injects a trusted table/tenant predicate and GET-only eager sync. Inventory an existing GET/POST-subset proxy now; make its subset flows required when stage 7 enables subsets. Apply the same rule to SSE flows.

Assert that the configured origin/path is retained for all network requests, `handle` is always a query parameter, existing/custom params survive with the original encoding, and initial GET, long polling, resume, refresh and refetch keep their original method/query conventions. Add SSE and subset method/query assertions at their stage 7 gates. Check no new bootstrap/descriptor/HEAD request, transport-specific query flag, mandatory request header, redirect or direct Function/object-store URL is required. Retry/expiry must not bypass the proxy or its authorization.

Test raw repeated/encoded query values, existing URL search params, dynamically resolved params and proxy-supplied overrides. Verify affinity remains stable as `offset`/`cursor` change and that distinct handles remain distinct streams even though their path is identical. Test the public error/reset boundary as well as successful data reads. Record body-transforming proxy limitations explicitly; do not silently edit a fixture to hide them.

### Durable Streams delivery

Run a pinned server conformance suite against a generic fixture harness using our exact handler/store implementation. Provide test-only creation/append controls to establish streams. Select and report the applicable JSON/read/HEAD/long-poll/expiry/cache/error tests; add SSE tests when stage 7 implements SSE. HEAD tests exercise the native handler and do not require application proxies to support HEAD. Keep application authorization tests separate. The full suite also tests generic producers and forks, which the public shape service does not need to expose.

Test against the real Durable Streams reader throughout. If that dependency is patched, add its client conformance suite. Do not claim full server conformance from a partial test selection.

### Shape correctness and failure

The scenario inventory below is assigned to the capability matrix, not made unconditionally required at stage 2. Baseline/long-poll/resume cases enter at stage 2; real encoding/MVCC at stage 3; Function/storage failures at stages 4–5; integration/distribution at stage 6; and mobile/subset/advanced-mode cases at their stage 7 gates. Required scenarios for the enabled capability include:

- Empty and multi-chunk baselines; writes during baseline; prompt initial handle delivery behind a short-timeout unchanged proxy; no shape-ready notification before covered data is installed; correct readiness/timestamps on refetch.
- `subscribe: false` across several temporary tails, slow schema/baseline availability, cancellation and reconnect during initialization, failure/reset and expiry before baseline completion. Resume an observed handle without creating another owner/subscription. If the initial response is lost before its handle is observed, a retry may allocate a separate handle; bound and clean up that orphan by its absolute expiry.
- Several complete publications plus progress in one response, with a subset cut between publications; numeric sequence comparison, replay/rechunking stability and data-before-proof ordering. Commit a batch's transport checkpoint only after all its envelopes are consumed; interrupted processing resumes from an earlier checkpoint and suppresses already-applied sequences without losing later events.
- Insert/update/delete, primary-key changes, quoted identifiers, projection, NULLs and entering/leaving filters. Compare parsed values with independent expected values.
- Multiple readers sharing a handle; independent initial requests; disconnect and resume across object/tail boundaries; resuming with schema recovered from normal response metadata on the same endpoint.
- Slow asynchronous subscribers; cancellation; refresh; visibility/wake; auth refresh; transient errors, rate limits and permanent errors. Avoid deadlock on supported reentrant snapshot requests.
- MVCC evidence arriving after covered data; transactions with no visible effect; multiple transaction IDs in a publication; exact transaction-ID and LSN conversions, including wraparound cases. At the subset gate, include changes delivered before a delayed subset response, overlapping cuts, deletes and changes-only initialization.
- Transport fragmentation/truncation, object upload/read failures, owner death, upstream replacement baseline, absolute expiry during initialization/read/snapshot, and requests routed to a dead owner.
- No readers, writes, then reconnect under the validated bounded ingestion mechanism; cleanup of subscriptions, registered work, timers, pending polls, cached rows and object references. An expired handle never becomes a different history; new handles use the same endpoint.
- Authorization on initial GET, resumed reads, snapshots and deletion; the authorized base predicate always applies to subsets.

Use explicit synchronization barriers and fake clocks for deterministic tests rather than fixed sleeps. Add property-based tests with replayable seeds for row/log state, resets and snapshot interleavings. Real database tests compare against SQL results at a known synchronization point; merely waiting an arbitrary duration is not evidence of convergence. Do not require a one-to-one historic operation count where Realtime legitimately coalesces publications.

### Performance and future deployed behavior

Measure baseline peak memory, retained-row memory, log growth, upload throughput, object request count, first usable snapshot latency, live latency, reader fan-out and refetch cost under `neon dev` against staging Postgres/Realtime. Record local size/rate/fan-out limits and fixture versus live storage coverage, including whether baseline and catch-up can finish within the shape lifetime. Actual platform concurrency accounting and deployed latency/capacity remain deferred until deployed testing is possible; local timings include the connection to staging and are not same-region deployed performance evidence.

Storage conformance does not prove crash survival. The POC accepts owner-bound shapes and early invalidation on loss; tests must verify safe recovery under that contract. A future promise of replay after owner loss requires durable log/index publication and its own crash tests.

## CI and release gates

| Run | Gates |
|---|---|
| Every change | Build/lint; public API and type checks; pure tests and unchanged URL/proxy fixtures required for the current milestone; its supported transport conformance and local integration with real Durable Streams packages once introduced at stage 2 |
| Client/distribution changes | Fresh packed-package alias fixtures, integration compilation, module resolution and browser smoke tests |
| SDK/dependency/patch changes | Clean frozen install; patch-application, exported declaration/runtime and Function artifact checks; real Realtime adapter/transaction gates |
| Integration/nightly | Enabled Realtime/Postgres and storage gates, property/fault runs, optional original Electric differential scenarios |
| Local POC/client candidate | Full required local/staging matrix for the advertised capability set, real 15-minute expiry run under `neon dev`, package install matrix, local performance report and capability document |
| Deployed service candidate (future) | Actual affinity/fallback, bounded ingestion, idle/eviction/concurrency and deployed performance checks; unavailable until platform affinity exists |

CI reports deferred capabilities separately from passing tests, and fails on skipped or missing current-milestone requirements. The local POC can complete without deployed platform evidence; deployed service acceptance remains a separate future gate. Credentialed integration tests use staging Postgres/Realtime and a local Function, not a deployed Function.

Local scripts should make these layers independently runnable (`test:types`, `test:client`, `test:transport`, `test:integration`, `test:browser`, `test:package`, `test:sdk-patch`, `test:local-lifecycle`). Reserve `test:platform-lifecycle` for the deferred deployed suite. These names describe planned commands; they do not exist yet. Isolate schemas/object prefixes per test run and clean them up. Tests needing staging credentials fail clearly when invoked without them; service-free development tests remain useful offline.

The local eager POC is complete after the early local lifecycle check and stages 0–6 for its documented scope: the copied client uses the real Durable Streams reader; real staging Postgres/Realtime data flows through the Function running under `neon dev` and the object-backed log; initialization responds promptly and one-shot reads await the baseline barrier; saved offsets replay within a valid handle; expiry/loss safely refetch at the same configured URL; eager integrations and real transaction acknowledgement pass; and required unchanged proxy plus packed-package type/alias fixtures pass. Report whether the Object Storage tests used a fixture or a live bucket. A reproducible SDK patch can satisfy the source gate; waiting for its upstream release is not mandatory. Stage 7 earns broader/full API claims incrementally in the same local/staging setup. Actual deployed service claims additionally require the deferred platform gates. Never mark deferred runtime features as implemented because their types compile.

## First implementation change

Begin with the workspace, pristine client/test import, provenance and type/consumer baseline. Run the small local Function lifecycle experiment alongside this early work. The next client change extracts the transport seam. The first end-to-end delivery change uses the real Durable Streams client and reference server with controlled shape input; then attach staging Neon Realtime through the pinned adapter, patched if needed. Run the Function with `neon dev`; leave deployed Function tests deferred. This keeps each change reviewable and gives us a passing comparison point before changing both sides of the connection.

No workspace implementation, deployment or live test run is part of this draft.
