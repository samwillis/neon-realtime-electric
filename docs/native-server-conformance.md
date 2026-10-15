# Native server conformance profile

Run `pnpm test:native-server`. The suite is pinned to
`@durable-streams/server-conformance-tests@0.3.7`; the runner verifies that version
and requires every reviewed test name to pass on both storage backends. It runs
in CI on Node 22 and 24. The local evidence below was obtained on Node 22.23.2.

The current profile passes 46 unchanged upstream tests plus 18 supplemental
JSON checks. Of the 676 upstream cases registered across the two backends,
630 are excluded by the explicit profile. This is not full server conformance.

| Selected upstream family | Per backend | Both backends |
| --- | --- | --- |
| Entire JSON Mode suite | 16 | 32 |
| JSON SSE content type, paired data/control, array framing, newline/injection safety, no binary encoding header | 5 | 10 |
| Empty JSON read at `offset=now` | 1 | 2 |
| HEAD for a missing stream | 1 | 2 |
| Supplemental JSON read profile | 9 | 18 |

The exact names are in `tests/conformance/profile.mjs`. The runner fails if a
selected case disappears, skips, duplicates or fails, or another case starts
passing without an inventory update. Raw results and a selected/excluded name
report stay in ignored `.artifacts/native-server-conformance/`.

## Production boundary under test

`readJsonStream` implements JSON GET/HEAD, long polling, SSE framing, offset
validation, native cursors, expiry revalidation and cache/security headers. The
shape handler calls this function and adds shape authorization, owner lifecycle,
schema, Electric aliases and proof-bearing cursors. The byte index, immutable
ranges, object checksums, uploads, eviction, replay and waits are shared by the
memory and object logs. A shape codec retains contiguous sequence and
covered-data validation; regression cases prove that rejected evidence cannot
advance committed offsets on either backend.

The test fixture provides PUT/POST/DELETE producer controls and a resource map.
JSON HTTP arrays are append batches; nested arrays remain individual messages.
The fixture does not wrap values in shape envelopes or alter upstream expected
bodies. Object runs seal every append and actually read the objects through
`ObjectJsonLog`, using an in-process object store. They are not live S3 evidence;
real staging Object Storage coverage is recorded separately.

The supplemental cases cover JSON HEAD metadata without object reads, immutable
saved-offset replay, empty/now reads, waiting long polls released by appends,
numeric cursor collisions and timeout headers, malformed/duplicate parameters,
absolute expiry using an injected clock, cancellation and no-store/security policy. Wait-entry probes
observe the real log waiter before appending or aborting. These are our tests,
not modified upstream tests.

## Excluded scope

Most upstream read/HEAD/long-poll/cache cases create text or binary streams.
Those bodies are outside this JSON shape profile; the fixture rejects those
media types. Applicable behavior is exercised with JSON in the supplemental
cases. Text/binary and base64 SSE assertions are not counted as passing.

The service uses `Cache-Control: no-store`. ETag/304 cache validation is outside
this profile; no conditional-cache support is claimed. Absolute owner expiry is
supported, not a sliding producer TTL. Generic producer sequence/idempotence,
closure, forks and metadata subscriptions are also outside the shape endpoint's
public API. Test-only creation controls do not establish their support.

The profile proves the shared read/store boundary. Shape authorization, proof
semantics, unchanged application proxies, real Realtime data/transaction
acknowledgements and owner-loss refetch have separate server, proxy and staging
gates. It does not prove deployed Function affinity or execution lifetime.

## Framing correction

Non-live empty JSON reads now return `200` with `[]`, including saved-tail and
existing-handle `offset=now` reads. Long-poll timeouts return `204` with no body.
Both preserve the next offset and up-to-date metadata. The existing facade
accepts both statuses; its public rows and lifecycle behavior are unchanged.
Both SSE and long polling require an explicit offset. The configured application
URL and query-string handle remain unchanged.

After extraction, build/lint/typecheck, the 553 client/declaration checks,
120 server checks, 59 transport checks, 18 unchanged proxy checks, API comparison
and clean frozen patch installation pass. The live Object Storage range/replay/
cleanup test passes. The actual Function running under `neon dev` also passes
baseline, no-reader/live lifecycle and polling/SSE changes-only/now/full-replica
mode checks with real staging services, both with the default seal threshold and
with forced small objects. This local evidence does not establish deployed
execution guarantees.
