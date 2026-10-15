# Shape envelopes, version 1

The browser uses the published Durable Streams reader (`stream` and
`subscribeJson`) in JSON mode. Each stream item is one object, never a bare array
of changes. The controlled fixture currently runs the real reference server;
the Function handler and Neon source are subsequent gates.

All envelopes contain `version: 1`, a `kind`, and a canonical positive decimal
`seq`. Sequences increase by one per handle. They retain their values when a
reader replays data or response grouping changes. They are not transport offsets
or PostgreSQL WAL positions.

| Kind                | Additional fields                 | Meaning                                                          |
| ------------------- | --------------------------------- | ---------------------------------------------------------------- |
| `initializing`      | `handle`, `expiresAt`             | First logged event; baseline construction has not completed      |
| `snapshot-chunk`    | `changes`                         | Raw insert messages, without a public ready notification         |
| `snapshot-complete` | `coveredSeq`, `evidence.snapshot` | Baseline barrier after all preceding chunks                      |
| `publication`       | `changes`, `evidence`             | One complete publication, including all changes                  |
| `progress`          | `coveredSeq`, `evidence`          | Ordered proof after the immediately preceding installed boundary |

Raw changes retain Electric keys, operations, cells, optional old values and
dependency headers. Move events retain patterns. The shared client still parses
cells and maps columns. Wire evidence uses exact decimal strings for transaction
IDs and LSNs. Public change headers use 32-bit numeric transaction IDs, matching
Electric's wrapping semantics. The complete unwrapped IDs remain in the
envelope. Baseline snapshots contain `xmin`, `xmax` and `xip_list`; these are
fixture evidence until the real source gate proves their provenance and order.

The browser-safe validator rejects unknown versions/kinds, invalid sequences,
non-JSON values, malformed changes and invalid MVCC bounds. The consumer rejects
sequence gaps, publications before the baseline barrier, replacement baselines
inside an existing handle, and evidence that overtakes installed data.

## Resume proof through existing proxy headers

Every GET stays on the configured endpoint, with the handle in the query string.
The initial GET responds with the initialization event and handle before schema
or baseline rows become available. Row-bearing reads, including resumed reads,
repeat `electric-schema`.

`electric-offset` aliases the native response-end `Stream-Next-Offset`.
`electric-up-to-date: true` aliases native physical-tail header presence. It says
nothing by itself about shape readiness. The adapter never turns a native 204
into public shape readiness.

The opaque `electric-cursor` carries `neon1.` followed by URI-encoded JSON:

```json
{
  "version": 1,
  "offset": "0000000000000000_0000000000000123",
  "afterSeq": "7",
  "baselineComplete": true,
  "nativeCursor": "optional-native-cursor"
}
```

This describes the **requested starting offset**, not the response-end offset:
the preceding sequence and whether the logged baseline barrier is already
covered there. The server must derive it from the immutable log index. A fresh
client can therefore resume an early handle during initialization, or recognize
readiness after resuming at a quiet tail, using ordinary GETs even when a proxy
forwards only Electric headers. The cursor is opaque to application code and
the copied client state machine. Native cursor values are normalized internally
for the real reader. No descriptor endpoint, extra query flag, custom
negotiation header or mandatory HEAD request is introduced.

The adapter latches handle/schema metadata before consuming the first body,
without advancing the public offset. After each successful envelope callback it
records the logical sequence. Only after all items in a native batch succeed
does it commit that batch's response-end checkpoint. A failure replays from the
earlier checkpoint; already applied logical sequences are suppressed. An exact
subset cut between publications must use sequences, even when those publications
share one response-end offset. Subset capture and reconciliation are still phase
7 work.

## Reader lifecycle patch

Published `@durable-streams/client@0.2.7` can settle `closed` when its response
producer reaches the physical tail, before JSON parsing or an asynchronous
`subscribeJson` callback completes. Later failures cannot reject an already
settled promise. The adapter needs a reliable consumption completion boundary.

The committed pnpm patch makes `closed`, after subscribing to JSON, observe that
subscription's completion and failures. It also stops swallowing arbitrary
`TypeError`s containing “Body”. ESM, CJS and package source contain the change;
the public declarations are unchanged. The client build bundles this patched
reader and the private wire module, so package consumers need neither a pnpm
patch nor an unpublished protocol dependency.

Verification commands:

```sh
pnpm test:transport
pnpm test:durable-reader-install
pnpm test:durable-reader
pnpm build
pnpm test:package
pnpm test:proxy
```

The clean-install check copies manifests/lockfile/patches into an isolated
directory, performs a frozen install, and probes both runtime formats. The
conformance command verifies that the upstream source adapter resolves the same
patched artifact, then runs its consumer/lifecycle suites. The published suite's
`--run ts` shortcut refers to a missing packaged adapter path; our runner invokes
the unmodified source adapter explicitly. The original and packed replacement
run through the same checked-in GET-only proxy routes in `test:proxy`; run
`test:package` first to create those isolated installs.

These checks do not establish Function lifetime, real source metadata, Object
Storage, SSE/subsets in the facade, or React/TanStack integration support.
