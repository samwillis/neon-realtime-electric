# Electric client provenance

Imported from `electric-sql/electric`, revision
`3f9809fb0bd894cb74470d434749673492ad7acf`, package
`@electric-sql/client@1.5.28` under `packages/typescript-client/`.

The complete package subtree and repository `LICENSE` were imported. The
[manifest](../../upstream/electric-client.json) records every original Git blob
hash. `pnpm check:upstream` verifies the unchanged files. To reproduce a pristine
import in a fresh directory:

```sh
node scripts/import-electric-client.mjs --destination .artifacts/pristine-client
```

Workspace adaptations at the initial import:

- `package.json`: independent package name/version; build and test scripts and
  shared development tooling. Public conditional exports and runtime dependency
  remain unchanged.
- `tsconfig.json`: ES module/bundler resolution for this workspace's Vitest
  version. Strict checks, including library declaration checks, are enabled.
  The root TypeScript configuration maps upstream's one absolute test import
  (`packages/typescript-client/src`) to this package without editing that test.
- Added `vitest.baseline.config.ts`: all service-free tests and both declaration
  suites, including files omitted by upstream's unit configuration. The three
  excluded suites require a real Electric service/Postgres/cache proxy; they are
  retained and tracked in `docs/compatibility.md`.

The original package is an isolated test oracle, never a runtime dependency of
this fork. Commit `133f2b1` is the verified pristine source/test baseline.

## Phase 1 transport boundary

- `src/client.ts`: uses a private `ShapeTransport` for reads and subset fetching.
  URL construction, parsing/mapping, state transitions, retries, pause/refresh,
  snapshot tracking and subscriber serialization remain in the shared client.
- `src/transport/types.ts`: normalized metadata/read/snapshot callbacks; no new
  public constructor option or export.
- `src/transport/electric-http.ts`: the existing fetch wrapper chain, HTTP/SSE
  consumption and snapshot decoding, kept as the reference adapter. It returns
  metadata and raw messages, not synthetic HTTP responses.
- All imported tests/fixtures remain unchanged. `src/client.ts` is explicitly
  listed as a reviewed local modification in the import manifest; the importer
  still reproduces its pristine upstream content in a fresh destination.

The existing HTTP adapter is still selected during this refactor. It is not the
final production transport; phase 2 selects the real Durable Streams reader.
