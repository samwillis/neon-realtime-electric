# Implementation status and compatibility gates

Target: the full implementation plan through phase 7. No phase is complete until
its exit evidence is recorded. Deferred platform affinity is outside the current
local/staging test target, as agreed in `AGENTS.md`.

| Phase | Status | Evidence / remaining gate |
|---|---|---|
| 0: pristine import and compatibility baseline | Passed | 80-file pinned import; 78 unchanged-file checks; build/typecheck; 553 runtime/declaration checks; identical packed consumers on TS 5.5.4/5.9.3; 62 exports across four declaration entry points |
| 1: internal transport boundary | Pending | Original HTTP behavior and unchanged types must pass after refactor |
| 2: real Durable Streams delivery | Pending | Reference server, initialization barriers, sequences, proxy/read/reset tests |
| 3: real staging Realtime | Pending | Ordered SDK evidence, raw values, filter membership and transaction acknowledgement |
| 4: Function handler and lifecycle | Pending | Real handler through `neon dev`, local routing/failure tests |
| 5: Object Storage | Pending | Stable replay ranges, upload/read failures and actual 15-minute local TTL |
| 6: replacement package/integrations | Pending | Packed aliases, React and eager TanStack browser/type behavior |
| 7: complete API expansion | Pending | SSE, replica modes, changes-only/now, types, mobile lifecycle, broader SQL, subsets and exact MVCC/cut reconciliation |

## Upstream test disposition

All upstream files are retained in their original relative layout. The baseline
configuration discovers all `test/**/*.test.ts` except these service-bound suites:

| Suite | Current disposition | Required subsequent evidence |
|---|---|---|
| `client.test.ts` | Original Electric service reference | Preserve application assertions through the transport-fixture adaptation and real Neon integration suite |
| `integration.test.ts` | Original Electric service/Postgres reference | Port relevant behavior to staging Postgres/Realtime and real Durable Streams |
| `cache.test.ts` | Original Electric/nginx/Docker reference | Replace protocol-specific assertions with native read/cache conformance and unchanged proxy fixtures |
| All other runtime tests | Unchanged, service-free baseline | Run with the initial source import; preserve or explicitly map when transport changes |
| `client.test-d.ts`, `helpers.test-d.ts` | Unchanged declaration tests | Keep passing alongside public API reports and packed consumer compilation |

These dispositions describe test execution, not feature support. An imported test
that has not been executed is not passing evidence. The fork still uses Electric
HTTP at phase 0; no Neon compatibility is claimed yet.

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
