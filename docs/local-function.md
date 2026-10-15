# Local Function development

Use Sam's staging Postgres/Realtime and keep the Function local. The project is
linked in ignored `.neon`; credentials are in ignored `.env.local`.

```sh
# Configure any custom hosts and CLI profile in ignored .env.local.
pnpm install --frozen-lockfile
# The linked branch must already have Realtime; no deploy/config apply is needed.
pnpm dev:function
```

The local example serves the configured `public.electric_items` table. Apply
`apps/function/example.sql` to the dedicated staging test database to initialize
it. This is an explicit local example allowlist, not an application authorization
implementation. Real applications supply authentication and a trusted predicate
in the handler's `authorize` callback, which runs on every read.

```sh
pnpm test:local-baseline
pnpm test:local-lifecycle
# After changes are stable; leave neon dev running for the entire test:
pnpm test:local-expiry
```

`test:local-baseline` uses the copied Electric facade with the real Durable Streams
reader against `http://localhost:4327/api/shape`. Override `SHAPE_URL` to test
another existing proxy path. It checks snapshot readiness, unchanged path/origin,
GET-only requests and saved-offset resume. The separate lifecycle script proves real no-reader writes/replay, live updates,
filter exits/reentries, deletes and public transaction-ID conversion against SQL.
Logical replication must be enabled; see `testing-environment.md`.

The expiry script runs a real 15-minute interval and checks the handle shortly
before and after its absolute deadline, then requires a replacement query-string
handle and no redirect. A cancelled run is not passing evidence.

Neon CLI 8.3.5 rejected `realtime` in `neon.ts`, so that service was provisioned
through the staging API before the Function declaration was added. The checked-in
config declares the local Function only. The local CLI injects database variables;
it inherits server-only Realtime variables from the loaded environment. If needed,
start it with Node's `--env-file=.env.local` and the installed Neon CLI entry point.
Do not deploy merely to satisfy a local configuration mismatch.

In the observed single-source CLI workflow, changes inside workspace dependencies
did not cause a rebundle. Restart `neon dev` after changing `packages/server` to
ensure tests run the new bundle. Restarting kills owner state, so do it before
starting an expiry/lifecycle acceptance run.

The handler's initial profile supports GET/optional HEAD/DELETE, JSON catch-up and
long polling, configured primary-keyed tables, projections and a bounded filter
grammar (scalar comparisons/IS NULL joined with AND). Identifiers are resolved
against the allowlist and operands are bound parameters. Unsupported options fail
explicitly. SSE, advanced SQL, subsets and replica modes remain later gates.

For Object Storage mode, load `.env.storage.local` alongside `.env.local` and set
`NEON_SHAPE_LOG_BUCKET=your-private-shape-log-bucket`. `NEON_SHAPE_SEAL_BYTES=1` forces small
sealed objects for acceptance tests; omit it for the normal 256 KiB threshold.
`NEON_SHAPE_DIAGNOSTICS=true` logs event names and owner closure reasons without
credentials. The local process sweeps expired prefixes at startup and every
minute; this is not a platform background-execution guarantee.

To investigate full-duration failures, optionally set
`NEON_SHAPE_DIAGNOSTICS_FILE` to an absolute ignored file path alongside
`NEON_SHAPE_DIAGNOSTICS=true`. Each safe event record includes a process ID and
module-instance ID; boot and owner-close records identify reloads or source loss.
The expiry runner checks optional HEAD liveness every 30 seconds and saves both a
latest report and a per-run report. These probes are test instrumentation; normal
application/proxy flows still require GET only. Finish code/build changes before
starting the acceptance run to avoid mixing hot reload with expiry behavior.
