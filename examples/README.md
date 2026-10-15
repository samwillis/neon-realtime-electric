# Package replacement examples

These small consumers keep their `@electric-sql/client` imports. The React and
TanStack examples use the published integration libraries without modifications.
The custom basic example demonstrates column mapping and a custom text parser.
All use one configured proxy URL, with query-string handles and dynamic auth.
The example auth tokens are test values; production applications supply their
own authentication. TanStack's mutation endpoint is supplied by the test harness.

From the repository root:

```sh
pnpm test:integration-packages
# Keep the object-backed local Function running against staging services:
pnpm test:browser
```

The first command packs the replacement, creates independent reference and
candidate installations under ignored `.artifacts/integration-packages`, and
copies these same sources to each. Both compile on TypeScript 5.5.4 and 5.9.3
and build with Vite. A direct dependency plus pnpm override ensures that the
application and both integrations resolve the same client package and exported
error class. The reference install is outside the candidate override.

The browser test uses the built candidate, installed Chrome (override
`CHROME_PATH` when needed), real staging Postgres and the Function running locally
under `neon dev`. It exercises baseline/live rows, custom parser/mapping, auth
recovery, owner loss/refetch and an optimistic TanStack write acknowledged by
its real database transaction ID, plus acknowledgement of a transaction that
leaves the row unchanged. Only test-owned rows and handles are deleted.
Each basic/custom/React/TanStack example runs in both polling and SSE mode.
Reports are written under ignored `.artifacts/staging`.

No client package has been published. The install fixture aliases a local tarball;
use a released package version in an application once one exists. See the main
implementation plan for the alias/override template. Browser fixtures do not
require consumers to configure our monorepo's pnpm patches.
