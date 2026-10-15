# Realtime SDK evidence adapter

The server pins published `@neon/realtime@0.1.0`. The pnpm patch in
`patches/@neon__realtime@0.1.0.patch` exposes evidence already present in its
validated protocol and reconciler. It does not generate source evidence.

The added `subscription.onSource()` callback exposes:

- schema at admission, before baseline buffering;
- raw baseline rows and the baseline's MVCC snapshot after reconciliation;
- complete publication changes, exact transaction IDs and the commit frontier;
- ordered MVCC progress, including progress with no visible row changes;
- reset/connection synchronization loss so an existing handle can fail closed.

`{ materialize: false, rawValues: true }` bypasses Postgres value parsing and
preserves text/null/base64 cells, including unknown/custom type OIDs. The shape
owner converts bytea cells to PostgreSQL hex text and leaves other values for
the original Electric parser. Schema/type metadata remains a separate boundary.

The patch updates the built runtime and declarations selected by the export map,
including the server's managed direct-subscription wrapper. It passes a complete
publication frontier through reconciliation, retaining it for publications
buffered during a baseline. Progress still follows the SDK's existing rule: it
is delivered only to live subscriptions, after their covered data is installed.

The server adapter invalidates a handle on a source reset or replacement baseline.
It does not rely on SDK reconnection to preserve a shape epoch. It isolates SDK
errors and cancellation, and never returns a sealed query to a browser.

Run `pnpm test:server` for the real-WebSocket adapter/owner/handler checks and
`pnpm test:sdk-patch` for an independent frozen install, runtime wire probe and
exported declaration checks. The latter also checks the Durable Streams reader
patch. Missing staging credentials fail `pnpm test:integration`; fixture passes
are not a substitute for that gate.

This is a server workspace install patch, not a patch browser consumers need to
install. Before distributing a server package, use an upstream supported release
or explicitly bundle and verify the patched SDK with its attribution. Upstream
source changes and release adoption remain outstanding; the local `neon-pkgs`
checkout is not an install dependency.
