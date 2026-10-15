# Upstream changes

## Electric client

The complete pinned import, license, manifest and reproduction command are
documented in `packages/client/UPSTREAM.md`. The original source/test baseline is
commit `133f2b1`. The import script checks all untouched files and names locally
modified source files explicitly; it never silently updates the pinned revision.

The first implementation change extracts an internal transport contract from
`ShapeStream`. The public API, original URL builder, parser/materializer and
behavior tests stay in place. HTTP-specific delivery is a reference adapter;
Durable Streams will provide the production adapter without making the browser
consume a fabricated Electric HTTP response body.

## Neon Realtime

No SDK patch exists yet. The implementation plan permits a pinned pnpm patch to
expose existing ordered baseline/MVCC/progress/frontier evidence if the published
SDK hides it. Any patch must modify the runtime and declaration artifacts used by
the export map, pass a clean frozen install and real staging integration tests,
and remain confined to the server application dependency boundary.

## Durable Streams reader lifecycle

`@durable-streams/client@0.2.7` required a demonstrated JSON subscription lifecycle
fix: response producer completion could settle `closed` before parsing and async
callbacks. The committed pnpm patch changes only JSON subscription completion
and error propagation, with ESM/CJS/source artifacts kept together. Existing
public declarations are unchanged. The client bundles the patched dependency;
consumers do not inherit or need this repository's pnpm patch configuration.

Run `test:durable-reader-install`, `test:durable-reader`, `test:transport`, packed
package/proxy checks and public API checks when updating/removing the patch.
The upstream consumer/lifecycle suite's declared skips remain recorded in
`docs/compatibility.md`; they are not counted as passes. This patch is separate
from any future Neon Realtime SDK metadata patch.
