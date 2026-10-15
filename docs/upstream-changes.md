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
