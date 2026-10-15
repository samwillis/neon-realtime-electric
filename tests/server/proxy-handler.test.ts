import { createServer } from 'node:http'
import { once } from 'node:events'
import { describe, expect, it } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { resolveConfiguredShape } from '../../packages/server/src/definition.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import { proxyFixture } from '../compatibility/proxies/fixtures.js'

for (const backend of ['memory', 'objects'])
  describe(`production handler through unchanged proxy routes (${backend})`, () => {
    for (const mode of [
      'transparent',
      'electric-allowlist',
      'trusted-predicate',
    ] as const)
      it(`preserves eager/live/refetch through ${mode}`, async () => {
        const observers: SourceObserver[] = []
        const queries: unknown[] = []
        let label = 'initial'
        const schema = {
          id: { type: 'int4' },
          label: { type: 'text' },
          tenant_id: { type: 'text' },
        }
        const baseline = (observer: SourceObserver) =>
          observer.event({
            type: 'baseline',
            rows: [
              {
                row_key: 'a'.repeat(64),
                values: ['1', label, 'trusted-tenant'],
              },
            ],
            mvcc: { xmin: '1', xmax: '2', xip: [] },
          })
        const store = new FixtureObjectStore()
        const handler = createShapeHandler({
          longPollMs: 20,
          createLog:
            backend === 'objects'
              ? ({ handle, expiresAt }) =>
                  new ObjectShapeLog({
                    store,
                    prefix: `proxies/${expiresAt}/${handle}`,
                    sealBytes: 1,
                  })
              : undefined,
          source: {
            async subscribe(query, observer) {
              queries.push(query)
              observers.push(observer)
              observer.event({
                type: 'schema',
                columns: [
                  { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
                  { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
                  {
                    name: 'tenant_id',
                    type_oid: 25,
                    typmod: -1,
                    codec: 'pg_text',
                  },
                ],
              })
              if (observers.length > 1) baseline(observer)
              return { close() {} }
            },
            close() {},
          },
          async authorize(request) {
            const url = new URL(request.url)
            // Fixed server-side table supplies the route's trusted shape when
            // an unchanged Electric allowlist intentionally drops table/where.
            if (!url.searchParams.has('table'))
              url.searchParams.set('table', 'items')
            return {
              authorizationKey: 'proxy-test',
              definition: resolveConfiguredShape(url, [
                {
                  namespace: 'public',
                  table: 'items',
                  primaryKey: ['id'],
                  columns: schema,
                },
              ]),
            }
          },
        })
        const http = createServer((req, res) => {
          void (async () => {
            const abort = new AbortController()
            res.on('close', () => abort.abort())
            const response = await handler.fetch(
              new Request(`http://local${req.url}`, { signal: abort.signal })
            )
            res.writeHead(
              response.status,
              Object.fromEntries(response.headers.entries())
            )
            res.end(Buffer.from(await response.arrayBuffer()))
          })().catch(() => {
            if (!res.destroyed) res.writeHead(500).end()
          })
        })
        http.listen(0, '127.0.0.1')
        await once(http, 'listening')
        const proxy = await proxyFixture(
          `http://127.0.0.1:${(http.address() as { port: number }).port}/api/shape`,
          mode,
          500
        )
        const abort = new AbortController()
        try {
          const stream = new ShapeStream({
            url: `${proxy.url}/application/shapes?custom=a%2Bb`,
            params: { table: 'items' },
            signal: abort.signal,
          })
          const shape = new Shape(stream)
          await expect.poll(() => stream.shapeHandle).toBeTruthy()
          expect(shape.isUpToDate).toBe(false)
          baseline(observers[0])
          await expect
            .poll(() => shape.currentRows)
            .toEqual([{ id: 1, label: 'initial', tenant_id: 'trusted-tenant' }])
          label = 'changed'
          observers[0].event({
            type: 'publication',
            changes: [
              {
                op: 'upsert',
                row_key: 'a'.repeat(64),
                values: ['1', label, 'trusted-tenant'],
              },
            ],
            txids: ['2'],
            frontier: { lsn: '0/12' },
          })
          await expect.poll(() => shape.currentRows[0]?.label).toBe('changed')
          const previous = stream.shapeHandle
          observers[0].error(new Error('intentional owner loss'))
          await expect.poll(() => stream.shapeHandle).not.toBe(previous)
          await expect.poll(() => shape.currentRows[0]?.label).toBe('changed')
          expect(queries.length).toBe(2)
          if (mode === 'trusted-predicate')
            expect(queries[0]).toMatchObject({ parameters: ['trusted-tenant'] })
          for (const url of proxy.requests) {
            expect(url.pathname).toBe('/application/shapes')
            expect(url.searchParams.get('custom')).toBe('a+b')
          }
        } finally {
          abort.abort()
          await proxy.stop()
          handler.close()
          await handler.drain()
          http.closeAllConnections()
          await new Promise<void>((resolve) => http.close(() => resolve()))
        }
      })
  })
