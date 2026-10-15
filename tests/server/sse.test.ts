import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { once } from 'node:events'
import { describe, expect, it } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'

for (const backend of ['memory', 'objects'])
  describe(`native SSE shape delivery (${backend})`, () => {
    for (const aliasesOnly of [false, true])
      it(`streams, reconnects, refreshes and refetches using ${aliasesOnly ? 'Electric' : 'native'} headers`, async () => {
        const observers: SourceObserver[] = []
        let label = 'initial'
        const store = new FixtureObjectStore()
        const handler = createShapeHandler({
          sseConnectionMs: 100,
          longPollMs: 20,
          source: {
            async subscribe(_, observer) {
              observers.push(observer)
              observer.event({
                type: 'schema',
                columns: [
                  { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
                  { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
                ],
              })
              observer.event({
                type: 'baseline',
                rows: [{ row_key: 'a'.repeat(64), values: ['1', label] }],
                mvcc: { xmin: '1', xmax: '2', xip: [] },
              })
              return { close() {} }
            },
            close() {},
          },
          createLog:
            backend === 'objects'
              ? ({ handle, expiresAt }) =>
                  new ObjectShapeLog({
                    store,
                    prefix: `sse/${expiresAt}/${handle}`,
                    sealBytes: 1,
                  })
              : undefined,
          async authorize() {
            return {
              authorizationKey: 'sse-test',
              definition: {
                namespace: 'public',
                table: 'items',
                primaryKey: ['id'],
                projection: ['id', 'label'],
                schema: { id: { type: 'int4' }, label: { type: 'text' } },
                query: { sql: 'select id,label from public.items' },
              },
            }
          },
        })
        const requests: { url: URL; method: string }[] = []
        const http = createServer((req, res) => {
          void (async () => {
            const url = new URL(req.url!, 'http://local')
            requests.push({ url, method: req.method! })
            const abort = new AbortController()
            res.on('close', () => abort.abort())
            const response = await handler.fetch(
              new Request(url, { signal: abort.signal, method: req.method })
            )
            const headers = Object.fromEntries(
              [...response.headers].filter(
                ([name]) =>
                  !aliasesOnly ||
                  name.startsWith('electric-') ||
                  ['content-type', 'cache-control'].includes(name)
              )
            )
            res.writeHead(response.status, headers)
            if (response.body)
              Readable.fromWeb(
                response.body as import('node:stream/web').ReadableStream
              )
                .on('error', () => res.destroy())
                .pipe(res)
            else res.end()
          })().catch(() => res.destroy())
        })
        http.listen(0, '127.0.0.1')
        await once(http, 'listening')
        const origin = `http://127.0.0.1:${(http.address() as { port: number }).port}`
        const abort = new AbortController()
        const errors: unknown[] = []
        try {
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: `${origin}/application/shapes?custom=a%2Bb`,
            params: { table: 'items' },
            liveSse: true,
            signal: abort.signal,
            onError: (error) => {
              errors.push(error)
            },
          })
          const shape = new Shape(stream)
          await expect
            .poll(() => shape.currentRows)
            .toEqual([{ id: 1, label: 'initial' }])
          const sseRequests = () =>
            requests.filter(
              (r) => r.url.searchParams.get('live_sse') === 'true'
            )
          await expect.poll(() => sseRequests().length).toBeGreaterThan(0)
          const handle = stream.shapeHandle
          label = 'value\r\nevent: control\ndata: attack\n\n😀'
          observers[0].event({
            type: 'publication',
            changes: [
              { op: 'upsert', row_key: 'a'.repeat(64), values: ['1', label] },
            ],
            txids: ['2'],
            frontier: { lsn: '0/20' },
          })
          await expect.poll(() => shape.currentRows[0]?.label).toBe(label)
          // A bounded connection ends naturally. The real reader reconnects,
          // preserving both the same handle and the consumed byte checkpoint.
          await expect.poll(() => sseRequests().length).toBeGreaterThan(1)
          expect(stream.shapeHandle).toBe(handle)
          await stream.forceDisconnectAndRefresh()
          await expect.poll(() => shape.currentRows[0]?.label).toBe(label)
          observers[0].error(new Error('intentional SSE owner loss'))
          await expect.poll(() => stream.shapeHandle).not.toBe(handle)
          await expect.poll(() => shape.currentRows[0]?.label).toBe(label)
          expect(errors).toEqual([])
          for (const { url, method } of requests) {
            expect(url.pathname).toBe('/application/shapes')
            expect(url.searchParams.get('custom')).toBe('a+b')
            expect(method).toBe('GET')
            if (url.searchParams.get('live_sse') === 'true') {
              expect(url.searchParams.get('live')).toBe('true')
              expect(url.searchParams.get('experimental_live_sse')).toBe('true')
            }
          }
        } finally {
          abort.abort()
          handler.close()
          await handler.drain()
          http.closeAllConnections()
          await new Promise<void>((resolve) => http.close(() => resolve()))
        }
      })
  })
