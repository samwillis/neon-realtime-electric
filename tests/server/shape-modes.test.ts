import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { once } from 'node:events'
import {
  Shape,
  ShapeStream,
  type Message,
} from '../../packages/client/src/index.js'
import { describe, expect, it } from 'vitest'
import { ShapeOwner } from '../../packages/server/src/shape-owner.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { resolveConfiguredShape } from '../../packages/server/src/definition.js'
import type {
  SourceObserver,
  RealtimeSource,
} from '../../packages/server/src/realtime-source.js'
import { decodeReadProof } from '../../packages/protocol/src/read-proof.js'
import type { RawChange } from '../../packages/protocol/src/index.js'

const schema = {
  id: { type: 'int4' },
  label: { type: 'text' },
  unchanged: { type: 'text' },
}
function sourceFixture() {
  const observers: SourceObserver[] = []
  const source: RealtimeSource = {
    async subscribe(_, observer) {
      observers.push(observer)
      observer.event({
        type: 'schema',
        columns: [
          { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
          { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
          { name: 'unchanged', type_oid: 25, typmod: -1, codec: 'pg_text' },
        ],
      })
      observer.event({
        type: 'baseline',
        rows: [{ row_key: 'a'.repeat(64), values: ['1', 'before', 'same'] }],
        mvcc: { xmin: '5', xmax: '6', xip: [] },
      })
      return { close() {} }
    },
    close() {},
  }
  return { source, observers }
}
function definition(params = '') {
  const url = new URL(`http://local/shape?${params}`)
  if (!url.searchParams.has('table')) url.searchParams.set('table', 'items')
  return resolveConfiguredShape(url, [
    {
      namespace: 'public',
      table: 'items',
      primaryKey: ['id'],
      columns: schema,
    },
  ])
}
describe('replica and changes-only boundaries', () => {
  it.each(['default', 'full'] as const)(
    'emits %s replica updates/deletes with the correct projection and old changed values',
    async (replica) => {
      const f = sourceFixture()
      const owner = new ShapeOwner({
        source: f.source,
        definition: definition(`replica=${replica}`),
      })
      try {
        await owner.start()
        await owner.settled()
        const offset = owner.log.head.offset
        f.observers[0].event({
          type: 'publication',
          changes: [
            {
              op: 'upsert',
              row_key: 'a'.repeat(64),
              values: ['1', null, 'same'],
            },
          ],
          txids: ['6'],
          frontier: { lsn: '0/16' },
        })
        f.observers[0].event({
          type: 'publication',
          changes: [{ op: 'remove', row_key: 'a'.repeat(64) }],
          txids: ['7'],
          frontier: { lsn: '0/18' },
        })
        await owner.settled()
        const changes = (await owner.log.read(offset)).envelopes.flatMap((e) =>
          e.kind === 'publication' ? e.changes : []
        ) as RawChange[]
        expect(changes[0].value).toEqual(
          replica === 'full'
            ? { id: '1', label: null, unchanged: 'same' }
            : { id: '1', label: null }
        )
        expect(changes[0].old_value).toEqual(
          replica === 'full' ? { label: 'before' } : undefined
        )
        expect(changes[1].value).toEqual(
          replica === 'full'
            ? { id: '1', label: null, unchanged: 'same' }
            : { id: '1' }
        )
        expect(changes[1]).not.toHaveProperty('old_value')
      } finally {
        owner.close()
      }
    }
  )
  it.each(['log=changes_only', 'offset=now', 'log=changes_only&offset=now'])(
    'starts empty at a real source baseline barrier for %s, then streams real update classification',
    async (mode) => {
      const f = sourceFixture()
      const handler = createShapeHandler({
        source: f.source,
        async authorize(request) {
          return {
            authorizationKey: 'modes',
            definition: definition(
              new URL(request.url).searchParams.toString()
            ),
          }
        },
      })
      const request = (params: string) => {
        const url = new URL(`http://local/shape?table=items&${mode}`)
        for (const [key, value] of new URLSearchParams(params))
          url.searchParams.set(key, value)
        return handler.fetch(new Request(url))
      }
      try {
        const first = await request('')
        expect(first.status).toBe(200)
        const handle = first.headers.get('electric-handle')!
        const firstProof = decodeReadProof(first.headers.get('electric-cursor'))
        expect(firstProof.offset).toBe(
          mode.includes('offset=now') ? 'now' : '-1'
        )
        let response = first
        let envelopes = await first.json()
        while (
          !envelopes.some(
            (e: { kind: string }) => e.kind === 'snapshot-complete'
          )
        ) {
          response = await request(
            `handle=${handle}&offset=${response.headers.get('electric-offset')}`
          )
          envelopes.push(...(await response.json()))
        }
        expect(envelopes.map((e: { kind: string }) => e.kind)).toEqual([
          'initializing',
          'snapshot-complete',
        ])
        expect(envelopes.at(-1).evidence.snapshot).toEqual({
          xmin: '5',
          xmax: '6',
          xip_list: [],
        })
        f.observers[0].event({
          type: 'publication',
          changes: [
            {
              op: 'upsert',
              row_key: 'a'.repeat(64),
              values: ['1', 'live', 'same'],
            },
          ],
          txids: ['6'],
          frontier: { lsn: '0/16' },
        })
        const next = await request(
          `handle=${handle}&offset=${response.headers.get('electric-offset')}`
        )
        const body = await next.json()
        expect(body).toMatchObject([
          {
            kind: 'publication',
            changes: [
              {
                headers: { operation: 'update' },
                value: { id: '1', label: 'live' },
              },
            ],
          },
        ])
      } finally {
        handler.close()
        await handler.drain()
      }
    }
  )
  it('hydrates offset=now on an existing handle without replaying its history', async () => {
    const f = sourceFixture()
    const handler = createShapeHandler({
      source: f.source,
      async authorize() {
        return { authorizationKey: 'modes', definition: definition() }
      },
    })
    try {
      const first = await handler.fetch(new Request('http://local/shape'))
      const handle = first.headers.get('electric-handle')!
      await first.arrayBuffer()
      const head = await handler.fetch(
        new Request(`http://local/shape?handle=${handle}&offset=now`)
      )
      expect(head.status).toBe(204)
      expect(
        decodeReadProof(head.headers.get('electric-cursor'))
      ).toMatchObject({ offset: 'now', baselineComplete: true })
      expect(f.observers).toHaveLength(1)
    } finally {
      handler.close()
      await handler.drain()
    }
  })
})

for (const liveSse of [false, true])
  describe(`cold changes-only/now through the real facade and reader (SSE=${liveSse})`, () => {
    it.each([
      { log: 'full' as const, offset: 'now' as const },
      { log: 'changes_only' as const, offset: '-1' as const },
      { log: 'changes_only' as const, offset: 'now' as const },
    ])(
      'keeps history private and classifies subsequent changes for $log/$offset',
      async (options) => {
        const f = sourceFixture()
        const handler = createShapeHandler({
          source: f.source,
          longPollMs: 20,
          sseConnectionMs: 100,
          async authorize(request) {
            return {
              authorizationKey: 'modes',
              definition: definition(
                new URL(request.url).searchParams.toString()
              ),
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
            res.writeHead(response.status, Object.fromEntries(response.headers))
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
        const url = `http://127.0.0.1:${(http.address() as { port: number }).port}/application/shapes`
        const abort = new AbortController()
        const errors: unknown[] = []
        const messages: Message[] = []
        try {
          const stream = new ShapeStream({
            url,
            params: { table: 'items' },
            ...options,
            liveSse,
            signal: abort.signal,
            onError: (e) => {
              errors.push(e)
            },
          })
          const shape = new Shape(stream)
          stream.subscribe((batch) => {
            messages.push(...batch)
          })
          await expect.poll(() => shape.isUpToDate).toBe(true)
          expect(shape.currentRows).toEqual([])
          f.observers[0].event({
            type: 'publication',
            changes: [
              {
                op: 'upsert',
                row_key: 'a'.repeat(64),
                values: ['1', 'changed', 'same'],
              },
            ],
            txids: ['6'],
            frontier: { lsn: '0/16' },
          })
          await expect
            .poll(() =>
              messages.some(
                (m) =>
                  'operation' in m.headers && m.headers.operation === 'update'
              )
            )
            .toBe(true)
          if (options.log === 'changes_only')
            expect(shape.currentRows).toEqual([])
          f.observers[0].event({
            type: 'publication',
            changes: [
              {
                op: 'upsert',
                row_key: 'b'.repeat(64),
                values: ['2', 'new', 'same'],
              },
            ],
            txids: ['7'],
            frontier: { lsn: '0/18' },
          })
          await expect
            .poll(() => shape.currentRows.some((row) => row.id === 2))
            .toBe(true)
          f.observers[0].event({
            type: 'publication',
            changes: [{ op: 'remove', row_key: 'b'.repeat(64) }],
            txids: ['8'],
            frontier: { lsn: '0/20' },
          })
          await expect
            .poll(() => shape.currentRows.some((row) => row.id === 2))
            .toBe(false)
          const resume = new ShapeStream({
            url,
            params: { table: 'items' },
            log: options.log,
            handle: stream.shapeHandle,
            offset: 'now',
            subscribe: false,
            signal: abort.signal,
            onError: (e) => {
              errors.push(e)
            },
          })
          expect(await new Shape(resume).rows).toEqual([])
          expect(resume.shapeHandle).toBe(stream.shapeHandle)
          expect(f.observers).toHaveLength(1)
          expect(errors).toEqual([])
        } finally {
          abort.abort()
          handler.close()
          await handler.drain()
          http.closeAllConnections()
          await new Promise<void>((resolve) => http.close(() => resolve()))
        }
      }
    )
  })
