import { createServer } from 'node:http'
import { once } from 'node:events'
import { afterEach, describe, expect, it } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import type {
  SourceEvent,
  SourceObserver,
} from '../../packages/server/src/realtime-source.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import { decodeReadProof } from '../../packages/protocol/src/read-proof.js'

const cleanup: (() => unknown)[] = []
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close()
})
const columns: Extract<SourceEvent, { type: 'schema' }>['columns'] = [
  { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
  { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
]
const baseline: SourceEvent = {
  type: 'baseline',
  rows: [{ row_key: 'a'.repeat(64), values: ['1', 'initial'] }],
  mvcc: { xmin: '10', xmax: '10', xip: [] },
}

function fixture(storage = false) {
  const objectStore = new FixtureObjectStore()
  const logs: ObjectShapeLog[] = []
  const observers: SourceObserver[] = []
  let now = 1000
  let stopped = 0
  const handler = createShapeHandler({
    createLog: storage
      ? ({ handle, expiresAt }) => {
          const log = new ObjectShapeLog({
            store: objectStore,
            prefix: `handler/${expiresAt}/${handle}`,
            sealBytes: 1,
          })
          logs.push(log)
          return log
        }
      : undefined,
    source: {
      async subscribe(_, observer, signal) {
        observers.push(observer)
        signal.addEventListener('abort', () => stopped++)
        return { close() {} }
      },
      close() {},
    },
    now: () => now,
    longPollMs: 25,
    maxEnvelopesPerRead: 2,
    async authorize(request) {
      const url = new URL(request.url)
      if (url.searchParams.get('table') !== 'items')
        throw new Error('Unauthorized')
      return {
        authorizationKey: request.headers.get('authorization') ?? 'public',
        definition: {
          query: { sql: 'select id, label from public.items' },
          namespace: 'public',
          table: 'items',
          primaryKey: ['id'],
          projection: ['id', 'label'],
          schema: { id: { type: 'int4' }, label: { type: 'text' } },
        },
      }
    },
  })
  cleanup.push(async () => {
    handler.close()
    await Promise.all(logs.map((log) => log.disposed))
  })
  const request = (params = '', method = 'GET', headers?: HeadersInit) =>
    handler.fetch(
      new Request(`http://local.test/proxy/shape?table=items&${params}`, {
        method,
        headers,
      })
    )
  const push = (event: SourceEvent, index = 0) => observers[index].event(event)
  return {
    handler,
    objectStore,
    logs,
    request,
    push,
    observers,
    setNow: (value: number) => {
      now = value
    },
    stopped: () => stopped,
  }
}

describe.each(['memory', 'objects'])(
  'Function-compatible shape read handler: %s',
  (backend) => {
    const createFixture = () => fixture(backend === 'objects')
    it('returns an early handle on the same URL and truthful start-boundary proofs across split reads', async () => {
      const f = createFixture()
      const first = await f.request('offset=-1')
      expect(first.status).toBe(200)
      const handle = first.headers.get('electric-handle')!
      expect(first.headers.has('location')).toBe(false)
      expect(first.headers.has('electric-schema')).toBe(false)
      expect((await first.json()).map((e: { kind: string }) => e.kind)).toEqual(
        ['initializing']
      )
      const offset = first.headers.get('electric-offset')!
      f.push({ type: 'schema', columns })
      f.push(baseline)
      const next = await f.request(`handle=${handle}&offset=${offset}`)
      expect(
        decodeReadProof(next.headers.get('electric-cursor'))
      ).toMatchObject({
        offset,
        afterSeq: '1',
        baselineComplete: false,
      })
      expect((await next.json()).map((e: { kind: string }) => e.kind)).toEqual([
        'snapshot-chunk',
        'snapshot-complete',
      ])
      const end = next.headers.get('electric-offset')!
      const tail = await f.request(`handle=${handle}&offset=${end}&live=true`)
      expect(tail.status).toBe(204)
      expect(
        decodeReadProof(tail.headers.get('electric-cursor')).baselineComplete
      ).toBe(true)
      expect(tail.headers.get('electric-schema')).toBe(
        JSON.stringify({ id: { type: 'int4' }, label: { type: 'text' } })
      )
    })

    it('requires authorization on every read and binds handles to the authorized definition/context', async () => {
      const f = createFixture()
      const first = await f.request('offset=-1', 'GET', {
        authorization: 'tenant-a',
      })
      const handle = first.headers.get('electric-handle')!
      expect(
        (
          await f.request(`handle=${handle}`, 'GET', {
            authorization: 'tenant-b',
          })
        ).status
      ).toBe(403)
      expect(
        (
          await f.handler.fetch(
            new Request(
              `http://local.test/proxy/shape?table=secret&handle=${handle}`
            )
          )
        ).status
      ).toBe(403)
      expect(f.observers).toHaveLength(1)
    })

    it('refetches after absolute expiry, owner loss and source failure; deletion cancels ingestion', async () => {
      const f = createFixture()
      const first = await f.request()
      const handle = first.headers.get('electric-handle')!
      f.setNow(901000)
      const expired = await f.request(`handle=${handle}`)
      expect(expired.status).toBe(409)
      const secondHandle = expired.headers.get('electric-handle')!
      expect(secondHandle).not.toBe(handle)
      expect(f.stopped()).toBe(1)
      expect(f.handler.activeShapes).toBe(1)
      f.observers[1].error(new Error('source failed'))
      const failed = await f.request(`handle=${secondHandle}`)
      expect(failed.status).toBe(409)
      const thirdHandle = failed.headers.get('electric-handle')!
      expect((await f.request(`handle=${thirdHandle}`, 'DELETE')).status).toBe(
        204
      )
      expect(f.handler.activeShapes).toBe(0)
      expect(f.stopped()).toBe(3)
      const missing = await f.request('handle=missing')
      expect(missing.status).toBe(409)
      expect(missing.headers.get('electric-handle')).toBeTruthy()
    })

    it('provides optional HEAD and explicit errors without requiring a bootstrap endpoint', async () => {
      const f = createFixture()
      expect((await f.request('', 'HEAD')).status).toBe(404)
      const first = await f.request()
      const handle = first.headers.get('electric-handle')!
      const readsBeforeHead = f.objectStore.gets.length
      const head = await f.request(`handle=${handle}`, 'HEAD')
      expect(head.status).toBe(200)
      expect(head.headers.get('Stream-Up-To-Date')).toBe('true')
      expect(f.objectStore.gets.length).toBe(readsBeforeHead)
      expect((await f.request(`handle=${handle}&offset=123_456`)).status).toBe(
        400
      )
      expect((await f.request('', 'POST')).status).toBe(405)
      expect((await f.request('offset=now')).status).toBe(200)
      expect((await f.request('live=sse')).status).toBe(400)
    })

    it('runs the production copied client and native reader against the real Fetch handler over HTTP', async () => {
      const f = createFixture()
      const server = createServer((req, res) => {
        const abort = new AbortController()
        res.on('close', () => abort.abort())
        const request = new Request(`http://127.0.0.1${req.url}`, {
          method: req.method,
          headers: req.headers as Record<string, string>,
          signal: abort.signal,
        })
        void f.handler
          .fetch(request)
          .then(async (response) => {
            res.writeHead(response.status, Object.fromEntries(response.headers))
            res.end(Buffer.from(await response.arrayBuffer()))
          })
          .catch(() => {
            if (!res.destroyed) res.writeHead(500).end()
          })
      })
      server.listen(0, '127.0.0.1')
      await once(server, 'listening')
      cleanup.push(
        () =>
          new Promise<void>((resolve) => {
            server.closeAllConnections()
            server.close(() => resolve())
          })
      )
      const port = (server.address() as { port: number }).port
      const abort = new AbortController()
      const stream = new ShapeStream<{ id: number; label: string }>({
        url: `http://127.0.0.1:${port}/proxy/shape`,
        params: { table: 'items' },
        signal: abort.signal,
      })
      const shape = new Shape(stream)
      cleanup.push(() => abort.abort())
      await expect.poll(() => f.observers.length).toBe(1)
      expect(shape.isLoading()).toBe(true)
      f.push({ type: 'schema', columns })
      f.push(baseline)
      await expect
        .poll(() => shape.currentRows)
        .toEqual([{ id: 1, label: 'initial' }])
      await expect.poll(() => shape.isLoading()).toBe(false)
      const handle = stream.shapeHandle
      f.push({
        type: 'publication',
        changes: [
          { op: 'upsert', row_key: 'a'.repeat(64), values: ['1', 'updated'] },
        ],
        txids: ['11'],
        frontier: { lsn: '1/100' },
      })
      await expect
        .poll(() => shape.currentRows)
        .toEqual([{ id: 1, label: 'updated' }])
      f.observers[0].error(new Error('owner continuity lost'))
      await expect.poll(() => f.observers.length).toBe(2)
      f.push({ type: 'schema', columns }, 1)
      f.push(baseline, 1)
      await expect
        .poll(() => shape.currentRows)
        .toEqual([{ id: 1, label: 'initial' }])
      expect(stream.shapeHandle).not.toBe(handle)
    })
  }
)

describe('object replay failure boundary', () => {
  it('rechecks absolute expiry after an asynchronous object download', async () => {
    const f = fixture(true)
    const first = await f.request()
    const handle = first.headers.get('electric-handle')!
    await f.logs[0].flush()
    f.objectStore.beforeGet = async () => {
      f.setNow(901000)
    }
    const expired = await f.request(`handle=${handle}&offset=-1`)
    expect(expired.status).toBe(409)
    expect(expired.headers.get('electric-handle')).not.toBe(handle)
    expect(f.stopped()).toBe(1)
  })

  it('invalidates a handle when a sealed range cannot be replayed', async () => {
    const f = fixture(true)
    const first = await f.request()
    const handle = first.headers.get('electric-handle')!
    await f.logs[0].flush()
    f.objectStore.getError = new Error('storage unavailable')
    const next = await f.request(`handle=${handle}&offset=-1`)
    expect(next.status).toBe(409)
    expect(next.headers.get('electric-handle')).not.toBe(handle)
    expect(f.stopped()).toBe(1)
    f.objectStore.getError = undefined
  })
})
