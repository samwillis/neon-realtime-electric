import { describe, expect, it, vi } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'

function fixture(objects: boolean, electricOnly: boolean, durationMs = 5) {
  const observers: SourceObserver[] = []
  const requests: Request[] = []
  const store = new FixtureObjectStore()
  const handler = createShapeHandler({
    longPollMs: 5_000,
    get sseConnectionMs() {
      return durationMs
    },
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
          rows: [{ row_key: 'a'.repeat(64), values: ['1', 'initial'] }],
          mvcc: { xmin: '10', xmax: '10', xip: [] },
        })
        return { close() {} }
      },
      close() {},
    },
    createLog: objects
      ? ({ handle }) =>
          new ObjectShapeLog({ store, prefix: handle, sealBytes: 1 })
      : undefined,
    async authorize() {
      return {
        authorizationKey: 'sse-fallback',
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
  const fetchClient: typeof fetch = async (input, init) => {
    const request = new Request(input, init)
    requests.push(request)
    const response = await handler.fetch(request)
    if (!electricOnly) return response
    const headers = new Headers(response.headers)
    for (const name of [...headers.keys()])
      if (name.startsWith('stream-')) headers.delete(name)
    return new Response(response.body, { status: response.status, headers })
  }
  const sse = () =>
    requests.filter(
      (r) => new URL(r.url).searchParams.get('live_sse') === 'true'
    )
  const polls = () =>
    requests.filter((r) => {
      const url = new URL(r.url)
      return (
        url.searchParams.get('live') === 'true' &&
        !url.searchParams.has('live_sse')
      )
    })
  return {
    handler,
    fetchClient,
    requests,
    observers,
    sse,
    polls,
    setDuration(ms: number) {
      durationMs = ms
    },
  }
}

for (const objects of [false, true])
  for (const electricOnly of [false, true])
    describe(`SSE fallback (objects=${objects}, Electric headers=${electricOnly})`, () => {
      it('does not count refresh-aborted connections toward the three short-connection threshold', async () => {
        const f = fixture(objects, electricOnly, 5_000)
        const abort = new AbortController()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        const errors: unknown[] = []
        const stream = new ShapeStream({
          url: 'http://local/application/shapes',
          params: { table: 'items' },
          liveSse: true,
          signal: abort.signal,
          fetchClient: f.fetchClient,
          onError(error) {
            errors.push(error)
          },
        })
        const shape = new Shape(stream)
        try {
          await shape.rows
          await expect.poll(() => f.sse().length).toBe(1)
          for (let index = 0; index < 4; index++) {
            const previous = f.sse().at(-1)!
            await stream.forceDisconnectAndRefresh()
            await expect.poll(() => f.sse().length).toBe(index + 2)
            expect(previous.signal.aborted).toBe(true)
          }
          expect(f.polls()).toHaveLength(0)
          expect(
            warn.mock.calls.some(([message]) =>
              String(message).includes('Falling back to long polling')
            )
          ).toBe(false)
          // Genuine short closes still require three new physical connections.
          const count = f.sse().length
          f.setDuration(5)
          await stream.forceDisconnectAndRefresh()
          await expect
            .poll(() => f.polls().length, { timeout: 4_000 })
            .toBeGreaterThan(0)
          expect(f.sse()).toHaveLength(count + 3)
          expect(errors).toEqual([])
        } finally {
          abort.abort()
          stream.unsubscribeAll()
          f.handler.close()
          await f.handler.drain()
          warn.mockRestore()
        }
      })

      it('falls back after three short connections, retains fallback across refresh, and retries SSE after owner loss', async () => {
        const f = fixture(objects, electricOnly)
        const abort = new AbortController()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        const errors: unknown[] = []
        const stream = new ShapeStream<{ id: number; label: string }>({
          url: 'http://local/application/shapes?tenant=a%2Bb',
          params: { table: 'items' },
          liveSse: true,
          signal: abort.signal,
          fetchClient: f.fetchClient,
          onError(error) {
            errors.push(error)
          },
        })
        const shape = new Shape(stream)
        try {
          expect(await shape.rows).toEqual([{ id: 1, label: 'initial' }])
          await expect
            .poll(() => f.polls().length, { timeout: 4_000 })
            .toBeGreaterThan(0)
          expect(f.sse()).toHaveLength(3)
          const handle = stream.shapeHandle
          const pollCount = f.polls().length
          await stream.forceDisconnectAndRefresh()
          await expect
            .poll(() => f.polls().length, { timeout: 4_000 })
            .toBeGreaterThan(pollCount)
          expect(f.sse()).toHaveLength(3)
          expect(stream.shapeHandle).toBe(handle)
          f.observers[0].error(new Error('intentional fallback owner loss'))
          await expect.poll(() => stream.shapeHandle).not.toBe(handle)
          await expect.poll(() => f.sse().length).toBeGreaterThan(3)
          expect(f.observers).toHaveLength(2)
          expect(shape.currentRows).toEqual([{ id: 1, label: 'initial' }])
          expect(errors).toEqual([])
          expect(
            warn.mock.calls.some(([message]) =>
              String(message).includes('Falling back to long polling')
            )
          ).toBe(true)
          for (const request of f.requests) {
            const url = new URL(request.url)
            expect(url.pathname).toBe('/application/shapes')
            expect(url.searchParams.get('tenant')).toBe('a+b')
          }
        } finally {
          abort.abort()
          stream.unsubscribeAll()
          f.handler.close()
          await f.handler.drain()
          warn.mockRestore()
        }
      })
    })
