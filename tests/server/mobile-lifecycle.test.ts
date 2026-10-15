import { describe, expect, it, vi } from 'vitest'
import {
  Shape,
  ShapeStream,
  createReactNativeRuntimeVisibilityAdapter,
} from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import type {
  RealtimeSource,
  SourceObserver,
} from '../../packages/server/src/realtime-source.js'

type Visibility = 'active' | 'background' | 'inactive'
const flush = () => new Promise<void>((resolve) => setImmediate(resolve))

function fixture(object: boolean, electricOnly: boolean) {
  const store = new FixtureObjectStore()
  const subscriptions: { observer: SourceObserver; signal: AbortSignal }[] = []
  let label = 'before'
  const source: RealtimeSource = {
    async subscribe(_, observer, signal) {
      subscriptions.push({ observer, signal })
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
        mvcc: { xmin: '10', xmax: '11', xip: [] },
      })
      return { close() {} }
    },
    close() {},
  }
  let authorized = 0
  const handler = createShapeHandler({
    source,
    longPollMs: 5_000,
    createLog: object
      ? ({ handle }) =>
          new ObjectShapeLog({ store, prefix: handle, sealBytes: 1 })
      : undefined,
    async authorize() {
      authorized++
      return {
        authorizationKey: 'mobile-user',
        definition: {
          namespace: 'public',
          table: 'items',
          primaryKey: ['id'],
          projection: ['id', 'label'],
          query: { sql: 'select id,label from public.items' },
          schema: { id: { type: 'int4' }, label: { type: 'text' } },
        },
      }
    },
  })
  const requests: { url: URL; signal: AbortSignal }[] = []
  const fetchClient: typeof fetch = async (input, init) => {
    const request = new Request(input, init)
    requests.push({ url: new URL(request.url), signal: request.signal })
    const response = await handler.fetch(request)
    if (!electricOnly) return response
    const headers = new Headers(response.headers)
    for (const key of [...headers.keys()])
      if (key.toLowerCase().startsWith('stream-')) headers.delete(key)
    return new Response(response.body, { status: response.status, headers })
  }
  return {
    handler,
    requests,
    fetchClient,
    subscriptions,
    store,
    authorized: () => authorized,
    publish(value: string) {
      label = value
      subscriptions.at(-1)!.observer.event({
        type: 'publication',
        changes: [
          { op: 'upsert', row_key: 'a'.repeat(64), values: ['1', label] },
        ],
        txids: ['11'],
        frontier: { lsn: '0/20' },
      })
    },
  }
}

for (const object of [false, true])
  for (const electricOnly of [false, true])
    describe(`${object ? 'object' : 'memory'} log / ${electricOnly ? 'Electric' : 'native'} headers`, () => {
      it.each([false, true])(
        'pauses on AppState background, catches up at the same handle, and refetches after owner loss (SSE=%s)',
        async (liveSse) => {
          const f = fixture(object, electricOnly)
          const abort = new AbortController()
          const errors: unknown[] = []
          let state: Visibility = 'active'
          let listener: ((state: Visibility) => void) | undefined
          const appState = {
            get currentState() {
              return state
            },
            addEventListener(
              _: 'change',
              callback: (state: Visibility) => void
            ) {
              listener = callback
              return {
                remove() {
                  listener = undefined
                },
              }
            },
          }
          const setState = (next: Visibility) => {
            state = next
            listener?.(next)
          }
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: 'http://local/api/shapes',
            params: { table: 'items' },
            liveSse,
            fetchClient: f.fetchClient,
            signal: abort.signal,
            runtimeVisibility:
              createReactNativeRuntimeVisibilityAdapter(appState),
            onError(error) {
              errors.push(error)
            },
          })
          const shape = new Shape(stream)
          try {
            expect(await shape.rows).toEqual([{ id: 1, label: 'before' }])
            await expect
              .poll(() => f.requests.at(-1)?.url.searchParams.get('live'))
              .toBe('true')
            const handle = stream.shapeHandle
            const active = f.requests.at(-1)!
            setState('background')
            await expect.poll(() => active.signal.aborted).toBe(true)
            await flush()
            const pausedCount = f.requests.length
            f.publish('while hidden')
            await flush()
            expect(f.requests).toHaveLength(pausedCount)
            expect(shape.currentRows).toEqual([{ id: 1, label: 'before' }])
            setState('active')
            await expect
              .poll(() => shape.currentRows)
              .toEqual([{ id: 1, label: 'while hidden' }])
            expect(stream.shapeHandle).toBe(handle)
            const catchup = f.requests[pausedCount]
            expect(catchup.url.searchParams.get('handle')).toBe(handle)
            expect(catchup.url.searchParams.has('live')).toBe(false)
            expect(f.subscriptions).toHaveLength(1)

            setState('inactive')
            await flush()
            f.subscriptions[0].observer.error(
              new Error('Owner continuity lost while backgrounded')
            )
            setState('active')
            await expect.poll(() => stream.shapeHandle).not.toBe(handle)
            await expect.poll(() => shape.isUpToDate).toBe(true)
            expect(shape.currentRows).toEqual([
              { id: 1, label: 'while hidden' },
            ])
            expect(f.subscriptions).toHaveLength(2)
            expect(f.subscriptions[0].signal.aborted).toBe(true)
            expect(
              f.requests.every((r) => r.url.pathname === '/api/shapes')
            ).toBe(true)
            expect(f.authorized()).toBe(f.requests.length)
            expect(errors).toEqual([])
            if (object) expect(f.store.gets.length).toBeGreaterThan(0)
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
            expect(listener).toBeUndefined()
            expect(f.subscriptions.every((s) => s.signal.aborted)).toBe(true)
            expect(f.store.objects.size).toBe(0)
          }
        }
      )

      it.each([false, true])(
        'recovers a hung native read after a clock gap with reason-less AbortController=%s',
        async (dropReason) => {
          // Only the wake detector clock/intervals are fake. HTTP bodies, log
          // persistence, promise delivery and long-poll deadlines stay real.
          vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
          const originalAbort = AbortController.prototype.abort
          if (dropReason)
            vi.spyOn(AbortController.prototype, 'abort').mockImplementation(
              function (this: AbortController) {
                originalAbort.call(this)
              }
            )
          const f = fixture(object, electricOnly)
          const abort = new AbortController()
          const errors: unknown[] = []
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: 'http://local/api/shapes',
            params: { table: 'items' },
            fetchClient: f.fetchClient,
            signal: abort.signal,
            onError(error) {
              errors.push(error)
            },
          })
          const shape = new Shape(stream)
          try {
            await shape.rows
            await expect
              .poll(() => f.requests.at(-1)?.url.searchParams.get('live'))
              .toBe('true')
            const handle = stream.shapeHandle
            const hung = f.requests.at(-1)!
            const start = f.requests.length
            vi.setSystemTime(Date.now() + 10_000)
            await vi.advanceTimersByTimeAsync(2_001)
            await expect.poll(() => hung.signal.aborted).toBe(true)
            await expect.poll(() => f.requests.length).toBeGreaterThan(start)
            expect(f.requests[start].url.searchParams.has('live')).toBe(false)
            f.publish('after wake')
            await expect
              .poll(() => shape.currentRows)
              .toEqual([{ id: 1, label: 'after wake' }])
            expect(stream.shapeHandle).toBe(handle)
            expect(f.subscriptions).toHaveLength(1)
            expect(errors).toEqual([])
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
            expect(vi.getTimerCount()).toBe(0)
            vi.useRealTimers()
            vi.restoreAllMocks()
          }
        }
      )
    })
