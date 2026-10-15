import { describe, expect, it } from 'vitest'
import {
  FetchError,
  Shape,
  ShapeStream,
  isChangeMessage,
} from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}
function fixture(
  objects: boolean,
  electricOnly: boolean,
  connectionMs = 5_000
) {
  const observers: SourceObserver[] = []
  const requests: Request[] = []
  const store = new FixtureObjectStore()
  let txid = 10
  const handler = createShapeHandler({
    longPollMs: connectionMs,
    sseConnectionMs: connectionMs,
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
    async authorize(request) {
      return {
        authorizationKey: 'client-lifecycle',
        definition: {
          namespace: 'public',
          logMode:
            new URL(request.url).searchParams.get('log') === 'changes_only'
              ? 'changes_only'
              : 'full',
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
  return {
    handler,
    observers,
    requests,
    fetchClient,
    publish(label: string, operation: 'upsert' | 'remove' = 'upsert') {
      txid++
      observers[0].event({
        type: 'publication',
        changes: [
          operation === 'remove'
            ? { op: 'remove', row_key: 'a'.repeat(64) }
            : { op: 'upsert', row_key: 'a'.repeat(64), values: ['1', label] },
        ],
        txids: [String(txid)],
        frontier: { lsn: `0/${txid.toString(16).toUpperCase()}` },
      })
      return txid
    },
  }
}

for (const objects of [false, true])
  for (const electricOnly of [false, true])
    for (const liveSse of [false, true])
      describe(`client lifecycle (objects=${objects}, Electric headers=${electricOnly}, SSE=${liveSse})`, () => {
        it('removes one stream and shape subscriber while the remaining subscribers keep receiving updates', async () => {
          const f = fixture(objects, electricOnly)
          const abort = new AbortController()
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: 'http://local/application/shapes',
            liveSse,
            signal: abort.signal,
            fetchClient: f.fetchClient,
          })
          const shape = new Shape(stream)
          let removedStream = 0,
            keptStream = 0,
            removedShape = 0,
            keptShape = 0
          const unsubscribeStream = stream.subscribe(() => {
            removedStream++
          })
          stream.subscribe(() => {
            keptStream++
          })
          const unsubscribeShape = shape.subscribe(() => {
            removedShape++
          })
          shape.subscribe(() => {
            keptShape++
          })
          try {
            expect(await shape.rows).toEqual([{ id: 1, label: 'initial' }])
            const handle = stream.shapeHandle
            const counts = {
              removedStream,
              keptStream,
              removedShape,
              keptShape,
            }
            unsubscribeStream()
            unsubscribeShape()
            expect(shape.numSubscribers).toBe(1)
            f.publish('next')
            await expect
              .poll(() => shape.currentRows)
              .toEqual([{ id: 1, label: 'next' }])
            expect(removedStream).toBe(counts.removedStream)
            expect(removedShape).toBe(counts.removedShape)
            expect(keptStream).toBeGreaterThan(counts.keptStream)
            expect(keptShape).toBeGreaterThan(counts.keptShape)
            expect(stream.shapeHandle).toBe(handle)
            expect(f.observers).toHaveLength(1)
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
          }
        })

        it('finishes subscribe:false with a ready snapshot and a disconnected public status', async () => {
          const f = fixture(objects, electricOnly)
          const abort = new AbortController()
          const stream = new ShapeStream({
            url: 'http://local/application/shapes',
            liveSse,
            subscribe: false,
            signal: abort.signal,
            fetchClient: f.fetchClient,
          })
          const shape = new Shape(stream)
          try {
            expect(await shape.rows).toEqual([{ id: 1, label: 'initial' }])
            await expect.poll(() => stream.isConnected()).toBe(false)
            expect(stream.isLoading()).toBe(false)
            expect(stream.isUpToDate).toBe(true)
            expect(shape.isUpToDate).toBe(true)
            expect(stream.lastSyncedAt()).toEqual(expect.any(Number))
            expect(stream.shapeHandle).toBeTruthy()
            expect(stream.lastOffset).not.toBe('-1')
            expect(
              f.requests.every((r) => !new URL(r.url).searchParams.has('live'))
            ).toBe(true)
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
          }
        })

        it('disconnects on a live auth error and resumes the same history after asynchronous repair', async () => {
          const f = fixture(objects, electricOnly)
          const abort = new AbortController()
          const entered = deferred<Error>()
          const repaired = deferred<void>()
          let rejectNext = false
          const headers: string[] = []
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: 'http://local/application/shapes?tenant=a%2Bb',
            params: { table: 'items' },
            liveSse,
            signal: abort.signal,
            headers: { authorization: 'Bearer before' },
            fetchClient: async (input, init) => {
              const request = new Request(input, init)
              headers.push(request.headers.get('authorization')!)
              if (rejectNext) {
                rejectNext = false
                return new Response('expired', { status: 401 })
              }
              return f.fetchClient(input, init)
            },
            onError: async (error) => {
              entered.resolve(error)
              await repaired.promise
              return { headers: { authorization: 'Bearer after' } }
            },
          })
          const shape = new Shape(stream)
          let refresh: Promise<void> | undefined
          try {
            expect(await shape.rows).toEqual([{ id: 1, label: 'initial' }])
            expect(stream.isConnected()).toBe(true)
            const handle = stream.shapeHandle
            rejectNext = true
            refresh = stream.forceDisconnectAndRefresh()
            const error = await entered.promise
            expect(error).toBeInstanceOf(FetchError)
            expect((error as FetchError).status).toBe(401)
            expect(stream.isConnected()).toBe(false)
            expect(stream.error).toBe(error)
            expect(shape.currentRows).toEqual([{ id: 1, label: 'initial' }])
            f.publish('written during auth repair')
            repaired.resolve()
            await refresh
            await expect
              .poll(() => shape.currentRows)
              .toEqual([{ id: 1, label: 'written during auth repair' }])
            expect(stream.isConnected()).toBe(true)
            expect(stream.error).toBeNull()
            expect(stream.shapeHandle).toBe(handle)
            expect(f.observers).toHaveLength(1)
            expect(headers.at(-1)).toBe('Bearer after')
            for (const request of f.requests) {
              const url = new URL(request.url)
              expect(url.pathname).toBe('/application/shapes')
              expect(url.searchParams.get('tenant')).toBe('a+b')
              if (url.searchParams.has('handle'))
                expect(url.searchParams.get('handle')).toBe(handle)
            }
          } finally {
            repaired.resolve()
            abort.abort()
            await refresh?.catch(() => {})
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
          }
        })

        it.each(['network', 'fetch-error', 'http-500', 'http-429'] as const)(
          'disconnects during native %s backoff and resumes the same history without the public error handler',
          async (failure) => {
            const f = fixture(objects, electricOnly, 100)
            const abort = new AbortController()
            const failed = deferred<void>()
            const errors: Error[] = []
            let failing = false
            let failedAttempts = 0
            const stream = new ShapeStream<{ id: number; label: string }>({
              url: 'http://local/application/shapes',
              liveSse,
              signal: abort.signal,
              backoffOptions: {
                initialDelay: 1,
                maxDelay: 5,
                multiplier: 1,
                onFailedAttempt() {
                  failedAttempts++
                  failed.resolve()
                },
              },
              fetchClient: async (input, init) => {
                if (failing) {
                  if (failure === 'network') throw new TypeError('Offline')
                  if (failure === 'fetch-error')
                    throw new FetchError(
                      500,
                      'Transient custom fetch failure',
                      undefined,
                      {},
                      String(input)
                    )
                  return new Response('Transient failure', {
                    status: failure === 'http-500' ? 500 : 429,
                  })
                }
                return f.fetchClient(input, init)
              },
              onError(error) {
                errors.push(error)
              },
            })
            const shape = new Shape(stream)
            try {
              expect(await shape.rows).toEqual([{ id: 1, label: 'initial' }])
              expect(stream.isConnected()).toBe(true)
              const handle = stream.shapeHandle
              // Let the ongoing poll/SSE connection complete naturally. A forced
              // refresh would itself disconnect and could mask a broken failure hook.
              failing = true
              await failed.promise
              expect(stream.isConnected()).toBe(false)
              expect(stream.error).toBeNull()
              f.publish('queued during backoff')
              failing = false
              await expect
                .poll(() => shape.currentRows)
                .toEqual([{ id: 1, label: 'queued during backoff' }])
              expect(stream.isConnected()).toBe(true)
              expect(stream.error).toBeNull()
              expect(failedAttempts).toBeGreaterThan(0)
              expect(errors).toEqual([])
              expect(stream.shapeHandle).toBe(handle)
              expect(f.observers).toHaveLength(1)
            } finally {
              abort.abort()
              stream.unsubscribeAll()
              f.handler.close()
              await f.handler.drain()
            }
          }
        )

        it('advances lastSyncedAt on unchanged live tails without rotating or publishing row changes', async () => {
          const f = fixture(objects, electricOnly, 100)
          const abort = new AbortController()
          const stream = new ShapeStream({
            url: 'http://local/application/shapes',
            liveSse,
            signal: abort.signal,
            fetchClient: f.fetchClient,
          })
          const shape = new Shape(stream)
          let changes = 0
          stream.subscribe((messages) => {
            changes += messages.filter(isChangeMessage).length
          })
          try {
            await shape.rows
            const handle = stream.shapeHandle
            const offset = stream.lastOffset
            const synced = stream.lastSyncedAt()!
            const before = changes
            await expect.poll(() => stream.lastSyncedAt()! > synced).toBe(true)
            expect(stream.isConnected()).toBe(true)
            expect(stream.lastOffset).toBe(offset)
            expect(stream.shapeHandle).toBe(handle)
            expect(changes).toBe(before)
            expect(shape.currentRows).toEqual([{ id: 1, label: 'initial' }])
            expect(f.observers).toHaveLength(1)
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
          }
        })

        it.each(['consumed', 'locked'] as const)(
          'reports a %s custom-fetch response as a terminal public FetchError without retrying',
          async (state) => {
            const f = fixture(objects, electricOnly)
            const abort = new AbortController()
            const handled: Error[] = []
            let attempts = 0
            let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
            const stream = new ShapeStream({
              url: 'http://local/application/shapes',
              liveSse,
              signal: abort.signal,
              fetchClient: async (input, init) => {
                attempts++
                const response = await f.fetchClient(input, init)
                if (state === 'consumed') await response.text()
                else reader = response.body!.getReader()
                return response
              },
              onError(error) {
                handled.push(error)
              },
            })
            try {
              const failure = await new Promise<Error>((resolve) =>
                stream.subscribe(() => {}, resolve)
              )
              expect(failure).toBeInstanceOf(FetchError)
              expect(failure.message).toMatch(/body|Response/i)
              expect(stream.error).toBe(failure)
              expect(stream.isConnected()).toBe(false)
              expect(handled).toEqual([failure])
              expect(attempts).toBe(1)
            } finally {
              await reader?.cancel()
              reader?.releaseLock()
              abort.abort()
              stream.unsubscribeAll()
              f.handler.close()
              await f.handler.drain()
            }
          }
        )

        it('ignores unseen-key updates/deletes until an insert is observed in changes-only mode', async () => {
          const f = fixture(objects, electricOnly)
          const abort = new AbortController()
          const stream = new ShapeStream<{ id: number; label: string }>({
            url: 'http://local/application/shapes',
            log: 'changes_only',
            liveSse,
            signal: abort.signal,
            fetchClient: f.fetchClient,
          })
          const shape = new Shape(stream)
          const operations: string[] = []
          stream.subscribe((messages) => {
            for (const message of messages)
              if (isChangeMessage(message))
                operations.push(message.headers.operation)
          })
          const deliver = async (
            label: string,
            operation: 'upsert' | 'remove' = 'upsert'
          ) => {
            const delivered = deferred<void>()
            let txid: number
            const unsubscribe = stream.subscribe((messages) => {
              if (
                messages.some(
                  (message) =>
                    isChangeMessage(message) &&
                    message.headers.txids?.includes(txid)
                )
              )
                delivered.resolve()
            })
            try {
              txid = f.publish(label, operation)
              await delivered.promise
              await expect.poll(() => shape.isUpToDate).toBe(true)
            } finally {
              unsubscribe()
            }
          }
          try {
            expect(await shape.rows).toEqual([])
            await deliver('unseen update')
            expect(shape.currentRows).toEqual([])
            await deliver('', 'remove')
            expect(shape.currentRows).toEqual([])
            await deliver('observed insertion')
            expect(shape.currentRows).toEqual([
              { id: 1, label: 'observed insertion' },
            ])
            await deliver('observed update')
            expect(shape.currentRows).toEqual([
              { id: 1, label: 'observed update' },
            ])
            await deliver('', 'remove')
            expect(shape.currentRows).toEqual([])
            expect(operations).toEqual([
              'update',
              'delete',
              'insert',
              'update',
              'delete',
            ])
            expect(f.observers).toHaveLength(1)
          } finally {
            abort.abort()
            stream.unsubscribeAll()
            f.handler.close()
            await f.handler.drain()
          }
        })
      })
