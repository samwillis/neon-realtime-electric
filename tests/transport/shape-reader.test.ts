import { afterEach, describe, expect, it } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index'
import { QueryShapeServer } from './support/query-shape-server'
import type { FixtureShape } from './support/query-shape-server'

const servers: QueryShapeServer[] = []
afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => server.stop()))
})

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

async function baseline(server: QueryShapeServer, owner: FixtureShape) {
  owner.schema = {
    id: { type: 'int4', not_null: true },
    name: { type: 'text' },
    done: { type: 'bool' },
  }
  await server.append(owner, {
    version: 1,
    seq: '2',
    kind: 'snapshot-chunk',
    changes: [
      {
        key: '"public"."items"/"1"',
        value: { id: '1', name: 'first', done: 'f' },
        headers: { operation: 'insert' },
      },
    ],
  })
  await server.append(owner, {
    version: 1,
    seq: '3',
    kind: 'snapshot-complete',
    coveredSeq: '2',
    evidence: { snapshot: { xmin: '1', xmax: '2', xip_list: [] }, lsn: '500' },
  })
}

for (const electricOnly of [false, true]) {
  describe(
    electricOnly ? 'Electric header allowlist' : 'native headers',
    () => {
      it('exposes progress MVCC to eager transaction acknowledgement only after covered changes are installed', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const created = deferred<FixtureShape>()
        server.onCreate = (owner) => created.resolve(owner)
        const abort = new AbortController()
        const stream = new ShapeStream<{
          id: number
          name: string
          done: boolean
        }>({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          signal: abort.signal,
        })
        const shape = new Shape(stream)
        let covered = false
        stream.subscribe((messages) => {
          for (const message of messages)
            if (
              'control' in message.headers &&
              message.headers.control === 'snapshot-end' &&
              message.headers.xmin === '100'
            ) {
              expect(shape.currentRows[0].name).toBe('covered')
              covered = true
            }
        })
        try {
          const owner = await created.promise
          await baseline(server, owner)
          await shape.rows
          await server.append(owner, {
            version: 1,
            seq: '4',
            kind: 'publication',
            changes: [
              {
                key: '"public"."items"/"1"',
                value: { id: '1', name: 'covered', done: 'f' },
                headers: { operation: 'update' },
              },
            ],
            evidence: { txids: ['99'], lsn: '501' },
          })
          await server.append(owner, {
            version: 1,
            seq: '5',
            kind: 'progress',
            coveredSeq: '4',
            evidence: { snapshot: { xmin: '100', xmax: '101', xip_list: [] } },
          })
          await expect.poll(() => covered).toBe(true)
        } finally {
          abort.abort()
        }
      })

      it('does not pair a retried body with metadata from a response that failed normalization', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        let prepared = Promise.resolve()
        server.onCreate = (owner) => {
          prepared = baseline(server, owner)
        }
        let faulted = false
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          subscribe: false,
          signal: controller.signal,
          backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1 },
          fetchClient: async (input, init) => {
            const response = await fetch(input, init)
            await prepared
            if (!faulted) {
              faulted = true
              const headers = new Headers(response.headers)
              headers.delete('electric-schema')
              const broken = new Response(await response.arrayBuffer(), {
                status: 200,
                headers,
              })
              Object.defineProperty(broken, 'body', {
                get() {
                  throw new TypeError('Transient custom-fetch body failure')
                },
              })
              return broken
            }
            return response
          },
        })
        try {
          const shape = new Shape(stream)
          expect(await shape.rows).toEqual([
            { id: 1, name: 'first', done: false },
          ])
          expect(faulted).toBe(true)
          expect(server.shapes.size).toBe(1)
        } finally {
          controller.abort()
        }
      })

      it('refetches safely when a shape expires before baseline completion', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        let now = Date.now()
        server.now = () => now
        const firstCreated = deferred<FixtureShape>()
        const nextCreated = deferred<FixtureShape>()
        let allocations = 0
        server.onCreate = (owner) => {
          ;(++allocations === 1 ? firstCreated : nextCreated).resolve(owner)
        }
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          signal: controller.signal,
          subscribe: false,
        })
        const shape = new Shape(stream)
        const ready = shape.rows
        try {
          const first = await firstCreated.promise
          await expect.poll(() => stream.shapeHandle).toBe(first.handle)
          expect(stream.isUpToDate).toBe(false)
          expect(stream.lastSyncedAt()).toBeUndefined()
          now = first.expiresAt
          const next = await nextCreated.promise
          expect(stream.isUpToDate).toBe(false)
          expect(stream.lastSyncedAt()).toBeUndefined()
          await baseline(server, next)
          expect(await ready).toEqual([{ id: 1, name: 'first', done: false }])
          expect(next.handle).not.toBe(first.handle)
          expect(server.shapes.has(first.handle)).toBe(false)
          expect(
            server.requests.every(
              (request) => request.url.pathname === '/api/shape'
            )
          ).toBe(true)
        } finally {
          controller.abort()
        }
      })

      it('surfaces resolver failures during native catch-up without a network retry loop', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        server.maxEnvelopesPerRead = 1
        let prepared = Promise.resolve()
        server.onCreate = (owner) => {
          prepared = baseline(server, owner)
        }
        server.beforeRead = () => prepared
        const failure = new Error('auth resolver failed')
        const reported = deferred<unknown>()
        let calls = 0
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          signal: controller.signal,
          headers: {
            Authorization: () => {
              if (++calls === 2) throw failure
              return 'Bearer valid'
            },
          },
          onError: (error) => {
            reported.resolve(error)
          },
        })
        stream.subscribe(() => {})
        try {
          expect(await reported.promise).toBe(failure)
          expect(calls).toBe(2)
          expect(server.requests).toHaveLength(1)
          expect(stream.isUpToDate).toBe(false)
        } finally {
          controller.abort()
        }
      })

      it('cancels an initializing one-shot read without marking the shape ready', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const waiting = deferred<void>()
        const cancelled = deferred<void>()
        const controller = new AbortController()
        let calls = 0
        let errors = 0
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          subscribe: false,
          signal: controller.signal,
          onError: () => {
            errors++
          },
          fetchClient: async (input, init) => {
            if (++calls === 1) return fetch(input, init)
            waiting.resolve()
            return new Promise<Response>((_resolve, reject) => {
              init!.signal!.addEventListener(
                'abort',
                () => {
                  cancelled.resolve()
                  reject(new DOMException('cancelled', 'AbortError'))
                },
                { once: true }
              )
            })
          },
        })
        stream.subscribe(() => {
          throw new Error('No public messages before the baseline')
        })
        await waiting.promise
        expect(stream.shapeHandle).toBeDefined()
        controller.abort()
        await cancelled.promise
        expect(stream.isUpToDate).toBe(false)
        expect(stream.lastSyncedAt()).toBeUndefined()
        expect(errors).toBe(0)
      })

      it('reevaluates auth and params on native multi-response catch-up and retries', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        server.maxEnvelopesPerRead = 1
        server.failReadAt = 2
        let prepared = Promise.resolve()
        server.onCreate = (owner) => {
          prepared = baseline(server, owner)
        }
        server.beforeRead = () => prepared
        let authCalls = 0
        let paramCalls = 0
        let retryCalls = 0
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          signal: controller.signal,
          subscribe: false,
          headers: { Authorization: () => `Bearer ${++authCalls}` },
          params: { table: 'items', auth_generation: () => `${++paramCalls}` },
          backoffOptions: {
            initialDelay: 1,
            maxDelay: 1,
            multiplier: 1,
            maxRetries: 1,
            onFailedAttempt: () => {
              retryCalls++
            },
          },
        })
        const shape = new Shape(stream)
        try {
          expect(await shape.rows).toEqual([
            { id: 1, name: 'first', done: false },
          ])
          expect(server.requests).toHaveLength(4)
          expect(
            server.requests.map((request) => request.authorization)
          ).toEqual(['Bearer 1', 'Bearer 2', 'Bearer 3', 'Bearer 4'])
          expect(
            server.requests.map((request) =>
              request.url.searchParams.get('auth_generation')
            )
          ).toEqual(['1', '2', '3', '4'])
          expect(retryCalls).toBe(1)
          expect(server.requests[1].url.searchParams.get('offset')).toBe(
            server.requests[2].url.searchParams.get('offset')
          )
          expect(server.shapes.size).toBe(1)
        } finally {
          controller.abort()
        }
      })

      it('keeps multiple baseline chunks private until completion and supports an empty baseline', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const created = deferred<FixtureShape>()
        server.onCreate = (owner) => {
          created.resolve(owner)
        }
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          subscribe: false,
          signal: controller.signal,
        })
        const shape = new Shape(stream)
        let notifications = 0
        shape.subscribe(() => {
          notifications++
        })
        const ready = shape.rows
        try {
          const owner = await created.promise
          owner.schema = { id: { type: 'int4', not_null: true } }
          await server.append(owner, {
            version: 1,
            seq: '2',
            kind: 'snapshot-chunk',
            changes: [
              {
                key: 'one',
                value: { id: '1' },
                headers: { operation: 'insert' },
              },
            ],
          })
          await expect
            .poll(() => stream.lastOffset)
            .toBe(Array.from(owner.boundaries.keys()).at(-1))
          expect(notifications).toBe(0)
          expect(stream.isUpToDate).toBe(false)
          await server.append(owner, {
            version: 1,
            seq: '3',
            kind: 'snapshot-chunk',
            changes: [
              {
                key: 'two',
                value: { id: '2' },
                headers: { operation: 'insert' },
              },
            ],
          })
          await server.append(owner, {
            version: 1,
            seq: '4',
            kind: 'snapshot-complete',
            coveredSeq: '3',
            evidence: { snapshot: { xmin: '1', xmax: '2', xip_list: [] } },
          })
          expect(await ready).toEqual([{ id: 1 }, { id: 2 }])
          expect(notifications).toBe(1)
        } finally {
          controller.abort()
        }
        const emptyCreated = deferred<FixtureShape>()
        server.onCreate = (owner) => {
          emptyCreated.resolve(owner)
        }
        const emptyAbort = new AbortController()
        const empty = new Shape(
          new ShapeStream({
            url: `${server.url}/api/shape`,
            params: { table: 'items' },
            subscribe: false,
            signal: emptyAbort.signal,
          })
        )
        try {
          const owner = await emptyCreated.promise
          await server.append(owner, {
            version: 1,
            seq: '2',
            kind: 'snapshot-complete',
            coveredSeq: '1',
            evidence: { snapshot: { xmin: '1', xmax: '1', xip_list: [] } },
          })
          expect(await empty.rows).toEqual([])
        } finally {
          emptyAbort.abort()
        }
      })

      it('replays changes written while disconnected from a saved handle and offset', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const created = deferred<FixtureShape>()
        server.onCreate = (owner) => {
          created.resolve(owner)
        }
        const firstAbort = new AbortController()
        const initial = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          subscribe: false,
          signal: firstAbort.signal,
        })
        const shape = new Shape(initial)
        const owner = await created.promise
        await baseline(server, owner)
        await shape.rows
        const offset = initial.lastOffset
        firstAbort.abort()
        await server.append(owner, {
          version: 1,
          seq: '4',
          kind: 'publication',
          evidence: { txids: ['10'] },
          changes: [
            {
              key: '"public"."items"/"1"',
              value: { name: 'offline change' },
              headers: { operation: 'update' },
            },
          ],
        })
        const secondAbort = new AbortController()
        const resumed = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          handle: owner.handle,
          offset,
          subscribe: false,
          signal: secondAbort.signal,
        })
        const received: unknown[] = []
        const ready = deferred<void>()
        resumed.subscribe((batch) => {
          received.push(...batch)
          if (batch.some((message) => message.headers.control === 'up-to-date'))
            ready.resolve()
        })
        try {
          await ready.promise
          expect(received).toEqual([
            {
              key: '"public"."items"/"1"',
              value: { name: 'offline change' },
              headers: { operation: 'update', txids: [10] },
            },
            { headers: { control: 'up-to-date' } },
          ])
          expect(server.shapes.size).toBe(1)
        } finally {
          secondAbort.abort()
        }
      })

      it('retains an early handle and retries its original offset after body corruption', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const controller = new AbortController()
        const failed = deferred<{
          handle: string | undefined
          offset: string
        }>()
        let corrupt = true
        const stream = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          signal: controller.signal,
          subscribe: false,
          backoffOptions: {
            initialDelay: 1,
            maxDelay: 1,
            multiplier: 1,
            maxRetries: 0,
          },
          fetchClient: async (input, init) => {
            const response = await fetch(input, init)
            if (!corrupt) return response
            corrupt = false
            await response.body?.cancel()
            return new Response('[{"truncated":', { headers: response.headers })
          },
          onError: () => {
            failed.resolve({
              handle: stream.shapeHandle,
              offset: stream.lastOffset,
            })
            return {}
          },
        })
        const shape = new Shape(stream)
        const rows = shape.rows
        try {
          const early = await failed.promise
          expect(early.offset).toBe('-1')
          expect(early.handle).toBeDefined()
          const owner = server.shapes.get(early.handle!)!
          await baseline(server, owner)
          expect(await rows).toHaveLength(1)
          expect(server.shapes.size).toBe(1)
          expect(server.requests[1].url.searchParams.get('handle')).toBe(
            early.handle
          )
          expect(server.requests[1].url.searchParams.get('offset')).toBe('-1')
        } finally {
          controller.abort()
        }
      })

      it('refetches with a new handle after owner loss while keeping the same endpoint', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const firstCreated = deferred<FixtureShape>()
        const replacementCreated = deferred<FixtureShape>()
        let allocations = 0
        server.onCreate = (owner) => {
          ;(++allocations === 1 ? firstCreated : replacementCreated).resolve(
            owner
          )
        }
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${server.url}/api/shape?tenant=one`,
          params: { table: 'items' },
          signal: controller.signal,
        })
        const shape = new Shape(stream)
        let refetches = 0
        stream.subscribe((batch) => {
          refetches += batch.filter(
            (message) => message.headers.control === 'must-refetch'
          ).length
        })
        try {
          const owner = await firstCreated.promise
          await baseline(server, owner)
          await shape.rows
          server.shapes.delete(owner.handle)
          const replacement = await replacementCreated.promise
          await baseline(server, replacement)
          await expect.poll(() => stream.isUpToDate).toBe(true)
          await expect.poll(() => stream.shapeHandle).toBe(replacement.handle)
          expect(replacement.handle).not.toBe(owner.handle)
          expect(refetches).toBe(1)
          expect(await shape.rows).toEqual([
            { id: 1, name: 'first', done: false },
          ])
          expect(
            server.requests.every(
              (request) =>
                request.url.pathname === '/api/shape' &&
                request.url.searchParams.get('tenant') === 'one'
            )
          ).toBe(true)
        } finally {
          controller.abort()
        }
      })

      it('delivers live inserts, updates, deletes and refresh with fresh auth at the same endpoint', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const created = deferred<FixtureShape>()
        server.onCreate = (owner) => {
          created.resolve(owner)
        }
        const controller = new AbortController()
        let token = 'first'
        const stream = new ShapeStream({
          url: `${server.url}/api/shape?tenant=trusted`,
          params: { table: 'items', request_token: () => token },
          headers: { Authorization: () => `Bearer ${token}` },
          signal: controller.signal,
        })
        const shape = new Shape(stream)
        const messages: unknown[] = []
        stream.subscribe((batch) => {
          messages.push(...batch)
        })
        try {
          const owner = await created.promise
          await baseline(server, owner)
          await shape.rows
          const checkpoint = stream.lastOffset
          await server.append(owner, {
            version: 1,
            seq: '4',
            kind: 'publication',
            evidence: { txids: ['4294967300'], lsn: '9007199254740993' },
            changes: [
              {
                key: '"public"."items"/"1"',
                value: { name: 'updated' },
                headers: { operation: 'update' },
              },
              {
                key: '"public"."items"/"2"',
                value: { id: '2', name: 'second', done: 't' },
                headers: { operation: 'insert' },
              },
            ],
          })
          await expect
            .poll(() => shape.currentRows)
            .toEqual([
              { id: 1, name: 'updated', done: false },
              { id: 2, name: 'second', done: true },
            ])
          expect(stream.lastOffset).not.toBe(checkpoint)
          expect(messages).toContainEqual(
            expect.objectContaining({
              headers: expect.objectContaining({
                operation: 'update',
                txids: [4],
              }),
            })
          )
          await server.append(owner, {
            version: 1,
            seq: '5',
            kind: 'publication',
            evidence: {},
            changes: [
              {
                key: '"public"."items"/"1"',
                value: { id: '1' },
                headers: { operation: 'delete' },
              },
            ],
          })
          await expect
            .poll(() => shape.currentRows)
            .toEqual([{ id: 2, name: 'second', done: true }])
          token = 'refreshed'
          let timeout: ReturnType<typeof setTimeout> | undefined
          try {
            await Promise.race([
              stream.forceDisconnectAndRefresh(),
              new Promise<never>((_resolve, reject) => {
                timeout = setTimeout(
                  () =>
                    reject(
                      new Error(
                        `Refresh stalled: ${JSON.stringify({ offset: stream.lastOffset, upToDate: stream.isUpToDate, error: stream.error, requests: server.requests.slice(-5).map((request) => request.url.toString()) })}`
                      )
                    ),
                  2000
                )
              }),
            ])
          } finally {
            clearTimeout(timeout)
          }
          await expect
            .poll(() =>
              server.requests.some(
                (request) => request.authorization === 'Bearer refreshed'
              )
            )
            .toBe(true)
          expect(
            server.requests
              .filter((request) => request.authorization === 'Bearer refreshed')
              .every(
                (request) =>
                  request.url.searchParams.get('request_token') === 'refreshed'
              )
          ).toBe(true)
          expect(server.shapes.size).toBe(1)
          expect(
            server.requests.some(
              (request) => request.url.searchParams.get('live') === 'true'
            )
          ).toBe(true)
          expect(
            server.requests.every(
              (request) =>
                request.url.pathname === '/api/shape' &&
                request.method === 'GET'
            )
          ).toBe(true)
        } finally {
          controller.abort()
        }
      })

      it('preserves the configured URL and waits across temporary tails for one-shot readiness', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const created = deferred<FixtureShape>()
        server.onCreate = (shape) => {
          created.resolve(shape)
        }
        const controller = new AbortController()
        const options = {
          url: `${server.url}/api/shape?tenant=a&tag=a%2Bb&tag=c`,
          params: { table: 'items' },
          subscribe: false,
          signal: controller.signal,
        }
        const stream = new ShapeStream(options)
        const shape = new Shape(stream)
        let notified = false
        const initialRows = shape.rows.then((rows) => {
          notified = true
          return rows
        })
        try {
          const owner = await created.promise
          // Synchronize on the second read, proving the initial response was
          // consumed and its temporary tail did not finish the one-shot read.
          await expect
            .poll(() => server.requests.length)
            .toBeGreaterThanOrEqual(2)
          expect(stream.shapeHandle).toBe(owner.handle)
          expect(notified).toBe(false)
          expect(stream.isUpToDate).toBe(false)
          await baseline(server, owner)
          expect(await initialRows).toEqual([
            { id: 1, name: 'first', done: false },
          ])
          expect(stream.isUpToDate).toBe(true)
          expect(server.shapes.size).toBe(1)
          for (const { url, method } of server.requests) {
            expect(url.origin).toBe(server.url)
            expect(url.pathname).toBe('/api/shape')
            expect(url.searchParams.getAll('tag')).toEqual(['a+b', 'c'])
            expect(url.searchParams.get('tenant')).toBe('a')
            expect(url.searchParams.get('table')).toBe('items')
            expect(method).toBe('GET')
            expect(
              url.searchParams.has('handle')
                ? url.searchParams.get('handle')
                : owner.handle
            ).toBe(owner.handle)
          }
        } finally {
          controller.abort()
        }
      })

      it('restores readiness at a saved offset even when no later envelopes exist', async () => {
        const server = new QueryShapeServer(electricOnly)
        servers.push(server)
        await server.start()
        const ownerReady = deferred<FixtureShape>()
        server.onCreate = (owner) => {
          ownerReady.resolve(owner)
        }
        const firstAbort = new AbortController()
        const first = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          subscribe: false,
          signal: firstAbort.signal,
        })
        const firstShape = new Shape(first)
        const owner = await ownerReady.promise
        await baseline(server, owner)
        await firstShape.rows
        const offset = first.lastOffset
        firstAbort.abort()
        const resumedAbort = new AbortController()
        const resumed = new ShapeStream({
          url: `${server.url}/api/shape`,
          params: { table: 'items' },
          handle: owner.handle,
          offset,
          subscribe: false,
          signal: resumedAbort.signal,
        })
        const resumedShape = new Shape(resumed)
        try {
          expect(await resumedShape.rows).toEqual([])
          expect(resumed.isUpToDate).toBe(true)
          expect(resumed.lastOffset).toBe(offset)
          expect(server.shapes.size).toBe(1)
        } finally {
          resumedAbort.abort()
        }
      })
    }
  )
}
