import { describe, expect, it } from 'vitest'
import {
  FetchError,
  Shape,
  ShapeStream,
  snakeCamelMapper,
  type ShapeStreamOptions,
  type Row,
} from '../../packages/client/src/index.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'
import { MissingHeadersError } from '../../packages/client/src/error.js'

function fixture(electricOnly: boolean) {
  const observers: SourceObserver[] = []
  const requests: Request[] = []
  const date = '2026-10-16T12:00:00+00:00'
  const handler = createShapeHandler({
    longPollMs: 1_000,
    sseConnectionMs: 1_000,
    source: {
      async subscribe(_, observer) {
        observers.push(observer)
        observer.event({
          type: 'schema',
          columns: [
            { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
            {
              name: 'display_name',
              type_oid: 25,
              typmod: -1,
              codec: 'pg_text',
            },
            {
              name: 'created_at',
              type_oid: 1184,
              typmod: -1,
              codec: 'pg_text',
            },
          ],
        })
        observer.event({
          type: 'baseline',
          rows: [{ row_key: 'a'.repeat(64), values: ['1', 'before', date] }],
          mvcc: { xmin: '10', xmax: '10', xip: [] },
        })
        return { close() {} }
      },
      close() {},
    },
    async authorize() {
      return {
        authorizationKey: 'option-tests',
        definition: {
          namespace: 'public',
          table: 'items',
          primaryKey: ['id'],
          projection: ['id', 'display_name', 'created_at'],
          schema: {
            id: { type: 'int4' },
            display_name: { type: 'text' },
            created_at: { type: 'timestamptz' },
          },
          query: { sql: 'select id,display_name,created_at from public.items' },
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
    for (const key of [...headers.keys()])
      if (key.startsWith('stream-')) headers.delete(key)
    return new Response(response.body, { status: response.status, headers })
  }
  return {
    handler,
    requests,
    fetchClient,
    observers,
    date,
    async close<T extends Row<unknown>>(
      stream: ShapeStream<T>,
      abort: AbortController
    ) {
      abort.abort()
      stream.unsubscribeAll()
      handler.close()
      await handler.drain()
    },
  }
}

for (const electricOnly of [false, true])
  for (const liveSse of [false, true])
    describe(`native option behavior (Electric headers=${electricOnly}, SSE=${liveSse})`, () => {
      it.each(['params', 'headers', 'both', 'empty'] as const)(
        'recovers after an async error handler returns %s and preserves other auth context',
        async (kind) => {
          const f = fixture(electricOnly)
          const abort = new AbortController()
          const errors: unknown[] = []
          const attempts: Request[] = []
          const stream = new ShapeStream({
            url: 'http://local/application/shapes?tenant=a%2Bb',
            params: { table: 'items', token: 'before' },
            headers: { authorization: 'Bearer before', 'x-custom': 'keep' },
            liveSse,
            signal: abort.signal,
            backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1 },
            fetchClient: async (input, init) => {
              const request = new Request(input, init)
              attempts.push(request)
              if (attempts.length === 1)
                return new Response('authorization expired', { status: 401 })
              return f.fetchClient(input, init)
            },
            onError: async (error) => {
              errors.push(error)
              await Promise.resolve()
              return {
                ...(kind === 'params' || kind === 'both'
                  ? { params: { token: 'after' } }
                  : {}),
                ...(kind === 'headers' || kind === 'both'
                  ? { headers: { authorization: 'Bearer after' } }
                  : {}),
              }
            },
          })
          expect(stream.hasStarted()).toBe(false)
          expect(stream.isConnected()).toBe(false)
          expect(stream.isLoading()).toBe(true)
          expect(stream.lastSyncedAt()).toBeUndefined()
          const shape = new Shape(stream)
          try {
            expect(await shape.rows).toMatchObject([
              { id: 1, display_name: 'before' },
            ])
            expect(errors).toHaveLength(1)
            expect(errors[0]).toBeInstanceOf(FetchError)
            expect((errors[0] as FetchError).status).toBe(401)
            await expect
              .poll(() =>
                f.requests.some(
                  (r) => new URL(r.url).searchParams.get('live') === 'true'
                )
              )
              .toBe(true)
            for (const request of attempts.slice(1)) {
              const url = new URL(request.url)
              expect(url.pathname).toBe('/application/shapes')
              expect(url.searchParams.get('tenant')).toBe('a+b')
              expect(url.searchParams.get('table')).toBe('items')
              expect(url.searchParams.get('token')).toBe(
                kind === 'params' || kind === 'both' ? 'after' : 'before'
              )
              expect(request.headers.get('authorization')).toBe(
                kind === 'headers' || kind === 'both'
                  ? 'Bearer after'
                  : 'Bearer before'
              )
              expect(request.headers.get('x-custom')).toBe('keep')
            }
            expect(stream.error).toBeNull()
            expect(stream.isConnected()).toBe(true)
            expect(stream.isLoading()).toBe(false)
            expect(f.observers).toHaveLength(1)
          } finally {
            await f.close(stream, abort)
          }
        }
      )

      it.each(['null', 'undefined', 'no-handler'] as const)(
        'propagates one public error and stops when onError is %s',
        async (kind) => {
          const f = fixture(electricOnly)
          const abort = new AbortController()
          let attempts = 0
          let handled = 0
          // JavaScript callers can return null even though the TS contract is void.
          const onError = (
            kind === 'no-handler'
              ? undefined
              : () => {
                  handled++
                  return kind === 'null' ? null : undefined
                }
          ) as ShapeStreamOptions['onError']
          const stream = new ShapeStream({
            url: 'http://local/application/shapes',
            signal: abort.signal,
            liveSse,
            fetchClient: async () => {
              attempts++
              return new Response('forbidden', { status: 403 })
            },
            onError,
          })
          try {
            const failure = await new Promise<Error>((resolve) => {
              stream.subscribe(() => {}, resolve)
            })
            expect(failure).toBeInstanceOf(FetchError)
            expect((failure as FetchError).status).toBe(403)
            expect(stream.error).toBe(failure)
            expect(stream.isConnected()).toBe(false)
            expect(handled).toBe(kind === 'no-handler' ? 0 : 1)
            expect(attempts).toBe(1)
            expect(f.observers).toHaveLength(0)
          } finally {
            await f.close(stream, abort)
          }
        }
      )

      it('treats missing row schema as fatal even if onError asks to retry', async () => {
        const f = fixture(electricOnly)
        const abort = new AbortController()
        const errors: unknown[] = []
        const stream = new ShapeStream({
          url: 'http://local/application/shapes',
          signal: abort.signal,
          liveSse,
          fetchClient: async (input, init) => {
            const response = await f.fetchClient(input, init)
            const headers = new Headers(response.headers)
            headers.delete('electric-schema')
            return new Response(response.body, {
              status: response.status,
              headers,
            })
          },
          onError(error) {
            errors.push(error)
            return {}
          },
        })
        try {
          const error = await new Promise<Error>((resolve) => {
            stream.subscribe(() => {}, resolve)
          })
          expect(error).toBeInstanceOf(MissingHeadersError)
          expect(errors).toEqual([error])
          expect(stream.error).toBe(error)
          expect(stream.isConnected()).toBe(false)
          expect(f.observers).toHaveLength(1)
        } finally {
          await f.close(stream, abort)
        }
      })

      it('parses custom values, maps columns, then transforms baseline and live rows', async () => {
        const f = fixture(electricOnly)
        const abort = new AbortController()
        type Item = { id: number; displayName: string; createdAt: Date }
        const transformed: unknown[] = []
        const options: ShapeStreamOptions<Date> = {
          url: 'http://local/application/shapes',
          params: { table: 'items' },
          signal: abort.signal,
          liveSse,
          fetchClient: f.fetchClient,
          parser: { timestamptz: (value) => new Date(value) },
          columnMapper: snakeCamelMapper(),
          transformer: (row) => {
            transformed.push({ ...row })
            if ('createdAt' in row) expect(row.createdAt).toBeInstanceOf(Date)
            expect(row).not.toHaveProperty('display_name')
            return {
              ...row,
              displayName: String(row.displayName).toUpperCase(),
            }
          },
        }
        const stream = new ShapeStream<Item>(options)
        const shape = new Shape(stream)
        try {
          expect(await shape.rows).toEqual([
            { id: 1, displayName: 'BEFORE', createdAt: new Date(f.date) },
          ])
          f.observers[0].event({
            type: 'publication',
            changes: [
              {
                op: 'upsert',
                row_key: 'a'.repeat(64),
                values: ['1', 'after', f.date],
              },
            ],
            txids: ['11'],
            frontier: { lsn: '0/20' },
          })
          await expect
            .poll(() => shape.currentRows)
            .toEqual([
              { id: 1, displayName: 'AFTER', createdAt: new Date(f.date) },
            ])
          expect(transformed).toEqual([
            { id: 1, displayName: 'before', createdAt: new Date(f.date) },
            { id: 1, displayName: 'after' },
          ])
        } finally {
          await f.close(stream, abort)
        }
      })
    })
