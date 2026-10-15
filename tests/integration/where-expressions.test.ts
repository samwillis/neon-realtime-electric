import { randomUUID } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { Client } from 'pg'
import { describe, expect, it } from 'vitest'
import { resolveConfiguredShape } from '../../packages/server/src/definition.js'
import { createShapeHandler } from '../../packages/server/src/handler.js'
import {
  Shape,
  ShapeStream,
  isControlMessage,
  isVisibleInSnapshot,
  type PostgresSnapshot,
} from '../../packages/client/src/index.js'
import { recordRealtimeWire } from './support/wire-recorder.js'
import { createServer } from 'node:http'
import { Readable } from 'node:stream'
import { once } from 'node:events'
import {
  createNeonRealtimeSource,
  type SourceEvent,
  type SourceQuery,
  type SourceObserver,
  SourceQueryRejectedError,
} from '../../packages/server/src/realtime-source.js'

function required(key: string): string {
  const value = process.env[key]
  if (!value) throw new Error(`WHERE integration requires ${key}`)
  return value
}
// Real Realtime queries; dependency changes may invalidate an owner and require
// a client refetch. Compare only after real installed MVCC proves synchronization.
describe('real Realtime WHERE dependency tracking', () => {
  it('reports a permanent Realtime WHERE rejection as one safe 400 rather than refetching forever', async () => {
    const databaseUrl = required('DATABASE_URL')
    const client = new Client({
      connectionString: databaseUrl,
      connectionTimeoutMillis: 15_000,
      query_timeout: 15_000,
    })
    const source = createNeonRealtimeSource({
      url: required('NEON_REALTIME_URL'),
      secret: required('NEON_REALTIME_SECRET'),
      db: new URL(databaseUrl).pathname.slice(1),
    })
    const schema = `electric_rejected_${randomUUID().replaceAll('-', '')}`
    let subscriptions = 0
    let sourceError: Error | undefined
    let handler: ReturnType<typeof createShapeHandler> | undefined
    try {
      await client.connect()
      await client.query(`create schema "${schema}"`)
      await client.query(`create table "${schema}".items(id int primary key)`)
      const definition = resolveConfiguredShape(
        new URL(
          'http://local/shape?table=items&where=missing_column%20%3D%20true'
        ),
        [
          {
            namespace: schema,
            table: 'items',
            primaryKey: ['id'],
            columns: { id: { type: 'int4' } },
          },
        ]
      )
      handler = createShapeHandler({
        source: {
          ...source,
          subscribe(query, observer, signal) {
            subscriptions++
            return source.subscribe(
              query,
              {
                event: observer.event,
                error(error) {
                  sourceError = error
                  observer.error(error)
                },
              },
              signal
            )
          },
        },
        async authorize() {
          return { authorizationKey: 'rejection', definition }
        },
      })
      const first = await handler.fetch(new Request('http://local/shape'))
      expect(first.status).toBe(200)
      const handle = first.headers.get('electric-handle')!
      await first.arrayBuffer()
      await expect
        .poll(() => sourceError, { timeout: 15_000 })
        .toBeInstanceOf(SourceQueryRejectedError)
      const response = await handler.fetch(
        new Request(`http://local/shape?handle=${handle}&offset=-1`)
      )
      expect(response.status).toBe(400)
      expect(await response.json()).toEqual({
        error: 'Realtime rejected the shape query',
      })
      expect(response.headers.has('electric-handle')).toBe(false)
      expect(subscriptions).toBe(1)
    } finally {
      handler?.close()
      await handler?.drain()
      source.close()
      try {
        await client.query(`drop schema if exists "${schema}" cascade`)
      } finally {
        await client.end()
      }
    }
  })

  it.each(['or-in', 'in', 'exists', 'not-exists'] as const)(
    '%s agrees with Postgres after dependency-only changes and overlapping transactions',
    async (mode) => {
      const databaseUrl = required('DATABASE_URL')
      const config = {
        connectionString: databaseUrl,
        connectionTimeoutMillis: 15_000,
        query_timeout: 15_000,
      }
      const client = new Client(config)
      const concurrent = new Client(config)
      const relay = await recordRealtimeWire(required('NEON_REALTIME_URL'))
      const source = createNeonRealtimeSource({
        url: relay.url,
        secret: required('NEON_REALTIME_SECRET'),
        db: new URL(databaseUrl).pathname.slice(1),
      })
      const schema = `electric_where_${randomUUID().replaceAll('-', '')}`
      const members = `"${schema}".memberships`
      const predicates = {
        'or-in': `enabled OR id IN (SELECT item_id FROM ${members} WHERE tenant_id = $1 AND active)`,
        in: `id IN (SELECT item_id FROM ${members} WHERE tenant_id = $1 AND active)`,
        exists: `EXISTS (SELECT 1 FROM ${members} m WHERE m.item_id = items.id AND m.tenant_id = $1 AND m.active)`,
        'not-exists': `NOT EXISTS (SELECT 1 FROM ${members} m WHERE m.item_id = items.id AND m.tenant_id = $1 AND m.active)`,
      }
      const events: SourceEvent[] = []
      const errors: Error[] = []
      const checkpoints: { name: string; txids: string[]; rows: unknown[] }[] =
        []
      let handler: ReturnType<typeof createShapeHandler> | undefined
      let http: ReturnType<typeof createServer> | undefined
      const abort = new AbortController()
      const requests: {
        path: string
        method: string
        handle: string | null
      }[] = []
      const clientErrors: unknown[] = []
      const snapshots = new Map<string, PostgresSnapshot>()
      try {
        await Promise.all([client.connect(), concurrent.connect()])
        await client.query(`create schema "${schema}"`)
        await client.query(
          `create table "${schema}".items(id int primary key, label text not null, enabled boolean not null); create table ${members}(item_id int primary key, tenant_id int not null, active boolean not null)`
        )
        await client.query(
          `insert into "${schema}".items values(1,'first',false),(2,'second',true),(3,'third',false); insert into ${members} values(1,7,true),(3,8,true)`
        )
        const definition = resolveConfiguredShape(
          new URL(
            `http://local/shape?${new URLSearchParams({
              table: 'items',
              where: predicates[mode],
              'params[1]': '7',
              replica: 'full',
            })}`
          ),
          [
            {
              namespace: schema,
              table: 'items',
              primaryKey: ['id'],
              columns: {
                id: { type: 'int4' },
                label: { type: 'text' },
                enabled: { type: 'bool' },
              },
            },
          ]
        )
        handler = createShapeHandler({
          longPollMs: 500,
          source: {
            ...source,
            subscribe(
              query: SourceQuery,
              observer: SourceObserver,
              signal: AbortSignal
            ) {
              return source.subscribe(
                query,
                {
                  event(event) {
                    events.push(event)
                    observer.event(event)
                  },
                  error(error) {
                    errors.push(error)
                    observer.error(error)
                  },
                },
                signal
              )
            },
          },
          async authorize(request) {
            // This test's trusted definition is immutable across handle refetch.
            // Every retry still passes through authorization at the same URL.
            expect(new URL(request.url).pathname).toBe('/proxy/shapes')
            return { authorizationKey: 'where-integration', definition }
          },
        })
        http = createServer((req, res) => {
          void (async () => {
            const url = new URL(req.url!, 'http://local')
            requests.push({
              path: url.pathname,
              method: req.method!,
              handle: url.searchParams.get('handle'),
            })
            const signal = new AbortController()
            res.on('close', () => signal.abort())
            const response = await handler!.fetch(
              new Request(url, { signal: signal.signal })
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
        const origin = `http://127.0.0.1:${(http.address() as { port: number }).port}`
        const stream = new ShapeStream<{
          id: number
          label: string
          enabled: boolean
        }>({
          url: `${origin}/proxy/shapes`,
          params: {
            table: 'items',
            where: predicates[mode],
            params: { '1': '7' },
            replica: 'full',
          },
          signal: abort.signal,
          onError(error) {
            clientErrors.push(error)
          },
        })
        const shape = new Shape(stream)
        stream.subscribe((batch) => {
          for (const message of batch)
            if (
              isControlMessage(message) &&
              message.headers.control === 'snapshot-end' &&
              stream.shapeHandle
            )
              snapshots.set(stream.shapeHandle, message.headers)
        })
        const check = async (name: string, txids: string[] = []) => {
          await expect
            .poll(
              () => {
                if (clientErrors.length) throw clientErrors[0]
                const snapshot = stream.shapeHandle
                  ? snapshots.get(stream.shapeHandle)
                  : undefined
                return (
                  shape.isUpToDate &&
                  !!snapshot &&
                  txids.every((tx) =>
                    isVisibleInSnapshot(tx as `${bigint}`, snapshot)
                  )
                )
              },
              { timeout: 25_000, interval: 50 }
            )
            .toBe(true)
          // Writers are quiescent here. The currently installed client's MVCC
          // covers every test transaction; capture rows before independent SQL.
          const actual = [...shape.currentRows].sort((a, b) => a.id - b.id)
          const expected = (
            await client.query(
              `select id, label, enabled from "${schema}".items where (${predicates[mode]}) order by id`,
              ['7']
            )
          ).rows
          expect(
            actual,
            `${mode}/${name}: acknowledged client state differs from SQL`
          ).toEqual(expected)
          checkpoints.push({ name, txids, rows: actual })
        }
        await check('baseline')
        const mutate = async (sql: string) =>
          (
            await client.query(
              `${sql} returning pg_current_xact_id()::text as txid`
            )
          ).rows[0].txid as string
        const inserted = await mutate(`insert into ${members} values(2,7,true)`)
        await check('dependency-only insert', [inserted])
        const removed = await mutate(
          `update ${members} set active=false where item_id=1`
        )
        await check('dependency-only filter exit', [removed])
        const changedTenant = await mutate(
          `update ${members} set tenant_id=7 where item_id=3`
        )
        await check('dependency-only tenant entry', [changedTenant])
        const deleted = await mutate(`delete from ${members} where item_id=2`)
        await check('dependency-only delete', [deleted])
        // Both writers have an assigned transaction ID before either commits.
        // Root/dependency modifications overlap, and commit order reverses start.
        await Promise.all([client.query('begin'), concurrent.query('begin')])
        const rootTx = (
          await client.query(
            `update "${schema}".items set label='concurrent root', enabled=true where id=1 returning pg_current_xact_id()::text as txid`
          )
        ).rows[0].txid as string
        const dependencyTx = (
          await concurrent.query(
            `update ${members} set active=true where item_id=1 returning pg_current_xact_id()::text as txid`
          )
        ).rows[0].txid as string
        await concurrent.query('commit')
        await client.query('commit')
        await check('overlapping root/dependency writes', [
          rootTx,
          dependencyTx,
        ])
        expect(clientErrors).toEqual([])
        expect(
          requests.every(
            (r) => r.path === '/proxy/shapes' && r.method === 'GET'
          )
        ).toBe(true)
      } finally {
        abort.abort()
        handler?.close()
        await handler?.drain()
        source.close()
        if (http) {
          http.closeAllConnections()
          await new Promise<void>((resolve) => http!.close(() => resolve()))
        }
        await relay.close()
        await mkdir('.artifacts/staging', { recursive: true })
        await writeFile(
          `.artifacts/staging/where-${mode}.json`,
          JSON.stringify(
            {
              mode,
              wire: relay.messages,
              requests,
              events,
              checkpoints,
              errors: errors.map((e) => e.message),
            },
            null,
            2
          )
        )
        await Promise.allSettled([
          client.query('rollback'),
          concurrent.query('rollback'),
        ])
        try {
          await client.query(`drop schema if exists "${schema}" cascade`)
        } finally {
          await Promise.all([client.end(), concurrent.end()])
        }
      }
    },
    120_000
  )
})
