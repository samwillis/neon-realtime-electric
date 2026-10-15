import { randomUUID } from 'node:crypto'
import { Client } from 'pg'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { startLocalFunction } from './support/local-function.js'
import {
  FetchError,
  Shape,
  ShapeStream,
  isControlMessage,
  isVisibleInSnapshot,
  type PostgresSnapshot,
} from '../../packages/client/src/index.js'

const schema = `electric_quoted_${randomUUID().replaceAll('-', '')}`
const table = `"${schema}".items`
let client: Client
let local: Awaited<ReturnType<typeof startLocalFunction>> | undefined
let origin: string

beforeAll(async () => {
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required')
  client = new Client({
    connectionString: process.env.DATABASE_URL,
    connectionTimeoutMillis: 15_000,
    query_timeout: 15_000,
  })
  await client.connect()
  await client.query(`create schema "${schema}"`)
  await client.query(`create table ${table} (
    id int primary key, normal text, "has,comma" text,
    "has""quote" text, "has space" text)`)
  if (!process.env.NEON_REALTIME_URL)
    throw new Error('NEON_REALTIME_URL is required')
  local = await startLocalFunction({
    realtimeUrl: process.env.NEON_REALTIME_URL,
    artifact: 'quoted',
    tables: [
      {
        namespace: schema,
        table: 'items',
        primaryKey: ['id'],
        columns: {
          id: { type: 'int4' },
          normal: { type: 'text' },
          'has,comma': { type: 'text' },
          'has"quote': { type: 'text' },
          'has space': { type: 'text' },
        },
      },
    ],
  })
  origin = local.origin
}, 35_000)

afterAll(async () => {
  await local?.stop()
  try {
    await client?.query(`drop schema if exists "${schema}" cascade`)
  } finally {
    await client?.end()
  }
})

it.each([false, true])(
  'projects comma/quote/space identifiers with stable incremental history (SSE=%s)',
  async (liveSse) => {
    await client.query(`truncate ${table}`)
    await client.query(
      `insert into ${table} values(1,'initial','comma','quote','excluded')`
    )
    const abort = new AbortController()
    const errors: Error[] = []
    const handles = new Set<string>()
    const requests: URL[] = []
    const columns = ['id', 'normal', 'has,comma', 'has"quote']
    let snapshot: PostgresSnapshot | undefined
    const stream = new ShapeStream({
      url: `${origin}/proxy/shapes?tenant=a%2Bb`,
      params: { table: 'items', columns },
      liveSse,
      signal: abort.signal,
      fetchClient: async (input, init) => {
        const request = new Request(input, init)
        requests.push(new URL(request.url))
        expect(request.method).toBe('GET')
        const response = await fetch(request)
        const handle = response.headers.get('electric-handle')
        if (handle) handles.add(handle)
        return response
      },
      onError(error) {
        errors.push(error)
      },
    })
    const shape = new Shape(stream)
    stream.subscribe((messages) => {
      for (const message of messages)
        if (
          isControlMessage(message) &&
          message.headers.control === 'snapshot-end'
        )
          snapshot = message.headers
    })
    const compare = async (txid?: `${bigint}`) => {
      await expect
        .poll(
          () => {
            if (errors.length) throw errors[0]
            return (
              shape.isUpToDate &&
              !!snapshot &&
              (!txid || isVisibleInSnapshot(txid, snapshot))
            )
          },
          { timeout: 20_000, interval: 50 }
        )
        .toBe(true)
      const expected = (
        await client.query(
          `select id, normal, "has,comma", "has""quote" from ${table} order by id`
        )
      ).rows
      expect(shape.currentRows).toEqual(expected)
      expect(shape.currentRows[0]).not.toHaveProperty('has space')
      expect(handles.size).toBe(1)
    }
    try {
      await compare()
      const handle = stream.shapeHandle
      const changed = await client.query(`update ${table}
        set normal='updated', "has,comma"='comma updated', "has""quote"='quote updated', "has space"='still excluded'
        where id=1 returning pg_current_xact_id()::text as txid`)
      await compare(changed.rows[0].txid)
      expect(stream.shapeHandle).toBe(handle)
      expect(errors).toEqual([])
      for (const request of requests) {
        expect(request.origin).toBe(origin)
        expect(request.pathname).toBe('/proxy/shapes')
        expect(request.searchParams.get('tenant')).toBe('a+b')
        if (request.searchParams.has('handle'))
          expect(request.searchParams.get('handle')).toBe(handle)
      }
    } finally {
      abort.abort()
      stream.unsubscribeAll()
      if (stream.shapeHandle) {
        const cleanup = new URL(`${origin}/proxy/shapes`)
        cleanup.searchParams.set('table', 'items')
        cleanup.searchParams.set(
          'columns',
          columns.map((c) => `"${c.replaceAll('"', '""')}"`).join(',')
        )
        cleanup.searchParams.set('handle', stream.shapeHandle)
        await fetch(cleanup, {
          method: 'DELETE',
          signal: AbortSignal.timeout(5000),
        })
      }
    }
  }
)

it.each([false, true])(
  'terminates invalid SQL with the safe native public error contract (SSE=%s)',
  async (liveSse) => {
    const abort = new AbortController()
    const handled: Error[] = []
    const stream = new ShapeStream({
      url: `${origin}/proxy/shapes`,
      params: { table: 'items', where: '1 x 1' },
      liveSse,
      signal: abort.signal,
      onError(error) {
        handled.push(error)
      },
    })
    try {
      const failure = await new Promise<Error>((resolve) =>
        stream.subscribe(() => {}, resolve)
      )
      expect(failure).toBeInstanceOf(FetchError)
      expect((failure as FetchError).status).toBe(400)
      expect((failure as FetchError).json).toEqual({
        error: 'Realtime rejected the shape query',
      })
      expect(stream.error).toBe(failure)
      expect(stream.isConnected()).toBe(false)
      expect(handled).toEqual([failure])
    } finally {
      abort.abort()
      stream.unsubscribeAll()
    }
  }
)
