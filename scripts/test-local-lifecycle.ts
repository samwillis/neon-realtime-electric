import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import pg from 'pg'
import {
  Shape,
  ShapeStream,
  type Message,
  type Offset,
} from '../packages/client/src/index.js'

type Item = { id: number; label: string; enabled: boolean }
const base = process.env.SHAPE_URL ?? 'http://localhost:4327/api/shape'
const databaseUrl = process.env.DATABASE_URL
assert(databaseUrl, 'Local staging lifecycle test requires DATABASE_URL')
const db = new pg.Client({
  connectionString: databaseUrl,
  connectionTimeoutMillis: 15_000,
  query_timeout: 15_000,
})
const controllers: AbortController[] = []
const handles = new Set<string>()
const errors: unknown[] = []
const params = { table: 'electric_items', where: 'enabled = true' }
const id = 100 + Math.floor(Math.random() * 1_000_000_000)
const messages: Message<Item>[] = []
const report: Record<string, unknown> = {
  startedAt: new Date().toISOString(),
  id,
  status: 'running',
}
const open = (
  options: {
    handle?: string
    offset?: Offset
    subscribe?: boolean
  } = {}
) => {
  const abort = new AbortController()
  controllers.push(abort)
  const stream = new ShapeStream<Item>({
    url: base,
    params,
    signal: abort.signal,
    onError: (error) => {
      errors.push(error)
    },
    ...options,
  })
  stream.subscribe((batch) => {
    messages.push(...batch)
  })
  const shape = new Shape(stream)
  return { stream, shape, abort }
}
async function until(predicate: () => boolean) {
  const end = Date.now() + 20_000
  while (Date.now() < end) {
    if (errors.length) throw errors[0]
    if (predicate()) return
    await delay(25)
  }
  throw new Error('Local lifecycle condition timed out')
}
await db.connect()
try {
  const initial = open()
  const baseline = await initial.shape.rows
  await delay(30)
  const handle = initial.stream.shapeHandle!
  const offset = initial.stream.lastOffset
  handles.add(handle)
  assert(handle && offset !== '-1')
  initial.abort.abort()
  await db.query(
    'insert into public.electric_items (id,label,enabled) values ($1,$2,true)',
    [id, 'written without readers']
  )
  await delay(750)
  const replay = open({ handle, offset, subscribe: false })
  const replayed = await replay.shape.rows
  assert(
    replayed.some(
      (row) => row.id === id && row.label === 'written without readers'
    )
  )
  assert.equal(
    replay.stream.shapeHandle,
    handle,
    'No-reader replay unexpectedly replaced the owner'
  )
  const combined = new Map(baseline.map((row) => [row.id, row]))
  for (const row of replayed) combined.set(row.id, row)
  const current = await db.query(
    'select id,label,enabled from public.electric_items where enabled = true order by id'
  )
  assert.deepEqual(
    [...combined.values()].sort((a, b) => a.id - b.id),
    current.rows
  )
  replay.abort.abort()
  const live = open()
  await live.shape.rows
  handles.add(live.stream.shapeHandle!)
  const txids: string[] = []
  for (const [sql, values, label] of [
    [
      'update public.electric_items set label=$2 where id=$1 returning pg_current_xact_id()::text as txid',
      [id, 'updated'],
      'updated',
    ],
    [
      'update public.electric_items set enabled=false where id=$1 returning pg_current_xact_id()::text as txid',
      [id],
      null,
    ],
    [
      'update public.electric_items set enabled=true where id=$1 returning pg_current_xact_id()::text as txid',
      [id],
      'updated',
    ],
    [
      'delete from public.electric_items where id=$1 returning pg_current_xact_id()::text as txid',
      [id],
      null,
    ],
  ] as const) {
    const result = await db.query(sql, [...values])
    txids.push(result.rows[0].txid)
    await until(() =>
      label === null
        ? !live.shape.currentRows.some((row) => row.id === id)
        : live.shape.currentRows.some(
            (row) => row.id === id && row.label === label
          )
    )
  }
  await until(() =>
    txids.every((tx) =>
      messages.some(
        (message) =>
          'operation' in message.headers &&
          Array.isArray(message.headers.txids) &&
          message.headers.txids.includes(Number(BigInt.asUintN(32, BigInt(tx))))
      )
    )
  )
  assert.equal(errors.length, 0)
  Object.assign(report, {
    status: 'passed',
    completedAt: new Date().toISOString(),
    handle,
    offset,
    baselineCount: baseline.length,
    replayedCount: replayed.length,
    txids,
  })
  console.log(
    'Local neon dev lifecycle passed: no-reader replay, live update/delete/filter transitions and transaction IDs'
  )
} catch (error) {
  Object.assign(report, {
    status: 'failed',
    error: error instanceof Error ? error.message : String(error),
  })
  throw error
} finally {
  for (const controller of controllers) controller.abort()
  await db.query('delete from public.electric_items where id=$1', [id])
  for (const handle of handles) {
    const url = new URL(base)
    for (const [key, value] of Object.entries({ ...params, handle }))
      url.searchParams.set(key, value)
    await fetch(url, { method: 'DELETE', signal: AbortSignal.timeout(10_000) })
  }
  await db.end()
  await mkdir('.artifacts/staging', { recursive: true })
  await writeFile(
    '.artifacts/staging/local-lifecycle.json',
    JSON.stringify(report, null, 2)
  )
}
