import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'
import { mkdir, writeFile } from 'node:fs/promises'
import pg from 'pg'
import {
  Shape,
  ShapeStream,
  isChangeMessage,
  type Message,
} from '../packages/client/src/index.js'

type Item = { id: number; label: string; enabled: boolean }
const url = process.env.SHAPE_URL ?? 'http://localhost:4327/api/shape'
assert(process.env.DATABASE_URL, 'Live mode tests require staging DATABASE_URL')
const db = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 15_000,
  query_timeout: 15_000,
})
const start = 100 + Math.floor(Math.random() * 1_000_000_000)
const handles: { handle: string; log: string }[] = []
const controllers: AbortController[] = []
const report: {
  startedAt: string
  status: string
  cases: unknown[]
  error?: string
} = { startedAt: new Date().toISOString(), status: 'running', cases: [] }
await db.connect()
try {
  for (const liveSse of [false, true])
    for (const options of [
      { log: 'changes_only' as const, offset: '-1' as const },
      { log: 'changes_only' as const, offset: 'now' as const },
      { log: 'full' as const, offset: 'now' as const },
    ]) {
      await db.query('delete from public.electric_items where id in ($1,$2)', [
        start,
        start + 1,
      ])
      await db.query('insert into public.electric_items values($1,$2,true)', [
        start,
        'historical',
      ])
      const abort = new AbortController()
      controllers.push(abort)
      const messages: Message<Item>[] = []
      const errors: unknown[] = []
      const stream = new ShapeStream<Item>({
        url,
        params: { table: 'electric_items', replica: 'full' },
        ...options,
        liveSse,
        signal: abort.signal,
        onError: (error) => {
          errors.push(error)
        },
      })
      const shape = new Shape(stream)
      stream.subscribe((batch) => {
        messages.push(...batch)
      })
      const until = async (predicate: () => boolean) => {
        const end = Date.now() + 20_000
        while (Date.now() < end) {
          if (errors.length) throw errors[0]
          if (predicate()) return
          await delay(25)
        }
        throw new Error(
          `Live ${options.log}/${options.offset}/SSE=${liveSse} condition timed out`
        )
      }
      await until(() => shape.isUpToDate)
      assert.deepEqual(shape.currentRows, [])
      handles.push({ handle: stream.shapeHandle!, log: options.log })
      await db.query('update public.electric_items set label=$2 where id=$1', [
        start,
        'historical changed',
      ])
      await until(() =>
        messages.some(
          (message) =>
            isChangeMessage(message) &&
            message.headers.operation === 'update' &&
            message.value.id === start &&
            message.value.label === 'historical changed'
        )
      )
      if (options.log === 'changes_only')
        assert.deepEqual(shape.currentRows, [])
      const change = messages.find(
        (message) =>
          isChangeMessage(message) &&
          message.headers.operation === 'update' &&
          message.value.id === start
      )!
      assert('value' in change && change.value.enabled === true)
      assert('old_value' in change && change.old_value?.label === 'historical')
      assert(
        'old_value' in change &&
          !Object.hasOwn(change.old_value ?? {}, 'enabled')
      )
      await db.query('insert into public.electric_items values($1,$2,true)', [
        start + 1,
        'future insert',
      ])
      await until(() =>
        shape.currentRows.some(
          (row) => row.id === start + 1 && row.label === 'future insert'
        )
      )
      await db.query('update public.electric_items set label=$2 where id=$1', [
        start + 1,
        'future updated',
      ])
      await until(() =>
        shape.currentRows.some(
          (row) => row.id === start + 1 && row.label === 'future updated'
        )
      )
      await db.query('delete from public.electric_items where id=$1', [
        start + 1,
      ])
      await until(() => !shape.currentRows.some((row) => row.id === start + 1))
      const before = stream.shapeHandle
      const resumed = new ShapeStream<Item>({
        url,
        params: { table: 'electric_items', replica: 'full' },
        log: options.log,
        offset: 'now',
        handle: before,
        subscribe: false,
        signal: abort.signal,
        onError: (error) => {
          errors.push(error)
        },
      })
      assert.deepEqual(await new Shape(resumed).rows, [])
      assert.equal(resumed.shapeHandle, before)
      assert.equal(errors.length, 0)
      report.cases.push({ ...options, liveSse, status: 'passed' })
      abort.abort()
    }
  report.status = 'passed'
  console.log(
    'Live changes-only/now and full replica modes pass over long polling and SSE against local neon dev/staging'
  )
} catch (error) {
  report.status = 'failed'
  report.error = String(error)
  throw error
} finally {
  for (const controller of controllers) controller.abort()
  await db.query('delete from public.electric_items where id in ($1,$2)', [
    start,
    start + 1,
  ])
  for (const { handle, log } of handles) {
    const target = new URL(url)
    for (const [key, value] of Object.entries({
      handle,
      log,
      replica: 'full',
      table: 'electric_items',
    }))
      target.searchParams.set(key, value)
    await fetch(target, {
      method: 'DELETE',
      signal: AbortSignal.timeout(10_000),
    })
  }
  await db.end()
  await mkdir('.artifacts/staging', { recursive: true })
  await writeFile(
    '.artifacts/staging/local-modes.json',
    JSON.stringify(report, null, 2)
  )
}
