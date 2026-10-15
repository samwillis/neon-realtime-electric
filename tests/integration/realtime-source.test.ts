import { ShapeOwner } from '../../packages/server/src/shape-owner.js'
import { randomUUID } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { Client } from 'pg'
import { recordRealtimeWire } from './support/wire-recorder.js'
import { describe, expect, it } from 'vitest'
import {
  createNeonRealtimeSource,
  type SourceEvent,
} from '../../packages/server/src/realtime-source.js'

function required(name: string): string {
  const value = process.env[name]
  if (!value)
    throw new Error(
      `Staging integration requires ${name}; load the ignored .env.local file`
    )
  return value
}

function visible(
  mvcc: { xmin: string; xmax: string; xip: readonly string[] },
  txid: string
): boolean {
  const id = BigInt(txid)
  return (
    id < BigInt(mvcc.xmin) ||
    (id < BigInt(mvcc.xmax) && !mvcc.xip.includes(txid))
  )
}

// This gate deliberately uses real services. Missing credentials are a failure.
describe('real staging Realtime source evidence', () => {
  it('preserves raw rows and ordered acknowledgements for baseline, filtered changes, unrelated writes and empty shapes', async () => {
    const databaseUrl = required('DATABASE_URL')
    const client = new Client({
      connectionString: databaseUrl,
      connectionTimeoutMillis: 15_000,
      query_timeout: 15_000,
    })
    const relay = await recordRealtimeWire(required('NEON_REALTIME_URL'))
    const source = createNeonRealtimeSource({
      url: relay.url,
      secret: required('NEON_REALTIME_SECRET'),
      db: new URL(databaseUrl).pathname.slice(1),
    })
    const schema = `electric_test_${randomUUID().replaceAll('-', '')}`
    const events: SourceEvent[] = []
    const emptyEvents: SourceEvent[] = []
    const errors: Error[] = []
    const abort = new AbortController()
    let connected = false
    try {
      await client.connect()
      connected = true
      expect(
        (await client.query('show wal_level')).rows[0].wal_level,
        'Enable logical replication on the dedicated staging project before live tests'
      ).toBe('logical')
      // The generated identifier contains only a constant prefix and UUID hex.
      await client.query(`create schema "${schema}"`)
      await client.query(
        `create table "${schema}".items (id bigint primary key, label text not null, enabled boolean not null, amount numeric); create table "${schema}".other (id int primary key)`
      )
      const seeded = await client.query(
        `insert into "${schema}".items values (9007199254740993, 'initial', true, 123456789012345678.123456) returning pg_current_xact_id()::text as txid`
      )
      const beforeTx = seeded.rows[0].txid as string
      const sql = `select id, label, enabled, amount from "${schema}".items where enabled = true`
      await source.subscribe(
        { sql },
        { event: (e) => events.push(e), error: (e) => errors.push(e) },
        abort.signal
      )
      await source.subscribe(
        {
          sql: `select id, label from "${schema}".items where id = $1`,
          parameters: ['42'],
        },
        { event: (e) => emptyEvents.push(e), error: (e) => errors.push(e) },
        abort.signal
      )
      const wait = async (predicate: () => boolean) => {
        await expect
          .poll(
            () => {
              if (errors.length) throw errors[0]
              return predicate()
            },
            { timeout: 30_000, interval: 50 }
          )
          .toBe(true)
      }
      await wait(
        () =>
          events.some((e) => e.type === 'baseline') &&
          emptyEvents.some((e) => e.type === 'baseline')
      )
      const baseline = events.find((e) => e.type === 'baseline')!
      expect(baseline.type).toBe('baseline')
      if (baseline.type !== 'baseline') throw new Error('Missing baseline')
      expect(baseline.rows.map((r) => r.values)).toEqual([
        ['9007199254740993', 'initial', 't', '123456789012345678.123456'],
      ])
      expect(visible(baseline.mvcc, beforeTx)).toBe(true)
      expect(events[0].type).toBe('schema')
      expect(emptyEvents.find((e) => e.type === 'baseline')).toMatchObject({
        rows: [],
      })
      const inserted = await client.query(
        `insert into "${schema}".items values (2, 'second', true, null) returning pg_current_xact_id()::text as txid`
      )
      const insertTx = inserted.rows[0].txid as string
      await wait(() =>
        events.some(
          (e) => e.type === 'publication' && e.txids.includes(insertTx)
        )
      )
      const publication = events.find(
        (e) => e.type === 'publication' && e.txids.includes(insertTx)
      )!
      expect(publication).toMatchObject({
        changes: [{ op: 'upsert', values: ['2', 'second', 't', null] }],
      })
      if (publication.type !== 'publication')
        throw new Error('Missing publication')
      expect(publication.frontier.lsn).toMatch(/^[0-9A-F]+\/[0-9A-F]+$/)
      const removed = await client.query(
        `update "${schema}".items set enabled = false where id = 2 returning pg_current_xact_id()::text as txid`
      )
      const removeTx = removed.rows[0].txid as string
      await wait(() =>
        events.some(
          (e) => e.type === 'publication' && e.txids.includes(removeTx)
        )
      )
      expect(
        events.find(
          (e) => e.type === 'publication' && e.txids.includes(removeTx)
        )
      ).toMatchObject({ changes: [{ op: 'remove' }] })
      const unrelated = await client.query(
        `insert into "${schema}".other values (1) returning pg_current_xact_id()::text as txid`
      )
      const unrelatedTx = unrelated.rows[0].txid as string
      const acknowledges = (list: SourceEvent[], tx: string) =>
        list.some((e) =>
          e.type === 'progress' || e.type === 'baseline'
            ? visible(e.mvcc, tx)
            : e.type === 'publication' && e.txids.includes(tx)
        )
      await wait(
        () =>
          acknowledges(events, unrelatedTx) &&
          acknowledges(emptyEvents, unrelatedTx)
      )
      // State after all publications agrees with a fresh SQL query.
      const rows = new Map(baseline.rows.map((r) => [r.row_key, r.values]))
      for (const event of events)
        if (event.type === 'publication')
          for (const change of event.changes) {
            if (change.op === 'remove') rows.delete(change.row_key)
            else rows.set(change.row_key, change.values)
          }
      const current = await client.query(
        `select id::text, label, case when enabled then 't' else 'f' end as enabled, amount::text from "${schema}".items where enabled = true`
      )
      expect([...rows.values()]).toEqual(
        current.rows.map((r) => [r.id, r.label, r.enabled, r.amount])
      )
      expect(errors).toEqual([])
    } finally {
      await mkdir('.artifacts/staging', { recursive: true })
      await writeFile(
        '.artifacts/staging/source-evidence.json',
        JSON.stringify(
          {
            wire: relay.messages,
            events,
            emptyEvents,
            errors: errors.map((e) => e.message),
          },
          null,
          2
        )
      )
      abort.abort()
      source.close()
      await relay.close()
      try {
        if (connected)
          await client.query(`drop schema if exists "${schema}" cascade`)
      } finally {
        await client.end()
      }
    }
  })

  it('reconciles writes committed while baseline delivery is held, preserving a multichunk owner snapshot and publication order', async () => {
    const databaseUrl = required('DATABASE_URL')
    const client = new Client({
      connectionString: databaseUrl,
      connectionTimeoutMillis: 15_000,
      query_timeout: 15_000,
    })
    const relay = await recordRealtimeWire(required('NEON_REALTIME_URL'), {
      holdBaselineEnd: true,
    })
    const source = createNeonRealtimeSource({
      url: relay.url,
      secret: required('NEON_REALTIME_SECRET'),
      db: new URL(databaseUrl).pathname.slice(1),
    })
    const schema = `electric_cut_${randomUUID().replaceAll('-', '')}`
    const events: SourceEvent[] = []
    let owner: ShapeOwner | undefined
    try {
      await client.connect()
      await client.query(`create schema "${schema}"`)
      await client.query(
        `create table "${schema}".items(id int primary key, label text, enabled boolean not null)`
      )
      await client.query(
        `insert into "${schema}".items select i, 'seed ' || i, true from generate_series(1,260) i`
      )
      const observing = {
        ...source,
        async subscribe(
          query: Parameters<typeof source.subscribe>[0],
          observer: Parameters<typeof source.subscribe>[1],
          signal: AbortSignal
        ) {
          return source.subscribe(
            query,
            {
              event(event) {
                events.push(event)
                observer.event(event)
              },
              error: observer.error,
            },
            signal
          )
        },
      }
      owner = new ShapeOwner({
        source: observing,
        definition: {
          query: {
            sql: `select id,label from "${schema}".items where enabled=true`,
          },
          namespace: schema,
          table: 'items',
          primaryKey: ['id'],
          projection: ['id', 'label'],
          schema: { id: { type: 'int4' }, label: { type: 'text' } },
        },
      })
      await owner.start()
      await Promise.race([
        relay.baselineHeld,
        new Promise((_, reject) => {
          const timer = setTimeout(
            () => reject(new Error('Baseline delivery barrier timed out')),
            20_000
          )
          timer.unref()
        }),
      ])
      expect(events.some((e) => e.type === 'baseline')).toBe(false)
      await client.query('begin')
      await client.query(`update "${schema}".items set label=null where id=1`)
      await client.query(
        `update "${schema}".items set enabled=false where id=2`
      )
      await client.query(`update "${schema}".items set id=301 where id=3`)
      await client.query(
        `insert into "${schema}".items values(302,'during baseline',true)`
      )
      const txid = (
        await client.query('select pg_current_xact_id()::text as txid')
      ).rows[0].txid as string
      await client.query('commit')
      await expect
        .poll(
          () =>
            relay.messages.some((message) => {
              const frame = message as { type: string; txids?: string[] }
              return (
                frame.type === 'keyed_results' && frame.txids?.includes(txid)
              )
            }),
          { timeout: 20_000 }
        )
        .toBe(true)
      relay.releaseBaseline()
      await expect
        .poll(
          () =>
            events.some(
              (e) => e.type === 'publication' && e.txids.includes(txid)
            ),
          { timeout: 20_000 }
        )
        .toBe(true)
      await owner.settled()
      const baseline = events.find((e) => e.type === 'baseline')!
      if (baseline.type !== 'baseline') throw new Error('Missing baseline')
      expect(baseline.rows).toHaveLength(260)
      expect(visible(baseline.mvcc, txid)).toBe(false)
      expect(events.findIndex((e) => e.type === 'baseline')).toBeLessThan(
        events.findIndex((e) => e.type === 'publication')
      )
      const rows = new Map<string, unknown>()
      const log = await owner.log.read('-1')
      expect(
        log.envelopes.filter((e) => e.kind === 'snapshot-chunk')
      ).toHaveLength(3)
      for (const envelope of log.envelopes) {
        if (
          envelope.kind === 'snapshot-chunk' ||
          envelope.kind === 'publication'
        )
          for (const change of envelope.changes) {
            if (!('key' in change))
              throw new Error('Unexpected move in root shape')
            if (change.headers.operation === 'delete') rows.delete(change.key)
            else rows.set(change.key, change.value)
          }
      }
      const expected = await client.query(
        `select id::text,label from "${schema}".items where enabled=true order by items.id`
      )
      const actual = [...rows.values()] as {
        id: string
        label: string | null
      }[]
      expect(actual.sort((a, b) => Number(a.id) - Number(b.id))).toEqual(
        expected.rows
      )
      expect(log.envelopes.find((e) => e.kind === 'publication')).toMatchObject(
        { evidence: { txids: [txid] } }
      )
    } finally {
      relay.releaseBaseline()
      owner?.close()
      source.close()
      await relay.close()
      await mkdir('.artifacts/staging', { recursive: true })
      await writeFile(
        '.artifacts/staging/baseline-concurrency.json',
        JSON.stringify({ events, wire: relay.messages }, null, 2)
      )
      try {
        await client.query(`drop schema if exists "${schema}" cascade`)
      } finally {
        await client.end()
      }
    }
  })
})
