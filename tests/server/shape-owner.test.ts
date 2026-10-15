import { describe, expect, it } from 'vitest'
import {
  ShapeOwner,
  lsnToDecimal,
  type OwnerDefinition,
} from '../../packages/server/src/shape-owner.js'
import type {
  RealtimeSource,
  SourceEvent,
  SourceObserver,
} from '../../packages/server/src/realtime-source.js'

const definition: OwnerDefinition = {
  query: {
    sql: 'select id, label from public.items where enabled = $1',
    parameters: [true],
  },
  namespace: 'public',
  table: 'items',
  primaryKey: ['id'],
  projection: ['label'],
  schema: { label: { type: 'text' } },
}
const columns: Extract<SourceEvent, { type: 'schema' }>['columns'] = [
  { name: 'id', type_oid: 20, typmod: -1, codec: 'pg_text' },
  { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
]
const mvcc = {
  xmin: '9007199254740993',
  xmax: '9007199254740999',
  xip: ['9007199254740996'],
}
const row = { row_key: 'a'.repeat(64), values: ['9007199254740993', 'first'] }

async function fixture(
  now?: () => number,
  replica: 'default' | 'full' = 'default'
) {
  let observer: SourceObserver
  let cancelled = false
  const source: RealtimeSource = {
    async subscribe(query, next, signal) {
      expect(query).toEqual(definition.query)
      observer = next
      signal.addEventListener('abort', () => {
        cancelled = true
      })
      return {
        close() {
          cancelled = true
        },
      }
    },
    close() {
      cancelled = true
    },
  }
  const owner = new ShapeOwner({
    source,
    definition: { ...definition, replica },
    now,
  })
  await owner.start()
  return {
    owner,
    push: (e: SourceEvent) => observer.event(e),
    fail: (e: Error) => observer.error(e),
    cancelled: () => cancelled,
  }
}

describe('serialized source shape owner', () => {
  it('keeps early initialization and schema separate from the explicit baseline barrier', async () => {
    const f = await fixture()
    try {
      expect(f.owner.schema).toBeUndefined()
      expect(
        (await f.owner.log.read('-1')).envelopes.map((e) => e.kind)
      ).toEqual(['initializing'])
      f.push({ type: 'schema', columns })
      await f.owner.settled()
      expect(f.owner.schema).toEqual(definition.schema)
      expect(f.owner.log.head.baselineComplete).toBe(false)
      f.push({ type: 'baseline', rows: [row], mvcc })
      await f.owner.settled()
      const read = await f.owner.log.read('-1')
      expect(read.envelopes.map((e) => e.kind)).toEqual([
        'initializing',
        'snapshot-chunk',
        'snapshot-complete',
      ])
      expect(read.envelopes[1]).toMatchObject({
        changes: [
          {
            key: '"public"."items"/"9007199254740993"',
            value: { label: 'first' },
            headers: { operation: 'insert' },
          },
        ],
      })
      expect(read.envelopes[2]).toMatchObject({
        coveredSeq: '2',
        evidence: {
          snapshot: { xmin: mvcc.xmin, xmax: mvcc.xmax, xip_list: mvcc.xip },
        },
      })
      expect(read.end.baselineComplete).toBe(true)
    } finally {
      f.owner.close()
    }
  })

  it('classifies updates, removes, reinserts and key changes; ingestion continues without readers', async () => {
    const f = await fixture(undefined, 'full')
    try {
      f.push({ type: 'schema', columns })
      f.push({ type: 'baseline', rows: [row], mvcc })
      await f.owner.settled()
      const offset = f.owner.log.head.offset
      f.push({
        type: 'publication',
        changes: [{ op: 'upsert', ...row, values: [row.values[0], 'second'] }],
        txids: ['9007199254740995'],
        frontier: { lsn: 'FFFFFFFF/FFFFFFFF' },
      })
      f.push({
        type: 'publication',
        changes: [{ op: 'remove', row_key: row.row_key }],
        txids: ['9007199254740997'],
        frontier: { lsn: 'FFFFFFFF/FFFFFFFF' },
      })
      f.push({
        type: 'publication',
        changes: [{ op: 'upsert', ...row }],
        txids: ['9007199254740998'],
        frontier: { lsn: 'FFFFFFFF/FFFFFFFF' },
      })
      f.push({
        type: 'publication',
        changes: [{ op: 'upsert', ...row, values: ['2', 'changed key'] }],
        txids: ['9007199254740999'],
        frontier: { lsn: 'FFFFFFFF/FFFFFFFF' },
      })
      f.push({
        type: 'progress',
        mvcc: { xmin: '9007199254741000', xmax: '9007199254741000', xip: [] },
      })
      await f.owner.settled()
      const read = await f.owner.log.read(offset)
      expect(read.envelopes.map((e) => e.seq)).toEqual([
        '4',
        '5',
        '6',
        '7',
        '8',
      ])
      const operations = read.envelopes.flatMap((e) =>
        e.kind === 'publication'
          ? e.changes.map((c) => c.headers.operation)
          : []
      )
      expect(operations).toEqual([
        'update',
        'delete',
        'insert',
        'delete',
        'insert',
      ])
      expect(read.envelopes[0]).toMatchObject({
        evidence: { txids: ['9007199254740995'], lsn: '18446744073709551615' },
        changes: [
          { old_value: { label: 'first' }, value: { label: 'second' } },
        ],
      })
      expect(read.envelopes.at(-1)).toMatchObject({ coveredSeq: '7' })
    } finally {
      f.owner.close()
    }
  })

  it('invalidates on source error/reset and on absolute expiry, cancelling ingestion', async () => {
    let now = 1000
    const f = await fixture(() => now)
    expect(f.owner.expiresAt).toBe(901000)
    now = 901000
    expect(() => f.owner.assertValid()).toThrow('expired')
    expect(f.cancelled()).toBe(true)
    const reset = await fixture()
    reset.push({ type: 'reset' })
    await expect(reset.owner.settled()).rejects.toThrow('refetch')
    expect(reset.cancelled()).toBe(true)
    const error = await fixture()
    error.fail(new Error('upstream failed'))
    await expect(error.owner.settled()).rejects.toThrow('upstream failed')
    expect(error.cancelled()).toBe(true)
  })

  it('fails closed on schema mismatch and orphan removes, never advancing a partial publication', async () => {
    const bad = await fixture()
    bad.push({ type: 'schema', columns: columns.slice(1) })
    await expect(bad.owner.settled()).rejects.toThrow('missing')
    const f = await fixture()
    f.push({ type: 'schema', columns })
    f.push({ type: 'baseline', rows: [], mvcc })
    await f.owner.settled()
    const offset = f.owner.log.head.offset
    f.push({
      type: 'publication',
      changes: [{ op: 'remove', row_key: 'unknown' }],
      txids: ['10'],
      frontier: { lsn: '1/0' },
    })
    await expect(f.owner.settled()).rejects.toThrow('unknown row')
    expect(f.owner.log.head.offset).toBe(offset)
  })

  it('converts WAL LSN without number rounding and rejects malformed evidence', () => {
    expect(lsnToDecimal('1/ABCDEF01')).toBe('7177367297')
    expect(lsnToDecimal('0/0')).toBe('0')
    expect(() => lsnToDecimal('1/100000000')).toThrow('Invalid')
  })
})
