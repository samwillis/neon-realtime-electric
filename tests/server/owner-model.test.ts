import fc from 'fast-check'
import { expect, it } from 'vitest'
import { EnvelopeConsumer } from '../../packages/protocol/src/consumer.js'
import type { Envelope, RawRow } from '../../packages/protocol/src/index.js'
import { ShapeOwner } from '../../packages/server/src/shape-owner.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import type { SourceObserver } from '../../packages/server/src/realtime-source.js'
import { FixtureObjectStore } from './support/object-store.js'

const command = fc.record({
  kind: fc.constantFrom('upsert', 'upsert', 'remove', 'rotate'),
  slot: fc.integer({ min: 0, max: 5 }),
  keyVersion: fc.integer({ min: 0, max: 3 }),
  label: fc.option(fc.string({ maxLength: 20 }), { nil: null }),
  readLimit: fc.integer({ min: 1, max: 5 }),
  interrupt: fc.boolean(),
})

for (const objects of [false, true])
  for (const replica of ['default', 'full'] as const)
    it(`preserves the independent row model across generated key changes, replay and owner rotation (objects=${objects}, replica=${replica})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.array(command, { minLength: 5, maxLength: 30 }),
          async (commands) => {
            // Source identity and public primary key are deliberately distinct.
            // This model only assigns/deletes whole source rows; it does not use
            // owner classification, public keys or the emitted patches as input.
            const sourceRows = new Map<
              number,
              { id: number; label: string | null }
            >()
            const materialized = new Map<string, RawRow>()
            const store = new FixtureObjectStore()
            let observer!: SourceObserver
            let owner!: ShapeOwner
            let consumer!: EnvelopeConsumer
            let txid = 10
            const wire = (
              slot: number,
              row: { id: number; label: string | null }
            ) => ({
              row_key: slot.toString(16).padStart(64, '0'),
              values: [String(row.id), row.label],
            })
            const apply = async (envelope: Envelope) => {
              if (
                envelope.kind !== 'publication' &&
                envelope.kind !== 'snapshot-chunk'
              )
                return
              for (const change of envelope.changes) {
                if (!('key' in change))
                  throw new Error(
                    'Unexpected move in a configured root-table stream'
                  )
                const before = materialized.get(change.key)
                if (change.old_value)
                  for (const [column, value] of Object.entries(
                    change.old_value
                  ))
                    expect(before?.[column]).toEqual(value)
                switch (change.headers.operation) {
                  case 'insert':
                    expect(before).toBeUndefined()
                    materialized.set(change.key, { ...change.value })
                    break
                  case 'update':
                    expect(before).toBeDefined()
                    materialized.set(change.key, { ...before, ...change.value })
                    break
                  case 'delete':
                    expect(before).toBeDefined()
                    materialized.delete(change.key)
                    break
                }
              }
            }
            const start = async () => {
              owner = new ShapeOwner({
                definition: {
                  namespace: 'public',
                  table: 'items',
                  primaryKey: ['id'],
                  projection: ['id', 'label'],
                  replica,
                  schema: { id: { type: 'int4' }, label: { type: 'text' } },
                  query: { sql: 'select id,label from public.items' },
                },
                source: {
                  async subscribe(_, next) {
                    observer = next
                    return { close() {} }
                  },
                  close() {},
                },
                createLog: objects
                  ? ({ handle }) =>
                      new ObjectShapeLog({
                        store,
                        prefix: handle,
                        sealBytes: 1,
                      })
                  : undefined,
              })
              consumer = new EnvelopeConsumer()
              materialized.clear()
              await owner.start()
              observer.event({
                type: 'schema',
                columns: [
                  { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
                  { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
                ],
              })
              observer.event({
                type: 'baseline',
                rows: [...sourceRows].map(([slot, row]) => wire(slot, row)),
                mvcc: { xmin: String(txid), xmax: String(txid), xip: [] },
              })
              await owner.settled()
            }
            const catchUp = async (limit: number, interrupt: boolean) => {
              if (owner.log instanceof ObjectShapeLog) await owner.log.flush()
              let failed = false
              while (
                consumer.state.checkpoint.offset !== owner.log.head.offset
              ) {
                const before = consumer.state.checkpoint.offset
                const read = await owner.log.read(before, limit)
                const batch = { items: read.envelopes, offset: read.end.offset }
                if (interrupt && !failed && read.envelopes.length > 1) {
                  let delivered = 0
                  await expect(
                    consumer.consume(batch, async (envelope) => {
                      if (++delivered === 2)
                        throw new Error('Interrupted response')
                      await apply(envelope)
                    })
                  ).rejects.toThrow('Interrupted response')
                  expect(consumer.state.checkpoint.offset).toBe(before)
                  failed = true
                }
                await consumer.consume(batch, apply)
              }
              expect(consumer.state.ready).toBe(true)
              const expected = [...sourceRows.values()]
                .map((row) => ({ id: String(row.id), label: row.label }))
                .sort((a, b) => Number(a.id) - Number(b.id))
              const actual = [...materialized.values()].sort(
                (a, b) => Number(a.id) - Number(b.id)
              )
              expect(actual).toEqual(expected)
              for (const [key, row] of materialized)
                expect(key).toBe(`"public"."items"/"${row.id}"`)
              expect(owner.retainedRowCount).toBe(sourceRows.size)
            }
            try {
              await start()
              await catchUp(2, true)
              for (const next of commands) {
                txid++
                if (next.kind === 'rotate') {
                  const previous = owner.handle
                  owner.close(new Error('Generated owner loss'))
                  expect(() => owner.assertValid()).toThrow(
                    'Generated owner loss'
                  )
                  await owner.log.disposed
                  await start()
                  expect(owner.handle).not.toBe(previous)
                } else if (next.kind === 'remove') {
                  if (sourceRows.delete(next.slot))
                    observer.event({
                      type: 'publication',
                      changes: [
                        {
                          op: 'remove',
                          row_key: next.slot.toString(16).padStart(64, '0'),
                        },
                      ],
                      txids: [String(txid)],
                      frontier: { lsn: `0/${txid.toString(16).toUpperCase()}` },
                    })
                } else {
                  const row = {
                    id: next.keyVersion * 8 + next.slot,
                    label: next.label,
                  }
                  sourceRows.set(next.slot, row)
                  observer.event({
                    type: 'publication',
                    changes: [{ op: 'upsert', ...wire(next.slot, row) }],
                    txids: [String(txid)],
                    frontier: { lsn: `0/${txid.toString(16).toUpperCase()}` },
                  })
                }
                if (next.kind !== 'rotate')
                  observer.event({
                    type: 'progress',
                    mvcc: {
                      xmin: String(txid + 1),
                      xmax: String(txid + 1),
                      xip: [],
                    },
                  })
                await owner.settled()
                await catchUp(next.readLimit, next.interrupt)
              }
            } finally {
              owner?.close()
              await owner?.log.disposed
              expect(store.objects.size).toBe(0)
            }
          }
        ),
        { seed: 925383, numRuns: 50 }
      )
    }, 30_000)
