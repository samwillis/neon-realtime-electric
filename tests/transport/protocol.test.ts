import { describe, expect, it } from 'vitest'
import fc from 'fast-check'
import {
  compareSequence,
  parseEnvelope,
} from '../../packages/protocol/src/index'
import type { Envelope } from '../../packages/protocol/src/index'
import { EnvelopeConsumer } from '../../packages/protocol/src/consumer'

export const initialization: Envelope = {
  version: 1,
  seq: '1',
  kind: 'initializing',
  handle: 'shape-a',
  expiresAt: '2026-10-15T12:15:00.000Z',
}
export const chunk: Envelope = {
  version: 1,
  seq: '2',
  kind: 'snapshot-chunk',
  changes: [
    {
      key: '"public"."items"/"1"',
      value: { id: '1', name: null },
      headers: { operation: 'insert' },
    },
  ],
}
export const complete: Envelope = {
  version: 1,
  seq: '3',
  kind: 'snapshot-complete',
  coveredSeq: '2',
  evidence: {
    snapshot: {
      xmin: '4294967296',
      xmax: '4294967299',
      xip_list: ['4294967297'],
    },
    lsn: '9007199254740993',
  },
}
export const publication: Envelope = {
  version: 1,
  seq: '4',
  kind: 'publication',
  changes: [
    {
      key: '"public"."items"/"1"',
      value: { name: 'changed' },
      headers: { operation: 'update' },
    },
  ],
  evidence: { txids: ['4294967300'], lsn: '9007199254741000' },
}

describe('versioned raw envelopes', () => {
  it('retains exact source evidence and raw cells through JSON', () => {
    for (const envelope of [initialization, chunk, complete, publication]) {
      expect(parseEnvelope(JSON.parse(JSON.stringify(envelope)))).toEqual(
        envelope
      )
    }
  })

  it.each([
    { ...initialization, version: 2 },
    { ...initialization, seq: '01' },
    { ...initialization, seq: '-1' },
    { ...initialization, seq: 1 },
    { ...initialization, seq: '0' },
    { ...initialization, expiresAt: 'never' },
    {
      ...chunk,
      changes: [{ key: 'x', value: {}, headers: { control: 'up-to-date' } }],
    },
    {
      ...chunk,
      changes: [{ key: 'x', value: {}, headers: { operation: 'update' } }],
    },
    { ...complete, coveredSeq: '3' },
    { ...complete, evidence: {} },
    {
      ...complete,
      evidence: { snapshot: { xmin: '4', xmax: '3', xip_list: [] } },
    },
    {
      ...complete,
      evidence: { snapshot: { xmin: '1', xmax: '3', xip_list: ['3'] } },
    },
    { ...publication, evidence: { txids: [9007199254740993] } },
    {
      ...publication,
      changes: [
        {
          key: 'x',
          value: { bad: Infinity },
          headers: { operation: 'insert' },
        },
      ],
    },
  ])('rejects invalid or misleading wire data %#', (envelope) => {
    expect(() => parseEnvelope(envelope)).toThrow()
  })

  it('compares sequences numerically beyond the JS safe integer range', () => {
    fc.assert(
      fc.property(
        fc.bigInt({
          min: BigInt(0),
          max: BigInt('999999999999999999999999999999999999'),
        }),
        fc.bigInt({
          min: BigInt(0),
          max: BigInt('999999999999999999999999999999999999'),
        }),
        (a, b) => {
          expect(compareSequence(`${a}`, `${b}`)).toBe(
            a === b ? 0 : a < b ? -1 : 1
          )
        }
      )
    )
    expect(compareSequence('9', '10')).toBe(-1)
    expect(compareSequence('9007199254740992', '9007199254740993')).toBe(-1)
  })
})

describe('ordered consumption and resume', () => {
  it('locates a cut between publications that share one response-end offset', async () => {
    const consumer = new EnvelopeConsumer({
      appliedSeq: '3',
      ready: true,
      checkpoint: { offset: '000_030' },
    })
    const before: string[] = []
    const after: string[] = []
    await consumer.consume(
      {
        items: [
          publication,
          { ...publication, seq: '5' },
          { ...publication, seq: '6' },
        ],
        offset: '000_060',
      },
      async (envelope) => {
        expect(consumer.state.checkpoint.offset).toBe('000_030')
        const side = compareSequence(envelope.seq, '5') <= 0 ? before : after
        side.push(envelope.seq)
      }
    )
    expect(before).toEqual(['4', '5'])
    expect(after).toEqual(['6'])
    expect(consumer.state.checkpoint.offset).toBe('000_060')
  })
  it('does not equate a temporary physical tail with baseline readiness', async () => {
    const consumer = new EnvelopeConsumer()
    await consumer.consume(
      { items: [initialization], offset: '000_010' },
      async () => {}
    )
    expect(consumer.state.ready).toBe(false)
    await consumer.consume({ items: [], offset: '000_010' }, async () => {})
    expect(consumer.state.ready).toBe(false)
    await consumer.consume(
      { items: [chunk, complete], offset: '000_030' },
      async () => {}
    )
    expect(consumer.state.ready).toBe(true)
  })

  it('commits one checkpoint only after every envelope callback succeeds', async () => {
    const consumer = new EnvelopeConsumer()
    await consumer.consume(
      { items: [initialization], offset: '000_010' },
      async () => {}
    )
    const delivered: string[] = []
    await expect(
      consumer.consume(
        { items: [chunk, complete, publication], offset: '000_040' },
        async (envelope) => {
          expect(consumer.state.checkpoint.offset).toBe('000_010')
          if (envelope.kind === 'publication') throw new Error('interrupted')
          delivered.push(envelope.seq)
        }
      )
    ).rejects.toThrow('interrupted')
    expect(consumer.state).toMatchObject({
      appliedSeq: '3',
      ready: true,
      checkpoint: { offset: '000_010' },
    })
    await consumer.consume(
      {
        items: [chunk, complete, publication],
        offset: '000_040',
        cursor: 'cursor',
      },
      async (envelope) => {
        delivered.push(envelope.seq)
      }
    )
    expect(delivered).toEqual(['2', '3', '4'])
    expect(consumer.state.checkpoint).toEqual({
      offset: '000_040',
      cursor: 'cursor',
    })
  })

  it('suppresses interrupted replay under arbitrary response grouping', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: 1, max: 4 }), {
          minLength: 1,
          maxLength: 10,
        }),
        async (sizes) => {
          const consumer = new EnvelopeConsumer()
          const source = [initialization, chunk, complete, publication]
          const seen: string[] = []
          let index = 0
          for (const size of [...sizes, 4]) {
            const end = Math.min(index + size, source.length)
            await consumer.consume(
              { items: source.slice(0, end), offset: `000_${end}` },
              async (e) => {
                seen.push(e.seq)
              }
            )
            index = end
          }
          expect(seen).toEqual(['1', '2', '3', '4'])
          expect(consumer.state.appliedSeq).toBe('4')
        }
      ),
      { numRuns: 50 }
    )
  })

  it('rejects gaps, replacement baselines and false coverage', async () => {
    const consumer = new EnvelopeConsumer()
    await consumer.consume(
      { items: [initialization], offset: '000_010' },
      async () => {}
    )
    await expect(
      consumer.consume({ items: [complete], offset: '000_030' }, async () => {})
    ).rejects.toThrow('Gap')
    await expect(
      consumer.consume(
        { items: [{ ...initialization, seq: '2' }], offset: '000_020' },
        async () => {}
      )
    ).rejects.toThrow('Initialization')
    await consumer.consume(
      { items: [chunk], offset: '000_020' },
      async () => {}
    )
    await expect(
      consumer.consume(
        { items: [{ ...complete, coveredSeq: '1' }], offset: '000_030' },
        async () => {}
      )
    ).rejects.toThrow('preceding')
  })
})
