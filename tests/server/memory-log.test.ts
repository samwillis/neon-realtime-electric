import { describe, expect, it } from 'vitest'
import { MemoryShapeLog } from '../../packages/server/src/memory-log.js'
import type { Envelope } from '../../packages/protocol/src/index.js'

const init: Envelope = {
  version: 1,
  seq: '1',
  kind: 'initializing',
  handle: 'handle',
  expiresAt: '2030-01-01T00:00:00Z',
}
describe('immutable shape byte log', () => {
  it('indexes each committed logical boundary independently of read grouping', () => {
    const log = new MemoryShapeLog()
    const one = log.append(init)
    const two = log.append({
      version: 1,
      seq: '2',
      kind: 'snapshot-complete',
      coveredSeq: '1',
      evidence: { snapshot: { xmin: '10', xmax: '10', xip_list: [] } },
    })
    log.append({
      version: 1,
      seq: '3',
      kind: 'progress',
      coveredSeq: '2',
      evidence: {},
    })
    expect(one.offset).toMatch(/^\d{16}_\d{16}$/)
    expect(one.offset < two.offset).toBe(true)
    expect(log.read(one.offset, 1)).toMatchObject({
      boundary: { afterSeq: '1', baselineComplete: false },
      end: { afterSeq: '2', baselineComplete: true },
      atTail: false,
    })
    expect(log.read(two.offset)).toMatchObject({
      boundary: { afterSeq: '2', baselineComplete: true },
      end: { afterSeq: '3' },
      atTail: true,
    })
    expect(log.read('-1').envelopes).toHaveLength(3)
    expect(() => log.read('0_77')).toThrow('Invalid')
  })
  it('isolates committed bytes from callers and rejects gaps', () => {
    const log = new MemoryShapeLog()
    const original = structuredClone(init)
    log.append(original)
    original.handle = 'mutated'
    const first = log.read('-1')
    first.envelopes[0].seq = '888'
    first.end.afterSeq = '88'
    const head = log.head
    head.afterSeq = '999'
    expect(log.read('-1').envelopes[0]).toEqual(init)
    expect(log.head.afterSeq).toBe('1')
    expect(() => log.append({ ...init, seq: '3' })).toThrow('contiguous')
  })
  it('wakes long-poll readers on append, close, abort and timeout', async () => {
    const log = new MemoryShapeLog()
    log.append(init)
    const abort = new AbortController()
    const wait = log.wait(log.head.offset, abort.signal, 1000)
    log.append({
      version: 1,
      seq: '2',
      kind: 'progress',
      coveredSeq: '1',
      evidence: {},
    })
    await wait
    await log.wait(log.head.offset, abort.signal, 1)
    const pending = log.wait(log.head.offset, abort.signal, 1000)
    abort.abort(new Error('cancelled'))
    await expect(pending).rejects.toThrow('cancelled')
    const closed = log.wait(log.head.offset, new AbortController().signal, 1000)
    log.close()
    await closed
    expect(() => log.append({ ...init, seq: '3' })).toThrow('closed')
  })
})
