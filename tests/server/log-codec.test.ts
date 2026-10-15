import { describe, expect, it } from 'vitest'
import { MemoryShapeLog } from '../../packages/server/src/memory-log.js'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { FixtureObjectStore } from './support/object-store.js'

// Generic JSON conformance must not bypass the production shape codec.
describe.each(['memory', 'objects'])('shape log codec: %s', (backend) => {
  it('rejects sequence gaps and evidence ahead of data without advancing committed offsets', async () => {
    const store = new FixtureObjectStore()
    const log =
      backend === 'memory'
        ? new MemoryShapeLog()
        : new ObjectShapeLog({ store, prefix: 'codec-test', sealBytes: 1 })
    try {
      const initial = log.append({
        version: 1,
        seq: '1',
        kind: 'initializing',
        handle: 'shape',
        expiresAt: '2030-01-01T00:00:00Z',
      })
      expect(() =>
        log.append({
          version: 1,
          seq: '3',
          kind: 'progress',
          coveredSeq: '1',
          evidence: {},
        })
      ).toThrow('contiguous')
      expect(() =>
        log.append({
          version: 1,
          seq: '2',
          kind: 'progress',
          coveredSeq: '2',
          evidence: {},
        })
      ).toThrow(/evidence/i)
      expect(log.head).toEqual(initial)
      log.append({
        version: 1,
        seq: '2',
        kind: 'snapshot-complete',
        coveredSeq: '1',
        evidence: { snapshot: { xmin: '10', xmax: '10', xip_list: [] } },
      })
      if (log instanceof ObjectShapeLog) await log.flush()
      const replay = await log.read('-1')
      expect(replay.envelopes.map((value) => value.seq)).toEqual(['1', '2'])
      expect(replay.end.baselineComplete).toBe(true)
      if (backend === 'objects') expect(store.gets.length).toBeGreaterThan(0)
    } finally {
      log.close()
      if (log instanceof ObjectShapeLog) await log.disposed
    }
  })
})
