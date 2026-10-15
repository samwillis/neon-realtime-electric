import { randomUUID } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { createS3LogStore } from '../../packages/server/src/s3-store.js'
import type { Envelope } from '../../packages/protocol/src/index.js'

function required(name: string): string {
  const value = process.env[name]
  if (!value)
    throw new Error(
      `Live staging storage test requires ${name}; load .env.storage.local`
    )
  return value
}
describe('live Neon staging Object Storage', () => {
  it('seals immutable ranges, replays from storage plus tail and removes test-owned objects', async () => {
    for (const name of [
      'AWS_ACCESS_KEY_ID',
      'AWS_SECRET_ACCESS_KEY',
      'AWS_ENDPOINT_URL_S3',
      'AWS_REGION',
    ])
      required(name)
    const store = createS3LogStore({
      bucket: required('NEON_STORAGE_TEST_BUCKET'),
    })
    const deployment = `integration-${randomUUID()}`
    const expiresAt = Date.now() + 900_000
    const prefix = `${deployment}/${expiresAt}/owner`
    const log = new ObjectShapeLog({ store, prefix, sealBytes: 1024 })
    const initial: Envelope = {
      version: 1,
      seq: '1',
      kind: 'initializing',
      handle: 'owner',
      expiresAt: new Date(expiresAt).toISOString(),
    }
    const baseline: Envelope = {
      version: 1,
      seq: '2',
      kind: 'snapshot-complete',
      coveredSeq: '1',
      evidence: { snapshot: { xmin: '10', xmax: '10', xip_list: [] } },
    }
    const progress: Envelope = {
      version: 1,
      seq: '3',
      kind: 'progress',
      coveredSeq: '2',
      evidence: { snapshot: { xmin: '11', xmax: '11', xip_list: [] } },
    }
    try {
      const first = log.append(initial)
      await log.flush()
      const second = log.append(baseline)
      await log.flush()
      expect(log.sealedRanges).toBe(2)
      expect(log.residentBytes).toBe(0)
      log.append(progress)
      expect(log.residentBytes).toBeGreaterThan(0)
      expect((await log.read('-1')).envelopes).toEqual([
        initial,
        baseline,
        progress,
      ])
      expect(await log.read(first.offset, 1)).toMatchObject({
        envelopes: [baseline],
        end: second,
        atTail: false,
      })
      // Exercise the crash-cleanup path on a separate, already expired prefix.
      const orphanKey = `${deployment}/${Date.now() - 1000}/lost-owner/orphan.ndjson`
      await store.put(orphanKey, new TextEncoder().encode('orphan'))
      expect(await store.sweepExpired(deployment)).toBe(1)
      await expect(store.get(orphanKey)).rejects.toMatchObject({
        name: 'NoSuchKey',
      })
    } finally {
      log.close()
      try {
        await log.disposed
      } finally {
        await store.sweepExpired(deployment, expiresAt + 1)
        store.close()
      }
    }
  })
})
