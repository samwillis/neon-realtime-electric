import { afterEach, describe, expect, it } from 'vitest'
import { ObjectShapeLog } from '../../packages/server/src/object-log.js'
import { MemoryShapeLog } from '../../packages/server/src/memory-log.js'
import type { Envelope } from '../../packages/protocol/src/index.js'
import { FixtureObjectStore } from './support/object-store.js'

const init: Envelope = {
  version: 1,
  seq: '1',
  kind: 'initializing',
  handle: 'handle',
  expiresAt: '2030-01-01T00:00:00Z',
}
const complete: Envelope = {
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
const cleanup: (() => unknown)[] = []
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close()
})
function objects(
  options: { sealBytes?: number; maxResidentBytes?: number } = {}
) {
  const store = new FixtureObjectStore()
  const log = new ObjectShapeLog({
    store,
    prefix: 'tests/1900000000000/handle',
    sealBytes: 1024,
    maxResidentBytes: 10240,
    ...options,
  })
  cleanup.push(async () => {
    log.close()
    await log.disposed.catch(() => {})
  })
  return { store, log }
}

describe.each(['memory', 'objects'] as const)(
  'shared immutable log contract: %s',
  (backend) => {
    it('preserves byte checkpoints and proofs across sealing, regrouping and late appends', async () => {
      const log = backend === 'memory' ? new MemoryShapeLog() : objects().log
      const one = log.append(init)
      const two = log.append(complete)
      const expected = await log.read('-1')
      if (log instanceof ObjectShapeLog) await log.flush()
      log.append(progress)
      const split = await log.read(one.offset, 1)
      expect(split.boundary).toEqual(one)
      expect(split.end).toEqual(two)
      expect(split.atTail).toBe(false)
      expect(split.envelopes).toEqual([complete])
      expect((await log.read('-1')).envelopes).toEqual([
        ...expected.envelopes,
        progress,
      ])
      const tail = await log.read(log.head.offset)
      expect(tail).toMatchObject({
        envelopes: [],
        atTail: true,
        boundary: { afterSeq: '3', baselineComplete: true },
      })
      await expect(async () => log.read('123_456')).rejects.toThrow('Invalid')
      expect(() => log.append({ ...progress, seq: '5' })).toThrow('contiguous')
      log.close()
    })
    it('wakes waiters on append, close and abort while preserving detached reads', async () => {
      const log = backend === 'memory' ? new MemoryShapeLog() : objects().log
      log.append(init)
      const offset = log.head.offset
      const controller = new AbortController()
      const pending = log.wait(offset, controller.signal, 1000)
      log.append(complete)
      await pending
      if (log instanceof ObjectShapeLog) await log.flush()
      const first = await log.read('-1')
      first.envelopes[0].seq = '999'
      expect((await log.read('-1')).envelopes[0]).toEqual(init)
      const cancelled = log.wait(log.head.offset, controller.signal, 1000)
      controller.abort(new Error('cancelled'))
      await expect(cancelled).rejects.toThrow('cancelled')
      const closed = log.wait(
        log.head.offset,
        new AbortController().signal,
        1000
      )
      log.close()
      await closed
      expect(() => log.append(progress)).toThrow('closed')
    })
  }
)

describe('object-backed shape replay and failure handling', () => {
  it('evicts sealed bytes and reads across multiple objects and a resident tail', async () => {
    const { log, store } = objects({ sealBytes: 200 })
    log.append(init)
    await log.flush()
    log.append(complete)
    await log.flush()
    expect(log.residentBytes).toBe(0)
    expect(log.sealedRanges).toBe(2)
    const checkpoint = log.head.offset
    log.append(progress)
    expect(log.residentBytes).toBeGreaterThan(0)
    const read = await log.read('-1')
    expect(read.envelopes).toEqual([init, complete, progress])
    expect(store.gets.length).toBeGreaterThan(0)
    expect((await log.read(checkpoint)).envelopes).toEqual([progress])
    expect(
      store.puts.every((p) => p.key.startsWith('tests/1900000000000/handle/'))
    ).toBe(true)
  })
  it('retains promised bytes while upload is pending, and retries failed uploads without changing offsets', async () => {
    const { log, store } = objects()
    let release!: () => void
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    store.beforePut = () => gate
    log.append(init)
    log.append(complete)
    const head = log.head
    const upload = log.flush()
    await expect.poll(() => store.puts.length).toBe(1)
    expect((await log.read('-1')).envelopes).toEqual([init, complete])
    expect(store.gets).toHaveLength(0)
    expect(log.residentBytes).toBeGreaterThan(0)
    store.putError = new Error('upload unavailable')
    release()
    await expect(upload).rejects.toThrow('upload unavailable')
    expect(log.head).toEqual(head)
    expect((await log.read('-1')).envelopes).toEqual([init, complete])
    store.putError = undefined
    await log.flush()
    expect(store.puts[0]).toEqual(store.puts[1])
    expect(log.residentBytes).toBe(0)
    expect(log.head).toEqual(head)
    expect(log.uploadError).toBeUndefined()
  })
  it('reports unavailable/corrupt sealed ranges and respects read cancellation', async () => {
    const { log, store } = objects()
    log.append(init)
    await log.flush()
    const key = store.puts[0].key
    store.objects.set(key, new TextEncoder().encode('corrupt'))
    await expect(log.read('-1')).rejects.toThrow('refetch required')
    store.objects.set(key, store.puts[0].bytes)
    store.getError = new Error('read unavailable')
    await expect(log.read('-1')).rejects.toThrow('refetch required')
    const abort = new AbortController()
    abort.abort(new Error('cancelled'))
    await expect(log.read('-1', 128, abort.signal)).rejects.toThrow('cancelled')
  })
  it('aborts an in-flight storage read when the owner closes', async () => {
    const { log, store } = objects()
    log.append(init)
    await log.flush()
    store.beforeGet = (signal) =>
      new Promise((_, reject) =>
        signal!.addEventListener('abort', () => reject(signal!.reason), {
          once: true,
        })
      )
    const reading = log.read('-1')
    await expect.poll(() => store.gets.length).toBe(1)
    log.close()
    await expect(reading).rejects.toThrow('refetch required')
    await log.disposed
  })

  it('bounds resident bytes when the store fails, retaining every previously promised range', async () => {
    const size = Buffer.byteLength(JSON.stringify(init)) + 1
    const { log, store } = objects({ sealBytes: size, maxResidentBytes: size })
    store.putError = new Error('offline')
    log.append(init)
    await expect(log.flush()).rejects.toThrow('offline')
    const checkpoint = log.head
    expect(() => log.append(complete)).toThrow('byte limit')
    expect(log.head).toEqual(checkpoint)
    expect((await log.read('-1')).envelopes).toEqual([init])
  })
  it('deletes sealed and uncertain-upload keys on close and exposes cleanup failures', async () => {
    const { log, store } = objects()
    log.append(init)
    await log.flush()
    store.deleteError = new Error('delete unavailable')
    log.close()
    await expect(log.disposed).rejects.toThrow('cleanup failed')
    expect(store.deletes).toEqual(store.puts.map((p) => p.key))
    const failed = objects()
    failed.store.putError = new Error('put response lost')
    failed.log.append(init)
    await expect(failed.log.flush()).rejects.toThrow('put response lost')
    failed.log.close()
    await failed.log.disposed
    expect(failed.store.deletes).toEqual(failed.store.puts.map((p) => p.key))
  })
})
