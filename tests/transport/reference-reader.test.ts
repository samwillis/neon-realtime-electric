import { afterAll, beforeAll, expect, it } from 'vitest'
import { DurableStream, stream } from '@durable-streams/client'
import { DurableStreamTestServer } from '@durable-streams/server'
import { EnvelopeConsumer } from '../../packages/protocol/src/consumer'
import type { Envelope } from '../../packages/protocol/src/index'

const server = new DurableStreamTestServer({ port: 0 })
beforeAll(async () => {
  await server.start()
})
afterAll(async () => {
  await server.stop()
})

it('retains a synthetic logical cut inside one real response containing several publications', async () => {
  const writer = await DurableStream.create({
    url: `${server.url}/cut`,
    contentType: 'application/json',
  })
  const objects: Envelope[] = [
    {
      version: 1,
      seq: '1',
      kind: 'initializing',
      handle: 'cut',
      expiresAt: '2026-10-15T12:15:00Z',
    },
    {
      version: 1,
      seq: '2',
      kind: 'snapshot-complete',
      coveredSeq: '1',
      evidence: { snapshot: { xmin: '1', xmax: '1', xip_list: [] } },
    },
    ...(['3', '4', '5'] as const).map(
      (seq): Envelope => ({
        version: 1,
        seq,
        kind: 'publication',
        evidence: {},
        changes: [],
      })
    ),
  ]
  for (const object of objects) await writer.append(JSON.stringify(object))
  const consumer = new EnvelopeConsumer()
  const session = await stream<Envelope>({
    url: writer.url,
    live: false,
    json: true,
    warnOnHttp: false,
  })
  const positions: string[] = []
  let responseCount = 0
  session.subscribeJson(async (batch) => {
    responseCount++
    expect(batch.items).toHaveLength(5)
    await consumer.consume(batch, async (envelope) => {
      if (envelope.kind === 'publication')
        positions.push(
          `${envelope.seq}:${BigInt(envelope.seq) <= BigInt(4) ? 'before' : 'after'}`
        )
      expect(consumer.state.checkpoint.offset).toBe('-1')
    })
  })
  await session.closed
  expect(responseCount).toBe(1)
  expect(positions).toEqual(['3:before', '4:before', '5:after'])
  expect(consumer.state.checkpoint.offset).toBe(session.offset)
})

it('reads complete publication objects using the real reader and reference server', async () => {
  const writer = await DurableStream.create({
    url: `${server.url}/publication`,
    contentType: 'application/json',
  })
  const init: Envelope = {
    version: 1,
    seq: '1',
    kind: 'initializing',
    handle: 'a',
    expiresAt: '2026-10-15T12:15:00Z',
  }
  const baseline: Envelope = {
    version: 1,
    seq: '2',
    kind: 'snapshot-complete',
    coveredSeq: '1',
    evidence: { snapshot: { xmin: '1', xmax: '1', xip_list: [] } },
  }
  const changes: Envelope = {
    version: 1,
    seq: '3',
    kind: 'publication',
    evidence: { txids: ['4294967300'] },
    changes: [
      { key: 'a', value: { id: '1' }, headers: { operation: 'insert' } },
      { key: 'b', value: { id: '2' }, headers: { operation: 'insert' } },
    ],
  }
  await writer.append(JSON.stringify(init))
  const consumer = new EnvelopeConsumer()
  const seen: Envelope[] = []
  async function read() {
    const session = await stream<Envelope>({
      url: writer.url,
      offset: consumer.state.checkpoint.offset,
      live: false,
      json: true,
      warnOnHttp: false,
    })
    session.subscribeJson(async (batch) => {
      await consumer.consume(batch, async (envelope) => {
        seen.push(envelope)
      })
    })
    await session.closed
  }
  await read()
  expect(consumer.state.ready).toBe(false)
  const earlyCheckpoint = consumer.state.checkpoint.offset
  await writer.append(JSON.stringify(baseline))
  await writer.append(JSON.stringify(changes))
  await read()
  expect(consumer.state.ready).toBe(true)
  expect(seen).toEqual([init, baseline, changes])
  expect(consumer.state.checkpoint.offset).not.toBe(earlyCheckpoint)
  await read()
  expect(seen).toHaveLength(3)
})
