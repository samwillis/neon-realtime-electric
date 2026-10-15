import { createServer } from 'node:http'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { afterEach, describe, expect, it } from 'vitest'
import {
  createNeonRealtimeSource,
  SourceQueryRejectedError,
  type SourceEvent,
  type SourceSubscription,
} from '../../packages/server/src/realtime-source.js'

const require = createRequire(
  new URL('../../packages/server/package.json', import.meta.url)
)
const { WebSocketServer } = require('ws') as typeof import('ws')
const cleanup: (() => unknown)[] = []
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close()
})

const columns = [
  { name: 'id', type_oid: 20, typmod: -1, codec: 'pg_text' },
  { name: 'custom', type_oid: 99999, typmod: -1, codec: 'pg_text' },
  { name: 'bytes', type_oid: 17, typmod: -1, codec: 'bytes' },
]
const row = {
  row_key: 'a'.repeat(64),
  values: ['9007199254740993', '(one,two)', { base64: 'AP8=' }],
}
const mvcc = {
  xmin: '9007199254740993',
  xmax: '9007199254740999',
  xip: ['9007199254740996'],
}
const target = { live_id: '1', epoch: '1', sequence: '1' }

async function fixture(rejectInitial = false) {
  const http = createServer()
  const ws = new WebSocketServer({ server: http })
  http.listen(0, '127.0.0.1')
  await once(http, 'listening')
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        for (const socket of ws.clients) socket.terminate()
        ws.close(() => http.close(() => resolve()))
      })
  )
  const address = http.address() as { port: number }
  const socketReady = once(ws, 'connection').then(
    ([socket]) => socket as import('ws').WebSocket
  )
  const source = createNeonRealtimeSource({
    url: `ws://127.0.0.1:${address.port}`,
    // Valid local capability encryption key. Never accepted by a real service.
    secret: `nrt_live_1${Buffer.alloc(32, 7).toString('base64url')}`,
    db: 'neondb',
  })
  cleanup.push(() => source.close())
  const events: SourceEvent[] = []
  const errors: Error[] = []
  const controller = new AbortController()
  const control = await source.subscribe(
    { sql: 'select 1' },
    { event: (e) => events.push(e), error: (e) => errors.push(e) },
    controller.signal
  )
  const socket = await socketReady
  const send = (event: object) => socket.send(JSON.stringify(event))
  send({ type: 'ready' })
  const [raw] = await once(socket, 'message')
  const request = JSON.parse(String(raw))
  expect(request.type).toBe('subscribe')
  if (rejectInitial)
    send({
      type: 'subscribe_rejected',
      request_id: request.request_id,
      code: 'upstream_rejected',
      message: 'private predicate literal diagnostic',
    })
  else
    send({
      type: 'subscribed',
      request_id: request.request_id,
      live_id: '1',
      epoch: '1',
      first_sequence: '1',
      columns,
    })
  const start = () =>
    send({
      type: 'baseline_sync_start',
      live_id: '1',
      epoch: '1',
      baseline_sync_attempt: '1',
      mvcc,
    })
  const baseline = () => {
    start()
    send({
      type: 'baseline_sync_batch',
      live_id: '1',
      epoch: '1',
      baseline_sync_attempt: '1',
      index: 0,
      rows: [row],
    })
    send({
      type: 'baseline_sync_end',
      live_id: '1',
      epoch: '1',
      baseline_sync_attempt: '1',
      batch_count: 1,
    })
  }
  const publication = () => {
    send({ type: 'open', publication_id: 'p1' })
    send({
      type: 'keyed_results',
      publication_id: 'p1',
      index: 0,
      targets: [target],
      changes: [{ op: 'upsert', ...row }],
      txids: ['9007199254740995'],
    })
    send({
      type: 'commit',
      publication_id: 'p1',
      body_count: 1,
      frontier: { lsn: '1/ABCDEF01' },
    })
  }
  return {
    source,
    events,
    errors,
    controller,
    control,
    send,
    start,
    baseline,
    publication,
  }
}

describe('pinned patched Realtime SDK through real WebSocket delivery', () => {
  it('classifies permanent SQL rejection without exposing upstream diagnostic text', async () => {
    const f = await fixture(true)
    await expect.poll(() => f.errors.length).toBe(1)
    expect(f.errors[0]).toBeInstanceOf(SourceQueryRejectedError)
    expect(f.errors[0].message).toBe('Realtime rejected the shape query')
    expect(f.events).toEqual([])
  })

  it('exposes early schema, raw PostgreSQL cells, MVCC and publication frontier in order', async () => {
    const f = await fixture()
    await expect.poll(() => f.events.length).toBe(1)
    expect(f.events).toEqual([{ type: 'schema', columns }])
    f.baseline()
    f.publication()
    f.send({ type: 'progress', mvcc })
    await expect.poll(() => f.events.length).toBe(4)
    expect(f.errors).toEqual([])
    expect(f.events).toEqual([
      { type: 'schema', columns },
      { type: 'baseline', rows: [row], mvcc },
      {
        type: 'publication',
        changes: [{ op: 'upsert', ...row }],
        txids: ['9007199254740995'],
        frontier: { lsn: '1/ABCDEF01' },
      },
      { type: 'progress', mvcc },
    ])
  })

  it('keeps a publication buffered during baseline and its progress behind installed data', async () => {
    const f = await fixture()
    f.start()
    f.publication()
    f.send({ type: 'progress', mvcc }) // SDK deliberately does not forward progress before baseline is live.
    await expect.poll(() => f.events.length).toBe(1)
    f.send({
      type: 'baseline_sync_end',
      live_id: '1',
      epoch: '1',
      baseline_sync_attempt: '1',
      batch_count: 0,
    })
    f.send({ type: 'progress', mvcc })
    await expect.poll(() => f.events.length).toBe(4)
    expect(f.events.map((e) => e.type)).toEqual([
      'schema',
      'baseline',
      'publication',
      'progress',
    ])
    expect(f.events[2]).toMatchObject({ frontier: { lsn: '1/ABCDEF01' } })
    expect(f.errors).toEqual([])
  })

  it('invalidates the handle on an upstream reset without delivering a replacement baseline', async () => {
    const f = await fixture()
    f.baseline()
    await expect.poll(() => f.events.length).toBe(2)
    f.send({ type: 'open', publication_id: 'reset' })
    f.send({
      type: 'reset_required',
      publication_id: 'reset',
      index: 0,
      targets: [{ live_id: '1', epoch: '2', first_sequence: '1' }],
    })
    f.send({
      type: 'commit',
      publication_id: 'reset',
      body_count: 1,
      frontier: { lsn: '1/ABCDEF02' },
    })
    await expect.poll(() => f.errors.length).toBe(1)
    expect(f.errors[0].message).toContain('continuity lost')
    expect(f.events.map((e) => e.type)).toEqual(['schema', 'baseline'])
  })

  it('cancels without further delivery and closes all active subscriptions', async () => {
    const f = await fixture()
    await expect.poll(() => f.events.length).toBe(1)
    f.controller.abort()
    f.baseline()
    f.source.close()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(f.events).toHaveLength(1)
    expect(f.errors).toEqual([])
    await expect(
      f.source.subscribe(
        { sql: 'select 1' },
        { event: () => {}, error: () => {} },
        new AbortController().signal
      )
    ).rejects.toThrow('closed')
  })
})
