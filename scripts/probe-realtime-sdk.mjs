import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { once } from 'node:events'
import { setTimeout as delay } from 'node:timers/promises'
import { pathToFileURL } from 'node:url'
import { resolve } from 'node:path'

// Resolve only from the isolated install supplied by the clean-install runner.
const require = createRequire(
  pathToFileURL(resolve(process.argv[2], 'packages/server/package.json'))
)
const { createRealtime, rawSql } = await import(
  pathToFileURL(require.resolve('@neon/realtime/server')).href
)
const { WebSocketServer } = require('ws')
const server = new WebSocketServer({ port: 0, host: '127.0.0.1' })
await once(server, 'listening')
const connected = once(server, 'connection')
const sdk = createRealtime({
  url: `ws://127.0.0.1:${server.address().port}`,
  secret: `nrt_live_1${Buffer.alloc(32, 7).toString('base64url')}`,
  db: 'neondb',
})
try {
  const subscription = await sdk.subscribe(rawSql('select id from items'), {
    materialize: false,
    rawValues: true,
  })
  const events = []
  subscription.onSource((e) => events.push(e))
  const [socket] = await connected
  const send = (e) => socket.send(JSON.stringify(e))
  send({ type: 'ready' })
  const [raw] = await once(socket, 'message')
  const command = JSON.parse(raw.toString())
  const columns = [
    { name: 'id', type_oid: 99999, typmod: -1, codec: 'pg_text' },
  ]
  const row = { row_key: 'a'.repeat(64), values: ['9007199254740993'] }
  const mvcc = { xmin: '10', xmax: '10', xip: [] }
  send({
    type: 'subscribed',
    request_id: command.request_id,
    live_id: '1',
    epoch: '1',
    first_sequence: '1',
    columns,
  })
  send({
    type: 'baseline_sync_start',
    live_id: '1',
    epoch: '1',
    baseline_sync_attempt: '1',
    mvcc,
  })
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
  send({ type: 'open', publication_id: 'p1' })
  send({
    type: 'keyed_results',
    publication_id: 'p1',
    index: 0,
    targets: [{ live_id: '1', epoch: '1', sequence: '1' }],
    changes: [{ op: 'upsert', ...row }],
    txids: ['9007199254740995'],
  })
  send({
    type: 'commit',
    publication_id: 'p1',
    body_count: 1,
    frontier: { lsn: '1/ABCDEF01' },
  })
  send({ type: 'progress', mvcc: { xmin: '11', xmax: '11', xip: [] } })
  for (let i = 0; i < 200 && events.length < 4; i++) await delay(10)
  assert.deepEqual(
    events.map((e) => e.type),
    ['schema', 'baseline', 'publication', 'progress']
  )
  assert.deepEqual(events[1].rows, [row])
  assert.deepEqual(events[1].mvcc, mvcc)
  assert.deepEqual(events[2].frontier, { lsn: '1/ABCDEF01' })
  assert.deepEqual(events[2].txids, ['9007199254740995'])
  assert.equal(subscription.getState().status, 'live')
  console.log(
    'Clean frozen install: Realtime raw values, schema, MVCC, frontier and direct wrapper verified'
  )
} finally {
  sdk.close()
  for (const socket of server.clients) socket.terminate()
  await new Promise((resolve) => server.close(() => resolve()))
}
