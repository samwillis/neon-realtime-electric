import { createServer } from 'node:http'
import { createShapeHandler } from '../../../packages/server/src/handler.js'
import type { SourceObserver } from '../../../packages/server/src/realtime-source.js'

// Controlled upstream for process/routing tests. Never substituted for live-source gates.
let label = 'initial'
let txid = 10
const subscribers = new Map<
  SourceObserver,
  { signal: AbortSignal; live: boolean }
>()
const row_key = 'a'.repeat(64)
const handler = createShapeHandler({
  source: {
    async subscribe(_, observer, signal) {
      const state = { signal, live: false }
      subscribers.set(observer, state)
      signal.addEventListener('abort', () => subscribers.delete(observer), {
        once: true,
      })
      setTimeout(() => {
        if (signal.aborted) return
        observer.event({
          type: 'schema',
          columns: [
            { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
            { name: 'label', type_oid: 25, typmod: -1, codec: 'pg_text' },
          ],
        })
        observer.event({
          type: 'baseline',
          rows: [{ row_key, values: ['1', label] }],
          mvcc: { xmin: String(txid + 1), xmax: String(txid + 1), xip: [] },
        })
        state.live = true
      }, 5)
      return {
        close() {
          subscribers.delete(observer)
        },
      }
    },
    close() {
      subscribers.clear()
    },
  },
  longPollMs: 50,
  async authorize() {
    return {
      authorizationKey: 'routing-fixture',
      definition: {
        query: { sql: 'select id, label from public.items' },
        namespace: 'public',
        table: 'items',
        primaryKey: ['id'],
        projection: ['id', 'label'],
        schema: { id: { type: 'int4' }, label: { type: 'text' } },
      },
    }
  },
})
const server = createServer((req, res) => {
  void (async () => {
    if (req.url === '/__fixture/write' && req.method === 'POST') {
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(Buffer.from(chunk))
      label = JSON.parse(Buffer.concat(chunks).toString()).label
      txid++
      for (const [observer, state] of subscribers)
        if (!state.signal.aborted && state.live)
          observer.event({
            type: 'publication',
            changes: [{ op: 'upsert', row_key, values: ['1', label] }],
            txids: [String(txid)],
            frontier: { lsn: `1/${txid.toString(16).toUpperCase()}` },
          })
      res.writeHead(204).end()
      return
    }
    if (!req.url?.startsWith('/api/shape')) {
      res.writeHead(404).end()
      return
    }
    const abort = new AbortController()
    res.on('close', () => abort.abort())
    const request = new Request(`http://worker.test${req.url}`, {
      method: req.method,
      headers: req.headers as Record<string, string>,
      signal: abort.signal,
    })
    const response = await handler.fetch(request)
    res.writeHead(response.status, Object.fromEntries(response.headers))
    res.end(Buffer.from(await response.arrayBuffer()))
  })().catch(() => {
    if (!res.destroyed) res.writeHead(500).end()
  })
})
server.listen(0, '127.0.0.1', () =>
  process.send?.({
    port: (server.address() as { port: number }).port,
    pid: process.pid,
  })
)
process.once('SIGTERM', () => {
  handler.close()
  server.closeAllConnections()
  server.close(() => process.exit(0))
})
