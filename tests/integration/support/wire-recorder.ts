import { WebSocket, WebSocketServer } from 'ws'
import { once } from 'node:events'

/** Test-only transparent relay. Records server evidence, never client capabilities. */
export async function recordRealtimeWire(upstreamUrl: string) {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(server, 'listening')
  const messages: object[] = []
  const peers = new Set<WebSocket>()
  server.on('connection', (socket) => {
    const upstream = new WebSocket(upstreamUrl, ['neon.realtime.v1'])
    peers.add(upstream)
    const pending: string[] = []
    socket.on('message', (data) => {
      if (upstream.readyState === WebSocket.OPEN) upstream.send(data.toString())
      else pending.push(data.toString())
    })
    upstream.on('open', () => {
      for (const data of pending.splice(0)) upstream.send(data)
    })
    upstream.on('message', (data) => {
      messages.push(JSON.parse(data.toString()))
      if (socket.readyState === WebSocket.OPEN) socket.send(data.toString())
    })
    upstream.on('error', () => socket.close())
    upstream.on('close', () => socket.close())
    socket.on('close', () => {
      upstream.terminate()
      peers.delete(upstream)
    })
  })
  return {
    url: `ws://127.0.0.1:${(server.address() as { port: number }).port}`,
    messages,
    async close() {
      for (const socket of server.clients) socket.terminate()
      for (const socket of peers) socket.terminate()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    },
  }
}
