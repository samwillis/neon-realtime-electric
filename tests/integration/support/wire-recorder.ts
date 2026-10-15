import { WebSocket, WebSocketServer } from 'ws'
import { once } from 'node:events'

/** Test-only transparent relay. Records server evidence, never client capabilities. */
export async function recordRealtimeWire(
  upstreamUrl: string,
  options: { holdBaselineEnd?: boolean } = {}
) {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 })
  await once(server, 'listening')
  const messages: object[] = []
  const peers = new Set<WebSocket>()
  let release: (() => void) | undefined
  let held!: () => void
  const baselineHeld = new Promise<void>((resolve) => {
    held = resolve
  })
  server.on('connection', (socket) => {
    const upstream = new WebSocket(upstreamUrl, ['neon.realtime.v1'])
    peers.add(upstream)
    const pending: string[] = []
    const delayed: string[] = []
    let holding = false
    const forward = (data: string) => {
      if (socket.readyState === WebSocket.OPEN) socket.send(data)
    }
    socket.on('message', (data) => {
      if (upstream.readyState === WebSocket.OPEN) upstream.send(data.toString())
      else pending.push(data.toString())
    })
    upstream.on('open', () => {
      for (const data of pending.splice(0)) upstream.send(data)
    })
    upstream.on('message', (data) => {
      const message = JSON.parse(data.toString())
      messages.push(message)
      if (options.holdBaselineEnd && message.type === 'baseline_sync_end') {
        holding = true
        release = () => {
          holding = false
          for (const data of delayed.splice(0)) forward(data)
        }
        held()
      }
      if (holding) delayed.push(data.toString())
      else forward(data.toString())
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
    baselineHeld,
    releaseBaseline: () => release?.(),
    async close() {
      for (const socket of server.clients) socket.terminate()
      for (const socket of peers) socket.terminate()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    },
  }
}
