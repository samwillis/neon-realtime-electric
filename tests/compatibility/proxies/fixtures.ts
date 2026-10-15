import { createServer } from 'node:http'
import type { Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { ELECTRIC_PROTOCOL_QUERY_PARAMS } from '../../../packages/client/src/constants'

export type ProxyMode =
  | 'transparent'
  | 'electric-allowlist'
  | 'trusted-predicate'

async function listen(server: Server) {
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve)
  })
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    stop: async () => {
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()))
      })
    },
  }
}

/** Same proxy route implementation for the original and packed replacement. */
export async function proxyFixture(
  upstream: string,
  mode: ProxyMode,
  timeoutMs = 250
) {
  const requests: URL[] = []
  const server = createServer((req, res) => {
    void (async () => {
      const requestUrl = new URL(req.url!, 'https://proxy.invalid')
      requests.push(requestUrl)
      if (
        requestUrl.pathname !== '/application/shapes' ||
        req.method !== 'GET'
      ) {
        res.writeHead(405).end()
        return
      }
      const target = new URL(upstream)
      for (const [key, value] of requestUrl.searchParams) {
        if (
          mode !== 'electric-allowlist' ||
          ELECTRIC_PROTOCOL_QUERY_PARAMS.includes(key)
        )
          target.searchParams.append(key, value)
      }
      if (mode === 'trusted-predicate') {
        target.searchParams.set('table', 'items')
        target.searchParams.set('where', 'tenant_id = $1')
        target.searchParams.set('params[1]', 'trusted-tenant')
      }
      const response = await fetch(target, {
        signal: AbortSignal.timeout(timeoutMs),
      })
      const headers: Record<string, string> = {}
      for (const [key, value] of response.headers) {
        if (
          mode !== 'electric-allowlist' ||
          key.startsWith('electric-') ||
          ['content-type', 'cache-control'].includes(key)
        )
          headers[key] = value
      }
      delete headers['content-length']
      delete headers['content-encoding']
      res.writeHead(response.status, headers)
      res.end(Buffer.from(await response.arrayBuffer()))
    })().catch((error: Error) => {
      if (!res.destroyed) res.writeHead(504).end(error.message)
    })
  })
  return { ...(await listen(server)), requests }
}

/** Controlled original Electric HTTP service; proxy code never sees its body. */
export async function electricFixture() {
  const requests: URL[] = []
  const baseline = [
    {
      key: '"public"."items"/"1"',
      value: { id: '1', name: 'first', done: 'f' },
      headers: { operation: 'insert' },
    },
  ]
  const log: { position: number; messages: unknown[] }[] = []
  const polls = new Set<() => void>()
  let handle = 'original-handle'
  const server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url!, 'https://original.invalid')
      requests.push(url)
      if (
        url.searchParams.has('handle') &&
        url.searchParams.get('handle') !== handle
      ) {
        res
          .writeHead(409, {
            'electric-handle': handle,
            'Content-Type': 'application/json',
          })
          .end('[]')
        return
      }
      const offset = url.searchParams.get('offset') ?? '-1'
      const position = offset === '-1' ? 0 : Number(offset.split('_')[1])
      if (
        url.searchParams.get('live') === 'true' &&
        position >= log.length + 1
      ) {
        await new Promise<void>((resolve) => {
          const done = () => {
            clearTimeout(timer)
            polls.delete(done)
            resolve()
          }
          const timer = setTimeout(done, 50)
          polls.add(done)
        })
      }
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'electric-handle': handle,
        'electric-offset': `0_${log.length + 1}`,
        'electric-cursor': `cursor-${log.length + 1}`,
        'electric-schema': JSON.stringify({
          id: { type: 'int4', not_null: true },
          name: { type: 'text' },
          done: { type: 'bool' },
        }),
      })
      res.end(
        JSON.stringify([
          ...(offset === '-1' ? baseline : []),
          ...log
            .filter((entry) => entry.position > position)
            .flatMap((entry) => entry.messages),
          { headers: { control: 'up-to-date' } },
        ])
      )
    })().catch((error: Error) => {
      if (!res.destroyed) res.writeHead(500).end(error.message)
    })
  })
  return {
    ...(await listen(server)),
    requests,
    publish(messages: unknown[]) {
      log.push({ position: log.length + 2, messages })
      for (const wake of polls) wake()
    },
    expire() {
      handle = 'replacement-handle'
      log.length = 0
      for (const wake of polls) wake()
    },
  }
}
