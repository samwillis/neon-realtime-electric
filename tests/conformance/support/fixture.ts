import { createServer } from 'node:http'
import { once } from 'node:events'
import { Readable } from 'node:stream'
import { randomUUID } from 'node:crypto'
import { MemoryJsonLog } from '../../../packages/server/src/memory-log.js'
import { ObjectJsonLog } from '../../../packages/server/src/object-log.js'
import type {
  JsonLog,
  LogCodec,
} from '../../../packages/server/src/shape-log.js'
import { InvalidLogOffsetError } from '../../../packages/server/src/shape-log.js'
import {
  JsonStreamExpiredError,
  ReadRequestError,
  parseReadRequest,
  readHeaders,
  readJsonStream,
} from '../../../packages/server/src/json-read.js'
import { FixtureObjectStore } from '../../server/support/object-store.js'

type Json = null | boolean | number | string | Json[] | { [key: string]: Json }
const codec: LogCodec<Json> = {
  parse(value) {
    return JSON.parse(JSON.stringify(value)) as Json
  },
  advance(_, previous) {
    return {
      afterSeq: `${BigInt(previous.afterSeq) + 1n}` as `${bigint}`,
      baselineComplete: true,
    }
  },
  validateRead() {},
}

/** Test-only producer controls. All GET/HEAD framing, offsets, waits and object
 * replay use production code. No payload rewriting or expected-body adaptation.
 */
export async function createFixture(objects: boolean) {
  const store = new FixtureObjectStore()
  let time: number | undefined
  const now = () => time ?? Date.now()
  const waitObservers: (() => void)[] = []
  const streams = new Map<
    string,
    { log: JsonLog<Json>; expiresAt: number; contentType: string }
  >()
  const logs: JsonLog<Json>[] = []
  const response = (status: number, body?: unknown, headers?: HeadersInit) =>
    new Response(body === undefined ? null : JSON.stringify(body), {
      status,
      headers: { ...readHeaders, ...Object.fromEntries(new Headers(headers)) },
    })
  const fetchHandler = async (request: Request): Promise<Response> => {
    const url = new URL(request.url)
    // Validate read syntax before any resource or payload operation.
    if (request.method === 'GET' || request.method === 'HEAD')
      parseReadRequest(request)
    let entry = streams.get(url.pathname)
    if (entry && now() >= entry.expiresAt) {
      entry.log.close()
      streams.delete(url.pathname)
      entry = undefined
    }
    if (request.method === 'OPTIONS')
      return response(204, undefined, {
        'access-control-allow-methods': 'GET,HEAD,PUT,POST,DELETE,OPTIONS',
      })
    if (request.method === 'PUT') {
      const contentType =
        request.headers.get('content-type') ?? 'application/json'
      if (contentType.split(';')[0].trim().toLowerCase() !== 'application/json')
        return response(415)
      if (entry) return response(entry.contentType === contentType ? 200 : 409)
      const text = await request.text()
      const value: Json | undefined = text ? JSON.parse(text) : undefined
      const expiresAt = request.headers.get('Stream-Expires-At')
        ? Date.parse(request.headers.get('Stream-Expires-At')!)
        : now() + 900_000
      if (!Number.isFinite(expiresAt)) return response(400)
      const log = objects
        ? new ObjectJsonLog(
            { store, prefix: `conformance/${randomUUID()}`, sealBytes: 1 },
            codec
          )
        : new MemoryJsonLog(codec)
      logs.push(log)
      // Observe entry into the real waiter without changing its arguments or
      // completion. Supplemental tests can append/abort after registration.
      const wait = log.wait.bind(log)
      log.wait = (...args) => {
        const pending = wait(...args)
        waitObservers.shift()?.()
        return pending
      }
      entry = { log, expiresAt, contentType }
      streams.set(url.pathname, entry)
      if (value !== undefined)
        for (const message of Array.isArray(value) ? value : [value])
          log.append(message)
      if (log instanceof ObjectJsonLog) await log.flush()
      return response(201, undefined, {
        'Stream-Next-Offset': log.head.offset,
        location: request.url,
      })
    }
    if (!entry) return response(404)
    if (request.method === 'POST') {
      const text = await request.text()
      if (!text) return response(400)
      const value: Json = JSON.parse(text)
      if (Array.isArray(value) && !value.length) return response(400)
      for (const message of Array.isArray(value) ? value : [value])
        entry.log.append(message)
      if (entry.log instanceof ObjectJsonLog) await entry.log.flush()
      return response(204, undefined, {
        'Stream-Next-Offset': entry.log.head.offset,
      })
    }
    if (request.method === 'DELETE') {
      entry.log.close()
      streams.delete(url.pathname)
      await entry.log.disposed
      return response(204)
    }
    if (!['GET', 'HEAD'].includes(request.method)) return response(405)
    return readJsonStream(request, {
      log: entry.log,
      expiresAt: entry.expiresAt,
      now,
      longPollMs: 100,
      sseConnectionMs: 500,
      headers: { 'content-type': entry.contentType },
    })
  }
  const http = createServer((req, res) => {
    void (async () => {
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(Buffer.from(chunk))
      const abort = new AbortController()
      res.once('close', () => abort.abort())
      let result: Response
      try {
        const method = req.method ?? 'GET'
        result = await fetchHandler(
          new Request(new URL(req.url!, 'http://fixture'), {
            method,
            headers: req.headers as HeadersInit,
            ...(!['GET', 'HEAD'].includes(method)
              ? { body: Buffer.concat(chunks) }
              : {}),
            signal: abort.signal,
          })
        )
      } catch (error) {
        if (abort.signal.aborted) {
          res.destroy()
          return
        }
        result = response(
          error instanceof SyntaxError ||
            error instanceof ReadRequestError ||
            error instanceof InvalidLogOffsetError
            ? 400
            : error instanceof JsonStreamExpiredError
              ? 404
              : 500
        )
      }
      res.writeHead(result.status, Object.fromEntries(result.headers))
      if (!result.body || req.method === 'HEAD') res.end()
      else
        Readable.fromWeb(
          result.body as import('node:stream/web').ReadableStream
        )
          .on('error', () => res.destroy())
          .pipe(res)
    })().catch(() => res.destroy())
  })
  http.listen(0, '127.0.0.1')
  await once(http, 'listening')
  return {
    baseUrl: `http://127.0.0.1:${(http.address() as { port: number }).port}`,
    store,
    setNow(value: number) {
      time = value
    },
    nextWait: () => new Promise<void>((resolve) => waitObservers.push(resolve)),
    async close() {
      for (const log of logs) log.close()
      await Promise.all(logs.map((log) => log.disposed))
      http.closeAllConnections()
      await new Promise<void>((resolve, reject) =>
        http.close((error) => (error ? reject(error) : resolve()))
      )
    },
  }
}
