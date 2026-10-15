import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { randomUUID } from 'node:crypto'
import { DurableStream } from '@durable-streams/client'
import { DurableStreamTestServer } from '@durable-streams/server'
import type { Envelope } from '../../../packages/protocol/src/index'
import { encodeReadProof } from '../../../packages/protocol/src/read-proof'

type Boundary = { afterSeq: `${bigint}`; baselineComplete: boolean }
export type FixtureShape = {
  handle: string
  writer: DurableStream
  boundaries: Map<string, Boundary>
  seq: bigint
  ready: boolean
  expiresAt: number
  schema?: Record<string, unknown>
}

/** Native path-keyed reference server stays behind this query-keyed endpoint. */
export class QueryShapeServer {
  readonly reference = new DurableStreamTestServer({
    port: 0,
    longPollTimeout: 100,
  })
  readonly shapes = new Map<string, FixtureShape>()
  readonly requests: { url: URL; method: string; authorization?: string }[] = []
  onCreate?: (shape: FixtureShape) => void
  beforeRead?: () => Promise<void>
  maxEnvelopesPerRead?: number
  failReadAt?: number
  now: () => number = Date.now
  #appending: Promise<unknown> = Promise.resolve()
  #server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url!, this.url)
      this.requests.push({
        url,
        method: req.method!,
        authorization: req.headers.authorization,
      })
      if (this.requests.length === this.failReadAt) {
        res
          .writeHead(503, { 'Content-Type': 'application/json' })
          .end('{"error":"fixture transient failure"}')
        return
      }
      if (url.pathname !== '/api/shape') {
        res.writeHead(404).end()
        return
      }
      let shape = this.shapes.get(url.searchParams.get('handle') ?? '')
      if (shape && this.now() >= shape.expiresAt) {
        this.shapes.delete(shape.handle)
        shape = undefined
      }
      if (!shape && url.searchParams.has('handle')) {
        res.writeHead(409, { 'Content-Type': 'application/json' }).end('[]')
        return
      }
      if (!shape) {
        const handle = randomUUID()
        const writer = await DurableStream.create({
          url: `${this.reference.url}/shapes/${handle}`,
          contentType: 'application/json',
        })
        shape = {
          handle,
          writer,
          seq: BigInt(0),
          ready: false,
          expiresAt: this.now() + 900_000,
          boundaries: new Map([
            ['-1', { afterSeq: '0', baselineComplete: false }],
          ]),
        }
        this.shapes.set(handle, shape)
        await this.append(shape, {
          version: 1,
          seq: '1',
          kind: 'initializing',
          handle,
          expiresAt: new Date(shape.expiresAt).toISOString(),
        })
        this.onCreate?.(shape)
      }
      const offset = url.searchParams.get('offset') ?? '-1'
      await this.beforeRead?.()
      await this.#appending
      const boundary = shape.boundaries.get(offset)
      if (!boundary) {
        res.writeHead(400).end('invalid offset')
        return
      }
      const nativeUrl = new URL(shape.writer.url)
      nativeUrl.searchParams.set('offset', offset)
      if (url.searchParams.get('live') === 'true')
        nativeUrl.searchParams.set('live', 'long-poll')
      const controller = new AbortController()
      res.on('close', () => {
        controller.abort()
      })
      const response = await fetch(nativeUrl, { signal: controller.signal })
      const headers = Object.fromEntries(response.headers.entries())
      let body = Buffer.from(await response.arrayBuffer())
      await this.#appending
      if (this.maxEnvelopesPerRead && response.status === 200) {
        const items = JSON.parse(body.toString()) as Envelope[]
        if (items.length > this.maxEnvelopesPerRead) {
          const selected = items.slice(0, this.maxEnvelopesPerRead)
          const last = selected[selected.length - 1]
          const end = Array.from(shape.boundaries).find(
            ([, value]) => value.afterSeq === last.seq
          )?.[0]
          if (!end) throw new Error('Missing fixture boundary')
          headers['stream-next-offset'] = end
          delete headers['stream-up-to-date']
          body = Buffer.from(JSON.stringify(selected))
        }
      }
      headers['electric-handle'] = shape.handle
      headers['electric-offset'] = headers['stream-next-offset']
      headers['electric-cursor'] = encodeReadProof({
        version: 1,
        offset,
        ...boundary,
        nativeCursor: response.headers.get('Stream-Cursor') ?? undefined,
      })
      if (shape.schema)
        headers['electric-schema'] = JSON.stringify(shape.schema)
      if (headers['stream-up-to-date'] !== undefined)
        headers['electric-up-to-date'] = 'true'
      delete headers['content-length']
      delete headers['content-encoding']
      // This fixture deliberately exercises both native-header and existing
      // Electric-header allowlist forwarding without changing the URL contract.
      if (this.electricHeadersOnly) {
        for (const header of Object.keys(headers))
          if (header.startsWith('stream-')) delete headers[header]
      }
      res.writeHead(response.status, headers)
      res.end(body)
    })().catch((error: Error) => {
      if (!res.destroyed) res.writeHead(500).end(error.message)
    })
  })

  constructor(readonly electricHeadersOnly = false) {}

  get url() {
    return `http://127.0.0.1:${(this.#server.address() as AddressInfo).port}`
  }

  async start() {
    await this.reference.start()
    await new Promise<void>((resolve) => {
      this.#server.listen(0, '127.0.0.1', resolve)
    })
  }

  async stop() {
    this.#server.closeAllConnections()
    await new Promise<void>((resolve, reject) => {
      this.#server.close((error) => (error ? reject(error) : resolve()))
    })
    await this.reference.stop()
  }

  append(shape: FixtureShape, envelope: Envelope): Promise<string> {
    const operation = this.#appending.then(async () => {
      await shape.writer.append(JSON.stringify(envelope))
      shape.seq = BigInt(envelope.seq)
      if (envelope.kind === 'snapshot-complete') shape.ready = true
      const head = await shape.writer.head()
      if (!head.exists || !head.offset)
        throw new Error('Fixture stream disappeared after append')
      shape.boundaries.set(head.offset, {
        afterSeq: envelope.seq,
        baselineComplete: shape.ready,
      })
      return head.offset
    })
    // Readers cannot observe a byte range whose logical boundary is not yet
    // indexed. The reference server's append and HEAD are separate HTTP calls.
    this.#appending = operation
    return operation
  }
}
