import {
  stream,
  FetchError as DurableFetchError,
  FetchBackoffAbortError as DurableAbortError,
  DurableStreamError,
} from '@durable-streams/client'
import type { StreamResponse } from '@durable-streams/client'
import { EnvelopeConsumer } from '@neon/realtime-electric-protocol/consumer'
import { decodeReadProof } from '@neon/realtime-electric-protocol/read-proof'
import { ShapeProtocolError } from '@neon/realtime-electric-protocol'
import type { Envelope } from '@neon/realtime-electric-protocol'
import type { BackoffOptions } from '../fetch'
import {
  FetchError,
  FetchBackoffAbortError,
  InvalidShapeOptionsError,
  MissingHeadersError,
} from '../error'
import type { Message, Offset, Row, Schema } from '../types'
import type {
  ReadRequest,
  ResponseMetadata,
  ShapeTransport,
  SnapshotRequest,
  SnapshotResponse,
} from './types'

/** Real Durable Streams reader; shape readiness and commits remain above it. */
export class DurableStreamsTransport<T extends Row<unknown>>
  implements ShapeTransport<T>
{
  #consumer?: EnvelopeConsumer
  #handle?: string
  readonly #fetch: typeof fetch
  readonly #backoff?: BackoffOptions

  constructor(fetchClient: typeof fetch, backoff?: BackoffOptions) {
    this.#fetch = fetchClient
    this.#backoff = backoff
  }

  async read(request: ReadRequest<T>): Promise<void> {
    let session: StreamResponse<Envelope> | undefined
    const responses: { response: Response; offset: string }[] = []
    let sseResponse: { response: Response; offset: string } | undefined
    let observedHandle = request.url.searchParams.get('handle')
    const offset = request.url.searchParams.get('offset') ?? '-1'
    const controller = new AbortController()
    const abort = () => {
      controller.abort(request.signal.reason)
    }
    request.signal.addEventListener('abort', abort, { once: true })
    if (request.signal.aborted) abort()
    let resolutionFailure: { error: unknown } | undefined
    // A response header can expose a handle before its body succeeds. Keep the
    // byte checkpoint unchanged until consumption succeeds, including on retry.
    const checkpoint =
      this.#handle === observedHandle
        ? this.#consumer?.state.checkpoint.offset
        : undefined
    const startOffset = checkpoint ?? offset
    let fetchCount = 0
    const transportFetch: typeof fetch = async (input, init) => {
      const nativeUrl = new URL(
        input instanceof Request ? input.url : input.toString()
      )
      let resolved: { url: URL; headers: Record<string, string> }
      try {
        resolved =
          fetchCount++ === 0
            ? { url: request.url, headers: request.headers }
            : await request.resolveRequest()
      } catch (error) {
        // User resolver exceptions belong to the common error handler. Abort
        // the native network retry loop instead of treating them as offline IO.
        resolutionFailure = { error }
        controller.abort(error)
        throw error
      }
      const outward = new URL(resolved.url)
      const requestedOffset =
        nativeUrl.searchParams.get('offset') ?? startOffset
      outward.searchParams.set('offset', requestedOffset)
      if (observedHandle) outward.searchParams.set('handle', observedHandle)
      // Native live values are internal. Custom fetch hooks and proxies retain
      // Electric's existing query/method conventions.
      const live = nativeUrl.searchParams.get('live')
      if (live === 'long-poll' || live === 'sse')
        outward.searchParams.set('live', 'true')
      const cursor = nativeUrl.searchParams.get('cursor')
      if (cursor && !outward.searchParams.has('cursor'))
        outward.searchParams.set('cursor', cursor)
      const response = await request.withTimeout(
        this.#fetch(outward.toString(), { ...init, headers: resolved.headers })
      )
      const handle = response.headers.get('electric-handle')
      if (handle) observedHandle = handle
      const normalized = normalizeHeaders(response)
      // Only queue metadata for a response actually returned to the reader.
      // If normalization throws, native fetch retries must not leave a stale
      // entry that would be paired with a later successful response body.
      if (response.ok) {
        const entry = { response, offset: requestedOffset }
        if (response.headers.get('content-type')?.includes('text/event-stream'))
          sseResponse = entry
        else responses.push(entry)
      }
      return normalized
    }

    try {
      session = await stream<Envelope>({
        url: request.url,
        offset: startOffset,
        fetch: transportFetch,
        headers: request.headers,
        signal: controller.signal,
        live: request.mode === 'sse' ? 'sse' : false,
        json: true,
        backoffOptions: this.#backoff,
        warnOnHttp: false,
      })
      const first = responses[0] ?? sseResponse
      if (!first)
        throw new ShapeProtocolError('Missing initial response metadata')
      const firstMetadata = metadata(first.response)
      if (!(await request.onMetadata(firstMetadata))) return
      this.#restoreConsumer(first.response, first.offset)

      session.subscribeJson(async (batch) => {
        const jsonEntry = responses.shift()
        const isSse = !jsonEntry && !!sseResponse
        const entry = jsonEntry ?? sseResponse
        if (!entry)
          throw new ShapeProtocolError('Missing batch response metadata')
        const proof = decodeReadProof(
          isSse
            ? (batch.cursor ?? null)
            : entry.response.headers.get('electric-cursor')
        )
        const info = {
          ...metadata(entry.response),
          ...(isSse ? { cursor: batch.cursor ?? null } : {}),
        }
        // Every row response repeats schema, including resume from a noninitial
        // offset. Initialization is allowed to precede schema availability.
        if (!(await request.onMetadata(info)))
          throw new ShapeProtocolError('Ignored batch metadata')
        const readOffset = isSse
          ? this.#consumer!.state.checkpoint.offset
          : entry.offset
        if (
          proof.offset !== readOffset ||
          proof.afterSeq !== this.#consumer!.state.appliedSeq
        ) {
          // During interrupted replay the consumer may already have applied
          // part of this earlier checkpoint. It will suppress those sequences.
          if (proof.offset !== this.#consumer!.state.checkpoint.offset) {
            throw new ShapeProtocolError(
              'Read proof does not match the resume checkpoint'
            )
          }
        }
        let lastLsn: string | undefined
        await this.#consumer!.consume(batch, async (envelope) => {
          if (
            envelope.kind === 'initializing' &&
            envelope.handle !== info.handle
          ) {
            throw new ShapeProtocolError(
              'Envelope handle differs from response handle'
            )
          }
          let raw: unknown[] = []
          if (
            envelope.kind === 'snapshot-chunk' ||
            envelope.kind === 'publication'
          ) {
            if (!info.schema)
              throw new MissingHeadersError(request.url.toString(), [
                'electric-schema',
              ])
            raw = envelope.changes
            if (envelope.kind === 'publication' && envelope.evidence.txids) {
              const txids = envelope.evidence.txids.map((id) =>
                Number(BigInt.asUintN(32, BigInt(id)))
              )
              raw = envelope.changes.map((change) => ({
                ...change,
                headers: {
                  ...change.headers,
                  ...('operation' in change.headers ? { txids } : {}),
                },
              }))
            }
          }
          if (
            envelope.kind === 'snapshot-complete' ||
            envelope.kind === 'progress'
          ) {
            raw = [
              {
                headers: {
                  control: 'snapshot-end',
                  ...envelope.evidence.snapshot,
                },
              },
            ]
          }
          if ('evidence' in envelope && envelope.evidence.lsn !== undefined)
            lastLsn = envelope.evidence.lsn
          if (raw.length)
            await request.onMessages(
              request.decode(JSON.stringify(raw), info, 'array'),
              isSse
            )
        })
        // Shared state gets the fully consumed response offset. Early metadata
        // never advances it. Stream tail alone is insufficient for readiness.
        await request.onMetadata({ ...info, offset: batch.offset as Offset })
        if (batch.upToDate && this.#consumer!.state.ready) {
          const message: Message<T> = {
            headers: {
              control: 'up-to-date',
              ...(lastLsn ? { global_last_seen_lsn: lastLsn } : {}),
            },
          }
          await request.onMessages([message], isSse)
        }
      })
      await session.closed
      if (resolutionFailure) throw resolutionFailure.error
      // Pace temporary initialization tails so they do not trip the common
      // client's stale-cache loop guard or spin while the baseline is built.
      if (!this.#consumer!.state.ready) await pause(150, request.signal)
    } catch (error) {
      if (resolutionFailure) throw resolutionFailure.error
      if (request.signal.aborted || error instanceof DurableAbortError)
        throw new FetchBackoffAbortError()
      if (error instanceof DurableFetchError) {
        throw new FetchError(
          error.status,
          error.text,
          error.json,
          error.headers,
          error.url,
          error.message
        )
      }
      if (request.mode === 'sse' && error instanceof TypeError) {
        // Interrupted response bodies are connection failures. The retained
        // Electric SSE lifecycle retries from our last consumed checkpoint;
        // protocol/JSON errors below keep their public FetchError boundary.
        throw new Error('Durable Streams SSE connection interrupted')
      }
      if (
        error instanceof DurableStreamError ||
        error instanceof ShapeProtocolError ||
        error instanceof SyntaxError ||
        error instanceof TypeError
      ) {
        throw new FetchError(
          error instanceof DurableStreamError ? (error.status ?? 200) : 200,
          error.message,
          undefined,
          Object.fromEntries(session?.headers.entries() ?? []),
          request.url.toString()
        )
      }
      throw error
    } finally {
      session?.cancel()
      controller.abort()
      request.signal.removeEventListener('abort', abort)
    }
  }

  #restoreConsumer(response: Response, offset: string) {
    const handle = response.headers.get('electric-handle')
    if (!handle) throw new ShapeProtocolError('Missing shape handle')
    const proof = decodeReadProof(response.headers.get('electric-cursor'))
    if (proof.offset !== offset)
      throw new ShapeProtocolError('Read proof offset mismatch')
    if (this.#handle !== handle || !this.#consumer) {
      this.#handle = handle
      this.#consumer = new EnvelopeConsumer({
        appliedSeq: proof.afterSeq,
        ready: proof.baselineComplete,
        checkpoint: { offset },
      })
    }
  }

  async fetchSnapshot(_request: SnapshotRequest): Promise<SnapshotResponse> {
    throw new InvalidShapeOptionsError(
      'Durable Streams subset snapshots are not enabled yet'
    )
  }
}

function metadata(response: Response): ResponseMetadata {
  const rawSchema = response.headers.get('electric-schema')
  return {
    // A native 204 reports physical tail, which cannot imply shape readiness.
    status: response.status === 204 ? 200 : response.status,
    handle: response.headers.get('electric-handle'),
    offset: null,
    cursor: response.headers.get('electric-cursor'),
    schema: rawSchema ? (JSON.parse(rawSchema) as Schema) : undefined,
    errorHeaders: Object.fromEntries(response.headers.entries()),
    cancel: async () => {
      await response.body?.cancel()
    },
  }
}

/** Normalize behind Electric-header allowlists without modifying body framing. */
export function normalizeHeaders(response: Response): Response {
  const headers = new Headers(response.headers)
  if (!headers.has('Stream-Next-Offset') && headers.has('electric-offset')) {
    headers.set('Stream-Next-Offset', headers.get('electric-offset')!)
  }
  if (
    !headers.has('Stream-Up-To-Date') &&
    headers.get('electric-up-to-date') === 'true'
  ) {
    headers.set('Stream-Up-To-Date', 'true')
  }
  if (!headers.has('Stream-Cursor')) {
    try {
      const proof = decodeReadProof(headers.get('electric-cursor'))
      if (proof.nativeCursor) headers.set('Stream-Cursor', proof.nativeCursor)
    } catch {
      /* Validate proof outside the reader's fetch retry loop. */
    }
  }
  return new Response(
    [204, 205, 304].includes(response.status) ? null : response.body,
    {
      status: response.status,
      statusText: response.statusText,
      headers,
    }
  )
}

async function pause(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return
  await new Promise<void>((resolve) => {
    const finish = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', finish)
      resolve()
    }
    const timer = setTimeout(finish, ms)
    signal.addEventListener('abort', finish, { once: true })
  })
}
