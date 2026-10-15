import { fetchEventSource } from '@microsoft/fetch-event-source'
import type { Row, Offset } from '../types'
import { FetchError, MissingHeadersError } from '../error'
import {
  consumeResponseBody,
  createFetchWithChunkBuffer,
  createFetchWithConsumedMessages,
  createFetchWithResponseHeadersCheck,
} from '../fetch'
import {
  CHUNK_LAST_OFFSET_HEADER,
  LIVE_CACHE_BUSTER_HEADER,
  SHAPE_HANDLE_HEADER,
  SHAPE_SCHEMA_HEADER,
} from '../constants'
import { isUpToDateMessage } from '../helpers'
import type {
  ReadRequest,
  ResponseMetadata,
  ShapeTransport,
  SnapshotRequest,
  SnapshotResponse,
} from './types'

/** Reference adapter while the shared client is moved to Durable Streams. */
export class ElectricHttpTransport<T extends Row<unknown>>
  implements ShapeTransport<T>
{
  readonly #fetch: typeof fetch
  readonly #snapshotFetch: typeof fetch

  constructor(fetchWithBackoff: typeof fetch) {
    this.#fetch = createFetchWithResponseHeadersCheck(
      createFetchWithChunkBuffer(fetchWithBackoff)
    )
    this.#snapshotFetch = createFetchWithConsumedMessages(this.#fetch)
  }

  async read(request: ReadRequest<T>): Promise<void> {
    if (request.mode === 'sse') return this.#readSse(request)

    const raw = await request.withTimeout(
      this.#fetch(request.url.toString(), {
        signal: request.signal,
        headers: request.headers,
      })
    )
    const response = await consumeResponseBody(
      raw,
      request.url.toString(),
      request.signal
    )
    const metadata = responseMetadata(response)
    if (!(await request.onMetadata(metadata))) return
    const json = (await response.text()) || '[]'
    const messages = request.decode(json, metadata, 'array')
    await request.onMessages(messages, false)
  }

  async #readSse(request: ReadRequest<T>): Promise<void> {
    let metadata: ResponseMetadata
    let ignored = false
    let buffer: ReturnType<ReadRequest<T>['decode']> = []
    try {
      await fetchEventSource(request.url.toString(), {
        headers: { ...request.headers, Accept: 'text/event-stream' },
        fetch: this.#fetch,
        signal: request.signal,
        onopen: async (response) => {
          metadata = responseMetadata(response)
          if (!(await request.onMetadata(metadata))) {
            ignored = true
            throw new Error('stale response ignored')
          }
        },
        onmessage: (event) => {
          if (!event.data) return
          const messages = request.decode(event.data, metadata, 'message')
          buffer.push(...messages)
          if (isUpToDateMessage(messages[messages.length - 1])) {
            // Preserve the reference reader's callback behavior. The common
            // client's publish queue serializes async subscriber delivery.
            void request.onMessages(buffer, true)
            buffer = []
          }
        },
        onerror: (error) => {
          throw error
        },
      })
    } catch (error) {
      if (!ignored) throw error
    }
  }

  async fetchSnapshot(request: SnapshotRequest): Promise<SnapshotResponse> {
    const response = await this.#snapshotFetch(
      request.url.toString(),
      request.init
    )
    if (!response.ok) {
      throw await FetchError.fromResponse(response, request.url.toString())
    }
    const metadata = responseMetadata(response, request.schema)
    if (!request.schema && !response.headers.get(SHAPE_SCHEMA_HEADER)) {
      throw new MissingHeadersError(request.url.toString(), [
        SHAPE_SCHEMA_HEADER,
      ])
    }
    const body = await response.json()
    return { metadata: body.metadata, rawData: body.data, response: metadata }
  }
}

function responseMetadata(
  response: Response,
  knownSchema?: ResponseMetadata['schema']
): ResponseMetadata {
  const schema = response.headers.get(SHAPE_SCHEMA_HEADER)
  return {
    status: response.status,
    handle: response.headers.get(SHAPE_HANDLE_HEADER),
    offset: response.headers.get(CHUNK_LAST_OFFSET_HEADER) as Offset | null,
    cursor: response.headers.get(LIVE_CACHE_BUSTER_HEADER),
    schema: knownSchema ?? (schema ? JSON.parse(schema) : {}),
    errorHeaders: Object.fromEntries(response.headers.entries()),
    cancel: async () => {
      await response.body?.cancel()
    },
  }
}
