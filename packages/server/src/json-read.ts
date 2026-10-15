import { generateResponseCursor } from './cursor.js'
import type { JsonLog, LogRead } from './shape-log.js'

export const readHeaders = {
  'access-control-allow-origin': '*',
  'x-content-type-options': 'nosniff',
  'cross-origin-resource-policy': 'cross-origin',
  'cache-control': 'no-store',
}
export class ReadRequestError extends Error {}
export class JsonStreamExpiredError extends Error {}

/** Query validation shared by the shape endpoint and native conformance fixture. */
export function parseReadRequest(request: Request) {
  const params = new URL(request.url).searchParams
  for (const name of [
    'handle',
    'offset',
    'cursor',
    'live',
    'live_sse',
    'experimental_live_sse',
  ])
    if (params.getAll(name).length > 1)
      throw new ReadRequestError(`Repeated ${name} parameter`)
  const offset = params.get('offset')
  if (offset !== null && (!offset || /[\s,]/.test(offset)))
    throw new ReadRequestError('Invalid shape offset')
  const isSse =
    params.get('live') === 'sse' ||
    (params.get('live') === 'true' &&
      (params.get('live_sse') === 'true' ||
        params.get('experimental_live_sse') === 'true'))
  const isLongPoll =
    !isSse && ['true', 'long-poll'].includes(params.get('live') ?? '')
  if (request.method === 'GET' && (isLongPoll || isSse) && offset === null)
    throw new ReadRequestError('Live reads require an offset')
  return {
    offset: offset ?? '-1',
    isSse,
    isLongPoll,
    cursor: params.get('cursor') ?? undefined,
  }
}

export interface JsonReadOptions<T> {
  log: JsonLog<T>
  expiresAt: number
  now?: () => number
  longPollMs?: number
  maxMessages?: number
  sseConnectionMs?: number
  /** Shape initialization has its own now/baseline barrier semantics. */
  readOffset?: string
  cursor?: string
  headers?: HeadersInit
  settled?(): Promise<void>
  assertValid?(): void
  decorate?(read: LogRead<T>, headers: Headers, nativeCursor?: string): void
  eventCursor?(read: LogRead<T>, nativeCursor: string): string
  onStreamError?(error: unknown): void
}

/** Fetch JSON/read/HEAD/long-poll/SSE surface shared by both log backends. */
export async function readJsonStream<T>(
  request: Request,
  options: JsonReadOptions<T>
): Promise<Response> {
  const query = parseReadRequest(request)
  const { log } = options
  const now = options.now ?? Date.now
  const valid = () => {
    options.assertValid?.()
    if (now() >= options.expiresAt)
      throw new JsonStreamExpiredError('Stream expired')
  }
  await options.settled?.()
  valid()
  const position =
    options.readOffset ??
    (query.offset === 'now' ? log.head.offset : query.offset)
  let read: LogRead<T> =
    request.method === 'HEAD'
      ? { ...log.inspect(position), end: log.head, atTail: true, envelopes: [] }
      : await log.read(position, options.maxMessages, request.signal)
  if (request.method === 'GET' && query.isLongPoll && !read.envelopes.length) {
    await log.wait(
      position,
      request.signal,
      Math.min(
        options.longPollMs ?? 20_000,
        Math.max(1, options.expiresAt - now())
      )
    )
    await options.settled?.()
    valid()
    read = await log.read(position, options.maxMessages, request.signal)
  }
  valid()
  const headers = new Headers({
    ...readHeaders,
    'content-type': 'application/json',
    'Stream-Next-Offset': read.end.offset,
    'Stream-Expires-At': new Date(options.expiresAt).toISOString(),
  })
  for (const [name, value] of new Headers(options.headers))
    headers.set(name, value)
  const cursor = query.isLongPoll
    ? generateResponseCursor(options.cursor ?? query.cursor)
    : undefined
  if (cursor) headers.set('Stream-Cursor', cursor)
  if (read.atTail) headers.set('Stream-Up-To-Date', 'true')
  options.decorate?.(read, headers, cursor)
  if (request.method === 'HEAD')
    return new Response(null, { status: 200, headers })
  if (!query.isSse) {
    const timeout = query.isLongPoll && !read.envelopes.length
    return new Response(timeout ? null : JSON.stringify(read.envelopes), {
      status: timeout ? 204 : 200,
      headers,
    })
  }
  headers.set('content-type', 'text/event-stream')
  headers.set('x-accel-buffering', 'no')
  headers.delete('Stream-Up-To-Date')
  headers.delete('electric-up-to-date')
  const stop = new AbortController()
  const signal = AbortSignal.any([request.signal, stop.signal])
  const deadline = Math.min(
    options.expiresAt,
    now() + (options.sseConnectionMs ?? 20_000)
  )
  const encoder = new TextEncoder()
  let initial: LogRead<T> | undefined = read
  let offset = position
  let eventCursor = options.cursor ?? query.cursor
  return new Response(
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          signal.throwIfAborted()
          await options.settled?.()
          valid()
          let batch = initial
          initial = undefined
          if (!batch) {
            batch = await log.read(offset, options.maxMessages, signal)
            if (!batch.envelopes.length) {
              await log.wait(
                offset,
                signal,
                Math.max(1, Math.min(10_000, deadline - now()))
              )
              await options.settled?.()
              valid()
              batch = await log.read(offset, options.maxMessages, signal)
            }
          }
          valid()
          eventCursor = generateResponseCursor(eventCursor)
          const control = {
            streamNextOffset: batch.end.offset,
            streamCursor:
              options.eventCursor?.(batch, eventCursor) ?? eventCursor,
            upToDate: batch.atTail,
          }
          controller.enqueue(
            encoder.encode(
              `event: data\ndata: ${JSON.stringify(batch.envelopes)}\n\nevent: control\ndata: ${JSON.stringify(control)}\n\n`
            )
          )
          offset = batch.end.offset
          if (now() >= deadline) controller.close()
        } catch (error) {
          if (signal.aborted) controller.error(error)
          else {
            options.onStreamError?.(error)
            controller.close()
          }
        }
      },
      cancel() {
        stop.abort()
      },
    }),
    { status: 200, headers }
  )
}
