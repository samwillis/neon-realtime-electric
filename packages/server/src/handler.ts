import { InvalidLogOffsetError } from './shape-log.js'
import { ShapeDefinitionError } from './definition.js'
import { createHash } from 'node:crypto'
import { encodeReadProof } from '@neon/realtime-electric-protocol/read-proof'
import {
  SourceQueryRejectedError,
  type RealtimeSource,
} from './realtime-source.js'
import { ShapeOwner, type OwnerDefinition } from './shape-owner.js'
import { ObjectShapeLog } from './object-log.js'
import type { LogRead } from './shape-log.js'

export interface AuthorizedShape {
  definition: OwnerDefinition
  /** Stable server-derived tenant/user/policy identity; never read from a caller-supplied handle. */
  authorizationKey: string
}
export interface ShapeHandlerOptions {
  source: RealtimeSource
  /** Runs on every request, including resumed reads. Reject unauthorized requests here. */
  authorize(request: Request): Promise<AuthorizedShape>
  now?: () => number
  ttlMs?: number
  longPollMs?: number
  maxEnvelopesPerRead?: number
  /** Bound each SSE response; native readers reconnect at its consumed offset. */
  sseConnectionMs?: number
  createLog?: import('./shape-owner.js').ShapeOwnerOptions['createLog']
  onOwnerClosed?: import('./shape-owner.js').ShapeOwnerOptions['onClose']
}

/** Local single-owner Fetch handler; platform affinity remains a separate gate. */
export function createShapeHandler(options: ShapeHandlerOptions) {
  const owners = new Map<
    string,
    {
      owner: ShapeOwner
      identity: string
      cleanup: ReturnType<typeof setTimeout>
    }
  >()
  const now = options.now ?? Date.now
  let closed = false
  const pendingCleanup = new Set<Promise<void>>()
  let cleanupError: Error | undefined
  const cors = {
    'access-control-allow-origin': '*',
    'access-control-expose-headers':
      'electric-handle,electric-offset,electric-cursor,electric-schema,electric-up-to-date,Stream-Next-Offset,Stream-Up-To-Date,Stream-Cursor,Stream-Expires-At',
    'cache-control': 'no-store',
  }
  const remove = (handle: string) => {
    const entry = owners.get(handle)
    if (!entry) return
    owners.delete(handle)
    clearTimeout(entry.cleanup)
    entry.owner.close()
    if (entry.owner.log.disposed) {
      const operation = entry.owner.log.disposed
        .catch((error) => {
          cleanupError ??=
            error instanceof Error ? error : new Error(String(error))
        })
        .finally(() => pendingCleanup.delete(operation))
      pendingCleanup.add(operation)
    }
  }
  const create = (
    authorized: AuthorizedShape,
    identity: string,
    includeBaseline = true
  ) => {
    const owner = new ShapeOwner({
      source: options.source,
      definition: authorized.definition,
      now,
      ttlMs: options.ttlMs,
      createLog: options.createLog,
      onClose: options.onOwnerClosed,
      includeBaseline,
    })
    const cleanup = setTimeout(
      () => remove(owner.handle),
      owner.expiresAt - now()
    )
    cleanup.unref?.()
    const entry = { owner, identity, cleanup }
    owners.set(owner.handle, entry)
    void owner.start()
    return entry
  }
  const refetch = (
    authorized: AuthorizedShape,
    identity: string,
    method: string
  ) => {
    const headers = new Headers(cors)
    if (method === 'GET')
      headers.set('electric-handle', create(authorized, identity).owner.handle)
    return Response.json([{ headers: { control: 'must-refetch' } }], {
      status: 409,
      headers,
    })
  }
  const failedOwner = (
    owner: ShapeOwner,
    error: unknown,
    authorized: AuthorizedShape,
    identity: string,
    method: string
  ) => {
    // A log read may race source invalidation and throw its generic closed-log
    // error. Retain the owner's original typed terminal cause in that case.
    try {
      owner.assertValid()
    } catch (cause) {
      error = cause
    }
    remove(owner.handle)
    if (error instanceof SourceQueryRejectedError) {
      const headers = new Headers(cors)
      headers.set('content-type', 'application/json')
      return new Response(
        method === 'HEAD' ? null : JSON.stringify({ error: error.message }),
        { status: 400, headers }
      )
    }
    return refetch(authorized, identity, method)
  }
  return {
    async fetch(request: Request): Promise<Response> {
      if (closed)
        return Response.json(
          { error: 'Shape service stopped' },
          { status: 503, headers: cors }
        )
      if (request.method === 'OPTIONS')
        return new Response(null, {
          status: 204,
          headers: {
            ...cors,
            'access-control-allow-methods': 'GET,HEAD,DELETE,OPTIONS',
            'access-control-allow-headers': 'authorization,content-type',
          },
        })
      if (!['GET', 'HEAD', 'DELETE'].includes(request.method))
        return Response.json(
          { error: 'Method not supported for this shape profile' },
          {
            status: 405,
            headers: { ...cors, allow: 'GET,HEAD,DELETE,OPTIONS' },
          }
        )
      let authorized: AuthorizedShape
      try {
        authorized = await options.authorize(request)
      } catch (error) {
        if (error instanceof ShapeDefinitionError)
          return Response.json(
            { error: error.message },
            { status: 400, headers: cors }
          )
        return Response.json(
          { error: 'Shape authorization rejected' },
          { status: 403, headers: cors }
        )
      }
      request.signal.throwIfAborted()
      const identity = createHash('sha256')
        .update(
          JSON.stringify(authorized, (_, value) =>
            typeof value === 'bigint' ? value.toString() : value
          )
        )
        .digest('hex')
      const url = new URL(request.url)
      const isSse =
        url.searchParams.get('live') === 'sse' ||
        (url.searchParams.get('live') === 'true' &&
          (url.searchParams.get('live_sse') === 'true' ||
            url.searchParams.get('experimental_live_sse') === 'true'))
      let handle = url.searchParams.get('handle')
      let entry = handle ? owners.get(handle) : undefined
      if (handle && !entry) return refetch(authorized, identity, request.method)
      if (entry && entry.identity !== identity)
        return Response.json(
          { error: 'Shape definition or authorization changed' },
          { status: 403, headers: cors }
        )
      if (request.method === 'DELETE') {
        if (handle) remove(handle)
        return new Response(null, { status: 204, headers: cors })
      }
      if (!entry) {
        if (request.method === 'HEAD')
          return new Response(null, { status: 404, headers: cors })
        const offset = url.searchParams.get('offset') ?? '-1'
        if (offset !== '-1' && offset !== 'now')
          return refetch(authorized, identity, request.method)
        entry = create(authorized, identity, offset !== 'now')
      }
      const owner = entry.owner
      try {
        await owner.settled()
      } catch (error) {
        return failedOwner(owner, error, authorized, identity, request.method)
      }
      const offset = url.searchParams.get('offset') ?? '-1'
      // A new `now` request must first observe initialization and the real
      // source baseline barrier, without exposing historical rows. Existing
      // handles can hydrate directly at the captured, immutable tail boundary.
      const readOffset =
        offset === 'now' ? (handle ? owner.log.head.offset : '-1') : offset
      let read
      try {
        read =
          request.method === 'HEAD'
            ? {
                ...owner.log.inspect(readOffset),
                end: owner.log.head,
                atTail: true,
                envelopes: [],
              }
            : await owner.log.read(
                readOffset,
                options.maxEnvelopesPerRead,
                request.signal
              )
      } catch (error) {
        request.signal.throwIfAborted()
        if (!(error instanceof InvalidLogOffsetError)) {
          owner.close(error instanceof Error ? error : new Error(String(error)))
          return failedOwner(owner, error, authorized, identity, request.method)
        }
        return Response.json(
          { error: 'Invalid shape offset' },
          { status: 400, headers: cors }
        )
      }
      if (
        request.method === 'GET' &&
        !isSse &&
        read.envelopes.length === 0 &&
        ['true', 'long-poll'].includes(url.searchParams.get('live') ?? '')
      ) {
        await owner.log.wait(
          readOffset,
          request.signal,
          Math.min(
            options.longPollMs ?? 20_000,
            Math.max(1, owner.expiresAt - now())
          )
        )
        try {
          await owner.settled()
        } catch (error) {
          return failedOwner(owner, error, authorized, identity, request.method)
        }
        try {
          read = await owner.log.read(
            readOffset,
            options.maxEnvelopesPerRead,
            request.signal
          )
        } catch (error) {
          request.signal.throwIfAborted()
          owner.close(error instanceof Error ? error : new Error(String(error)))
          return failedOwner(owner, error, authorized, identity, request.method)
        }
      }
      try {
        owner.assertValid()
      } catch (error) {
        return failedOwner(owner, error, authorized, identity, request.method)
      }
      const end = request.method === 'HEAD' ? owner.log.head : read.end
      const headers = new Headers({
        ...cors,
        'content-type': 'application/json',
        'electric-handle': owner.handle,
        'electric-offset': end.offset,
        'Stream-Next-Offset': end.offset,
        'Stream-Expires-At': new Date(owner.expiresAt).toISOString(),
        'electric-cursor': encodeReadProof({
          version: 1,
          offset,
          afterSeq: read.boundary.afterSeq,
          baselineComplete: read.boundary.baselineComplete,
        }),
      })
      if (owner.schema)
        headers.set('electric-schema', JSON.stringify(owner.schema))
      if (read.atTail) {
        headers.set('Stream-Up-To-Date', 'true')
        headers.set('electric-up-to-date', 'true')
      }
      if (request.method === 'HEAD')
        return new Response(null, { status: 200, headers })
      if (isSse) {
        // The facade enters SSE after its eager baseline. Headers carry the
        // immutable schema for every subsequent data/control pair.
        if (!owner.schema)
          return Response.json(
            { error: 'Initialize the shape before opening SSE' },
            { status: 400, headers: cors }
          )
        headers.set('content-type', 'text/event-stream')
        headers.set('x-accel-buffering', 'no')
        headers.delete('Stream-Up-To-Date')
        headers.delete('electric-up-to-date')
        const stop = new AbortController()
        const signal = AbortSignal.any([request.signal, stop.signal])
        const deadline = Math.min(
          owner.expiresAt,
          now() + (options.sseConnectionMs ?? 20_000)
        )
        const encoder = new TextEncoder()
        let initial: LogRead | undefined = read
        let position = offset
        return new Response(
          new ReadableStream<Uint8Array>({
            async pull(controller) {
              try {
                signal.throwIfAborted()
                await owner.settled()
                let batch = initial
                initial = undefined
                if (!batch) {
                  batch = await owner.log.read(
                    position,
                    options.maxEnvelopesPerRead,
                    signal
                  )
                  if (!batch.envelopes.length) {
                    await owner.log.wait(
                      position,
                      signal,
                      Math.max(1, Math.min(10_000, deadline - now()))
                    )
                    await owner.settled()
                    batch = await owner.log.read(
                      position,
                      options.maxEnvelopesPerRead,
                      signal
                    )
                  }
                }
                owner.assertValid()
                const proof = encodeReadProof({
                  version: 1,
                  offset: batch.boundary.offset,
                  afterSeq: batch.boundary.afterSeq,
                  baselineComplete: batch.boundary.baselineComplete,
                })
                const control = {
                  streamNextOffset: batch.end.offset,
                  streamCursor: proof,
                  upToDate: batch.atTail,
                }
                // JSON encoding keeps embedded CR/LF in values inside one SSE
                // data line. Pair even empty data with its exact start proof.
                controller.enqueue(
                  encoder.encode(
                    `event: data\ndata: ${JSON.stringify(batch.envelopes)}\n\nevent: control\ndata: ${JSON.stringify(control)}\n\n`
                  )
                )
                position = batch.end.offset
                if (now() >= deadline) controller.close()
              } catch (error) {
                if (!signal.aborted) {
                  owner.close(
                    error instanceof Error ? error : new Error(String(error))
                  )
                  if (!(error instanceof SourceQueryRejectedError))
                    remove(owner.handle)
                  // A complete EOF sends the native reader back through HTTP,
                  // where the invalidated handle receives an explicit refetch.
                  controller.close()
                } else {
                  controller.error(error)
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
      return new Response(
        read.envelopes.length ? JSON.stringify(read.envelopes) : null,
        { status: read.envelopes.length ? 200 : 204, headers }
      )
    },
    close() {
      closed = true
      for (const handle of [...owners.keys()]) remove(handle)
      options.source.close()
    },
    async drain(): Promise<void> {
      await Promise.all([...pendingCleanup])
      if (cleanupError) throw cleanupError
    },
    get activeShapes() {
      return owners.size
    },
    get pendingObjectCleanups() {
      return pendingCleanup.size
    },
    /** In-process diagnostics for the local harness; never served over HTTP. */
    get diagnostics() {
      return [...owners.values()].map(({ owner }) => ({
        handle: owner.handle,
        expiresAt: owner.expiresAt,
        retainedRows: owner.retainedRowCount,
        ...owner.log.head,
        ...(owner.log instanceof ObjectShapeLog
          ? {
              residentLogBytes: owner.log.residentBytes,
              sealedRanges: owner.log.sealedRanges,
            }
          : {}),
      }))
    },
  }
}
