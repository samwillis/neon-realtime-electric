import { InvalidLogOffsetError } from './shape-log.js'
import { ShapeDefinitionError } from './definition.js'
import { createHash } from 'node:crypto'
import {
  decodeReadProof,
  encodeReadProof,
} from '@neon/realtime-electric-protocol/read-proof'
import {
  parseReadRequest,
  readHeaders,
  readJsonStream,
  ReadRequestError,
} from './json-read.js'
import {
  SourceQueryRejectedError,
  type RealtimeSource,
} from './realtime-source.js'
import { ShapeOwner, type OwnerDefinition } from './shape-owner.js'
import { ObjectShapeLog } from './object-log.js'

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
    ...readHeaders,
    'access-control-expose-headers':
      'electric-handle,electric-offset,electric-cursor,electric-schema,electric-up-to-date,Stream-Next-Offset,Stream-Up-To-Date,Stream-Cursor,Stream-Expires-At',
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
      let query: ReturnType<typeof parseReadRequest>
      try {
        query = parseReadRequest(request)
      } catch (error) {
        if (!(error instanceof ReadRequestError)) throw error
        return Response.json(
          { error: error.message },
          { status: 400, headers: cors }
        )
      }
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
      let clientCursor = url.searchParams.get('cursor') ?? undefined
      // Existing Electric proxies forward the proof-bearing cursor. Recover its
      // native cache cursor while keeping sequence/cut evidence independent.
      if (clientCursor?.startsWith('neon1.')) {
        try {
          clientCursor = decodeReadProof(clientCursor).nativeCursor
        } catch {
          clientCursor = undefined
        }
      }
      if (query.isSse && !owner.schema)
        return Response.json(
          { error: 'Initialize the shape before opening SSE' },
          { status: 400, headers: cors }
        )
      try {
        return await readJsonStream(request, {
          log: owner.log,
          expiresAt: owner.expiresAt,
          now,
          readOffset,
          cursor: clientCursor,
          longPollMs: options.longPollMs,
          maxMessages: options.maxEnvelopesPerRead,
          sseConnectionMs: options.sseConnectionMs,
          headers: cors,
          settled: () => owner.settled(),
          assertValid: () => owner.assertValid(),
          decorate(read, headers, nativeCursor) {
            headers.set('electric-handle', owner.handle)
            headers.set('electric-offset', read.end.offset)
            headers.set(
              'electric-cursor',
              encodeReadProof({
                version: 1,
                offset,
                afterSeq: read.boundary.afterSeq,
                baselineComplete: read.boundary.baselineComplete,
                ...(nativeCursor ? { nativeCursor } : {}),
              })
            )
            if (owner.schema)
              headers.set('electric-schema', JSON.stringify(owner.schema))
            if (read.atTail) headers.set('electric-up-to-date', 'true')
          },
          eventCursor(batch, nativeCursor) {
            return encodeReadProof({
              version: 1,
              offset: batch.boundary.offset,
              afterSeq: batch.boundary.afterSeq,
              baselineComplete: batch.boundary.baselineComplete,
              nativeCursor,
            })
          },
          onStreamError(error) {
            owner.close(
              error instanceof Error ? error : new Error(String(error))
            )
            if (!(error instanceof SourceQueryRejectedError))
              remove(owner.handle)
          },
        })
      } catch (error) {
        request.signal.throwIfAborted()
        if (
          error instanceof InvalidLogOffsetError ||
          error instanceof ReadRequestError
        )
          return Response.json(
            { error: error.message },
            { status: 400, headers: cors }
          )
        owner.close(error instanceof Error ? error : new Error(String(error)))
        return failedOwner(owner, error, authorized, identity, request.method)
      }
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
