import { InvalidLogOffsetError } from './shape-log.js'
import { ShapeDefinitionError } from './definition.js'
import { createHash } from 'node:crypto'
import { encodeReadProof } from '@neon/realtime-electric-protocol/read-proof'
import type { RealtimeSource } from './realtime-source.js'
import { ShapeOwner, type OwnerDefinition } from './shape-owner.js'

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
  createLog?: import('./shape-owner.js').ShapeOwnerOptions['createLog']
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
  const create = (authorized: AuthorizedShape, identity: string) => {
    const owner = new ShapeOwner({
      source: options.source,
      definition: authorized.definition,
      now,
      ttlMs: options.ttlMs,
      createLog: options.createLog,
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
      if (url.searchParams.get('live') === 'sse')
        return Response.json(
          { error: 'SSE is not enabled for this shape profile' },
          { status: 400, headers: cors }
        )
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
        if (offset !== '-1')
          return refetch(authorized, identity, request.method)
        entry = create(authorized, identity)
      }
      const owner = entry.owner
      try {
        await owner.settled()
      } catch {
        remove(owner.handle)
        return refetch(authorized, identity, request.method)
      }
      const offset = url.searchParams.get('offset') ?? '-1'
      let read
      try {
        read = await owner.log.read(
          offset,
          options.maxEnvelopesPerRead,
          request.signal
        )
      } catch (error) {
        request.signal.throwIfAborted()
        if (!(error instanceof InvalidLogOffsetError)) {
          owner.close(error instanceof Error ? error : new Error(String(error)))
          remove(owner.handle)
          return refetch(authorized, identity, request.method)
        }
        return Response.json(
          { error: 'Invalid shape offset' },
          { status: 400, headers: cors }
        )
      }
      if (
        request.method === 'GET' &&
        read.envelopes.length === 0 &&
        ['true', 'long-poll'].includes(url.searchParams.get('live') ?? '')
      ) {
        await owner.log.wait(
          offset,
          request.signal,
          Math.min(
            options.longPollMs ?? 20_000,
            Math.max(1, owner.expiresAt - now())
          )
        )
        try {
          await owner.settled()
        } catch {
          remove(owner.handle)
          return refetch(authorized, identity, request.method)
        }
        try {
          read = await owner.log.read(
            offset,
            options.maxEnvelopesPerRead,
            request.signal
          )
        } catch (error) {
          request.signal.throwIfAborted()
          owner.close(error instanceof Error ? error : new Error(String(error)))
          remove(owner.handle)
          return refetch(authorized, identity, request.method)
        }
      }
      try {
        owner.assertValid()
      } catch {
        remove(owner.handle)
        return refetch(authorized, identity, request.method)
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
  }
}
