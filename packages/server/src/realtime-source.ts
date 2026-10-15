import {
  createRealtime,
  rawSql,
  type RawSqlParameter,
} from '@neon/realtime/server'
import type {
  RawLiveQuerySubscription,
  RealtimeSourceEvent,
  RealtimeLogLevel,
  RealtimeLogger,
} from '@neon/realtime/client'

/** A permanent backend query rejection; retrying the same shape cannot recover. */
export class SourceQueryRejectedError extends Error {
  constructor() {
    super('Realtime rejected the shape query')
    this.name = 'SourceQueryRejectedError'
  }
}

/** Raw events from the pinned SDK's reconciled, ordered source callback. */
export type SourceEvent = RealtimeSourceEvent

export interface SourceQuery {
  sql: string
  parameters?: readonly RawSqlParameter[]
}

export interface SourceObserver {
  /** Synchronous delivery; the shape owner serializes persistence separately. */
  event(event: SourceEvent): void
  /** Terminal for this handle; never silently replace its baseline. */
  error(error: Error): void
}

export interface SourceSubscription {
  close(): void
}

export interface RealtimeSource {
  subscribe(
    query: SourceQuery,
    observer: SourceObserver,
    signal: AbortSignal
  ): Promise<SourceSubscription>
  close(): void
}

export interface NeonRealtimeSourceOptions {
  url: string
  secret: string
  db: string
  logLevel?: RealtimeLogLevel
  logger?: RealtimeLogger
}

/** Server-only boundary. Never expose a sealed query or project secret in a shape. */
export function createNeonRealtimeSource(
  options: NeonRealtimeSourceOptions
): RealtimeSource {
  const sdk = createRealtime(options)
  const active = new Set<SourceSubscription>()
  let disposed = false
  return {
    async subscribe(query, observer, signal) {
      if (disposed) throw new Error('Realtime source is closed')
      signal.throwIfAborted()
      const subscription = await sdk.subscribe(
        rawSql(query.sql, query.parameters),
        { materialize: false, rawValues: true }
      )
      let closed = false
      let baselineSeen = false
      const unlisten: (() => void)[] = []
      const control: SourceSubscription = {
        close() {
          if (closed) return
          closed = true
          active.delete(control)
          signal.removeEventListener('abort', control.close)
          for (const remove of unlisten) remove()
          subscription.unsubscribe()
        },
      }
      const fail = (error: Error) => {
        if (closed) return
        control.close()
        observer.error(error)
      }
      const deliver = (event: SourceEvent) => {
        if (closed) return
        // A handle has one source epoch. SDK reconnect/reset invalidates it.
        if (
          event.type === 'reset' ||
          (event.type === 'baseline' && baselineSeen)
        ) {
          fail(new Error('Realtime source continuity lost; refetch required'))
          return
        }
        if (event.type === 'baseline') baselineSeen = true
        try {
          observer.event(event)
        } catch (error) {
          fail(error instanceof Error ? error : new Error(String(error)))
        }
      }
      try {
        unlisten.push(subscription.onSource(deliver))
        unlisten.push(
          subscription.onStateChange(() => checkState(subscription, fail))
        )
      } catch (error) {
        control.close()
        throw error
      }
      active.add(control)
      signal.addEventListener('abort', control.close, { once: true })
      if (disposed || signal.aborted) control.close()
      else checkState(subscription, fail)
      return control
    },
    close() {
      if (disposed) return
      disposed = true
      for (const subscription of [...active]) subscription.close()
      sdk.close()
    },
  }
}

function checkState(
  subscription: RawLiveQuerySubscription<unknown>,
  fail: (error: Error) => void
): void {
  const state = subscription.getState()
  if (state.status === 'error')
    fail(
      state.error.code === 'upstream_rejected' && !state.error.retryable
        ? new SourceQueryRejectedError()
        : state.error
    )
  else if (state.status === 'stale' || state.status === 'closed') {
    fail(new Error(`Realtime source became ${state.status}; refetch required`))
  }
}
