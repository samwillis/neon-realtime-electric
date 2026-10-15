import {
  parseEnvelope,
  type Envelope,
  type Sequence,
} from '@neon/realtime-electric-protocol'

export interface LogBoundary {
  offset: string
  afterSeq: Sequence
  baselineComplete: boolean
}
export interface LogRead {
  boundary: LogBoundary
  end: LogBoundary
  envelopes: Envelope[]
  atTail: boolean
}
export interface ShapeLog {
  readonly head: LogBoundary
  readonly disposed?: Promise<void>
  append(value: Envelope): LogBoundary
  read(
    offset: string,
    limit?: number,
    signal?: AbortSignal
  ): LogRead | Promise<LogRead>
  wait(offset: string, signal: AbortSignal, timeoutMs: number): Promise<void>
  close(): void
}
export class InvalidLogOffsetError extends Error {}
export class LogStorageError extends Error {}

export interface LogEntry {
  boundary: LogBoundary
  /** Removed only after the storage backend has durably accepted these bytes. */
  encoded?: string
  bytes: number
}
export interface LogSelection {
  index: number
  entries: LogEntry[]
  boundary: LogBoundary
  end: LogBoundary
  atTail: boolean
}

/** One immutable offset/sequence index, shared by both persistence backends. */
export abstract class IndexedShapeLog implements ShapeLog {
  protected readonly entries: LogEntry[] = []
  readonly #index = new Map<string, number>([['-1', 0]])
  readonly #waiters = new Set<() => void>()
  #bytes = 0n
  #closed = false
  readonly initial: Readonly<LogBoundary> = Object.freeze({
    offset: '-1',
    afterSeq: '0',
    baselineComplete: false,
  })

  get head(): LogBoundary {
    return { ...(this.entries.at(-1)?.boundary ?? this.initial) }
  }
  append(value: Envelope): LogBoundary {
    if (this.#closed) throw new Error('Shape log is closed')
    const envelope = parseEnvelope(value)
    if (BigInt(envelope.seq) !== BigInt(this.head.afterSeq) + 1n)
      throw new Error('Shape append sequence is not contiguous')
    if (
      (envelope.kind === 'progress' || envelope.kind === 'snapshot-complete') &&
      BigInt(envelope.coveredSeq) > BigInt(this.head.afterSeq)
    )
      throw new Error('Shape evidence precedes covered data')
    const encoded = JSON.stringify(envelope)
    const bytes = Buffer.byteLength(encoded) + 1
    this.#bytes += BigInt(bytes)
    const boundary: LogBoundary = {
      offset: `0000000000000000_${String(this.#bytes).padStart(16, '0')}`,
      afterSeq: envelope.seq,
      baselineComplete:
        this.head.baselineComplete || envelope.kind === 'snapshot-complete',
    }
    this.entries.push({ encoded, bytes, boundary })
    this.#index.set(boundary.offset, this.entries.length)
    this.#wake()
    return { ...boundary }
  }
  protected select(offset: string, limit = 128): LogSelection {
    if (!Number.isInteger(limit) || limit < 1)
      throw new Error('Read limit must be a positive integer')
    const index = this.#index.get(offset)
    if (index === undefined)
      throw new InvalidLogOffsetError('Invalid shape offset')
    const entries = this.entries.slice(index, index + limit)
    const boundary = index ? this.entries[index - 1].boundary : this.initial
    return {
      index,
      entries,
      boundary: { ...boundary },
      end: { ...(entries.at(-1)?.boundary ?? boundary) },
      atTail: index + entries.length === this.entries.length,
    }
  }
  abstract read(
    offset: string,
    limit?: number,
    signal?: AbortSignal
  ): LogRead | Promise<LogRead>
  async wait(
    offset: string,
    signal: AbortSignal,
    timeoutMs: number
  ): Promise<void> {
    signal.throwIfAborted()
    if (this.head.offset !== offset || this.#closed) return
    await new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout>
      const finish = (error?: unknown) => {
        clearTimeout(timer)
        signal.removeEventListener('abort', abort)
        this.#waiters.delete(done)
        if (error !== undefined) reject(error)
        else resolve()
      }
      const done = () => finish()
      const abort = () => finish(signal.reason ?? new Error('Read aborted'))
      this.#waiters.add(done)
      signal.addEventListener('abort', abort, { once: true })
      timer = setTimeout(done, timeoutMs)
      if (signal.aborted) abort()
      else if (this.head.offset !== offset || this.#closed) done()
    })
  }
  close(): void {
    this.#closed = true
    this.#wake()
  }
  #wake(): void {
    for (const wake of [...this.#waiters]) wake()
  }
}
