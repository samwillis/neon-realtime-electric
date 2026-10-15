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
export interface LogRead<T = Envelope> {
  boundary: LogBoundary
  end: LogBoundary
  envelopes: T[]
  atTail: boolean
}
export interface JsonLog<T> {
  readonly head: LogBoundary
  readonly disposed?: Promise<void>
  append(value: T): LogBoundary
  inspect(offset: string, limit?: number): Omit<LogRead<T>, 'envelopes'>
  read(
    offset: string,
    limit?: number,
    signal?: AbortSignal
  ): LogRead<T> | Promise<LogRead<T>>
  wait(offset: string, signal: AbortSignal, timeoutMs: number): Promise<void>
  close(): void
}
export type ShapeLog = JsonLog<Envelope>

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

/** Validation and shape evidence, independent of storage and HTTP framing. */
export interface LogCodec<T> {
  parse(value: unknown): T
  advance(value: T, previous: LogBoundary): Omit<LogBoundary, 'offset'>
  validateRead(value: T, boundary: LogBoundary): void
}

export const shapeLogCodec: LogCodec<Envelope> = {
  parse: parseEnvelope,
  advance(envelope, previous) {
    if (BigInt(envelope.seq) !== BigInt(previous.afterSeq) + 1n)
      throw new Error('Shape append sequence is not contiguous')
    if (
      (envelope.kind === 'progress' || envelope.kind === 'snapshot-complete') &&
      BigInt(envelope.coveredSeq) > BigInt(previous.afterSeq)
    )
      throw new Error('Shape evidence precedes covered data')
    return {
      afterSeq: envelope.seq,
      baselineComplete:
        previous.baselineComplete || envelope.kind === 'snapshot-complete',
    }
  },
  validateRead(value, boundary) {
    if (value.seq !== boundary.afterSeq)
      throw new Error('Sealed range sequence mismatch')
  },
}

/** Shared byte index and waiters. The codec adds shape sequence/evidence checks. */
export abstract class IndexedJsonLog<T> implements JsonLog<T> {
  constructor(protected readonly codec: LogCodec<T>) {}
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
  append(value: T): LogBoundary {
    if (this.#closed) throw new Error('Shape log is closed')
    const parsed = this.codec.parse(value)
    const metadata = this.codec.advance(parsed, this.head)
    const encoded = JSON.stringify(parsed)
    const bytes = Buffer.byteLength(encoded) + 1
    this.#bytes += BigInt(bytes)
    const boundary: LogBoundary = {
      offset: `0000000000000000_${String(this.#bytes).padStart(16, '0')}`,
      ...metadata,
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
  inspect(offset: string, limit?: number): Omit<LogRead<T>, 'envelopes'> {
    const { entries: _, index: __, ...metadata } = this.select(offset, limit)
    return metadata
  }
  abstract read(
    offset: string,
    limit?: number,
    signal?: AbortSignal
  ): LogRead<T> | Promise<LogRead<T>>
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

/** Shape-specialized index retained for existing custom storage implementations. */
export abstract class IndexedShapeLog extends IndexedJsonLog<Envelope> {
  constructor() {
    super(shapeLogCodec)
  }
}
