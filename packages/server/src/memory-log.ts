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

/**
 * Append-only JSON records. Offsets count committed UTF-8 record bytes, not HTTP
 * framing bytes. Read boundaries and logical sequences are committed together.
 */
export class MemoryShapeLog {
  readonly #entries: { envelope: Envelope; boundary: LogBoundary }[] = []
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
    return { ...(this.#entries.at(-1)?.boundary ?? this.initial) }
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
    this.#bytes += BigInt(Buffer.byteLength(encoded) + 1)
    const boundary: LogBoundary = {
      offset: `0000000000000000_${String(this.#bytes).padStart(16, '0')}`,
      afterSeq: envelope.seq,
      baselineComplete:
        this.head.baselineComplete || envelope.kind === 'snapshot-complete',
    }
    // Own the immutable committed value; caller mutation cannot alter replay.
    this.#entries.push({ envelope: JSON.parse(encoded), boundary })
    this.#index.set(boundary.offset, this.#entries.length)
    this.#wake()
    return { ...boundary }
  }

  read(offset: string, limit = 128): LogRead {
    if (!Number.isInteger(limit) || limit < 1)
      throw new Error('Read limit must be a positive integer')
    const index = this.#index.get(offset)
    if (index === undefined) throw new Error('Invalid shape offset')
    const entries = this.#entries.slice(index, index + limit)
    const boundary = index ? this.#entries[index - 1].boundary : this.initial
    return {
      boundary: { ...boundary },
      end: { ...(entries.at(-1)?.boundary ?? boundary) },
      envelopes: entries.map((entry) => structuredClone(entry.envelope)),
      atTail: index + entries.length === this.#entries.length,
    }
  }

  /** Race-free registration: recheck the head after installing the waiter. */
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
