import { createHash } from 'node:crypto'
import { parseEnvelope, type Envelope } from '@neon/realtime-electric-protocol'
import {
  IndexedShapeLog,
  LogStorageError,
  type LogBoundary,
  type LogRead,
} from './shape-log.js'

/** Small injectable boundary, implemented by the real S3 adapter and fault fixtures. */
export interface LogObjectStore {
  put(key: string, bytes: Uint8Array, signal?: AbortSignal): Promise<void>
  get(key: string, signal?: AbortSignal): Promise<Uint8Array>
  delete(key: string): Promise<void>
}
export interface ObjectLogOptions {
  store: LogObjectStore
  /** Dedicated prefix for this handle, including its absolute expiry. */
  prefix: string
  sealBytes?: number
  maxResidentBytes?: number
}
interface SealedRange {
  key: string
  checksum: string
  start: number
  count: number
}

/** Immutable sealed JSON ranges plus a resident tail; the index never moves. */
export class ObjectShapeLog extends IndexedShapeLog {
  readonly #ranges: SealedRange[] = []
  readonly #attemptedKeys = new Set<string>()
  readonly #abort = new AbortController()
  readonly #sealBytes: number
  readonly #maxResidentBytes: number
  #residentBytes = 0
  #sealedCount = 0
  #scheduled = false
  #stopped = false
  #pending: Promise<void> = Promise.resolve()
  #disposal: Promise<void> = Promise.resolve()
  #uploadError?: Error

  constructor(private readonly options: ObjectLogOptions) {
    super()
    this.#sealBytes = options.sealBytes ?? 256 * 1024
    this.#maxResidentBytes = options.maxResidentBytes ?? 8 * 1024 * 1024
    if (!options.prefix || options.prefix.endsWith('/'))
      throw new Error(
        'Object log needs a nonempty handle prefix without a trailing slash'
      )
    if (
      !Number.isSafeInteger(this.#sealBytes) ||
      this.#sealBytes < 1 ||
      !Number.isSafeInteger(this.#maxResidentBytes) ||
      this.#maxResidentBytes < this.#sealBytes
    )
      throw new Error('Invalid object log byte limits')
  }
  get residentBytes(): number {
    return this.#residentBytes
  }
  get sealedRanges(): number {
    return this.#ranges.length
  }
  get uploadError(): Error | undefined {
    return this.#uploadError
  }
  /** Await cleanup to observe deletion failures; close also observes its rejection. */
  get disposed(): Promise<void> {
    return this.#disposal
  }

  override append(value: Envelope): LogBoundary {
    const bytes = Buffer.byteLength(JSON.stringify(value)) + 1
    if (this.#residentBytes + bytes > this.#maxResidentBytes) {
      throw new LogStorageError(
        'Object log resident byte limit exceeded; refetch required'
      )
    }
    const boundary = super.append(value)
    this.#residentBytes += bytes
    this.#schedule()
    return boundary
  }

  /** Seal everything committed at this call's boundary. Retries preserve keys/bytes. */
  flush(): Promise<void> {
    const target = this.entries.length
    const operation = this.#pending
      .catch(() => {})
      .then(() => this.#sealThrough(target))
    this.#pending = operation
    return operation
  }
  async #sealThrough(target: number): Promise<void> {
    while (!this.#stopped && this.#sealedCount < target) {
      const start = this.#sealedCount
      let end = start
      let size = 0
      while (end < target && (size < this.#sealBytes || end === start)) {
        size += this.entries[end].bytes
        end++
      }
      const bytes = Buffer.from(
        this.entries
          .slice(start, end)
          .map((e) => e.encoded! + '\n')
          .join('')
      )
      const checksum = digest(bytes)
      const key = `${this.options.prefix}/${this.entries[start].boundary.afterSeq}-${this.entries[end - 1].boundary.afterSeq}-${checksum}.ndjson`
      this.#attemptedKeys.add(key)
      try {
        await this.options.store.put(key, bytes, this.#abort.signal)
      } catch (error) {
        this.#uploadError = asError(error)
        throw error
      }
      // Publish the range descriptor before releasing resident record bytes.
      this.#ranges.push({ key, checksum, start, count: end - start })
      for (let i = start; i < end; i++) this.entries[i].encoded = undefined
      this.#sealedCount = end
      this.#residentBytes -= size
      this.#uploadError = undefined
    }
  }
  #schedule(): void {
    if (
      this.#scheduled ||
      this.#stopped ||
      this.#residentBytes < this.#sealBytes
    )
      return
    this.#scheduled = true
    void this.flush().then(
      () => {
        this.#scheduled = false
        this.#schedule()
      },
      () => {
        // Bytes remain available locally. A subsequent append or explicit flush
        // retries; the resident cap bounds growth during a storage outage.
        this.#scheduled = false
      }
    )
  }

  async read(
    offset: string,
    limit?: number,
    signal?: AbortSignal
  ): Promise<LogRead> {
    const readSignal = signal
      ? AbortSignal.any([signal, this.#abort.signal])
      : this.#abort.signal
    readSignal.throwIfAborted()
    const { entries, index, ...metadata } = this.select(offset, limit)
    // Capture resident bytes before awaiting a storage read/eviction.
    const records = entries.map((entry, i) => ({
      encoded: entry.encoded,
      index: index + i,
    }))
    const loaded = new Map<SealedRange, Envelope[]>()
    const envelopes: Envelope[] = []
    try {
      for (const record of records) {
        readSignal.throwIfAborted()
        if (record.encoded !== undefined) {
          envelopes.push(parseEnvelope(JSON.parse(record.encoded)))
          continue
        }
        const range = this.#ranges.find(
          (r) => record.index >= r.start && record.index < r.start + r.count
        )
        if (!range) throw new Error('Missing sealed range index')
        let values = loaded.get(range)
        if (!values) {
          const bytes = await this.options.store.get(range.key, readSignal)
          if (digest(bytes) !== range.checksum)
            throw new Error('Sealed range checksum mismatch')
          const text = Buffer.from(bytes).toString('utf8')
          if (!text.endsWith('\n')) throw new Error('Truncated sealed range')
          values = text
            .slice(0, -1)
            .split('\n')
            .map((line) => parseEnvelope(JSON.parse(line)))
          if (values.length !== range.count)
            throw new Error('Invalid sealed range count')
          for (let i = 0; i < values.length; i++) {
            if (
              values[i].seq !== this.entries[range.start + i].boundary.afterSeq
            )
              throw new Error('Sealed range sequence mismatch')
          }
          loaded.set(range, values)
        }
        envelopes.push(values[record.index - range.start])
      }
      readSignal.throwIfAborted()
      return { ...metadata, envelopes }
    } catch (error) {
      signal?.throwIfAborted()
      throw new LogStorageError(
        'Sealed shape range unavailable; refetch required',
        { cause: error }
      )
    }
  }
  override close(): void {
    if (this.#stopped) return
    this.#stopped = true
    this.#abort.abort(new Error('Shape log stopped'))
    super.close()
    this.#disposal = this.#pending
      .catch(() => {})
      .then(async () => {
        // Include attempted keys: a network failure may follow a successful put.
        const results = await Promise.allSettled(
          [...this.#attemptedKeys].map((key) => this.options.store.delete(key))
        )
        const errors = results
          .filter((r): r is PromiseRejectedResult => r.status === 'rejected')
          .map((r) => r.reason)
        if (errors.length)
          throw new AggregateError(errors, 'Shape object cleanup failed')
      })
    void this.#disposal.catch(() => {})
  }
}
function digest(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex')
}
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}
