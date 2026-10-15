import { randomUUID } from 'node:crypto'
import type {
  Envelope,
  RawChange,
  RawRow,
  Sequence,
  SnapshotEvidence,
} from '@neon/realtime-electric-protocol'
import type {
  RealtimeSource,
  SourceEvent,
  SourceQuery,
} from './realtime-source.js'
import type { ShapeLog } from './shape-log.js'
import { MemoryShapeLog } from './memory-log.js'

export interface OwnerDefinition {
  /** Prepared by the authorized query-definition layer, never interpolated here. */
  query: SourceQuery
  namespace: string
  table: string
  primaryKey: readonly string[]
  /** Public projection. The source query also includes hidden primary-key columns. */
  projection: readonly string[]
  schema: Record<string, { type: string; [key: string]: unknown }>
}

export interface ShapeOwnerOptions {
  source: RealtimeSource
  definition: OwnerDefinition
  now?: () => number
  ttlMs?: number
  onClose?: (event: { handle: string; error: Error }) => void
  createLog?: (context: { handle: string; expiresAt: number }) => ShapeLog
}

/** A handle has exactly one source epoch and an absolute, bounded lifetime. */
export class ShapeOwner {
  readonly handle = randomUUID()
  readonly log: ShapeLog
  readonly expiresAt: number
  readonly definition: OwnerDefinition
  readonly #abort = new AbortController()
  readonly #now: () => number
  readonly #expiry: ReturnType<typeof setTimeout>
  readonly #rows = new Map<string, RawRow>()
  #columns?: Extract<SourceEvent, { type: 'schema' }>['columns']
  #baselineComplete = false
  #error?: Error
  #seq = 0n
  #queue: Promise<void> = Promise.resolve()
  #start?: Promise<void>

  constructor(private readonly options: ShapeOwnerOptions) {
    this.definition = options.definition
    this.#now = options.now ?? Date.now
    const ttl = options.ttlMs ?? 900_000
    if (!Number.isFinite(ttl) || ttl <= 0 || ttl > 900_000)
      throw new Error('Shape TTL must be positive and at most 15 minutes')
    this.expiresAt = this.#now() + ttl
    this.log =
      options.createLog?.({ handle: this.handle, expiresAt: this.expiresAt }) ??
      new MemoryShapeLog()
    this.#append({
      kind: 'initializing',
      handle: this.handle,
      expiresAt: new Date(this.expiresAt).toISOString(),
    })
    this.#expiry = setTimeout(() => this.close(new Error('Shape expired')), ttl)
    this.#expiry.unref?.()
  }

  /** Does not wait for a baseline; first HTTP response can return immediately. */
  start(): Promise<void> {
    return (this.#start ??= this.options.source
      .subscribe(
        this.definition.query,
        {
          event: (event) => {
            const next = this.#queue.then(() => {
              this.assertValid()
              this.#accept(event)
            })
            // Register failure immediately. Readers always observe a terminal
            // handle rather than an unhandled rejection or an incomplete log.
            this.#queue = next.catch((error) => this.close(asError(error)))
          },
          error: (error) => this.close(error),
        },
        this.#abort.signal
      )
      .then(() => {})
      .catch((error) => this.close(asError(error))))
  }

  async settled(): Promise<void> {
    await this.#queue
    this.assertValid()
  }

  get schema(): Record<string, unknown> | undefined {
    return this.#columns ? this.definition.schema : undefined
  }

  assertValid(): void {
    if (this.#now() >= this.expiresAt) this.close(new Error('Shape expired'))
    if (this.#error) throw this.#error
  }

  close(error = new Error('Shape owner stopped')): void {
    if (this.#error) return
    this.#error = error
    clearTimeout(this.#expiry)
    this.#abort.abort(error)
    this.log.close()
    this.#rows.clear()
    this.options.onClose?.({ handle: this.handle, error })
  }

  #accept(event: SourceEvent): void {
    switch (event.type) {
      case 'schema': {
        if (this.#columns) throw new Error('Shape schema replaced')
        this.#columns = event.columns
        const names = new Set(event.columns.map((c) => c.name))
        if (names.size !== event.columns.length)
          throw new Error('Duplicate source column name')
        for (const name of [
          ...this.definition.primaryKey,
          ...this.definition.projection,
        ]) {
          if (!names.has(name))
            throw new Error(`Source column missing: ${name}`)
        }
        return
      }
      case 'baseline': {
        if (this.#baselineComplete) throw new Error('Replacement baseline')
        const changes: RawChange[] = []
        for (const wire of event.rows) {
          const row = this.#decode(wire.values)
          this.#rows.set(wire.row_key, row)
          changes.push(this.#change('insert', row))
          if (changes.length === 128)
            this.#append({ kind: 'snapshot-chunk', changes: changes.splice(0) })
        }
        if (changes.length) this.#append({ kind: 'snapshot-chunk', changes })
        this.#append({
          kind: 'snapshot-complete',
          coveredSeq: String(this.#seq) as Sequence,
          evidence: { snapshot: snapshot(event.mvcc) },
        })
        this.#baselineComplete = true
        return
      }
      case 'publication': {
        if (!this.#baselineComplete)
          throw new Error('Publication before baseline')
        const changes: RawChange[] = []
        for (const wire of event.changes) {
          const before = this.#rows.get(wire.row_key)
          if (wire.op === 'remove') {
            if (!before) throw new Error('Source removed an unknown row')
            this.#rows.delete(wire.row_key)
            changes.push(this.#change('delete', before))
          } else {
            const row = this.#decode(wire.values)
            // A primary-key change can retain the opaque source identity. Emit
            // remove+insert so Shape materialization never retains the old key.
            if (before && this.#key(before) !== this.#key(row)) {
              changes.push(this.#change('delete', before))
              changes.push(this.#change('insert', row))
            } else
              changes.push(
                this.#change(before ? 'update' : 'insert', row, before)
              )
            this.#rows.set(wire.row_key, row)
          }
        }
        this.#append({
          kind: 'publication',
          changes,
          evidence: {
            txids: event.txids.map(sequence),
            lsn: lsnToDecimal(event.frontier.lsn),
          },
        })
        return
      }
      case 'progress':
        if (!this.#baselineComplete) throw new Error('Progress before baseline')
        this.#append({
          kind: 'progress',
          coveredSeq: String(this.#seq) as Sequence,
          evidence: { snapshot: snapshot(event.mvcc) },
        })
        return
      case 'reset':
        throw new Error('Source reset; refetch required')
    }
  }

  #decode(values: readonly (string | null | { base64: string })[]): RawRow {
    if (!this.#columns || values.length !== this.#columns.length)
      throw new Error('Invalid source row schema')
    return Object.fromEntries(
      this.#columns.map((column, index) => {
        const value = values[index]
        if (value !== null && typeof value === 'object') {
          if (column.type_oid !== 17 || column.codec !== 'bytes')
            throw new Error('Unsupported binary source value')
          return [
            column.name,
            `\\x${Buffer.from(value.base64, 'base64').toString('hex')}`,
          ]
        }
        return [column.name, value]
      })
    )
  }

  #key(row: RawRow): string {
    const quote = (part: string) => `"${part.replaceAll('"', '""')}"`
    return `${quote(this.definition.namespace)}.${quote(this.definition.table)}/${this.definition.primaryKey
      .map((name) => {
        const value = row[name]
        if (value === null || value === undefined || typeof value === 'object')
          throw new Error('Missing scalar primary key')
        return quote(String(value))
      })
      .join('/')}`
  }

  #project(row: RawRow): RawRow {
    return Object.fromEntries(
      this.definition.projection.map((name) => [name, row[name]])
    )
  }

  #change(
    operation: 'insert' | 'update' | 'delete',
    row: RawRow,
    before?: RawRow
  ): RawChange {
    return {
      key: this.#key(row),
      value: this.#project(row),
      ...(before ? { old_value: this.#project(before) } : {}),
      headers: {
        operation,
        relation: [this.definition.namespace, this.definition.table],
      },
    }
  }

  #append(value: EnvelopeInput): void {
    this.#seq += 1n
    this.log.append({
      ...value,
      version: 1,
      seq: String(this.#seq) as Sequence,
    } as Envelope)
  }
}

type EnvelopeInput = Envelope extends infer E
  ? E extends Envelope
    ? Omit<E, 'version' | 'seq'>
    : never
  : never
function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value))
}
function sequence(value: string): Sequence {
  if (!/^(0|[1-9][0-9]*)$/.test(value))
    throw new Error('Invalid source integer')
  return value as Sequence
}
function snapshot(value: {
  xmin: string
  xmax: string
  xip: readonly string[]
}): SnapshotEvidence {
  return {
    xmin: sequence(value.xmin),
    xmax: sequence(value.xmax),
    xip_list: value.xip.map(sequence),
  }
}
export function lsnToDecimal(lsn: string): Sequence {
  if (!/^[0-9A-F]{1,8}\/[0-9A-F]{1,8}$/.test(lsn))
    throw new Error('Invalid PostgreSQL LSN')
  const [high, low] = lsn.split('/').map((part) => BigInt(`0x${part}`))
  return String((high << 32n) | low) as Sequence
}
