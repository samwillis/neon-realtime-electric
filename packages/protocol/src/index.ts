/** Browser-safe wire contract. No parsed client rows or server SDK types. */
export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json }
export type RawRow = { [column: string]: Json }
export type Sequence = `${bigint}`

export interface SnapshotEvidence {
  xmin: Sequence
  xmax: Sequence
  xip_list: Sequence[]
}

export interface Evidence {
  /** Full, unwrapped transaction IDs from the source, never JS numbers. */
  txids?: Sequence[]
  /** Decimal PostgreSQL WAL position, independent of any transport offset. */
  lsn?: Sequence
  snapshot?: SnapshotEvidence
}

export interface RawChange {
  key: string
  value: RawRow
  old_value?: RawRow
  headers: { operation: 'insert' | 'update' | 'delete'; [key: string]: Json }
}

export interface RawMove {
  headers: {
    event: 'move-in' | 'move-out'
    patterns: { pos: number; value: string }[]
    [key: string]: Json
  }
}

interface Base {
  version: 1
  seq: Sequence
}

export type Envelope =
  | (Base & { kind: 'initializing'; handle: string; expiresAt: string })
  | (Base & { kind: 'snapshot-chunk'; changes: RawChange[] })
  | (Base & {
      kind: 'snapshot-complete'
      coveredSeq: Sequence
      evidence: Evidence & { snapshot: SnapshotEvidence }
    })
  | (Base & {
      kind: 'publication'
      changes: (RawChange | RawMove)[]
      evidence: Evidence
    })
  | (Base & { kind: 'progress'; coveredSeq: Sequence; evidence: Evidence })

export class ShapeProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ShapeProtocolError'
  }
}

export function isSequence(value: unknown): value is Sequence {
  return typeof value === 'string' && /^(0|[1-9][0-9]*)$/.test(value)
}

/** Decimal string comparison avoids number precision loss and BigInt syntax. */
export function compareSequence(a: Sequence, b: Sequence): -1 | 0 | 1 {
  if (!isSequence(a) || !isSequence(b)) fail('Invalid sequence')
  if (a.length !== b.length) return a.length < b.length ? -1 : 1
  return a === b ? 0 : a < b ? -1 : 1
}

function fail(message: string): never {
  throw new ShapeProtocolError(message)
}

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function json(value: unknown): value is Json {
  if (value === null || typeof value === 'string' || typeof value === 'boolean')
    return true
  if (typeof value === 'number') return Number.isFinite(value)
  if (Array.isArray(value)) return value.every(json)
  return object(value) && Object.values(value).every(json)
}

function snapshot(value: unknown): value is SnapshotEvidence {
  return (
    object(value) &&
    isSequence(value.xmin) &&
    isSequence(value.xmax) &&
    compareSequence(value.xmin, value.xmax) <= 0 &&
    Array.isArray(value.xip_list) &&
    value.xip_list.every(
      (id) =>
        isSequence(id) &&
        compareSequence(id, value.xmin as Sequence) >= 0 &&
        compareSequence(id, value.xmax as Sequence) < 0
    )
  )
}

function evidence(value: unknown): value is Evidence {
  return (
    object(value) &&
    (value.txids === undefined ||
      (Array.isArray(value.txids) && value.txids.every(isSequence))) &&
    (value.lsn === undefined || isSequence(value.lsn)) &&
    (value.snapshot === undefined || snapshot(value.snapshot))
  )
}

function change(value: unknown): value is RawChange {
  return (
    object(value) &&
    typeof value.key === 'string' &&
    object(value.value) &&
    json(value.value) &&
    (value.old_value === undefined ||
      (object(value.old_value) && json(value.old_value))) &&
    object(value.headers) &&
    ['insert', 'update', 'delete'].includes(
      value.headers.operation as string
    ) &&
    value.headers.control === undefined &&
    value.headers.event === undefined &&
    json(value.headers)
  )
}

function move(value: unknown): value is RawMove {
  if (!object(value) || !object(value.headers)) return false
  const h = value.headers
  return (
    (h.event === 'move-in' || h.event === 'move-out') &&
    h.control === undefined &&
    h.operation === undefined &&
    Array.isArray(h.patterns) &&
    h.patterns.every(
      (p) =>
        object(p) &&
        Number.isSafeInteger(p.pos) &&
        (p.pos as number) >= 0 &&
        typeof p.value === 'string'
    ) &&
    json(h)
  )
}

/** Validate untrusted JSON before it reaches the parser or materializer. */
export function parseEnvelope(value: unknown): Envelope {
  if (
    !object(value) ||
    value.version !== 1 ||
    !isSequence(value.seq) ||
    value.seq === '0'
  ) {
    fail('Unsupported shape envelope version or sequence')
  }
  switch (value.kind) {
    case 'initializing':
      if (
        typeof value.handle !== 'string' ||
        !value.handle ||
        typeof value.expiresAt !== 'string' ||
        !Number.isFinite(Date.parse(value.expiresAt))
      ) {
        fail('Invalid initialization metadata')
      }
      break
    case 'snapshot-chunk':
      if (
        !Array.isArray(value.changes) ||
        !value.changes.every(
          (v) => change(v) && v.headers.operation === 'insert'
        )
      ) {
        fail('Invalid snapshot chunk')
      }
      break
    case 'publication':
      if (
        !Array.isArray(value.changes) ||
        !value.changes.every((v) => change(v) || move(v)) ||
        !evidence(value.evidence)
      ) {
        fail('Invalid publication')
      }
      break
    case 'snapshot-complete':
    case 'progress':
      if (
        !isSequence(value.coveredSeq) ||
        compareSequence(value.coveredSeq, value.seq) >= 0 ||
        !evidence(value.evidence) ||
        (value.kind === 'snapshot-complete' &&
          !snapshot(value.evidence.snapshot))
      ) {
        fail('Invalid ordered evidence')
      }
      break
    default:
      fail('Unknown shape envelope kind')
  }
  return value as unknown as Envelope
}
