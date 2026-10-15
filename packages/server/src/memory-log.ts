import { IndexedShapeLog, type LogRead } from './shape-log.js'
export type { LogBoundary, LogRead } from './shape-log.js'

/** All record bytes remain resident; the shared index owns offset semantics. */
export class MemoryShapeLog extends IndexedShapeLog {
  read(offset: string, limit?: number): LogRead {
    const { entries, index: _, ...metadata } = this.select(offset, limit)
    return {
      ...metadata,
      envelopes: entries.map((entry) => JSON.parse(entry.encoded!)),
    }
  }
}
