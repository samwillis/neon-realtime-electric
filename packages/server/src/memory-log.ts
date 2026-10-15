import type { Envelope } from '@neon/realtime-electric-protocol'
import { IndexedJsonLog, shapeLogCodec, type LogRead } from './shape-log.js'
export type { LogBoundary, LogRead } from './shape-log.js'

/** All record bytes remain resident; the shared index owns offset semantics. */
export class MemoryJsonLog<T> extends IndexedJsonLog<T> {
  read(offset: string, limit?: number): LogRead<T> {
    const { entries, index: _, ...metadata } = this.select(offset, limit)
    return {
      ...metadata,
      envelopes: entries.map((entry) => {
        const value = this.codec.parse(JSON.parse(entry.encoded!))
        this.codec.validateRead(value, entry.boundary)
        return value
      }),
    }
  }
}
export class MemoryShapeLog extends MemoryJsonLog<Envelope> {
  constructor() {
    super(shapeLogCodec)
  }
}
