import type { Message, Offset, Row, Schema, SnapshotMetadata } from '../types'

/** Internal transport contract; intentionally absent from public exports. */
export interface ResponseMetadata {
  status: number
  handle: string | null
  offset: Offset | null
  cursor: string | null
  schema?: Schema
  errorHeaders: Record<string, string>
  cancel(): Promise<void>
}

export interface ReadRequest<T extends Row<unknown>> {
  url: URL
  headers: Record<string, string>
  signal: AbortSignal
  mode: 'long-poll' | 'sse'
  /** Reevaluate dynamic authentication/parameters for native catch-up reads. */
  resolveRequest(): Promise<{ url: URL; headers: Record<string, string> }>
  withTimeout<U>(promise: Promise<U>): Promise<U>
  onMetadata(metadata: ResponseMetadata): Promise<boolean>
  /** Persist a native reader's fallback until this shape epoch resets. */
  onSseFallback?(): void
  decode(
    json: string,
    metadata: ResponseMetadata,
    framing: 'array' | 'message'
  ): Message<T>[]
  onMessages(messages: Message<T>[], isSse: boolean): Promise<void>
}

export interface SnapshotRequest {
  url: URL
  init: RequestInit
  schema?: Schema
}

export interface SnapshotResponse {
  metadata: SnapshotMetadata
  rawData: unknown[]
  response: ResponseMetadata
}

export interface ShapeTransport<T extends Row<unknown>> {
  read(request: ReadRequest<T>): Promise<void>
  fetchSnapshot(request: SnapshotRequest): Promise<SnapshotResponse>
}
