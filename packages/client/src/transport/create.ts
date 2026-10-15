import type { BackoffOptions } from '../fetch'
import type { Row } from '../types'
import { DurableStreamsTransport } from './durable-streams'
import type { ShapeTransport } from './types'

export function createShapeTransport<T extends Row<unknown>>(
  fetchClient: typeof fetch,
  backoff: BackoffOptions
): ShapeTransport<T> {
  return new DurableStreamsTransport<T>(fetchClient, backoff)
}
