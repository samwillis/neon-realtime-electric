import type { BackoffOptions } from '../src/fetch'
import { createFetchWithBackoff } from '../src/fetch'
import type { Row } from '../src/types'
import { ElectricHttpTransport } from '../src/transport/electric-http'
import type { ShapeTransport } from '../src/transport/types'

/** Only the historical HTTP reference suite selects this adapter. */
export function createShapeTransport<T extends Row<unknown>>(
  fetchClient: typeof fetch,
  backoff: BackoffOptions
): ShapeTransport<T> {
  return new ElectricHttpTransport<T>(
    createFetchWithBackoff(fetchClient, backoff)
  )
}
