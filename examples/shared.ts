import { FetchError, type ShapeStreamOptions } from '@electric-sql/client'

export type Item = { id: number; label: string; enabled: boolean }
export const abort = new AbortController()
let authReads = 0
export const options: ShapeStreamOptions = {
  url: new URL('/application/shapes?example=integration', location.origin).href,
  params: { table: 'electric_items' },
  liveSse: new URLSearchParams(location.search).get('mode')?.endsWith('-sse'),
  headers: { authorization: async () => `example-token-${++authReads}` },
  onError: (error) => {
    if (error instanceof FetchError && error.status === 401)
      return {
        headers: {
          authorization: async () => `refreshed-token-${++authReads}`,
        },
      }
    throw error
  },
  signal: abort.signal,
}
