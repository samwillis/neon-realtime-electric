import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { Shape, ShapeStream } from '../packages/client/src/index.js'

const url = process.env.SHAPE_URL ?? 'http://localhost:4327/api/shape'
const abort = new AbortController()
const timer = setTimeout(
  () => abort.abort(new Error('Local baseline timed out')),
  20_000
)
const requests: { url: string; method: string }[] = []
const start = performance.now()
try {
  const stream = new ShapeStream<{
    id: number
    label: string
    enabled: boolean
  }>({
    url,
    params: { table: 'electric_items' },
    subscribe: false,
    signal: abort.signal,
    fetchClient: async (input, init) => {
      const outward = String(input)
      const target = new URL(outward)
      assert.equal(target.origin, new URL(url).origin)
      assert.equal(target.pathname, new URL(url).pathname)
      requests.push({ url: outward, method: init?.method ?? 'GET' })
      return fetch(input, init)
    },
  })
  const shape = new Shape(stream)
  const rows = await shape.rows
  assert(
    rows.some(
      (row) =>
        row.id === 1 && row.label === 'First item' && row.enabled === true
    )
  )
  assert(stream.shapeHandle)
  assert(stream.lastOffset !== '-1')
  assert(requests.every((r) => r.method === 'GET'))
  const resumed = new URL(url)
  resumed.searchParams.set('table', 'electric_items')
  resumed.searchParams.set('handle', stream.shapeHandle)
  resumed.searchParams.set('offset', stream.lastOffset)
  const response = await fetch(resumed, { signal: abort.signal })
  assert.equal(response.status, 200)
  assert.deepEqual(await response.json(), [])
  assert(response.headers.has('electric-schema'))
  await mkdir('.artifacts/staging', { recursive: true })
  await writeFile(
    '.artifacts/staging/local-baseline.json',
    JSON.stringify(
      {
        rows,
        requests,
        handle: stream.shapeHandle,
        offset: stream.lastOffset,
        elapsedMs: performance.now() - start,
        resumeStatus: response.status,
      },
      null,
      2
    )
  )
  console.log(
    `Local neon dev baseline passed (${rows.length} rows, ${requests.length} GETs, saved-offset tail resume)`
  )
} finally {
  clearTimeout(timer)
  abort.abort()
}
