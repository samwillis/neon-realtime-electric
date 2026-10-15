import assert from 'node:assert/strict'
import { mkdir, writeFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'

const base = new URL(process.env.SHAPE_URL ?? 'http://localhost:4327/api/shape')
base.searchParams.set('table', 'electric_items')
const read = (url) => fetch(url, { signal: AbortSignal.timeout(20_000) })
const response = await read(base)
assert.equal(response.status, 200)
const envelopes = await response.json()
const init = envelopes.find((e) => e.kind === 'initializing')
assert(init)
const expiresAt = Date.parse(init.expiresAt)
assert(
  expiresAt - Date.now() > 890_000,
  'Expected a real 15-minute handle, not a reduced test TTL'
)
const handle = response.headers.get('electric-handle')
assert.equal(handle, init.handle)
const resume = new URL(base)
resume.searchParams.set('handle', handle)
resume.searchParams.set('offset', response.headers.get('electric-offset'))
const report = {
  startedAt: new Date().toISOString(),
  expiresAt: init.expiresAt,
  handle,
  status: 'running',
}
await mkdir('.artifacts/staging', { recursive: true })
const save = () =>
  writeFile(
    '.artifacts/staging/local-expiry.json',
    JSON.stringify(report, null, 2)
  )
await save()
console.log(
  `Real 15-minute neon dev expiry run started; expires ${init.expiresAt}`
)
await delay(Math.max(0, expiresAt - Date.now() - 1500))
const before = await read(resume)
assert(
  [200, 204].includes(before.status),
  'Handle expired before its absolute deadline'
)
await before.arrayBuffer()
await delay(Math.max(0, expiresAt - Date.now() + 100))
const after = await read(resume)
assert.equal(after.status, 409)
const replacement = after.headers.get('electric-handle')
assert(replacement && replacement !== handle)
assert.equal(after.headers.get('location'), null)
Object.assign(report, {
  status: 'passed',
  completedAt: new Date().toISOString(),
  beforeStatus: before.status,
  afterStatus: after.status,
  replacementHandle: replacement,
})
await save()
console.log(
  'Real 15-minute neon dev expiry passed; replacement handle stays on the same URL'
)
