import { cp, mkdir, readdir, readFile, writeFile, rm } from 'node:fs/promises'
import { spawnSync } from 'node:child_process'
import { join, resolve } from 'node:path'

const root = resolve('.')
const target = resolve('.artifacts/durable-reader/clean-install')
await rm(target, { recursive: true, force: true })
await mkdir(target, { recursive: true })
for (const name of [
  'package.json',
  'pnpm-lock.yaml',
  'pnpm-workspace.yaml',
  'patches',
]) {
  await cp(join(root, name), join(target, name), { recursive: true })
}
for (const area of ['packages', 'apps', 'examples']) {
  const entries = await readdir(join(root, area), {
    withFileTypes: true,
  }).catch((error) => {
    if (error.code === 'ENOENT') return []
    throw error
  })
  for (const entry of entries) {
    if (!entry.isDirectory()) continue
    const manifest = join(area, entry.name, 'package.json')
    try {
      const bytes = await readFile(join(root, manifest))
      await mkdir(join(target, area, entry.name), { recursive: true })
      await writeFile(join(target, manifest), bytes)
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
  }
}
run('pnpm', ['install', '--frozen-lockfile', '--ignore-scripts'])
await writeFile(
  join(target, 'probe.mjs'),
  `
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { stream } from '@durable-streams/client'
const require = createRequire(import.meta.url)
assert.equal(require('@durable-streams/client/package.json').version, '0.2.7')
for (const read of [stream, require('@durable-streams/client').stream]) {
  const headers = { 'Content-Type': 'application/json', 'Stream-Up-To-Date': 'true', 'Stream-Next-Offset': '000_001' }
  let release, enter
  const gate = new Promise(resolve => { release = resolve })
  const started = new Promise(resolve => { enter = resolve })
  const session = await read({ url: 'https://example.test/shape', live: false, fetch: async () => new Response('[{}]', { headers }) })
  session.subscribeJson(async () => { enter(); await gate })
  let finished = false
  const closed = session.closed.then(() => { finished = true })
  await started
  await Promise.resolve()
  assert.equal(finished, false, 'closed resolved before callback completion')
  release()
  await closed
  const broken = await read({ url: 'https://example.test/shape', live: false, fetch: async () => new Response('[', { headers }) })
  broken.subscribeJson(() => {})
  await assert.rejects(broken.closed, /Failed to parse JSON/)
}
console.log('Clean frozen install: patched ESM/CJS JSON lifecycle verified')
`
)
run(process.execPath, ['probe.mjs'])

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: target,
    encoding: 'utf8',
    stdio: 'inherit',
  })
  if (result.error) throw result.error
  if (result.status !== 0)
    throw new Error(`${command} failed with status ${result.status}`)
}
