import { createHash } from 'node:crypto'
import { readFile, mkdir, writeFile, access } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const manifestPath = path.join(root, 'upstream/electric-client.json')
const manifest = JSON.parse(await readFile(manifestPath, 'utf8'))
const args = process.argv.slice(2)
const checking = args.includes('--check')
const destinationIndex = args.indexOf('--destination')
const destination = path.resolve(
  root,
  destinationIndex === -1 ? 'packages/client' : args[destinationIndex + 1]
)
if (!checking) {
  try {
    await access(destination)
    throw new Error(
      `Refusing to overwrite ${destination}; use a fresh destination`
    )
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
}

let next = 0
let checked = 0
await Promise.all(
  Array.from({ length: 8 }, async () => {
    while (next < manifest.files.length) {
      const entry = manifest.files[next++]
      const target = path.join(destination, entry.path)
      if (checking && manifest.workspaceAdaptations.includes(entry.path))
        continue
      const bytes = checking
        ? await readFile(target)
        : await download(entry.upstreamPath)
      const actual = createHash('sha1')
        .update(`blob ${bytes.length}\0`)
        .update(bytes)
        .digest('hex')
      if (actual !== entry.gitBlob) {
        throw new Error(`Upstream content mismatch: ${entry.path}`)
      }
      if (!checking) {
        await mkdir(path.dirname(target), { recursive: true })
        await writeFile(target, bytes)
      }
      checked++
    }
  })
)
console.log(
  `${checking ? 'Verified' : 'Imported'} ${checked} upstream files at ${manifest.revision}`
)

async function download(upstreamPath) {
  const url = `https://raw.githubusercontent.com/electric-sql/electric/${manifest.revision}/${upstreamPath}`
  for (let attempt = 0; attempt < 3; attempt++) {
    const response = await fetch(url, { signal: AbortSignal.timeout(30_000) })
    if (response.ok) return Buffer.from(await response.arrayBuffer())
    if (response.status < 500 || attempt === 2) {
      throw new Error(`Import ${upstreamPath}: HTTP ${response.status}`)
    }
  }
}
