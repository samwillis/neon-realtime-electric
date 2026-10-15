import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { realpath, mkdir, writeFile } from 'node:fs/promises'
import { runConformanceTests } from '@durable-streams/client-conformance-tests'

const require = createRequire(import.meta.url)
const suiteRoot = dirname(
  require.resolve('@durable-streams/client-conformance-tests/package.json')
)
// The published --run ts shortcut points at an absent dist/adapters/*.ts.
// Run the package's actual, unmodified source adapter through its pinned tsx.
const adapter = join(suiteRoot, 'src/adapters/typescript-adapter.ts')
const adapterRequire = createRequire(adapter)
const candidatePath = await realpath(require.resolve('@durable-streams/client'))
if (
  (await realpath(adapterRequire.resolve('@durable-streams/client'))) !==
  candidatePath
) {
  throw new Error(
    'Conformance adapter does not resolve the tested client artifact'
  )
}
const summary = await runConformanceTests({
  clientAdapter: process.execPath,
  clientArgs: [require.resolve('tsx/cli'), adapter],
  suites: ['consumer', 'lifecycle'],
  testTimeout: 30_000,
})
await mkdir('.artifacts/durable-reader', { recursive: true })
await writeFile(
  '.artifacts/durable-reader/conformance.json',
  JSON.stringify({ candidatePath, summary }, null, 2) + '\n'
)
console.log(
  `Durable Streams reader conformance: ${summary.passed} passed, ${summary.failed} failed, ${summary.skipped} skipped`
)
if (summary.failed) process.exitCode = 1
