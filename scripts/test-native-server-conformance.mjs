import assert from 'node:assert/strict'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { spawnSync } from 'node:child_process'
import {
  upstreamTests,
  supplementalTests,
} from '../tests/conformance/profile.mjs'

const require = createRequire(import.meta.url)
const pinned = JSON.parse(
  await readFile(
    require.resolve('@durable-streams/server-conformance-tests/package.json'),
    'utf8'
  )
)
assert.equal(
  pinned.version,
  '0.3.7',
  'Review profile selection when upgrading upstream'
)
const target = '.artifacts/native-server-conformance'
await mkdir(target, { recursive: true })
const reportPath = `${target}/vitest.json`
const result = spawnSync(
  'pnpm',
  [
    'exec',
    'vitest',
    'run',
    '--config',
    'vitest.conformance.config.ts',
    '--reporter=dot',
    '--reporter=json',
    `--outputFile=${reportPath}`,
  ],
  { stdio: 'inherit' }
)
assert.equal(result.status, 0, 'Native server conformance profile failed')
const report = JSON.parse(await readFile(reportPath, 'utf8'))
const assertions = report.testResults.flatMap((file) => file.assertionResults)
const expected = ['memory', 'objects'].flatMap((backend) => [
  ...upstreamTests.map(
    (name) => `production JSON read/store: ${backend} ${name}`
  ),
  ...supplementalTests.map((name) => `JSON read profile: ${backend} ${name}`),
])
assert.equal(new Set(expected).size, expected.length)
for (const name of expected) {
  const selected = assertions.filter((test) => test.fullName === name)
  assert.equal(
    selected.length,
    1,
    `Missing or duplicate selected test: ${name}`
  )
  assert.equal(
    selected[0].status,
    'passed',
    `Required test did not pass: ${name}`
  )
}
assert.equal(
  assertions.filter((test) => test.status === 'passed').length,
  expected.length,
  'Update the reviewed profile inventory for additional tests'
)
const excluded = assertions
  .filter((test) => !expected.includes(test.fullName))
  .map((test) => test.fullName)
const summary = {
  package: `${pinned.name}@${pinned.version}`,
  scope:
    'Production JSON read/HEAD/long-poll/SSE and memory/object store profile; test-only producers',
  upstreamPassed: upstreamTests.length * 2,
  supplementalPassed: supplementalTests.length * 2,
  excludedByProfile: excluded.length,
  selected: expected,
  excluded,
}
await writeFile(
  `${target}/summary.json`,
  JSON.stringify(summary, null, 2) + '\n'
)
console.log(
  `Native JSON profile: ${summary.upstreamPassed} upstream + ${summary.supplementalPassed} supplemental passed; ${excluded.length} registered tests excluded by profile; full server conformance is not claimed`
)
