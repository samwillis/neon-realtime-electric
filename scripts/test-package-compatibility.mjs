import { spawnSync } from 'node:child_process'
import { mkdir, readFile, writeFile, readdir, rm } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import assert from 'node:assert/strict'

const root = fileURLToPath(new URL('../', import.meta.url))
const artifacts = path.join(root, '.artifacts/package-compatibility')
const packs = path.join(artifacts, 'packs')
await rm(artifacts, { recursive: true, force: true })
await mkdir(packs, { recursive: true })
run(
  'pnpm',
  ['--dir', 'packages/client', 'pack', '--pack-destination', packs],
  root
)
const packed = (await readdir(packs)).filter((name) => name.endsWith('.tgz'))
assert.equal(packed.length, 1, 'Expected exactly one candidate tarball')
const candidate = path.join(packs, packed[0])
const consumerBytes = await readFile(
  path.join(root, 'tests/compatibility/consumer.ts')
)
const consumerHash = createHash('sha256').update(consumerBytes).digest('hex')
const reports = {}

for (const kind of ['reference', 'candidate']) {
  const cwd = path.join(artifacts, kind)
  await mkdir(cwd, { recursive: true })
  await writeFile(
    path.join(cwd, 'package.json'),
    JSON.stringify(
      {
        name: `compatibility-${kind}`,
        private: true,
        type: 'module',
        dependencies: {
          '@electric-sql/client':
            kind === 'reference' ? '1.5.28' : `file:${candidate}`,
          typescript: '5.9.3',
          'typescript-minimum': 'npm:typescript@5.5.4',
        },
      },
      null,
      2
    ) + '\n'
  )
  await writeFile(path.join(cwd, 'consumer.ts'), consumerBytes)
  await writeFile(
    path.join(cwd, 'tsconfig.json'),
    JSON.stringify(
      {
        compilerOptions: {
          strict: true,
          skipLibCheck: false,
          noEmit: true,
          target: 'ES2022',
          lib: ['ES2022', 'DOM', 'DOM.Iterable'],
          module: 'ESNext',
          moduleResolution: 'Bundler',
          types: [],
        },
        include: ['consumer.ts'],
      },
      null,
      2
    ) + '\n'
  )
  run(
    'pnpm',
    ['install', '--ignore-workspace', '--ignore-scripts', '--prefer-offline'],
    cwd
  )
  for (const compiler of ['typescript', 'typescript-minimum']) {
    run(
      process.execPath,
      [`node_modules/${compiler}/bin/tsc`, '-p', 'tsconfig.json'],
      cwd
    )
  }
  const packageRoot = path.join(cwd, 'node_modules/@electric-sql/client')
  const metadata = JSON.parse(
    await readFile(path.join(packageRoot, 'package.json'), 'utf8')
  )
  assert.equal(
    metadata.name,
    kind === 'reference'
      ? '@electric-sql/client'
      : '@neon/realtime-electric-client'
  )
  const probe = (moduleType) => `
    ${moduleType === 'esm' ? "import * as api from '@electric-sql/client';" : "const api = require('@electric-sql/client');"}
    const stream = new api.ShapeStream({ url: 'https://example.test/api/shape?tenant=one', subscribe: false });
    if (stream.hasStarted()) throw new Error('Constructor unexpectedly started the stream');
    if (typeof stream.isPaused() !== 'boolean') throw new Error('isPaused contract missing');
    if (typeof stream.fetchSnapshot !== 'function') throw new Error('fetchSnapshot contract missing');
    console.log(JSON.stringify({
      exports: Object.keys(api).filter(key => key !== 'default').sort(),
      stream: Object.getOwnPropertyNames(api.ShapeStream.prototype).sort(),
      shape: Object.getOwnPropertyNames(api.Shape.prototype).sort(),
      params: api.ELECTRIC_PROTOCOL_QUERY_PARAMS,
      replica: api.ShapeStream.Replica,
      optionsUrl: stream.options.url,
      mode: stream.mode
    }));
  `
  const esm = JSON.parse(
    run(
      process.execPath,
      ['--input-type=module', '-e', probe('esm')],
      cwd,
      true
    ).trim()
  )
  const cjs = JSON.parse(
    run(
      process.execPath,
      ['--input-type=commonjs', '-e', probe('cjs')],
      cwd,
      true
    ).trim()
  )
  assert.deepEqual(esm, cjs, `${kind}: ESM/CJS public runtime differences`)
  for (const condition of ['browser', 'react-native']) {
    const result = run(
      process.execPath,
      [
        `--conditions=${condition}`,
        '--input-type=module',
        '-e',
        "console.log(import.meta.resolve('@electric-sql/client'))",
      ],
      cwd,
      true
    ).trim()
    assert.ok(
      result.endsWith(
        condition === 'browser'
          ? '/dist/index.mjs'
          : '/dist/index.react-native.mjs'
      ),
      result
    )
  }
  const nativeSubpath = run(
    process.execPath,
    [
      '--input-type=module',
      '-e',
      "console.log(import.meta.resolve('@electric-sql/client/react-native'))",
    ],
    cwd,
    true
  ).trim()
  assert.ok(nativeSubpath.endsWith('/dist/index.react-native.mjs'))
  reports[kind] = {
    ...esm,
    consumerHash,
    packageName: metadata.name,
    packageVersion: metadata.version,
  }
}
const {
  packageName: referenceName,
  packageVersion: referenceVersion,
  ...reference
} = reports.reference
const {
  packageName: candidateName,
  packageVersion: candidateVersion,
  ...replacement
} = reports.candidate
assert.deepEqual(
  replacement,
  reference,
  'Replacement package public runtime differs from reference'
)
await writeFile(
  path.join(artifacts, 'report.json'),
  JSON.stringify(reports, null, 2) + '\n'
)
console.log(
  'Packed replacement: identical consumer compiles on TS 5.5.4/5.9.3; ESM/CJS and browser/native resolution pass'
)

function run(command, args, cwd, capture = false) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    stdio: capture ? ['ignore', 'pipe', 'inherit'] : 'inherit',
    env: { ...process.env, CI: 'true' },
  })
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${command} ${args.join(' ')} failed`)
  return result.stdout
}
