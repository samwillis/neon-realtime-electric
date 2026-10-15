import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { cp, mkdir, readdir, rm, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
const root = fileURLToPath(new URL('../', import.meta.url))
const artifacts = path.join(root, '.artifacts/integration-packages')
await rm(artifacts, { recursive: true, force: true })
await mkdir(path.join(artifacts, 'packs'), { recursive: true })
run(
  'pnpm',
  [
    '--dir',
    'packages/client',
    'pack',
    '--pack-destination',
    path.join(artifacts, 'packs'),
  ],
  root
)
const [pack] = (await readdir(path.join(artifacts, 'packs'))).filter((p) =>
  p.endsWith('.tgz')
)
assert(pack)
for (const kind of ['reference', 'candidate']) {
  const cwd = path.join(artifacts, kind)
  await cp(path.join(root, 'examples'), cwd, { recursive: true })
  await cp(
    path.join(root, 'tests/compatibility/integrations/resolution.mjs'),
    path.join(cwd, 'resolution.mjs')
  )
  const replacement =
    kind === 'candidate'
      ? `file:${path.join(artifacts, 'packs', pack)}`
      : '1.5.28'
  await writeFile(
    path.join(cwd, 'package.json'),
    JSON.stringify(
      {
        name: `integration-${kind}`,
        private: true,
        type: 'module',
        dependencies: {
          '@electric-sql/client': replacement,
          '@electric-sql/react': '1.0.57',
          '@tanstack/electric-db-collection': '0.5.8',
          '@tanstack/react-db': '0.5.7',
          react: '19.2.0',
          'react-dom': '19.2.0',
          '@types/react': '19.2.2',
          '@types/react-dom': '19.2.2',
          typescript: '5.9.3',
          'typescript-minimum': 'npm:typescript@5.5.4',
          vite: '7.1.9',
          'playwright-core': '1.56.1',
        },
        pnpm: { overrides: { '@electric-sql/client': replacement } },
      },
      null,
      2
    )
  )
  await writeFile(
    path.join(cwd, 'tsconfig.json'),
    JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2022',
          module: 'ESNext',
          moduleResolution: 'Bundler',
          strict: true,
          skipLibCheck: false,
          noEmit: true,
          jsx: 'react-jsx',
          lib: ['ES2022', 'DOM', 'DOM.Iterable'],
          types: ['react', 'react-dom'],
        },
        include: ['**/*.ts', '**/*.tsx'],
        exclude: ['node_modules'],
      },
      null,
      2
    )
  )
  run(
    'pnpm',
    ['install', '--ignore-workspace', '--ignore-scripts', '--prefer-offline'],
    cwd
  )
  for (const compiler of ['typescript', 'typescript-minimum'])
    run(
      process.execPath,
      [`node_modules/${compiler}/bin/tsc`, '-p', 'tsconfig.json'],
      cwd
    )
  run(process.execPath, ['resolution.mjs'], cwd)
  run(process.execPath, ['node_modules/vite/bin/vite.js', 'build'], cwd)
}
console.log(
  'Identical basic/React/eager TanStack consumers compile and build against original and packed replacement'
)
function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd,
    stdio: 'inherit',
    env: { ...process.env, CI: 'true' },
  })
  if (result.error) throw result.error
  assert.equal(result.status, 0, `${command} ${args.join(' ')} failed`)
}
