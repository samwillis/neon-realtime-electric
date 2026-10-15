import ts from 'typescript'
import assert from 'node:assert/strict'
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import { createHash } from 'node:crypto'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const referenceRoot = path.join(
  root,
  '.artifacts/package-compatibility/reference/node_modules/@electric-sql/client'
)
const candidateRoot = path.join(root, 'packages/client')
assert.ok(
  existsSync(referenceRoot),
  'Run pnpm test:package to install the isolated reference first'
)
const entries = [
  'dist/index.d.ts',
  'dist/cjs/index.d.cts',
  'dist/index.react-native.d.ts',
  'dist/cjs/index.react-native.d.cts',
]
const report = {}
for (const entry of entries) {
  const reference = inventory(path.join(referenceRoot, entry))
  const candidate = inventory(path.join(candidateRoot, entry))
  assert.deepEqual(
    candidate,
    reference,
    `${entry}: public declarations differ from the pinned Electric client`
  )
  report[entry] = candidate
}
const originalPackage = JSON.parse(
  await readFile(path.join(referenceRoot, 'package.json'), 'utf8')
)
const forkPackage = JSON.parse(
  await readFile(path.join(candidateRoot, 'package.json'), 'utf8')
)
for (const field of [
  'exports',
  'main',
  'module',
  'react-native',
  'types',
  'typesVersions',
  'sideEffects',
]) {
  assert.deepEqual(
    forkPackage[field],
    originalPackage[field],
    `Package ${field} differs`
  )
}
const sourcePaths = JSON.parse(
  await readFile(path.join(root, 'upstream/electric-client.json'), 'utf8')
).files.filter(
  ({ path: relative }) =>
    relative.startsWith('src/') && relative !== 'src/react-native-shim.d.ts'
)
for (const { path: relative } of sourcePaths) {
  const published = await readFile(path.join(referenceRoot, relative))
  const candidate = await readFile(path.join(candidateRoot, relative))
  // Source identity proves the initial baseline. Later transport edits are
  // reviewed independently; they must still preserve the declaration report.
  if (process.argv.includes('--pristine'))
    assert.ok(
      published.equals(candidate),
      `${relative}: published source differs`
    )
}
await mkdir(path.join(root, '.artifacts'), { recursive: true })
await writeFile(
  path.join(root, '.artifacts/public-api.json'),
  JSON.stringify(report, null, 2) + '\n'
)
console.log(
  `Public API matches: ${report['dist/index.d.ts'].exports.length} exports, all four declaration entry points and package resolution fields`
)

function inventory(entry) {
  const program = ts.createProgram([entry], {
    target: ts.ScriptTarget.ESNext,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    strict: true,
    skipLibCheck: false,
    noEmit: true,
    types: [],
  })
  const checker = program.getTypeChecker()
  const file = program.getSourceFile(entry)
  assert.ok(file, `Missing declaration entry ${entry}`)
  const module = checker.getSymbolAtLocation(file)
  assert.ok(module, `Not a module: ${entry}`)
  const exports = checker
    .getExportsOfModule(module)
    .map((symbol) => symbol.name)
    .sort()
  const printer = ts.createPrinter({
    removeComments: true,
    newLine: ts.NewLineKind.LineFeed,
  })
  // Follow exported aliases and every package-local declaration they reference.
  // Include non-exported helper declarations, since their definitions can change
  // public options/return types without changing an exported signature's text.
  const reachable = new Map()
  const pending = checker.getExportsOfModule(module)
  const seen = new Set()
  while (pending.length) {
    let symbol = pending.pop()
    if (symbol.flags & ts.SymbolFlags.Alias)
      symbol = checker.getAliasedSymbol(symbol)
    if (seen.has(symbol)) continue
    seen.add(symbol)
    for (const declaration of symbol.declarations ?? []) {
      if (!isLocal(declaration.getSourceFile().fileName)) continue
      const transformed = ts.transform(declaration, [stripPrivateMembers])
      const text = printer
        .printNode(
          ts.EmitHint.Unspecified,
          transformed.transformed[0],
          declaration.getSourceFile()
        )
        .trim()
      transformed.dispose()
      const hash = createHash('sha256').update(text).digest('hex')
      reachable.set(`${symbol.name}:${hash}`, {
        name: symbol.name,
        declaration: text,
      })
      visit(declaration)
    }
  }
  return {
    exports,
    declarations: [...reachable.values()].sort(
      (a, b) =>
        a.name.localeCompare(b.name) ||
        a.declaration.localeCompare(b.declaration)
    ),
  }

  function isLocal(filename) {
    return filename.startsWith(path.dirname(entry) + path.sep)
  }
  function visit(node) {
    if (ts.isIdentifier(node)) {
      const symbol = checker.getSymbolAtLocation(node)
      if (symbol) pending.push(symbol)
    }
    // Private members are implementation details, not cross-install identity.
    if (isPrivate(node)) return
    ts.forEachChild(node, visit)
  }
}

function isPrivate(node) {
  return (
    (node.name && ts.isPrivateIdentifier(node.name)) ||
    !!(ts.getCombinedModifierFlags(node) & ts.ModifierFlags.Private)
  )
}

function stripPrivateMembers(context) {
  const visitor = (node) =>
    isPrivate(node) ? undefined : ts.visitEachChild(node, visitor, context)
  return (node) => ts.visitNode(node, visitor)
}
