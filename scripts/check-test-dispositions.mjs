import assert from 'node:assert/strict'
import { readFile, stat } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

const root = fileURLToPath(new URL('../', import.meta.url))
const read = (name) => readFile(path.join(root, name), 'utf8')
const manifest = JSON.parse(
  await read('upstream/service-test-dispositions.json')
)
const upstream = JSON.parse(await read('upstream/electric-client.json'))
assert.equal(
  manifest.revision,
  upstream.revision,
  'Review dispositions on upstream refresh'
)
assert.equal(manifest.version, 1)

function parse(name, bytes) {
  return ts.createSourceFile(name, bytes, ts.ScriptTarget.Latest, true)
}
function base(expression) {
  if (ts.isIdentifier(expression)) return expression.text
  if (
    ts.isPropertyAccessExpression(expression) ||
    ts.isCallExpression(expression)
  )
    return base(expression.expression)
  if (ts.isTaggedTemplateExpression(expression)) return base(expression.tag)
  return undefined
}
function walk(node, visit) {
  visit(node)
  ts.forEachChild(node, (child) => walk(child, visit))
}

// Check the actual baseline exclusions, not just the manifest's chosen scope.
const baseline = parse(
  'vitest.baseline.config.ts',
  await read('packages/client/vitest.baseline.config.ts')
)
const exclusions = []
walk(baseline, (node) => {
  if (
    ts.isPropertyAssignment(node) &&
    node.name.getText(baseline) === 'exclude'
  ) {
    assert.ok(
      ts.isArrayLiteralExpression(node.initializer),
      'Review dynamic exclusions'
    )
    for (const entry of node.initializer.elements) {
      assert.ok(ts.isStringLiteralLike(entry), 'Review dynamic exclusion entry')
      exclusions.push(`packages/client/${entry.text}`)
    }
  }
})
assert.ok(exclusions.length > 0)
assert.deepEqual(
  [...manifest.files].sort(),
  exclusions.sort(),
  'Every excluded service suite needs a disposition inventory'
)
assert.equal(new Set(manifest.files).size, manifest.files.length)

const inventory = []
for (const file of manifest.files) {
  const source = parse(file, await read(file))
  const roots = new Set(['it', 'test'])
  walk(source, (node) => {
    if (ts.isImportSpecifier(node)) {
      const exported = (node.propertyName ?? node.name).text
      if (['it', 'test'].includes(exported) || exported.startsWith('testWith'))
        roots.add(node.name.text)
    }
  })
  let changed = true
  while (changed) {
    changed = false
    walk(source, (node) => {
      if (
        ts.isVariableDeclaration(node) &&
        ts.isIdentifier(node.name) &&
        node.initializer &&
        roots.has(base(node.initializer)) &&
        !roots.has(node.name.text)
      ) {
        roots.add(node.name.text)
        changed = true
      }
    })
  }
  walk(source, (node) => {
    if (!ts.isCallExpression(node) || !roots.has(base(node.expression))) return
    const [name] = node.arguments
    if (
      !name ||
      !(ts.isStringLiteralLike(name) || ts.isTemplateExpression(name))
    )
      return
    if (
      !node.arguments.some(
        (argument) =>
          ts.isArrowFunction(argument) || ts.isFunctionExpression(argument)
      )
    )
      return
    inventory.push({
      file,
      line:
        source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1,
      name: ts.isStringLiteralLike(name)
        ? name.text
        : name.getText(source).replace(/\s+/g, ' '),
    })
  })
}
const key = (entry) => `${entry.file}:${entry.line}`
assert.equal(new Set(inventory.map(key)).size, inventory.length)
const registered = new Map()
const counts = {}
for (const entry of manifest.cases) {
  assert.ok(!registered.has(key(entry)), `Duplicate disposition: ${key(entry)}`)
  registered.set(key(entry), entry)
  const group = manifest.groups[entry.group]
  assert.ok(group, `Unknown group: ${entry.group}`)
  counts[group.status] = (counts[group.status] ?? 0) + 1
}
assert.equal(
  registered.size,
  inventory.length,
  'Missing or stale test dispositions'
)
for (const entry of inventory) {
  const recorded = registered.get(key(entry))
  assert.ok(recorded, `Missing test disposition: ${key(entry)} ${entry.name}`)
  assert.equal(recorded.name, entry.name, `Stale test name: ${key(entry)}`)
}
for (const [name, group] of Object.entries(manifest.groups)) {
  assert.ok(
    ['mapped', 'partial', 'pending', 'protocol-replaced'].includes(group.status)
  )
  assert.ok(
    group.scope && group.remaining !== undefined,
    `Explain scope and remaining work: ${name}`
  )
  assert.ok(
    manifest.cases.some((entry) => entry.group === name),
    `Unused group: ${name}`
  )
  if (group.status !== 'pending')
    assert.ok(group.evidence.length > 0, `Missing evidence references: ${name}`)
  for (const file of group.evidence) {
    assert.ok(!path.isAbsolute(file) && !file.split('/').includes('..'))
    assert.ok(
      (await stat(path.join(root, file))).isFile(),
      `Missing evidence file: ${file}`
    )
  }
}
console.log(
  `Historical service suites: ${inventory.length} declarations inventoried (${JSON.stringify(counts)})`
)
console.log(
  'Disposition coverage only: referenced files are not proof of test execution or full behavioral equivalence'
)
