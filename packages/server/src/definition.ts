import type { OwnerDefinition } from './shape-owner.js'

export class ShapeDefinitionError extends Error {}
export interface ConfiguredTable {
  namespace: string
  table: string
  primaryKey: readonly string[]
  columns: OwnerDefinition['schema']
}

const identifier = /^(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)/
const quote = (value: string) => `"${value.replaceAll('"', '""')}"`
function name(token: string): string {
  return token.startsWith('"')
    ? token.slice(1, -1).replaceAll('""', '"')
    : token.toLowerCase()
}
function fail(message: string): never {
  throw new ShapeDefinitionError(message)
}

/** Configured root/projection; authorized WHERE SQL passes directly to Realtime. */
export function resolveConfiguredShape(
  url: URL,
  tables: readonly ConfiguredTable[]
): OwnerDefinition {
  for (const key of ['table', 'columns', 'where', 'params', 'log', 'replica'])
    if (url.searchParams.getAll(key).length > 1)
      fail(`Repeated shape parameter: ${key}`)
  const requested = url.searchParams.get('table') ?? fail('Missing table')
  const table = tables.find(
    (t) =>
      requested === t.table ||
      requested === `${t.namespace}.${t.table}` ||
      requested === `${quote(t.namespace)}.${quote(t.table)}`
  )
  if (!table) fail('Table is not configured')
  if (!table.primaryKey.length) fail('Shape tables require a primary key')
  let projection = Object.keys(table.columns)
  const configured = url.searchParams.get('columns')
  if (configured !== null) {
    projection = []
    let remaining = configured.trim()
    while (remaining) {
      const match = identifier.exec(remaining)
      if (!match) fail('Invalid projection')
      projection.push(name(match[0]))
      remaining = remaining.slice(match[0].length).trimStart()
      if (remaining.startsWith(',')) {
        remaining = remaining.slice(1).trimStart()
        if (!remaining) fail('Invalid projection')
      } else if (remaining) fail('Invalid projection')
    }
    if (!projection.length || new Set(projection).size !== projection.length)
      fail('Invalid projection')
  }
  for (const col of [...projection, ...table.primaryKey])
    if (!Object.hasOwn(table.columns, col)) fail('Unknown column')
  const selection = [
    ...projection,
    ...table.primaryKey.filter((p) => !projection.includes(p)),
  ]
  const baseSql = `SELECT ${selection.map(quote).join(', ')} FROM ${quote(table.namespace)}.${quote(table.table)}`
  let sql = baseSql
  const parameters: (string | boolean | null)[] = []
  const where = url.searchParams.get('where')
  if (where !== null) {
    if (!where.trim()) fail('Empty WHERE expression')
    // Realtime owns SQL parsing/validation. The caller's authorize callback
    // must authorize the predicate before handing the definition to a shape.
    // A newline keeps a trailing line comment separate from the wrapper.
    sql = `${baseSql} WHERE (\n${where}\n)`
    let bindings: unknown = {}
    const encoded = url.searchParams.get('params')
    if (encoded !== null) {
      try {
        bindings = JSON.parse(encoded)
      } catch {
        fail('Invalid filter parameters')
      }
    }
    const bracketed = [...url.searchParams].filter(([key]) =>
      key.startsWith('params[')
    )
    if (bracketed.length) {
      if (encoded !== null) fail('Ambiguous filter parameter encodings')
      const values: Record<string, string> = Object.create(null)
      for (const [key, value] of bracketed) {
        const position = /^params\[([1-9][0-9]*)\]$/.exec(key)?.[1]
        if (
          !position ||
          !Number.isSafeInteger(Number(position)) ||
          Number(position) > 65535 ||
          Object.hasOwn(values, position)
        )
          fail('Invalid or repeated filter parameter')
        values[position] = value
      }
      bindings = values
    }
    if (!bindings || typeof bindings !== 'object')
      fail('Invalid filter parameters')
    const keys = Object.keys(bindings)
    if (
      !Array.isArray(bindings) &&
      keys.some((key) => !/^[1-9][0-9]*$/.test(key) || Number(key) > 65535)
    )
      fail('Invalid filter parameter position')
    const maxPosition = Array.isArray(bindings)
      ? bindings.length
      : keys.reduce((max, key) => Math.max(max, Number(key)), 0)
    if (maxPosition > 65535) fail('Too many filter parameters')
    for (let position = 1; position <= maxPosition; position++) {
      const key = Array.isArray(bindings)
        ? String(position - 1)
        : String(position)
      if (!Object.hasOwn(bindings, key)) fail('Missing filter parameter')
      let value = (bindings as Record<string, unknown>)[key]
      if (
        value !== null &&
        typeof value !== 'string' &&
        typeof value !== 'boolean' &&
        typeof value !== 'number'
      )
        fail('Filter parameter must be scalar')
      if (typeof value === 'number') {
        if (
          !Number.isFinite(value) ||
          (Number.isInteger(value) && !Number.isSafeInteger(value))
        )
          fail('Numeric parameter loses precision; pass a string')
        value = String(value)
      }
      parameters.push(value as string | boolean | null)
    }
  }
  const logMode = url.searchParams.get('log') ?? 'full'
  const replica = url.searchParams.get('replica') ?? 'default'
  if (!['full', 'changes_only'].includes(logMode)) fail('Unsupported log mode')
  if (!['default', 'full'].includes(replica)) fail('Unsupported replica mode')
  return {
    query: {
      sql,
      parameters,
    },
    namespace: table.namespace,
    table: table.table,
    primaryKey: table.primaryKey,
    projection,
    schema: Object.fromEntries(projection.map((p) => [p, table.columns[p]])),
    ...(logMode === 'changes_only' ? ({ logMode } as const) : {}),
    ...(replica === 'full' ? ({ replica } as const) : {}),
  }
}
