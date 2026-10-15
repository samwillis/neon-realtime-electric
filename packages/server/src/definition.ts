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

/** Bounded POC grammar. Every operand is bound; unsupported SQL fails explicitly. */
export function resolveConfiguredShape(
  url: URL,
  tables: readonly ConfiguredTable[]
): OwnerDefinition {
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
  const parameters: (string | boolean | null)[] = []
  const where = url.searchParams.get('where')
  let predicate = ''
  if (where) {
    let bindings: unknown = {}
    const encoded = url.searchParams.get('params')
    if (encoded) {
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
      if (encoded) fail('Ambiguous filter parameter encodings')
      const values: Record<string, string> = Object.create(null)
      for (const [key, value] of bracketed) {
        const position = /^params\[([1-9][0-9]*)\]$/.exec(key)?.[1]
        if (
          !position ||
          !Number.isSafeInteger(Number(position)) ||
          Object.hasOwn(values, position)
        )
          fail('Invalid or repeated filter parameter')
        values[position] = value
      }
      bindings = values
    }
    if (!bindings || typeof bindings !== 'object')
      fail('Invalid filter parameters')
    let rest = where.trim()
    const parts: string[] = []
    let count = 0
    while (rest) {
      if (++count > 32) fail('Filter is too complex')
      const col = identifier.exec(rest)
      if (!col) fail('Unsupported filter expression')
      const column = name(col[0])
      if (!Object.hasOwn(table.columns, column)) fail('Unknown filter column')
      rest = rest.slice(col[0].length).trimStart()
      const nullTest = /^IS\s+(NOT\s+)?NULL\b/i.exec(rest)
      if (nullTest) {
        parts.push(`${quote(column)} IS ${nullTest[1] ? 'NOT ' : ''}NULL`)
        rest = rest.slice(nullTest[0].length).trimStart()
      } else {
        const op = /^(<=|>=|<>|!=|=|<|>)/.exec(rest)
        if (!op) fail('Unsupported filter operator')
        rest = rest.slice(op[0].length).trimStart()
        const operand =
          /^(\$[1-9][0-9]*|'(?:[^']|'')*'|[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|true\b|false\b|null\b)/i.exec(
            rest
          )
        if (!operand) fail('Unsupported filter operand')
        const literal = operand[0]
        let value: unknown
        if (literal.startsWith('$')) {
          const position = Number(literal.slice(1))
          if (!Number.isSafeInteger(position))
            fail('Invalid parameter position')
          const key = Array.isArray(bindings)
            ? String(position - 1)
            : String(position)
          if (!Object.hasOwn(bindings, key)) fail('Missing filter parameter')
          value = (bindings as Record<string, unknown>)[key]
        } else if (literal.startsWith("'"))
          value = literal.slice(1, -1).replaceAll("''", "'")
        else if (/^true$/i.test(literal)) value = true
        else if (/^false$/i.test(literal)) value = false
        else if (/^null$/i.test(literal)) value = null
        else value = literal
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
        parts.push(`${quote(column)} ${op[0]} $${parameters.length}`)
        rest = rest.slice(literal.length).trimStart()
      }
      if (rest) {
        const conjunction = /^AND\s+/i.exec(rest)
        if (!conjunction)
          fail('Only conjunctions are supported in this shape profile')
        rest = rest.slice(conjunction[0].length)
        if (!rest) fail('Incomplete filter')
      }
    }
    predicate = ` WHERE ${parts.join(' AND ')}`
  }
  const logMode = url.searchParams.get('log') ?? 'full'
  const replica = url.searchParams.get('replica') ?? 'default'
  if (!['full', 'changes_only'].includes(logMode)) fail('Unsupported log mode')
  if (!['default', 'full'].includes(replica)) fail('Unsupported replica mode')
  return {
    query: {
      sql: `SELECT ${selection.map(quote).join(', ')} FROM ${quote(table.namespace)}.${quote(table.table)}${predicate}`,
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
