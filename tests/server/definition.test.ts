import { describe, expect, it } from 'vitest'
import {
  resolveConfiguredShape,
  type ConfiguredTable,
} from '../../packages/server/src/definition.js'
const tables: ConfiguredTable[] = [
  {
    namespace: 'public',
    table: 'items',
    primaryKey: ['id'],
    columns: {
      id: { type: 'int8' },
      label: { type: 'text' },
      enabled: { type: 'bool' },
    },
  },
]
async function resolve(params: Record<string, string | undefined>) {
  const url = new URL('https://proxy.test/shapes?table=items')
  for (const [key, value] of Object.entries(params))
    if (value !== undefined) url.searchParams.set(key, value)
  return resolveConfiguredShape(url, tables)
}
describe('configured root-table definitions with WHERE passthrough', () => {
  it('resolves only configured identifiers and keeps hidden primary keys in source projection', async () => {
    const shape = await resolve({
      table: '"public"."items"',
      columns: '"label",enabled',
      log: 'full',
      replica: 'default',
    })
    expect(shape.query.sql).toBe(
      'SELECT "label", "enabled", "id" FROM "public"."items"'
    )
    expect(shape.schema).toEqual({
      label: { type: 'text' },
      enabled: { type: 'bool' },
    })
    expect(shape.projection).toEqual(['label', 'enabled'])
  })
  it('preserves the original expression and sends scalar parameters separately in positional order', async () => {
    const where =
      "id > $2 AND label = 'a''b' AND enabled = $1 OR id > 9007199254740993"
    const shape = await resolve({
      where,
      params: JSON.stringify({ '1': true, '2': '9007199254740993' }),
    })
    expect(shape.query).toEqual({
      sql: `SELECT "id", "label", "enabled" FROM "public"."items" WHERE (\n${where}\n)`,
      parameters: [true, '9007199254740993'],
    })
    expect(
      (
        await resolve({
          where: 'id >= 9007199254740993 AND id < $1',
          params: '["9999999999999999"]',
        })
      ).query.parameters
    ).toEqual(['9999999999999999'])
  })
  it('preserves bracketed positional parameters used by existing trusted proxies', async () => {
    const shape = await resolve({
      where: 'label = $1 AND id > $2',
      'params[1]': "a'b + %",
      'params[2]': '9007199254740993',
    })
    expect(shape.query.parameters).toEqual(["a'b + %", '9007199254740993'])
    expect(shape.query.sql).toContain('WHERE (\nlabel = $1 AND id > $2\n)')
    expect(() =>
      resolveConfiguredShape(
        new URL(
          'http://local/?table=items&where=id%3D%241&params[1]=1&params[1]=2'
        ),
        tables
      )
    ).toThrow('repeated')
  })
  it.each([
    'id = 1 OR id = 2',
    'id IN (SELECT item_id FROM memberships WHERE tenant_id = $1)',
    'EXISTS (SELECT 1 FROM memberships m WHERE m.item_id = items.id AND m.tenant_id = $1)',
    'NOT EXISTS (SELECT 1 FROM memberships m WHERE m.item_id = items.id)',
    'id IN (SELECT item_id FROM a UNION SELECT item_id FROM b)',
    'CASE WHEN enabled THEN id > $1 ELSE label IS NOT NULL END',
    "label ILIKE '%a%' AND id BETWEEN 1 AND 10",
    "id = ANY ('{1,2}'::bigint[])",
    'id = $1 OR id = $1 /* repeated bound position */',
    "label = '$5' OR label = $$text with $6$$ -- not parameters",
    'id = $1 -- closing wrapper remains on its own line',
    'EXISTS (WITH candidates AS (SELECT id FROM a) SELECT 1 FROM candidates)',
  ])('passes through a complete WHERE expression: %s', async (where) => {
    const shape = await resolve({ where, 'params[1]': '4' })
    expect(shape.query.sql).toContain(`WHERE (\n${where}\n)`)
    expect(shape.query.parameters).toEqual(['4'])
    expect(shape.namespace).toBe('public')
    expect(shape.table).toBe('items')
  })
  it('accepts changes-only logs and both replica modes without interpolating mode values into SQL', async () => {
    expect(
      await resolve({ log: 'changes_only', replica: 'full' })
    ).toMatchObject({
      logMode: 'changes_only',
      replica: 'full',
    })
    expect((await resolve({ log: 'full', replica: 'default' })).query).toEqual(
      (await resolve({})).query
    )
  })
  it.each([
    { where: 'id=$1', 'params[1]': '1', params: '[2]' },
    { where: 'id=$1', 'params[0]': '1' },
    { table: 'unknown' },
    { table: 'items; select 1' },
    { columns: 'id,label,' },
    { columns: 'label,label' },
    { columns: 'unknown' },
    { where: 'id = $1', params: 'not json' },
    { where: 'id = $1', params: '[{}]' },
    { where: 'id = $1', params: '[9007199254740993]' },
    { where: 'id = $2', 'params[2]': '2' },
    { where: '' },
    { log: 'unknown' },
    { replica: 'unknown' },
  ])('rejects invalid or ambiguous definition metadata: %j', async (params) => {
    await expect(resolve(params)).rejects.toThrow()
  })
  it('passes text, comments and dollar-quoted strings through without interpreting them', async () => {
    const where = "label = $$); SELECT 1; --$$ OR label = 'ORDER BY x' /* ; */"
    expect((await resolve({ where })).query.sql).toContain(where)
  })
  it('rejects repeated shape-defining parameters rather than selecting a different authorization input', async () => {
    for (const key of [
      'where',
      'table',
      'params',
      'columns',
      'replica',
      'log',
    ]) {
      const url = new URL('https://proxy.test/shape?table=items')
      url.searchParams.append(key, key === 'table' ? 'items' : 'first')
      url.searchParams.append(key, 'second')
      expect(() => resolveConfiguredShape(url, tables)).toThrow('Repeated')
    }
  })
})
