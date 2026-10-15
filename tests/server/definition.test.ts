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
function resolve(params: Record<string, string>) {
  return resolveConfiguredShape(
    new URL(
      `https://proxy.test/shapes?${new URLSearchParams({ table: 'items', ...params })}`
    ),
    tables
  )
}
describe('configured root-table POC definitions', () => {
  it('resolves only configured identifiers and keeps hidden primary keys in source projection', () => {
    const shape = resolve({
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
  it('binds scalar operands, literal quotes and exact integer/decimal strings', () => {
    const shape = resolve({
      where: "id > $1 AND label = 'a''b' AND enabled = true",
      params: JSON.stringify({ '1': '9007199254740993' }),
    })
    expect(shape.query).toEqual({
      sql: 'SELECT "id", "label", "enabled" FROM "public"."items" WHERE "id" > $1 AND "label" = $2 AND "enabled" = $3',
      parameters: ['9007199254740993', "a'b", true],
    })
    expect(
      resolve({
        where: 'id >= 9007199254740993 AND id < $1',
        params: '["9999999999999999"]',
      }).query.parameters
    ).toEqual(['9007199254740993', '9999999999999999'])
    expect(
      resolve({ where: 'label IS NOT NULL AND id != -1.5e2' }).query.sql
    ).toContain('"label" IS NOT NULL AND "id" != $1')
  })
  it.each([
    { table: 'unknown' },
    { table: 'items; drop table items' },
    { columns: 'id,label,' },
    { columns: 'label,label' },
    { columns: 'unknown' },
    { where: 'unknown = 1' },
    { where: 'id = 1 OR id = 2' },
    { where: 'id = 1; select 1' },
    { where: 'id = $1' },
    { where: 'id = $1', params: 'not json' },
    { where: 'id = $1', params: '[{}]' },
    { where: 'id = $1', params: '[9007199254740993]' },
    { where: 'id = 1 AND ' },
    { where: 'exists (select 1)' },
    { log: 'changes_only' },
    { replica: 'full' },
  ])(
    'rejects unsupported or ambiguous definitions explicitly: %j',
    (params) => {
      expect(() =>
        resolve(params as unknown as Record<string, string>)
      ).toThrow()
    }
  )
})
