import { randomUUID } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { Client } from 'pg'
import { describe, expect, it } from 'vitest'
import {
  Shape,
  ShapeStream,
  isChangeMessage,
  isControlMessage,
  isVisibleInSnapshot,
  type Message,
  type PostgresSnapshot,
  type Row,
} from '../../packages/client/src/index.js'
import { startLocalFunction } from './support/local-function.js'
import { recordRealtimeWire } from './support/wire-recorder.js'

function required(key: string): string {
  const value = process.env[key]
  if (!value) throw new Error(`Type matrix integration requires ${key}`)
  return value
}
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`

// Explicit golden values, independent of the candidate parser. String-only
// types additionally compare against Postgres ::text from a separate query.
const allCases = [
  {
    name: 'enum_array',
    sqlType: 'custom_mood[]',
    qualified: true,
    type: 'custom_mood',
    dims: 1,
    sql: "'{sad,ok,happy}'",
    value: ['sad', 'ok', 'happy'],
    updatedSql: "'{sad,happy}'",
    updatedValue: ['sad', 'happy'],
  },
  {
    name: 'enum_matrix',
    sqlType: 'custom_mood[][]',
    qualified: true,
    type: 'custom_mood',
    dims: 2,
    sql: "'{{sad,ok},{ok,happy}}'",
    value: [
      ['sad', 'ok'],
      ['ok', 'happy'],
    ],
    updatedSql: "'{{sad,happy},{happy,ok}}'",
    updatedValue: [
      ['sad', 'happy'],
      ['happy', 'ok'],
    ],
  },
  {
    name: 'domain_array',
    sqlType: 'custom_positive[]',
    qualified: true,
    type: 'custom_positive',
    dims: 1,
    sql: "'{5,9,2}'",
    value: ['5', '9', '2'],
    updatedSql: "'{6,10,3}'",
    updatedValue: ['6', '10', '3'],
  },
  {
    name: 'composite_array',
    sqlType: 'custom_complex[]',
    qualified: true,
    type: 'custom_complex',
    dims: 1,
    sql: `'{"(1.1,2.2)","(3.3,4.4)"}'`,
    value: ['(1.1,2.2)', '(3.3,4.4)'],
    updatedSql: `'{"(2.2,3.3)","(4.4,5.5)"}'`,
    updatedValue: ['(2.2,3.3)', '(4.4,5.5)'],
  },
  {
    name: 'small',
    sqlType: 'smallint',
    type: 'int2',
    sql: '-32768',
    value: -32768,
  },
  {
    name: 'integer',
    sqlType: 'integer',
    type: 'int4',
    sql: '2147483647',
    value: 2147483647,
  },
  {
    name: 'big',
    sqlType: 'bigint',
    type: 'int8',
    sql: '9007199254740993',
    value: 9007199254740993n,
  },
  {
    name: 'boolean',
    sqlType: 'boolean',
    type: 'bool',
    sql: 'true',
    value: true,
  },
  { name: 'real', sqlType: 'real', type: 'float4', sql: '1.25', value: 1.25 },
  {
    name: 'double',
    sqlType: 'double precision',
    type: 'float8',
    sql: '-1.125',
    value: -1.125,
  },
  {
    name: 'nan',
    sqlType: 'double precision',
    type: 'float8',
    sql: "'NaN'",
    value: NaN,
  },
  {
    name: 'positive_infinity',
    sqlType: 'double precision',
    type: 'float8',
    sql: "'Infinity'",
    value: Infinity,
  },
  {
    name: 'negative_infinity',
    sqlType: 'double precision',
    type: 'float8',
    sql: "'-Infinity'",
    value: -Infinity,
  },
  {
    name: 'exact',
    sqlType: 'numeric(40,12)',
    type: 'numeric',
    sql: '12345678901234567890.123456789012',
  },
  {
    name: 'quoted " text',
    sqlType: 'text',
    type: 'text',
    sql: "E'Unicode 🐘, quote \\' and newline\\nbackslash\\\\'",
  },
  {
    name: 'varchar',
    sqlType: 'varchar(20)',
    type: 'varchar',
    sql: "'variable'",
  },
  {
    name: 'char',
    sqlType: 'char(5)',
    type: 'bpchar',
    sql: "'pad'",
    value: 'pad  ',
  },
  {
    name: 'uuid',
    sqlType: 'uuid',
    type: 'uuid',
    sql: "'0f4c6532-9204-4b89-a785-9c18d48524d5'",
  },
  {
    name: 'json',
    sqlType: 'json',
    type: 'json',
    sql: '\'{"value": [1, null, true, "é"], "n": 2}\'',
    value: { value: [1, null, true, 'é'], n: 2 },
  },
  {
    name: 'jsonb',
    sqlType: 'jsonb',
    type: 'jsonb',
    sql: '\'{"nested": {"x": null}, "list": []}\'',
    value: { nested: { x: null }, list: [] },
  },
  {
    name: 'binary',
    sqlType: 'bytea',
    type: 'bytea',
    sql: "decode('000aff80ff','hex')",
    value: '\\x000aff80ff',
  },
  { name: 'date', sqlType: 'date', type: 'date', sql: "'2024-02-29'" },
  { name: 'date_infinity', sqlType: 'date', type: 'date', sql: "'infinity'" },
  {
    name: 'timestamp',
    sqlType: 'timestamp',
    type: 'timestamp',
    sql: "'2026-10-15 12:34:56.123456'",
  },
  {
    name: 'timestamptz',
    sqlType: 'timestamptz',
    type: 'timestamptz',
    sql: "'2026-10-15 12:34:56.123456+00'",
  },
  { name: 'time', sqlType: 'time', type: 'time', sql: "'23:59:59.123456'" },
  {
    name: 'timetz',
    sqlType: 'timetz',
    type: 'timetz',
    sql: "'12:34:56.123456+02:30'",
  },
  {
    name: 'interval',
    sqlType: 'interval',
    type: 'interval',
    sql: "'2 years 3 mons 4 days 05:06:07.123456'",
  },
  { name: 'inet', sqlType: 'inet', type: 'inet', sql: "'2001:db8::1/64'" },
  { name: 'cidr', sqlType: 'cidr', type: 'cidr', sql: "'10.1.0.0/16'" },
  {
    name: 'macaddr',
    sqlType: 'macaddr',
    type: 'macaddr',
    sql: "'08:00:2b:01:02:03'",
  },
  { name: 'bits', sqlType: 'bit(5)', type: 'bit', sql: "B'10101'" },
  { name: 'varbits', sqlType: 'varbit', type: 'varbit', sql: "B'101'" },
  { name: 'range', sqlType: 'int4range', type: 'int4range', sql: "'[1,10)'" },
  { name: 'point', sqlType: 'point', type: 'point', sql: "'(1.25,-2.5)'" },
  {
    name: 'xml',
    sqlType: 'xml',
    type: 'xml',
    sql: "'<item>é &amp; value</item>'",
  },
  {
    name: 'ints',
    sqlType: 'integer[]',
    type: 'int4',
    dims: 1,
    sql: 'ARRAY[1,NULL,-2]',
    value: [1, null, -2],
  },
  {
    name: 'bigs',
    sqlType: 'bigint[]',
    type: 'int8',
    dims: 1,
    sql: 'ARRAY[9007199254740993,NULL,-9007199254740993]::bigint[]',
    value: [9007199254740993n, null, -9007199254740993n],
  },
  {
    name: 'texts',
    sqlType: 'text[]',
    type: 'text',
    dims: 1,
    sql: `ARRAY['NULL',NULL,'comma,quote"',E'back\\\\slash','é']`,
    value: ['NULL', null, 'comma,quote"', 'back\\slash', 'é'],
  },
  {
    name: 'bools',
    sqlType: 'boolean[]',
    type: 'bool',
    dims: 1,
    sql: 'ARRAY[true,false,NULL]',
    value: [true, false, null],
  },
  {
    name: 'matrix',
    sqlType: 'integer[][]',
    type: 'int4',
    dims: 2,
    sql: 'ARRAY[[1,2],[3,NULL]]',
    value: [
      [1, 2],
      [3, null],
    ],
  },
  {
    name: 'empty_array',
    sqlType: 'integer[]',
    type: 'int4',
    dims: 1,
    sql: "'{}'",
    value: [],
  },
  {
    name: 'json_array',
    sqlType: 'jsonb[]',
    type: 'jsonb',
    dims: 1,
    sql: `ARRAY['{"x":1}'::jsonb, 'null'::jsonb, NULL]`,
    value: [{ x: 1 }, null, null],
  },
]

const incrementalNames = new Set([
  'enum_array',
  'enum_matrix',
  'domain_array',
  'composite_array',
  'small',
  'integer',
  'big',
  'boolean',
  'real',
  'double',
  'nan',
  'positive_infinity',
  'negative_infinity',
  'exact',
  'quoted " text',
  'varchar',
  'char',
  'uuid',
  'json',
  'jsonb',
  'binary',
  'ints',
  'bigs',
  'texts',
  'bools',
  'matrix',
  'empty_array',
  'json_array',
  'date',
  'date_infinity',
  'timestamp',
  'timestamptz',
  'time',
  'timetz',
  'interval',
  'inet',
  'cidr',
  'macaddr',
  'bits',
  'varbits',
  'range',
  'point',
])
for (const profile of ['incremental', 'extended'] as const) {
  const cases =
    profile === 'incremental'
      ? allCases.filter((c) => incrementalNames.has(c.name))
      : allCases

  describe(`real Realtime Postgres type matrix / ${profile}`, () => {
    it.each([
      { liveSse: false, replica: 'default' },
      { liveSse: false, replica: 'full' },
      { liveSse: true, replica: 'default' },
      { liveSse: true, replica: 'full' },
    ] as const)(
      'preserves baseline, NULL transitions and live values ($replica, SSE=$liveSse)',
      async ({ liveSse, replica }) => {
        const databaseUrl = required('DATABASE_URL')
        const db = new Client({
          connectionString: databaseUrl,
          connectionTimeoutMillis: 15_000,
          query_timeout: 15_000,
        })
        const relay = await recordRealtimeWire(required('NEON_REALTIME_URL'))
        const namespace = `electric_types_${randomUUID().replaceAll('-', '')}`
        const table = `${quote(namespace)}.items`
        const schema = Object.fromEntries([
          ['id', { type: 'int4', not_null: true }],
          ...cases.map((c) => [
            c.name,
            { type: c.type, ...('dims' in c ? { dims: c.dims } : {}) },
          ]),
        ])
        const abort = new AbortController()
        const messages: Message<Row<unknown>>[] = []
        const errors: unknown[] = []
        const checkpoints: { name: string; txid?: string; rows: unknown[] }[] =
          []
        let snapshot: PostgresSnapshot | undefined
        let initialHandle: string | undefined
        let local: Awaited<ReturnType<typeof startLocalFunction>> | undefined
        try {
          await db.connect()
          // Match the documented Electric/Realtime wire settings for text oracles.
          // bpchar ::text strips padding, so its golden value stays explicit.
          await db.query(
            "set timezone='UTC'; set datestyle='ISO, YMD'; set intervalstyle='iso_8601'"
          )
          await db.query(`create schema ${quote(namespace)}`)
          await db.query(`
            create type ${quote(namespace)}.custom_mood as enum('sad','ok','happy');
            create domain ${quote(namespace)}.custom_positive as integer check(value>0);
            create type ${quote(namespace)}.custom_complex as (r double precision, i double precision)
          `)
          await db.query(
            `create table ${table}(id int primary key, ${cases.map((c) => `${quote(c.name)} ${'qualified' in c ? `${quote(namespace)}.` : ''}${c.sqlType}`).join(',')})`
          )
          await db.query(
            `insert into ${table} values(1,${cases.map((c) => c.sql).join(',')}),(2,${cases.map(() => 'NULL').join(',')})`
          )
          const oracle = (
            await db.query(
              `select ${cases.map((c) => `${quote(c.name)}::text as ${quote(c.name)}`).join(',')} from ${table} where id=1`
            )
          ).rows[0]
          const golden = Object.fromEntries(
            cases.map((c) => [c.name, 'value' in c ? c.value : oracle[c.name]])
          )
          const nulls = Object.fromEntries(cases.map((c) => [c.name, null]))
          local = await startLocalFunction({
            realtimeUrl: relay.url,
            artifact: `types-${profile}-${replica}-${liveSse ? 'sse' : 'poll'}`,
            tables: [
              {
                namespace,
                table: 'items',
                primaryKey: ['id'],
                columns: schema,
              },
            ],
          })
          const stream = new ShapeStream<Row<unknown>>({
            url: `${local.origin}/proxy/shapes`,
            params: { table: 'items', replica },
            liveSse,
            signal: abort.signal,
            fetchClient: async (input, init) => {
              const request = new Request(input, init)
              expect(new URL(request.url).pathname).toBe('/proxy/shapes')
              expect(request.method).toBe('GET')
              expect(new URL(request.url).origin).toBe(local!.origin)
              return fetch(request)
            },
            onError(error) {
              errors.push(error)
            },
          })
          const shape = new Shape(stream)
          stream.subscribe((batch) => {
            messages.push(...batch)
            for (const message of batch)
              if (
                isControlMessage(message) &&
                message.headers.control === 'snapshot-end'
              )
                snapshot = message.headers
          })
          const check = async (
            name: string,
            expected: Row<unknown>[],
            txid?: string
          ) => {
            await expect
              .poll(
                () => {
                  if (errors.length) throw errors[0]
                  return (
                    shape.isUpToDate &&
                    !!snapshot &&
                    (!txid ||
                      isVisibleInSnapshot(txid as `${bigint}`, snapshot))
                  )
                },
                { timeout: 30_000, interval: 50 }
              )
              .toBe(true)
            const rows = [...shape.currentRows].sort(
              (a, b) => Number(a.id) - Number(b.id)
            )
            expect(rows).toEqual(expected)
            if (profile === 'incremental') {
              initialHandle ??= stream.shapeHandle
              expect(stream.shapeHandle).toBe(initialHandle)
              expect(
                relay.messages.filter(
                  (m) => (m as { type: string }).type === 'baseline_sync_end'
                )
              ).toHaveLength(1)
            }
            checkpoints.push({ name, txid, rows })
          }
          await check('baseline', [
            { id: 1, ...golden },
            { id: 2, ...nulls },
          ])
          const resetTx = (
            await db.query(
              `update ${table} set ${cases.map((c) => `${quote(c.name)}=NULL`).join(',')} where id=1 returning pg_current_xact_id()::text as txid`
            )
          ).rows[0].txid as string
          await check(
            'all values to NULL',
            [
              { id: 1, ...nulls },
              { id: 2, ...nulls },
            ],
            resetTx
          )
          const update = messages.find(
            (m) =>
              isChangeMessage(m) &&
              m.headers.operation === 'update' &&
              m.value.id === 1
          )
          // Known incremental encodings have a strict operation/old-value gate.
          // The extended profile separately proves materialized values, including
          // backend cancellation/refetch. It does not satisfy event equivalence.
          if (profile === 'incremental') {
            expect(update).toBeDefined()
            if (replica === 'full')
              expect(update).toMatchObject({ old_value: golden })
            expect(
              relay.messages.filter(
                (m) => (m as { type: string }).type === 'baseline_sync_end'
              )
            ).toHaveLength(1)
          }
          const restoreTx = (
            await db.query(
              `update ${table} set ${cases.map((c) => `${quote(c.name)}=${c.sql}`).join(',')} where id=2 returning pg_current_xact_id()::text as txid`
            )
          ).rows[0].txid as string
          await check(
            'NULL to live values',
            [
              { id: 1, ...nulls },
              { id: 2, ...golden },
            ],
            restoreTx
          )
          const updatedCases = cases.filter((c) => 'updatedSql' in c)
          const updatedGolden = {
            ...golden,
            ...Object.fromEntries(
              updatedCases.map((c) => [
                c.name,
                'updatedValue' in c ? c.updatedValue : undefined,
              ])
            ),
          }
          const changedTx = (
            await db.query(
              `update ${table} set ${updatedCases.map((c) => `${quote(c.name)}=${'updatedSql' in c ? c.updatedSql : 'NULL'}`).join(',')} where id=2 returning pg_current_xact_id()::text as txid`
            )
          ).rows[0].txid as string
          await check(
            'custom arrays to different values',
            [
              { id: 1, ...nulls },
              { id: 2, ...updatedGolden },
            ],
            changedTx
          )
          const deleteTx = (
            await db.query(
              `delete from ${table} where id=2 returning pg_current_xact_id()::text as txid`
            )
          ).rows[0].txid as string
          await check('delete', [{ id: 1, ...nulls }], deleteTx)
          if (profile === 'incremental') {
            const live = messages
              .filter(isChangeMessage)
              .filter((message) => message.headers.txids?.length)
            expect(live.map((message) => message.headers.operation)).toEqual([
              'update',
              'update',
              'update',
              'delete',
            ])
            if (replica === 'full') {
              expect(live[1]).toMatchObject({ old_value: nulls })
              expect(live[2]).toMatchObject({
                old_value: Object.fromEntries(
                  updatedCases.map((c) => [c.name, golden[c.name]])
                ),
              })
            }
          }
          expect(errors).toEqual([])
        } finally {
          abort.abort()
          await local?.stop()
          await relay.close()
          await mkdir('.artifacts/staging', { recursive: true })
          await writeFile(
            `.artifacts/staging/types-${profile}-${replica}-${liveSse ? 'sse' : 'poll'}.json`,
            JSON.stringify(
              {
                columns: cases.map(({ name, sqlType, type }) => ({
                  name,
                  sqlType,
                  type,
                })),
                checkpoints,
                operations: messages
                  .filter(isChangeMessage)
                  .map((m) => ({ operation: m.headers.operation, key: m.key })),
                baselineEpochs: relay.messages.filter(
                  (m) => (m as { type: string }).type === 'baseline_sync_end'
                ).length,
                wire: relay.messages,
                errors: errors.map(String),
              },
              (_, value) =>
                typeof value === 'bigint'
                  ? `${value}n`
                  : typeof value === 'number' && !Number.isFinite(value)
                    ? String(value)
                    : value,
              2
            )
          )
          try {
            await db.query(`drop schema if exists ${quote(namespace)} cascade`)
          } finally {
            await db.end()
          }
        }
      },
      120_000
    )
  })
}
