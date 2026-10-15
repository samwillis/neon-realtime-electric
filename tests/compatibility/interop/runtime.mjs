import assert from 'node:assert/strict'
import { once } from 'node:events'
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import path from 'node:path'
import { Readable } from 'node:stream'
import { pathToFileURL } from 'node:url'
import { createShapeHandler } from '../../../packages/server/src/handler.ts'
import { ObjectShapeLog } from '../../../packages/server/src/object-log.ts'
import { FixtureObjectStore } from '../../server/support/object-store.ts'

const fixture = process.argv[2]
assert.ok(fixture, 'Pass the isolated package fixture directory')
const require = createRequire(path.join(fixture, 'package.json'))
async function load(name, format) {
  if (format === 'cjs') return require(name)
  const metadataPath = require.resolve(`${name}/package.json`)
  const metadata = JSON.parse(await readFile(metadataPath, 'utf8'))
  return import(
    pathToFileURL(
      path.resolve(
        path.dirname(metadataPath),
        metadata.exports['.'].import.default
      )
    ).href
  )
}

let scenarios = 0
for (const originalFormat of ['esm', 'cjs'])
  for (const candidateFormat of ['esm', 'cjs']) {
    const original = await load('electric-reference', originalFormat)
    const candidate = await load('electric-candidate', candidateFormat)
    assert.notEqual(original.Shape, candidate.Shape)
    for (const backend of ['memory', 'objects'])
      for (const liveSse of [false, true]) {
        await scenario(original, candidate, backend, liveSse)
        scenarios++
      }
  }
console.log(`Cross-package materializers: ${scenarios} scenarios passed`)

async function scenario(original, candidate, backend, liveSse) {
  const observers = []
  const requests = []
  const errors = []
  const store = new FixtureObjectStore()
  const date = '2026-10-15T12:00:00+00:00'
  let title = 'initial'
  let txid = 10
  const key = (id) => String(id).repeat(64)
  const handler = createShapeHandler({
    longPollMs: 30,
    sseConnectionMs: 100,
    createLog:
      backend === 'objects'
        ? ({ handle, expiresAt }) =>
            new ObjectShapeLog({
              store,
              prefix: `interop/${expiresAt}/${handle}`,
              sealBytes: 1,
            })
        : undefined,
    source: {
      async subscribe(_, observer) {
        observers.push(observer)
        observer.event({
          type: 'schema',
          columns: [
            { name: 'id', type_oid: 23, typmod: -1, codec: 'pg_text' },
            { name: 'title', type_oid: 25, typmod: -1, codec: 'pg_text' },
            {
              name: 'created_at',
              type_oid: 1184,
              typmod: -1,
              codec: 'pg_text',
            },
          ],
        })
        observer.event({
          type: 'baseline',
          rows: [{ row_key: key(1), values: ['1', title, date] }],
          mvcc: { xmin: '10', xmax: '10', xip: [] },
        })
        return { close() {} }
      },
      close() {},
    },
    async authorize() {
      return {
        authorizationKey: 'interop-fixture',
        definition: {
          namespace: 'public',
          table: 'tasks',
          primaryKey: ['id'],
          projection: ['id', 'title', 'created_at'],
          schema: {
            id: { type: 'int4' },
            title: { type: 'text' },
            created_at: { type: 'timestamptz' },
          },
          query: { sql: 'select id, title, created_at from public.tasks' },
        },
      }
    },
  })
  const server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url, 'http://fixture')
      requests.push({ url, method: req.method })
      const abort = new AbortController()
      res.on('close', () => abort.abort())
      const response = await handler.fetch(
        new Request(url, { method: req.method, signal: abort.signal })
      )
      res.writeHead(response.status, Object.fromEntries(response.headers))
      if (response.body)
        Readable.fromWeb(response.body)
          .on('error', () => res.destroy())
          .pipe(res)
      else res.end()
    })().catch((error) => {
      errors.push(error)
      res.destroy()
    })
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const abort = new AbortController()
  let stream
  const shapes = []
  try {
    stream = new candidate.ShapeStream({
      url: `http://127.0.0.1:${server.address().port}/application/shapes?tenant=a%2Bb`,
      params: { table: 'tasks' },
      parser: { timestamptz: (value) => new Date(value) },
      columnMapper: candidate.snakeCamelMapper(),
      signal: abort.signal,
      liveSse,
      onError: (error) => {
        errors.push(error)
      },
    })
    shapes.push(new original.Shape(stream), new candidate.Shape(stream))
    const row = (id, title) => ({ id, title, createdAt: new Date(date) })
    const expectRows = (rows) =>
      Promise.all(shapes.map((shape) => waitRows(shape, rows)))
    await expectRows([row(1, title)])
    const firstHandle = stream.shapeHandle
    assert.ok(firstHandle)
    const publish = (changes) => {
      txid++
      observers[0].event({
        type: 'publication',
        changes,
        txids: [String(txid)],
        frontier: { lsn: `0/${txid.toString(16).toUpperCase()}` },
      })
    }
    publish([
      { op: 'upsert', row_key: key(2), values: ['2', 'inserted', date] },
    ])
    await expectRows([row(1, title), row(2, 'inserted')])
    title = 'updated'
    publish([{ op: 'upsert', row_key: key(1), values: ['1', title, date] }])
    await expectRows([row(1, title), row(2, 'inserted')])
    publish([{ op: 'remove', row_key: key(2) }])
    await expectRows([row(1, title)])
    // A distinct baseline proves that both materializers consume the reset.
    title = 'after owner loss'
    observers[0].error(new Error('intentional interop owner loss'))
    await expectRows([row(1, title)])
    assert.notEqual(stream.shapeHandle, firstHandle)
    assert.equal(observers.length, 2)
    for (const shape of shapes) {
      assert.equal(shape.handle, stream.shapeHandle)
      assert.equal(shape.lastOffset, stream.lastOffset)
      assert.equal(shape.isUpToDate, true)
      assert.deepEqual(await shape.rows, [row(1, title)])
    }
    for (const { url, method } of requests) {
      assert.equal(url.pathname, '/application/shapes')
      assert.equal(url.searchParams.get('tenant'), 'a+b')
      assert.equal(method, 'GET')
    }
    assert.ok(requests.some(({ url }) => url.searchParams.has('handle')))
    if (liveSse)
      assert.ok(
        requests.some(({ url }) => url.searchParams.get('live_sse') === 'true')
      )
    if (backend === 'objects') assert.ok(store.gets.length > 0)
    assert.deepEqual(errors, [])
  } finally {
    abort.abort()
    for (const shape of shapes) shape.unsubscribeAll()
    stream?.unsubscribeAll()
    handler.close()
    await handler.drain()
    server.closeAllConnections()
    await new Promise((resolve) => server.close(resolve))
  }
}

function waitRows(shape, expected) {
  return new Promise((resolve, reject) => {
    let unsubscribe = () => {}
    const timer = setTimeout(() => {
      unsubscribe()
      reject(
        new Error(
          `Materializer did not reach ${JSON.stringify(expected)}; got ${JSON.stringify(shape.currentRows)}`
        )
      )
    }, 5_000)
    const check = () => {
      try {
        assert.deepEqual(shape.currentRows, expected)
        assert.equal(shape.isUpToDate, true)
      } catch {
        return
      }
      clearTimeout(timer)
      unsubscribe()
      resolve()
    }
    unsubscribe = shape.subscribe(check)
    check()
  })
}
