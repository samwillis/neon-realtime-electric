import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { pathToFileURL, fileURLToPath } from 'node:url'
import { mkdir, writeFile } from 'node:fs/promises'
import pg from 'pg'
const root = fileURLToPath(new URL('../', import.meta.url))
const cwd = `${root}.artifacts/integration-packages/candidate`
const require = createRequire(`${cwd}/package.json`)
const { chromium } = require('playwright-core')
const { preview } = await import(pathToFileURL(require.resolve('vite')).href)
assert(
  process.env.DATABASE_URL,
  'Browser integration requires staging DATABASE_URL'
)
const db = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 15_000,
  query_timeout: 15_000,
})
const serving = await preview({
  root: cwd,
  configFile: false,
  preview: {
    host: '127.0.0.1',
    port: 0,
    proxy: {
      '/application/shapes': {
        target: process.env.SHAPE_URL ?? 'http://localhost:4327',
        rewrite: (url) => url.replace(/^\/application\/shapes/, '/api/shape'),
      },
    },
  },
})
const origin = `http://127.0.0.1:${serving.httpServer.address().port}`
const browser = await chromium.launch({
  executablePath:
    process.env.CHROME_PATH ??
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  headless: true,
})
const report = {
  startedAt: new Date().toISOString(),
  status: 'running',
  modes: [],
  responses: [],
}
const id = 100 + Math.floor(Math.random() * 1_000_000_000)
const handles = new Set()
await db.connect()
try {
  await db.query(
    'insert into public.electric_items(id,label,enabled) values($1,$2,true)',
    [id, 'browser baseline']
  )
  for (const mode of ['basic', 'custom', 'react', 'tanstack']) {
    const context = await browser.newContext()
    const page = await context.newPage()
    let rejected = false
    await page.route('**/application/shapes?**', async (route) => {
      if (!rejected) {
        rejected = true
        await route.fulfill({
          status: 401,
          contentType: 'application/json',
          body: '{"error":"Expired example token"}',
        })
      } else await route.continue()
    })
    const errors = []
    const requests = []
    page.on('pageerror', (error) => errors.push(error.message))
    page.on('request', (request) => {
      if (new URL(request.url()).pathname === '/application/shapes')
        requests.push(request)
    })
    page.on('response', (response) => {
      const handle = response.headers()['electric-handle']
      if (handle) handles.add(handle)
      if (new URL(response.url()).pathname === '/application/shapes') {
        const entry = {
          mode,
          status: response.status(),
          offset: new URL(response.url()).searchParams.get('offset'),
          schema: response.headers()['electric-schema'],
          handle,
        }
        report.responses.push(entry)
        void response
          .json()
          .then((body) => {
            entry.kinds = Array.isArray(body) ? body.map((e) => e.kind) : []
          })
          .catch(() => {})
      }
    })
    const rows = async () => {
      const value = JSON.parse(await page.getByTestId('rows').innerText())
      return mode === 'custom'
        ? value.map((row) => ({
            id: row.itemId,
            label: row.title,
            enabled: row.enabled,
          }))
        : value
    }
    const expected = (label) =>
      mode === 'custom' ? label.toUpperCase() : label
    const until = async (predicate) => {
      const end = Date.now() + 25_000
      while (Date.now() < end) {
        assert.deepEqual(errors, [])
        if (await predicate()) return
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      throw new Error(`${mode}: condition timed out`)
    }
    await page.goto(`${origin}/?mode=${mode}&item=${id}`)
    await page.getByTestId('rows').waitFor()
    await until(async () =>
      (await rows()).some(
        (r) => r.id === id && r.label === expected('browser baseline')
      )
    )
    const label = `${mode} live`
    await db.query('update public.electric_items set label=$2 where id=$1', [
      id,
      label,
    ])
    await until(async () =>
      (await rows()).some((r) => r.id === id && r.label === expected(label))
    )
    // Kill only this owner and require transparent refetch through the same proxy.
    const oldHandle = [...requests]
      .reverse()
      .map((r) => new URL(r.url()).searchParams.get('handle'))
      .find(Boolean)
    assert(oldHandle)
    const deleteUrl = new URL(
      '/api/shape?table=electric_items',
      process.env.SHAPE_URL ?? 'http://localhost:4327'
    )
    deleteUrl.searchParams.set('handle', oldHandle)
    assert.equal((await fetch(deleteUrl, { method: 'DELETE' })).status, 204)
    await until(
      async () =>
        requests.some((r) => {
          const h = new URL(r.url()).searchParams.get('handle')
          return h && h !== oldHandle
        }) &&
        (await rows()).some((r) => r.id === id && r.label === expected(label))
    )
    if (mode === 'tanstack') {
      let release
      const gate = new Promise((resolve) => {
        release = resolve
      })
      let mutation
      await page.route('**/api/items', async (route) => {
        mutation = route.request().postDataJSON()
        await gate
        const result = await db.query(
          'update public.electric_items set label=$2 where id=$1 returning pg_current_xact_id()::text as txid',
          [mutation.id, mutation.label]
        )
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            txid: Number(BigInt.asUintN(32, BigInt(result.rows[0].txid))),
          }),
        })
      })
      await page.getByRole('button', { name: 'Update first row' }).click()
      await until(
        async () =>
          mutation &&
          (await rows()).some(
            (r) => r.id === mutation.id && r.label === 'optimistic'
          )
      )
      assert.equal(await page.getByTestId('mutation').innerText(), 'pending')
      const sqlBefore = await db.query(
        'select label from public.electric_items where id=$1',
        [mutation.id]
      )
      assert.notEqual(sqlBefore.rows[0].label, 'optimistic')
      release()
      await until(
        async () =>
          (await page.getByTestId('mutation').innerText()) === 'persisted'
      )
      await page.unroute('**/api/items')
      await page.route('**/api/items', async (route) => {
        const body = route.request().postDataJSON()
        assert.equal(body.noop, true)
        assert.equal(body.id, id)
        const result = await db.query(
          'update public.electric_items set label=label where id=$1 returning pg_current_xact_id()::text as txid',
          [id]
        )
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            txid: Number(BigInt.asUintN(32, BigInt(result.rows[0].txid))),
          }),
        })
      })
      await page
        .getByRole('button', { name: 'Acknowledge unchanged row' })
        .click()
      await until(
        async () =>
          (await page.getByTestId('mutation').innerText()) === 'acknowledged'
      )
    }
    assert(requests.length > 2)
    for (const request of requests) {
      const url = new URL(request.url())
      assert.equal(url.origin, origin)
      assert.equal(url.pathname, '/application/shapes')
      assert.equal(url.searchParams.get('table'), 'electric_items')
      assert.equal(url.searchParams.get('example'), 'integration')
      assert.equal(request.method(), 'GET')
    }
    assert(
      new Set(requests.map((r) => r.headers().authorization)).size > 1,
      'Dynamic auth was not refreshed'
    )
    assert(
      requests.some((r) =>
        r.headers().authorization?.startsWith('refreshed-token-')
      ),
      'Public FetchError identity or onError auth recovery failed'
    )
    report.modes.push({ mode, requests: requests.length, status: 'passed' })
    await context.close()
    await db.query('update public.electric_items set label=$2 where id=$1', [
      id,
      'browser baseline',
    ])
  }
  report.status = 'passed'
  console.log(
    'Packed basic, published React hook and eager TanStack browser integrations passed against local neon dev/staging data'
  )
} catch (error) {
  report.status = 'failed'
  report.error = String(error)
  throw error
} finally {
  report.completedAt = new Date().toISOString()
  await browser.close()
  serving.httpServer.closeAllConnections?.()
  await new Promise((resolve) => serving.httpServer.close(resolve))
  await db.query('delete from public.electric_items where id=$1', [id])
  for (const handle of handles) {
    const url = new URL(
      '/api/shape?table=electric_items',
      process.env.SHAPE_URL ?? 'http://localhost:4327'
    )
    url.searchParams.set('handle', handle)
    await fetch(url, { method: 'DELETE', signal: AbortSignal.timeout(10_000) })
  }
  await db.end()
  await mkdir(`${root}.artifacts/staging`, { recursive: true })
  await writeFile(
    `${root}.artifacts/staging/browser-integrations.json`,
    JSON.stringify(report, null, 2)
  )
}
