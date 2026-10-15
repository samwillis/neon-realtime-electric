import { fork, type ChildProcess } from 'node:child_process'
import { createServer } from 'node:http'
import { once } from 'node:events'
import { afterEach, describe, expect, it } from 'vitest'
import { Shape, ShapeStream } from '../../packages/client/src/index.js'

const cleanup: (() => unknown)[] = []
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close()
})
async function worker() {
  const child = fork(
    new URL('./support/owner-process.ts', import.meta.url),
    [],
    { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'pipe', 'pipe', 'ipc'] }
  )
  let errors = ''
  child.stderr!.on('data', (data) => {
    errors += data.toString()
  })
  const ready = await new Promise<{ port: number; pid: number }>(
    (resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`Owner fixture did not start: ${errors}`)),
        5000
      )
      child.once('message', (message) => {
        clearTimeout(timer)
        resolve(message as { port: number; pid: number })
      })
      child.once('error', (error) => {
        clearTimeout(timer)
        reject(error)
      })
      child.once('exit', (code) => {
        clearTimeout(timer)
        reject(new Error(`Owner exited ${code}: ${errors}`))
      })
    }
  )
  cleanup.push(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      const stopped = once(child, 'exit')
      child.kill('SIGTERM')
      await stopped
    }
  })
  return { ...ready, child, url: `http://127.0.0.1:${ready.port}` }
}

describe('two independent local owner processes with controlled affinity', () => {
  it('routes by query handle across offset/cursor changes and refetches at the same URL after owner death', async () => {
    const a = await worker(),
      b = await worker()
    const bindings = new Map<string, typeof a>()
    const requests: { handle: string | null; owner: number; path: string }[] =
      []
    const router = createServer((req, res) => {
      void (async () => {
        const outward = new URL(req.url!, 'http://router.test')
        const handle = outward.searchParams.get('handle')
        let target = handle ? (bindings.get(handle) ?? a) : a
        requests.push({ handle, owner: target.pid, path: outward.pathname })
        const controller = new AbortController()
        res.on('close', () => controller.abort())
        let response: Response
        try {
          response = await fetch(`${target.url}${req.url}`, {
            signal: controller.signal,
          })
        } catch (error) {
          if (controller.signal.aborted) throw error
          // Controlled fallback: a missing owner receives the old handle and
          // must create a fresh epoch, never reconstruct its in-memory index.
          target = b
          requests.push({ handle, owner: target.pid, path: outward.pathname })
          response = await fetch(`${target.url}${req.url}`, {
            signal: controller.signal,
          })
        }
        const assigned = response.headers.get('electric-handle')
        if (assigned) bindings.set(assigned, target)
        res.writeHead(response.status, Object.fromEntries(response.headers))
        res.end(Buffer.from(await response.arrayBuffer()))
      })().catch(() => {
        if (!res.destroyed) res.writeHead(502).end()
      })
    })
    router.listen(0, '127.0.0.1')
    await once(router, 'listening')
    cleanup.push(
      () =>
        new Promise<void>((resolve) => {
          router.closeAllConnections()
          router.close(() => resolve())
        })
    )
    const url = `http://127.0.0.1:${(router.address() as { port: number }).port}/api/shape`
    const initial = await fetch(`${url}?table=items&offset=-1`)
    const handle = initial.headers.get('electric-handle')!
    const firstOffset = initial.headers.get('electric-offset')!
    await initial.arrayBuffer()
    // Ingest while there are no readers, then resume from the initial boundary.
    for (const owner of [a, b])
      expect(
        (
          await fetch(`${owner.url}/__fixture/write`, {
            method: 'POST',
            body: JSON.stringify({ label: 'without readers' }),
          })
        ).status
      ).toBe(204)
    for (const cursor of ['1', '2']) {
      const response = await fetch(
        `${url}?table=items&handle=${handle}&offset=${firstOffset}&cursor=${cursor}`
      )
      expect(response.status).toBe(200)
      await response.arrayBuffer()
    }
    expect(
      requests
        .filter((r) => r.handle === handle)
        .every((r) => r.owner === a.pid)
    ).toBe(true)
    const controller = new AbortController()
    const stream = new ShapeStream<{ id: number; label: string }>({
      url,
      params: { table: 'items' },
      handle,
      offset: '-1',
      signal: controller.signal,
    })
    const shape = new Shape(stream)
    cleanup.push(() => controller.abort())
    await expect
      .poll(() => shape.currentRows)
      .toEqual([{ id: 1, label: 'without readers' }])
    const stopped = once(a.child, 'exit')
    a.child.kill('SIGKILL')
    await stopped
    await expect
      .poll(() => stream.shapeHandle !== handle, { timeout: 5000 })
      .toBe(true)
    await expect
      .poll(() => shape.currentRows)
      .toEqual([{ id: 1, label: 'without readers' }])
    expect(bindings.get(stream.shapeHandle!)?.pid).toBe(b.pid)
    expect(requests.every((r) => r.path === '/api/shape')).toBe(true)
    expect(requests.some((r) => r.handle === handle && r.owner === b.pid)).toBe(
      true
    )
    expect(a.pid).not.toBe(b.pid)
  })
})
