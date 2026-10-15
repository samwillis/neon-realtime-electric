import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { createFixture } from './support/fixture.js'

for (const objects of [false, true])
  describe(`JSON read profile: ${objects ? 'objects' : 'memory'}`, () => {
    let f: Awaited<ReturnType<typeof createFixture>>
    let url: URL
    beforeEach(async () => {
      f = await createFixture(objects)
      url = new URL('/fixture', f.baseUrl)
    })
    afterEach(() => f.close())
    const create = async (value?: unknown, expiresAt?: number) => {
      const result = await fetch(url, {
        method: 'PUT',
        headers: {
          'content-type': 'application/json',
          ...(expiresAt
            ? { 'Stream-Expires-At': new Date(expiresAt).toISOString() }
            : {}),
        },
        ...(value === undefined ? {} : { body: JSON.stringify(value) }),
      })
      expect(result.status).toBe(201)
      return result
    }
    const append = async (value: unknown) => {
      const result = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(value),
      })
      expect(result.status).toBe(204)
      return result
    }
    it('HEAD returns the tail and expiry without reading object bytes', async () => {
      const expiresAt = Date.now() + 60_000
      await create([{ id: 1 }, { id: 2 }], expiresAt)
      const before = f.store.gets.length
      const head = await fetch(url, { method: 'HEAD' })
      expect(head.status).toBe(200)
      expect(await head.text()).toBe('')
      expect(head.headers.get('content-type')).toBe('application/json')
      expect(head.headers.get('Stream-Expires-At')).toBe(
        new Date(expiresAt).toISOString()
      )
      expect(f.store.gets.length).toBe(before)
      const read = await fetch(url)
      expect(head.headers.get('Stream-Next-Offset')).toBe(
        read.headers.get('Stream-Next-Offset')
      )
      expect(await read.json()).toEqual([{ id: 1 }, { id: 2 }])
      if (objects) expect(f.store.gets.length).toBeGreaterThan(before)
    })
    it('resumes immutable JSON messages without duplicates across sealed ranges', async () => {
      await create([{ id: 1 }])
      const first = await fetch(url)
      expect(await first.json()).toEqual([{ id: 1 }])
      const offset = first.headers.get('Stream-Next-Offset')!
      await append([{ id: 2 }, [3, 4], null])
      url.searchParams.set('offset', offset)
      const replay = await fetch(url)
      expect(await replay.json()).toEqual([{ id: 2 }, [3, 4], null])
      const end = replay.headers.get('Stream-Next-Offset')!
      expect(end > offset).toBe(true)
      url.searchParams.set('offset', end)
      const tail = await fetch(url)
      expect(tail.status).toBe(200)
      expect(await tail.json()).toEqual([])
      expect(tail.headers.get('Stream-Up-To-Date')).toBe('true')
      url.searchParams.set('offset', '-1')
      expect(await (await fetch(url)).json()).toEqual([
        { id: 1 },
        { id: 2 },
        [3, 4],
        null,
      ])
    })
    it('long polling returns numeric cursors with data and on 204 timeout', async () => {
      await create([{ id: 1 }])
      url.searchParams.set('offset', '-1')
      url.searchParams.set('live', 'long-poll')
      const first = await fetch(url)
      expect(first.status).toBe(200)
      expect(await first.json()).toEqual([{ id: 1 }])
      expect(first.headers.get('Stream-Cursor')).toMatch(/^\d+$/)
      url.searchParams.set('offset', first.headers.get('Stream-Next-Offset')!)
      const future = String(BigInt(first.headers.get('Stream-Cursor')!) + 1000n)
      url.searchParams.set('cursor', future)
      const timeout = await fetch(url)
      expect(timeout.status).toBe(204)
      expect(await timeout.text()).toBe('')
      expect(timeout.headers.get('Stream-Up-To-Date')).toBe('true')
      expect(timeout.headers.get('Stream-Next-Offset')).toBe(
        url.searchParams.get('offset')
      )
      expect(BigInt(timeout.headers.get('Stream-Cursor')!)).toBeGreaterThan(
        BigInt(future)
      )
    })
    it('delivers new JSON at a saved tail and supports now without historical data', async () => {
      await create([{ id: 1 }])
      url.searchParams.set('offset', 'now')
      const now = await fetch(url)
      expect(await now.json()).toEqual([])
      const tail = now.headers.get('Stream-Next-Offset')!
      url.searchParams.set('offset', tail)
      url.searchParams.set('live', 'long-poll')
      const waiting = f.nextWait()
      const pending = fetch(url)
      await waiting
      await append([{ id: 2 }])
      const result = await pending
      expect(result.status).toBe(200)
      expect(await result.json()).toEqual([{ id: 2 }])
    })
    it('rejects malformed or ambiguous read parameters with security headers', async () => {
      await create()
      for (const query of [
        'offset=',
        'offset=a,b',
        'offset=a%20b',
        'offset=-1&offset=-1',
        'cursor=1&cursor=2',
        'live=sse&live=long-poll',
        'live=long-poll',
        'live=sse',
      ]) {
        url.search = query
        const result = await fetch(url)
        expect(result.status, query).toBe(400)
        expect(result.headers.get('x-content-type-options')).toBe('nosniff')
        expect(result.headers.get('cache-control')).toBe('no-store')
      }
    })
    it('keeps GET, HEAD, live and error responses out of caches', async () => {
      await create()
      for (const method of ['GET', 'HEAD']) {
        const result = await fetch(url, {
          method,
          headers: { 'if-none-match': '*' },
        })
        expect(result.status).toBe(200)
        expect(result.headers.get('cache-control')).toBe('no-store')
        expect(result.headers.get('x-content-type-options')).toBe('nosniff')
        expect(result.headers.get('cross-origin-resource-policy')).toBe(
          'cross-origin'
        )
        await result.body?.cancel()
      }
      url.search = 'offset=-1&live=long-poll'
      const timeout = await fetch(url)
      expect(timeout.status).toBe(204)
      expect(timeout.headers.get('cache-control')).toBe('no-store')
      url.pathname = '/missing'
      expect((await fetch(url)).status).toBe(404)
    })
    it('does not renew absolute expiry on reads or writes', async () => {
      f.setNow(1000)
      const expiresAt = 2000
      await create([{ id: 1 }], expiresAt)
      f.setNow(1500)
      await append([{ id: 2 }])
      f.setNow(1999)
      const head = await fetch(url, { method: 'HEAD' })
      expect(head.status).toBe(200)
      expect(head.headers.get('Stream-Expires-At')).toBe(
        new Date(expiresAt).toISOString()
      )
      expect(await (await fetch(url)).json()).toEqual([{ id: 1 }, { id: 2 }])
      f.setNow(expiresAt)
      expect((await fetch(url, { method: 'HEAD' })).status).toBe(404)
      expect((await fetch(url)).status).toBe(404)
    })
    it('revalidates expiry after a long-poll wait', async () => {
      f.setNow(1000)
      await create(undefined, 2000)
      url.search = 'offset=-1&live=long-poll'
      const waiting = f.nextWait()
      const pending = fetch(url)
      await waiting
      f.setNow(2000)
      expect((await pending).status).toBe(404)
    })
    it('cancels a waiting poll without poisoning the stream', async () => {
      await create()
      url.search = 'offset=-1&live=long-poll'
      const abort = new AbortController()
      const waiting = f.nextWait()
      const pending = fetch(url, { signal: abort.signal })
      await waiting
      abort.abort()
      await expect(pending).rejects.toMatchObject({ name: 'AbortError' })
      await append([{ id: 1 }])
      const result = await fetch(url)
      expect(await result.json()).toEqual([{ id: 1 }])
    })
  })
