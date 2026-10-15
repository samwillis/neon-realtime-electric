import { createServer } from 'node:http'
import { once } from 'node:events'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createS3LogStore } from '../../packages/server/src/s3-store.js'

const cleanup: (() => unknown)[] = []
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close()
  vi.unstubAllEnvs()
})

async function fixture() {
  const objects = new Map<string, Buffer>()
  const requests: {
    method: string
    path: string
    authorization?: string
    cursor: string | null
  }[] = []
  const server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url!, 'http://local.test')
      requests.push({
        method: req.method!,
        path: url.pathname,
        authorization: req.headers.authorization,
        cursor: url.searchParams.get('continuation-token'),
      })
      if (
        url.pathname !== '/branch/test-bucket' &&
        !url.pathname.startsWith('/branch/test-bucket/')
      ) {
        res.writeHead(404).end()
        return
      }
      const key = decodeURIComponent(
        url.pathname.slice('/branch/test-bucket/'.length)
      )
      if (req.method === 'PUT') {
        const chunks: Buffer[] = []
        for await (const chunk of req) chunks.push(Buffer.from(chunk))
        objects.set(key, Buffer.concat(chunks))
        res.writeHead(200, { ETag: '"fixture"' }).end()
        return
      }
      if (req.method === 'DELETE') {
        objects.delete(key)
        res.writeHead(204).end()
        return
      }
      if (url.searchParams.get('list-type') === '2') {
        const keys = [...objects.keys()]
          .filter((k) => k.startsWith(url.searchParams.get('prefix') ?? ''))
          .sort()
        const cursor = url.searchParams.get('continuation-token')
        const selected = keys.filter((k) => !cursor || k > cursor).slice(0, 1)
        const truncated =
          selected.length > 0 && keys.some((k) => k > selected[0])
        res
          .writeHead(200, { 'content-type': 'application/xml' })
          .end(
            `<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><IsTruncated>${truncated}</IsTruncated>${truncated ? `<NextContinuationToken>${selected[0]}</NextContinuationToken>` : ''}${selected.map((k) => `<Contents><Key>${k}</Key><Size>${objects.get(k)!.length}</Size></Contents>`).join('')}</ListBucketResult>`
          )
        return
      }
      const value = objects.get(key)
      if (!value) {
        res
          .writeHead(404, { 'content-type': 'application/xml' })
          .end('<Error><Code>NoSuchKey</Code></Error>')
        return
      }
      res
        .writeHead(200, {
          'content-type': 'application/x-ndjson',
          'content-length': String(value.length),
        })
        .end(value)
    })().catch(() => res.writeHead(500).end())
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  cleanup.push(
    () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections()
        server.close(() => resolve())
      })
  )
  vi.stubEnv(
    'AWS_ENDPOINT_URL_S3',
    `http://127.0.0.1:${(server.address() as { port: number }).port}/branch`
  )
  vi.stubEnv('AWS_REGION', 'us-east-2')
  vi.stubEnv('AWS_ACCESS_KEY_ID', 'fixture-access-key')
  vi.stubEnv('AWS_SECRET_ACCESS_KEY', 'fixture-secret-key')
  const store = createS3LogStore({ bucket: 'test-bucket' })
  cleanup.push(() => store.close())
  return { store, objects, requests }
}

describe('real AWS SDK adapter against an HTTP S3 fixture', () => {
  it('uses SigV4 path-style requests with endpoint paths, raw bytes and idempotent deletion', async () => {
    const { store, requests } = await fixture()
    const bytes = Uint8Array.from([0, 255, 10, 13, 42])
    await store.put('shapes/range.ndjson', bytes)
    expect(await store.get('shapes/range.ndjson')).toEqual(bytes)
    await store.delete('shapes/range.ndjson')
    await store.delete('shapes/range.ndjson')
    expect(
      requests.every(
        (r) => r.path === '/branch/test-bucket/shapes/range.ndjson'
      )
    ).toBe(true)
    expect(
      requests.every((r) => r.authorization?.startsWith('AWS4-HMAC-SHA256 '))
    ).toBe(true)
    await expect(store.get('missing')).rejects.toMatchObject({
      name: 'NoSuchKey',
    })
  })
  it('sweeps only expired shape prefixes, including all paginated pages', async () => {
    const { store, objects, requests } = await fixture()
    const values = [
      'shapes/1700000000000/a/1.ndjson',
      'shapes/1700000000001/b/1.ndjson',
      'shapes/1900000000000/c/1.ndjson',
      'shapes/not-an-expiry/file',
      'other/1700000000000/d/1.ndjson',
    ]
    for (const key of values) objects.set(key, Buffer.from('data'))
    expect(await store.sweepExpired('shapes', 1800000000000)).toBe(2)
    expect([...objects.keys()].sort()).toEqual(values.slice(2).sort())
    expect(requests.filter((r) => r.cursor).length).toBeGreaterThan(0)
    await expect(store.sweepExpired('')).rejects.toThrow('dedicated')
  })
})
