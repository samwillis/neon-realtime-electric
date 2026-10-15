import { pathToFileURL } from 'node:url'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import type * as ClientApi from '../../../packages/client/src/index'
import { QueryShapeServer } from '../../transport/support/query-shape-server'
import { electricFixture, proxyFixture } from './fixtures'
import type { FixtureShape } from '../../transport/support/query-shape-server'
import type { RawChange } from '../../../packages/protocol/src/index'

async function installBaseline(server: QueryShapeServer, owner: FixtureShape) {
  owner.schema = {
    id: { type: 'int4', not_null: true },
    name: { type: 'text' },
    done: { type: 'bool' },
  }
  await server.append(owner, {
    version: 1,
    seq: '2',
    kind: 'snapshot-chunk',
    changes: [
      {
        key: '"public"."items"/"1"',
        value: { id: '1', name: 'first', done: 'f' },
        headers: { operation: 'insert' },
      },
    ],
  })
  await server.append(owner, {
    version: 1,
    seq: '3',
    kind: 'snapshot-complete',
    coveredSeq: '2',
    evidence: { snapshot: { xmin: '1', xmax: '2', xip_list: [] } },
  })
}

for (const kind of ['reference', 'candidate'] as const) {
  // The package harness installs each oracle/candidate in its own graph. No
  // TypeScript paths or workspace aliases participate in this runtime import.
  const entry = resolve(
    `.artifacts/package-compatibility/${kind}/node_modules/@electric-sql/client/dist/index.mjs`
  )
  const { ShapeStream, Shape } = (await import(
    pathToFileURL(entry).href
  )) as typeof ClientApi
  describe(`${kind} packed client through unchanged proxies`, () => {
    for (const mode of [
      'transparent',
      'electric-allowlist',
      'trusted-predicate',
    ] as const) {
      it(`${mode}: saved-offset resume`, async () => {
        const original =
          kind === 'reference' ? await electricFixture() : undefined
        const neon = kind === 'candidate' ? new QueryShapeServer() : undefined
        await neon?.start()
        let owner: FixtureShape | undefined
        if (neon)
          neon.onCreate = (shape) => {
            owner = shape
          }
        const proxy = await proxyFixture(
          original ? `${original.url}/api/shape` : `${neon!.url}/api/shape`,
          mode
        )
        const firstAbort = new AbortController()
        const secondAbort = new AbortController()
        const url = `${proxy.url}/application/shapes`
        const first = new ShapeStream({
          url,
          params: { table: 'items' },
          subscribe: false,
          signal: firstAbort.signal,
        })
        const initial = new Shape(first)
        try {
          if (neon) {
            await expect.poll(() => owner).toBeDefined()
            await installBaseline(neon, owner!)
          }
          await initial.rows
          const offset = first.lastOffset
          const handle = first.shapeHandle!
          firstAbort.abort()
          const changes: RawChange[] = [
            {
              key: 'two',
              value: { id: '2', name: 'offline insert', done: 't' },
              headers: { operation: 'insert' },
            },
          ]
          if (neon)
            await neon.append(owner!, {
              version: 1,
              seq: '4',
              kind: 'publication',
              changes,
              evidence: {},
            })
          else original!.publish(changes)
          const resumed = new Shape(
            new ShapeStream({
              url,
              params: { table: 'items' },
              offset,
              handle,
              subscribe: false,
              signal: secondAbort.signal,
            })
          )
          expect(await resumed.rows).toEqual([
            { id: 2, name: 'offline insert', done: true },
          ])
          const request = proxy.requests[proxy.requests.length - 1]
          expect(request.pathname).toBe('/application/shapes')
          expect(request.searchParams.get('handle')).toBe(handle)
          expect(request.searchParams.get('offset')).toBe(offset)
          if (neon) expect(neon.shapes.size).toBe(1)
        } finally {
          firstAbort.abort()
          secondAbort.abort()
          await proxy.stop()
          await original?.stop()
          await neon?.stop()
        }
      })

      it(`${mode}: live delivery, refresh and refetch`, async () => {
        const original =
          kind === 'reference' ? await electricFixture() : undefined
        const neon = kind === 'candidate' ? new QueryShapeServer() : undefined
        await neon?.start()
        let owner: FixtureShape | undefined
        if (neon)
          neon.onCreate = (shape) => {
            owner = shape
          }
        const proxy = await proxyFixture(
          original ? `${original.url}/api/shape` : `${neon!.url}/api/shape`,
          mode
        )
        const controller = new AbortController()
        const stream = new ShapeStream({
          url: `${proxy.url}/application/shapes`,
          params: { table: 'items' },
          signal: controller.signal,
        })
        const shape = new Shape(stream)
        try {
          if (neon) {
            await expect.poll(() => owner).toBeDefined()
            await installBaseline(neon, owner!)
          }
          await shape.rows
          const oldHandle = stream.shapeHandle
          const changes: RawChange[] = [
            {
              key: '"public"."items"/"1"',
              value: { name: 'changed' },
              headers: { operation: 'update' as const },
            },
            {
              key: '"public"."items"/"2"',
              value: { id: '2', name: 'second', done: 't' },
              headers: { operation: 'insert' as const },
            },
          ]
          if (neon)
            await neon.append(owner!, {
              version: 1,
              seq: '4',
              kind: 'publication',
              changes,
              evidence: {},
            })
          else original!.publish(changes)
          await expect
            .poll(() => shape.currentRows)
            .toEqual([
              { id: 1, name: 'changed', done: false },
              { id: 2, name: 'second', done: true },
            ])
          const deletion = [
            {
              key: '"public"."items"/"1"',
              value: { id: '1' },
              headers: { operation: 'delete' as const },
            },
          ]
          if (neon)
            await neon.append(owner!, {
              version: 1,
              seq: '5',
              kind: 'publication',
              changes: deletion,
              evidence: {},
            })
          else original!.publish(deletion)
          await expect
            .poll(() => shape.currentRows)
            .toEqual([{ id: 2, name: 'second', done: true }])
          await stream.forceDisconnectAndRefresh()
          if (neon) {
            neon.shapes.delete(owner!.handle)
            owner = undefined
            await expect.poll(() => owner).toBeDefined()
            await installBaseline(neon, owner!)
          } else original!.expire()
          await expect.poll(() => stream.shapeHandle).not.toBe(oldHandle)
          await expect.poll(() => stream.isUpToDate).toBe(true)
          expect(await shape.rows).toEqual([
            { id: 1, name: 'first', done: false },
          ])
          expect(
            proxy.requests.some(
              (url) => url.searchParams.get('live') === 'true'
            )
          ).toBe(true)
          expect(
            proxy.requests.every(
              (url) => url.pathname === '/application/shapes'
            )
          ).toBe(true)
          expect(
            proxy.requests.every(
              (url) =>
                !['sse', 'long-poll'].includes(
                  url.searchParams.get('live') ?? ''
                )
            )
          ).toBe(true)
        } finally {
          controller.abort()
          await proxy.stop()
          await original?.stop()
          await neon?.stop()
        }
      })

      it(mode, async () => {
        const original =
          kind === 'reference' ? await electricFixture() : undefined
        const neon = kind === 'candidate' ? new QueryShapeServer() : undefined
        await neon?.start()
        let owner: FixtureShape | undefined
        if (neon)
          neon.onCreate = (shape) => {
            owner = shape
          }
        const proxy = await proxyFixture(
          original ? `${original.url}/api/shape` : `${neon!.url}/api/shape`,
          mode
        )
        const controller = new AbortController()
        const outward: URL[] = []
        const stream = new ShapeStream({
          url: `${proxy.url}/application/shapes?custom=a%2Bb&custom=c`,
          params: { table: 'items' },
          subscribe: false,
          signal: controller.signal,
          fetchClient: async (input, init) => {
            outward.push(
              new URL(input instanceof Request ? input.url : input.toString())
            )
            return fetch(input, init)
          },
        })
        const shape = new Shape(stream)
        const rows = shape.rows
        try {
          if (neon) {
            // Repeated reads prove early creation returned through a proxy
            // whose 250ms timeout is shorter than baseline initialization.
            await expect
              .poll(() => proxy.requests.length)
              .toBeGreaterThanOrEqual(4)
            expect(stream.isUpToDate).toBe(false)
            owner!.schema = {
              id: { type: 'int4', not_null: true },
              name: { type: 'text' },
              done: { type: 'bool' },
            }
            await neon.append(owner!, {
              version: 1,
              seq: '2',
              kind: 'snapshot-chunk',
              changes: [
                {
                  key: '"public"."items"/"1"',
                  value: { id: '1', name: 'first', done: 'f' },
                  headers: { operation: 'insert' },
                },
              ],
            })
            await neon.append(owner!, {
              version: 1,
              seq: '3',
              kind: 'snapshot-complete',
              coveredSeq: '2',
              evidence: { snapshot: { xmin: '1', xmax: '2', xip_list: [] } },
            })
          }
          expect(await rows).toEqual([{ id: 1, name: 'first', done: false }])
          expect(
            outward.every(
              (url) =>
                url.origin === proxy.url &&
                url.pathname === '/application/shapes'
            )
          ).toBe(true)
          expect(
            outward.every(
              (url) => url.searchParams.getAll('custom').join('|') === 'a+b|c'
            )
          ).toBe(true)
          if (kind === 'candidate')
            expect(
              outward
                .slice(1)
                .every(
                  (url) => url.searchParams.get('handle') === owner!.handle
                )
            ).toBe(true)
          const upstreamRequests =
            original?.requests ?? neon!.requests.map((request) => request.url)
          if (mode === 'trusted-predicate') {
            expect(
              upstreamRequests.every(
                (url) =>
                  url.searchParams.get('where') === 'tenant_id = $1' &&
                  url.searchParams.get('params[1]') === 'trusted-tenant'
              )
            ).toBe(true)
          }
        } finally {
          controller.abort()
          await proxy.stop()
          await original?.stop()
          await neon?.stop()
        }
      })
    }
  })
}
