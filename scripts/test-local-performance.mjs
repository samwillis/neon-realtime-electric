import assert from 'node:assert/strict'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import pg from 'pg'
import {
  Shape,
  ShapeStream,
  isChangeMessage,
  isControlMessage,
  isVisibleInSnapshot,
} from '../packages/client/dist/index.mjs'

const base = new URL(process.env.SHAPE_URL ?? 'http://localhost:4327/api/shape')
assert(
  ['localhost', '127.0.0.1', '[::1]'].includes(base.hostname),
  'Performance acceptance requires local neon dev'
)
assert(
  process.env.DATABASE_URL,
  'Performance acceptance requires private DATABASE_URL'
)
const diagnosticsFile = process.env.NEON_SHAPE_DIAGNOSTICS_FILE
assert(
  diagnosticsFile,
  'Start neon dev with NEON_SHAPE_DIAGNOSTICS=true and NEON_SHAPE_DIAGNOSTICS_FILE'
)
const sizes = (process.env.BENCHMARK_ROWS ?? '1000,5000').split(',').map(Number)
const readers = (process.env.BENCHMARK_READERS ?? '1,5').split(',').map(Number)
const payloadChars = Number(process.env.BENCHMARK_PAYLOAD_CHARS ?? 512)
const mutations = Number(process.env.BENCHMARK_MUTATIONS ?? 8)
const sealBytes = Number(process.env.NEON_SHAPE_SEAL_BYTES ?? 262144)
assert(Number.isSafeInteger(sealBytes) && sealBytes > 0)
assert(sizes.every((n) => Number.isSafeInteger(n) && n > 0 && n <= 20_000))
assert(readers.every((n) => Number.isSafeInteger(n) && n > 0 && n <= 20))
assert(
  Number.isSafeInteger(payloadChars) && payloadChars > 0 && payloadChars <= 2048
)
assert(Number.isSafeInteger(mutations) && mutations >= 3 && mutations <= 100)
const report = {
  startedAt: new Date().toISOString(),
  status: 'running',
  environment: 'local neon dev, real Postgres/Realtime and Object Storage',
  sampleIntervalMs: 100,
  sealBytes,
  replica: 'full',
  sizes,
  readers,
  payloadChars,
  mutations,
  scenarios: [],
  limits: [
    'Local-to-service network latency is included; no deployed capacity claim.',
    '100ms process memory samples can miss shorter peaks and include SDK/runtime overhead.',
    'Retained rows are counted; process heap/RSS deltas are not exact per-row allocation measurements.',
    'The fixture uses int4/text/bool and one source owner shared by readers.',
    'Fan-out replays deliberately reread stored history; no object cache is assumed.',
  ],
}
const db = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 15_000,
  query_timeout: 30_000,
})
const diagnostics = async () =>
  (await readFile(diagnosticsFile, 'utf8'))
    .split('\n')
    .filter(Boolean)
    .flatMap((line) => {
      // The final line can be incomplete while the Function appends its sample.
      try {
        return [JSON.parse(line)]
      } catch {
        return []
      }
    })
const summary = (values) => {
  const sorted = [...values].sort((a, b) => a - b)
  assert(sorted.length)
  const quantile = (p) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]
  return {
    count: sorted.length,
    min: sorted[0],
    p50: quantile(0.5),
    p95: quantile(0.95),
    max: sorted.at(-1),
  }
}
const until = async (predicate, label, errors = []) => {
  const end = Date.now() + 45_000
  while (Date.now() < end) {
    if (errors.length) throw errors[0]
    if (await predicate()) return
    await delay(10)
  }
  throw new Error(`Performance acceptance timed out: ${label}`)
}
const deleteHandle = async (params, handle, allowGone = true) => {
  const url = new URL(base)
  for (const [key, value] of Object.entries({ ...params, handle }))
    url.searchParams.set(
      key,
      typeof value === 'object' ? JSON.stringify(value) : value
    )
  const response = await fetch(url, {
    method: 'DELETE',
    signal: AbortSignal.timeout(15_000),
  })
  assert(
    [204, ...(allowGone ? [409] : [])].includes(response.status),
    'Handle cleanup must succeed'
  )
}
const output = '.artifacts/staging/local-performance.json'
await mkdir('.artifacts/staging', { recursive: true })
await db.connect()
try {
  // neon dev can evaluate the module in more than one context. Measure the
  // serving context that actually emits samples, rather than its last boot.
  let startEvents
  await until(async () => {
    startEvents = await diagnostics()
    return startEvents.some(
      (e) => e.event === 'function_metrics' && e.timestamp >= Date.now() - 5000
    )
  }, 'fresh Function memory samples')
  const instance = startEvents.findLast(
    (e) => e.event === 'function_metrics'
  )?.instance
  assert(
    instance,
    'No measured Function instance found; restart neon dev with diagnostics'
  )
  report.nodeVersion = process.version
  report.functionNodeVersion = startEvents.findLast(
    (e) => e.event === 'function_started' && e.instance === instance
  )?.nodeVersion
  for (const rowCount of sizes)
    for (const liveSse of [false, true])
      for (const readerCount of readers) {
        const lower = 100_000_000 + Math.floor(Math.random() * 1_000_000_000)
        const upper = lower + rowCount
        const params = {
          table: 'electric_items',
          replica: 'full',
          where: 'id >= $1 AND id < $2',
          params: { 1: String(lower), 2: String(upper) },
        }
        const clients = []
        const handles = new Set()
        const errors = []
        const requests = []
        const times = []
        const commitTimes = []
        const scenario = {
          rowCount,
          readerCount,
          liveSse,
          status: 'running',
          fixtureUtf8Bytes: rowCount * payloadChars,
        }
        report.scenarios.push(scenario)
        let began
        let timestampStart
        const open = (options = {}) => {
          const abort = new AbortController()
          const receivedTxids = new Set()
          let snapshot
          const client = {
            abort,
            stream: undefined,
            shape: undefined,
            snapshot: () => snapshot,
            receivedTxids,
            firstHeadersAt: undefined,
            bytes: 0,
          }
          const stream = new ShapeStream({
            url: base.toString(),
            params,
            signal: abort.signal,
            liveSse,
            ...options,
            onError(error) {
              errors.push(error)
            },
            fetchClient: async (input, init) => {
              const target = new URL(String(input))
              assert.equal(target.origin, base.origin)
              assert.equal(target.pathname, base.pathname)
              assert.equal(init?.method ?? 'GET', 'GET')
              requests.push({
                at: performance.now(),
                handle: target.searchParams.get('handle'),
              })
              const response = await fetch(input, init)
              client.firstHeadersAt ??= performance.now()
              const handle = response.headers.get('electric-handle')
              if (handle) handles.add(handle)
              if (!response.body) return response
              const body = response.body.getReader()
              return new Response(
                new ReadableStream({
                  async pull(controller) {
                    try {
                      const chunk = await body.read()
                      if (chunk.done) controller.close()
                      else {
                        client.bytes += chunk.value.byteLength
                        controller.enqueue(chunk.value)
                      }
                    } catch (error) {
                      controller.error(error)
                    }
                  },
                  cancel(reason) {
                    return body.cancel(reason)
                  },
                }),
                { status: response.status, headers: response.headers }
              )
            },
          })
          client.stream = stream
          client.shape = new Shape(stream)
          stream.subscribe((messages) => {
            for (const message of messages) {
              if (isChangeMessage(message))
                for (const txid of message.headers.txids ?? [])
                  receivedTxids.add(txid)
              if (
                isControlMessage(message) &&
                message.headers.control === 'snapshot-end'
              )
                snapshot = message.headers
            }
          })
          clients.push(client)
          return client
        }
        const samples = async () =>
          (await diagnostics()).filter(
            (e) => e.instance === instance && e.timestamp >= timestampStart
          )
        try {
          await db.query(
            'insert into public.electric_items(id,label,enabled) select i,repeat($3,$4),true from generate_series($1::int,$2::int) i',
            [lower, upper - 1, 'x', payloadChars]
          )
          const baselineMetrics = (await diagnostics()).findLast(
            (e) => e.instance === instance && e.event === 'function_metrics'
          )
          assert(baselineMetrics, 'No Function memory samples')
          scenario.beforeMemory = baselineMetrics.memory
          timestampStart = Date.now()
          began = performance.now()
          const primary = open()
          const baseline = await primary.shape.rows
          const baselineReadyAt = Date.now()
          scenario.firstUsableSnapshotMs = performance.now() - began
          scenario.firstHeadersMs = primary.firstHeadersAt - began
          assert.equal(baseline.length, rowCount)
          assert(
            baseline.every(
              (row) =>
                row.id >= lower &&
                row.id < upper &&
                row.label === 'x'.repeat(payloadChars) &&
                row.enabled === true
            )
          )
          const handle = primary.stream.shapeHandle
          assert(handle)
          const replayOffset = primary.stream.lastOffset
          assert(replayOffset !== '-1')
          await until(
            async () =>
              (await samples()).some(
                (e) =>
                  e.event === 'function_metrics' &&
                  e.shapes.some(
                    (s) =>
                      s.handle === handle &&
                      s.retainedRows === rowCount &&
                      s.baselineComplete
                  )
              ),
            'owner baseline memory sample',
            errors
          )
          scenario.afterBaseline = (await samples()).findLast(
            (e) =>
              e.event === 'function_metrics' &&
              e.shapes.some((s) => s.handle === handle)
          )
          const baselineSamples = (await samples()).filter(
            (e) =>
              e.event === 'function_metrics' && e.timestamp <= baselineReadyAt
          )
          scenario.sampledBaselinePeakRss = Math.max(
            ...[...baselineSamples, scenario.afterBaseline].map(
              (e) => e.memory.rss
            )
          )
          scenario.sampledBaselinePeakHeapUsed = Math.max(
            ...[...baselineSamples, scenario.afterBaseline].map(
              (e) => e.memory.heapUsed
            )
          )
          const sealStart = performance.now()
          await until(
            async () =>
              (await samples()).some(
                (e) =>
                  e.event === 'function_metrics' &&
                  e.shapes.some(
                    (s) =>
                      s.handle === handle &&
                      s.sealedRanges > 0 &&
                      s.residentLogBytes < sealBytes
                  )
              ),
            'baseline offloaded to real storage',
            errors
          )
          scenario.baselineOffloadMs = performance.now() - sealStart
          scenario.afterSeal = (await samples()).findLast(
            (e) =>
              e.event === 'function_metrics' &&
              e.shapes.some(
                (s) => s.handle === handle && s.residentLogBytes < sealBytes
              )
          )
          const getsBeforeFanout = (await samples()).filter(
            (e) => e.event === 'object_get'
          ).length
          const warmStart = performance.now()
          await Promise.all(
            Array.from({ length: readerCount - 1 }, async () => {
              const next = open({ handle, offset: '-1' })
              const rows = await next.shape.rows
              assert.deepEqual(rows, baseline)
            })
          )
          scenario.fanoutReadyMs = performance.now() - warmStart
          if (readerCount > 1)
            assert(
              (await samples()).filter((e) => e.event === 'object_get').length >
                getsBeforeFanout,
              'Warm readers must exercise stored objects'
            )
          for (let iteration = 0; iteration < mutations; iteration++) {
            const label = `${iteration}:` + 'x'.repeat(payloadChars)
            const start = performance.now()
            const result = await db.query(
              'update public.electric_items set label=$2 where id=$1 returning pg_current_xact_id()::text as txid',
              [lower, label]
            )
            commitTimes.push(performance.now() - start)
            const txid = result.rows[0].txid
            const wrapped = Number(BigInt.asUintN(32, BigInt(txid)))
            await until(
              () =>
                clients.every(
                  (c) =>
                    c.shape.currentRows.some(
                      (row) => row.id === lower && row.label === label
                    ) &&
                    (c.receivedTxids.has(wrapped) ||
                      (c.snapshot() && isVisibleInSnapshot(txid, c.snapshot())))
                ),
              'all readers observe and acknowledge committed write',
              errors
            )
            times.push(performance.now() - start)
            assert(
              clients.every((c) => c.stream.shapeHandle === handle),
              'Owner unexpectedly refetched during live measurement'
            )
          }
          scenario.liveEndToEndMs = summary(times)
          scenario.commitReturnMs = summary(commitTimes)
          const liveCompletedAt = Date.now()
          await until(
            async () =>
              (await samples()).some(
                (e) =>
                  e.event === 'function_metrics' &&
                  e.timestamp >= liveCompletedAt &&
                  e.shapes.some((s) => s.handle === handle)
              ),
            'post-write log sample',
            errors
          )
          scenario.afterLive = (await samples()).findLast(
            (e) =>
              e.event === 'function_metrics' &&
              e.shapes.some((s) => s.handle === handle)
          )
          const logBytes = (sample) =>
            BigInt(
              sample.shapes
                .find((s) => s.handle === handle)
                .offset.split('_')[1]
            )
          scenario.liveLogGrowthBytes = Number(
            logBytes(scenario.afterLive) - logBytes(scenario.afterSeal)
          )
          const expected = (
            await db.query(
              'select id,label,enabled from public.electric_items where id >= $1 and id < $2 order by id',
              [lower, upper]
            )
          ).rows
          for (const client of clients)
            assert.deepEqual(
              [...client.shape.currentRows].sort((a, b) => a.id - b.id),
              expected
            )
          for (const client of clients) {
            client.abort.abort()
            client.stream.unsubscribeAll()
          }
          const replayStart = performance.now()
          const replay = open({
            handle,
            offset: replayOffset,
            subscribe: false,
          })
          const delta = await replay.shape.rows
          scenario.deltaReplayMs = performance.now() - replayStart
          assert.deepEqual(delta, [expected[0]])
          assert.equal(replay.stream.shapeHandle, handle)
          scenario.deltaReplayBytes = replay.bytes
          replay.abort.abort()
          await deleteHandle(params, handle, false)
          const refetchStart = performance.now()
          const refetch = open({
            handle,
            offset: replay.stream.lastOffset,
            subscribe: false,
          })
          assert.deepEqual(
            (await refetch.shape.rows).sort((a, b) => a.id - b.id),
            expected
          )
          scenario.refetchMs = performance.now() - refetchStart
          assert.notEqual(refetch.stream.shapeHandle, handle)
          refetch.abort.abort()
          const observed = await samples()
          const metrics = observed.filter((e) => e.event === 'function_metrics')
          assert(metrics.length > 0)
          assert(
            metrics.flatMap((e) => e.shapes).some((s) => s.sealedRanges > 0),
            'Run this gate against an object-backed Function'
          )
          scenario.sampledPeakRss = Math.max(
            ...metrics.map((e) => e.memory.rss)
          )
          scenario.sampledPeakHeapUsed = Math.max(
            ...metrics.map((e) => e.memory.heapUsed)
          )
          scenario.totalResponseBytes = clients.reduce(
            (total, c) => total + c.bytes,
            0
          )
          scenario.getRequests = requests.length
          scenario.elapsedMs = performance.now() - began
          scenario.status = 'passed'
        } catch (error) {
          scenario.status = 'failed'
          scenario.error = String(error)
          throw error
        } finally {
          for (const client of clients) {
            client.abort.abort()
            client.stream.unsubscribeAll()
          }
          for (const handle of handles) await deleteHandle(params, handle)
          const deletedAt = Date.now()
          await db.query(
            'delete from public.electric_items where id >= $1 and id < $2',
            [lower, upper]
          )
          assert.equal(
            (
              await db.query(
                'select count(*)::int as count from public.electric_items where id >= $1 and id < $2',
                [lower, upper]
              )
            ).rows[0].count,
            0
          )
          if (timestampStart) {
            await until(async () => {
              const events = await samples()
              return events.some(
                (e) =>
                  e.event === 'function_metrics' &&
                  e.timestamp >= deletedAt &&
                  e.pendingObjectCleanups === 0 &&
                  !e.shapes.some((s) => handles.has(s.handle))
              )
            }, 'all handle and object cleanup completed')
            const observed = await samples()
            const puts = observed.filter((e) => e.event === 'object_put')
            const gets = observed.filter((e) => e.event === 'object_get')
            scenario.objectStore = {
              puts: puts.length,
              gets: gets.length,
              deletes: observed.filter((e) => e.event === 'object_delete')
                .length,
              putBytes: puts.reduce((n, e) => n + e.bytes, 0),
              getBytes: gets.reduce((n, e) => n + e.bytes, 0),
              putServiceTimeMs: puts.reduce((n, e) => n + e.elapsedMs, 0),
              failures: observed.filter(
                (e) =>
                  (e.event === 'object_put_failed' ||
                    e.event === 'object_get_failed') &&
                  !e.aborted
              ).length,
              cancelled: observed.filter(
                (e) =>
                  (e.event === 'object_put_failed' ||
                    e.event === 'object_get_failed') &&
                  e.aborted
              ).length,
            }
            assert.equal(
              scenario.objectStore.failures,
              0,
              'Unexpected storage failure during performance acceptance'
            )
            scenario.objectStore.putBytesPerServiceSecond =
              (scenario.objectStore.putBytes /
                scenario.objectStore.putServiceTimeMs) *
              1000
          }
          await writeFile(output, JSON.stringify(report, null, 2) + '\n')
        }
        console.log(
          `Performance passed: ${rowCount} rows, ${readerCount} readers, ${liveSse ? 'SSE' : 'polling'}; snapshot=${Math.round(scenario.firstUsableSnapshotMs)}ms live p95=${Math.round(scenario.liveEndToEndMs.p95)}ms`
        )
      }
  report.status = 'passed'
} catch (error) {
  report.status = 'failed'
  report.error = String(error)
  throw error
} finally {
  report.completedAt = new Date().toISOString()
  await db.end()
  await writeFile(output, JSON.stringify(report, null, 2) + '\n')
}
