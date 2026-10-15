import {
  createNeonRealtimeSource,
  createShapeHandler,
  resolveConfiguredShape,
  createS3LogStore,
  ObjectShapeLog,
} from '@neon/realtime-electric-server'

function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`Local shape Function requires ${name}`)
  return value
}
const source = createNeonRealtimeSource({
  url: required('NEON_REALTIME_URL'),
  secret: required('NEON_REALTIME_SECRET'),
  db: new URL(required('DATABASE_URL')).pathname.slice(1),
  logLevel: process.env.NEON_SHAPE_DIAGNOSTICS === 'true' ? 'debug' : 'silent',
  logger: (entry) =>
    console.log(
      JSON.stringify({
        event: entry.event,
        timestamp: entry.timestamp,
        ...('code' in entry ? { code: entry.code } : {}),
      })
    ),
})
const storage = process.env.NEON_SHAPE_LOG_BUCKET
  ? createS3LogStore({ bucket: process.env.NEON_SHAPE_LOG_BUCKET })
  : undefined
const objectPrefix = 'electric-shapes'
const sweep = () =>
  storage
    ?.sweepExpired(objectPrefix)
    .catch(() => console.error('Expired shape object sweep failed'))
void sweep()
const sweepTimer = storage ? setInterval(() => void sweep(), 60_000) : undefined
sweepTimer?.unref()
const handler = createShapeHandler({
  source,
  createLog: storage
    ? ({ handle, expiresAt }) =>
        new ObjectShapeLog({
          store: storage,
          prefix: `${objectPrefix}/${expiresAt}/${handle}`,
          sealBytes: process.env.NEON_SHAPE_SEAL_BYTES
            ? Number(process.env.NEON_SHAPE_SEAL_BYTES)
            : undefined,
        })
    : undefined,
  onOwnerClosed: ({ handle, error }) => {
    if (process.env.NEON_SHAPE_DIAGNOSTICS === 'true')
      console.log(
        JSON.stringify({
          event: 'shape_owner_closed',
          handle,
          timestamp: Date.now(),
          reason: error.message,
        })
      )
  },
  async authorize(request) {
    // This local example exposes only its dedicated test table. Applications must
    // supply their own authentication and trusted tenant predicate here.
    return {
      authorizationKey: 'local-example',
      definition: resolveConfiguredShape(new URL(request.url), [
        {
          namespace: 'public',
          table: 'electric_items',
          primaryKey: ['id'],
          columns: {
            id: { type: 'int4', not_null: true },
            label: { type: 'text', not_null: true },
            enabled: { type: 'bool', not_null: true },
          },
        },
      ]),
    }
  },
})
const stop = () => {
  clearInterval(sweepTimer)
  handler.close()
  void handler
    .drain()
    .catch(() => console.error('Shape object cleanup failed'))
    .finally(() => storage?.close())
}
process.once('SIGINT', stop)
process.once('SIGTERM', stop)
export default { fetch: (request: Request) => handler.fetch(request) }
