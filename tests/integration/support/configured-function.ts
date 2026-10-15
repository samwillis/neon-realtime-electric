import {
  createNeonRealtimeSource,
  createShapeHandler,
  resolveConfiguredShape,
  type ConfiguredTable,
} from '../../../packages/server/src/index.js'

function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`Integration Function requires ${name}`)
  return value
}
// Test-only allowlist. The runner supplies isolated schema/column definitions;
// no request can select an unconfigured table or change this configuration.
const tables = JSON.parse(
  required('NEON_TEST_SHAPE_CONFIG')
) as ConfiguredTable[]
const source = createNeonRealtimeSource({
  url: required('NEON_REALTIME_URL'),
  secret: required('NEON_REALTIME_SECRET'),
  db: new URL(required('DATABASE_URL')).pathname.slice(1),
})
const handler = createShapeHandler({
  source,
  longPollMs: 500,
  async authorize(request) {
    return {
      authorizationKey: 'isolated-integration-test',
      definition: resolveConfiguredShape(new URL(request.url), tables),
    }
  },
})
const stop = () => {
  handler.close()
  void handler.drain()
}
process.once('SIGTERM', stop)
process.once('SIGINT', stop)
export default { fetch: (request: Request) => handler.fetch(request) }
