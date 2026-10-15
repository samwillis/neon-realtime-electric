import {
  createNeonRealtimeSource,
  createShapeHandler,
  resolveConfiguredShape,
} from '../../../packages/server/src/index.js'

function required(name: string): string {
  const value = process.env[name]
  if (!value) throw new Error(`Quoted projection Function requires ${name}`)
  return value
}
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
      authorizationKey: 'quoted-projection-test',
      definition: resolveConfiguredShape(new URL(request.url), [
        {
          namespace: required('NEON_TEST_QUOTED_SCHEMA'),
          table: 'items',
          primaryKey: ['id'],
          columns: {
            id: { type: 'int4' },
            normal: { type: 'text' },
            'has,comma': { type: 'text' },
            'has"quote': { type: 'text' },
            'has space': { type: 'text' },
          },
        },
      ]),
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
