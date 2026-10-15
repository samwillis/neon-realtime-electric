import {
  createNeonRealtimeSource,
  createShapeHandler,
  resolveConfiguredShape,
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
})
const handler = createShapeHandler({
  source,
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
process.once('SIGINT', () => handler.close())
process.once('SIGTERM', () => handler.close())
export default { fetch: (request: Request) => handler.fetch(request) }
