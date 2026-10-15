import { createRequire } from 'node:module'
import { realpathSync } from 'node:fs'
import assert from 'node:assert/strict'
const require = createRequire(import.meta.url)
const client = realpathSync(require.resolve('@electric-sql/client'))
for (const integration of [
  '@electric-sql/react',
  '@tanstack/electric-db-collection',
]) {
  const nested = createRequire(require.resolve(integration))
  assert.equal(realpathSync(nested.resolve('@electric-sql/client')), client)
  assert.equal(
    nested('@electric-sql/client').FetchError,
    require('@electric-sql/client').FetchError
  )
}
console.log(
  'Application, React and TanStack resolve the same replacement and error identity'
)
