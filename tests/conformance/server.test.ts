import { afterAll, describe, expect } from 'vitest'
import { runConformanceTests } from '@durable-streams/server-conformance-tests'
import { createFixture } from './support/fixture.js'

for (const objects of [false, true]) {
  describe(`production JSON read/store: ${objects ? 'objects' : 'memory'}`, async () => {
    const fixture = await createFixture(objects)
    afterAll(async () => {
      try {
        if (objects) expect(fixture.store.gets.length).toBeGreaterThan(0)
      } finally {
        await fixture.close()
      }
    })
    runConformanceTests({ baseUrl: fixture.baseUrl, longPollTimeoutMs: 100 })
  })
}
