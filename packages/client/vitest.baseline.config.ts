import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    setupFiles: ['vitest-localstorage-mock'],
    include: ['test/**/*.test.ts'],
    exclude: [
      'test/client.test.ts',
      'test/cache.test.ts',
      'test/integration.test.ts',
    ],
    typecheck: {
      enabled: true,
      include: ['test/**/*.test-d.ts'],
    },
    environment: 'jsdom',
    silent: 'passed-only',
    testTimeout: 30_000,
  },
})
