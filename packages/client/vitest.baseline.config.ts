import { defineConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'

export default defineConfig({
  resolve: {
    alias: [
      {
        find: './transport/create',
        replacement: fileURLToPath(
          new URL('./test/reference-transport.ts', import.meta.url)
        ),
      },
    ],
  },
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
