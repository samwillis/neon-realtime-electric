import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/compatibility/proxies/**/*.test.ts'],
    environment: 'node',
    testTimeout: 10_000,
  },
})
