import { defineConfig } from 'vitest/config'
import {
  upstreamTests,
  supplementalTests,
} from './tests/conformance/profile.mjs'

const escape = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
const selected = ['memory', 'objects'].flatMap((backend) => [
  ...upstreamTests.map(
    (name) => `production JSON read/store: ${backend} ${name}`
  ),
  ...supplementalTests.map((name) => `JSON read profile: ${backend} ${name}`),
])
export default defineConfig({
  test: {
    include: ['tests/conformance/*.test.ts'],
    environment: 'node',
    testNamePattern: new RegExp(`^(?:${selected.map(escape).join('|')})$`),
    testTimeout: 10_000,
  },
})
