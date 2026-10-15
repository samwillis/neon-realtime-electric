import { defineConfig } from '@neon/config/v1'

// Local development only. Do not deploy until platform affinity is available.
export default defineConfig({
  functions: {
    shapes: {
      name: 'Electric-compatible shapes',
      source: 'apps/function/src/index.ts',
    },
  },
})
