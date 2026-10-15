import { createRequire } from 'node:module'
import { describe, expect, it } from 'vitest'
import { stream as esmStream } from '@durable-streams/client'

const { stream: cjsStream } = createRequire(import.meta.url)(
  '@durable-streams/client'
) as { stream: typeof esmStream }

function response(body: BodyInit | null = '[{"value":1}]') {
  return new Response(body, {
    headers: {
      'Content-Type': 'application/json',
      'Stream-Next-Offset': '0000000000000000_0000000000000001',
      'Stream-Up-To-Date': 'true',
    },
  })
}

for (const [format, stream] of [
  ['ESM', esmStream],
  ['CJS', cjsStream],
] as const) {
  describe(`${format} JSON subscription completion`, () => {
    it('waits for an asynchronous callback after physical tail closure', async () => {
      let release!: () => void
      let entered!: () => void
      const gate = new Promise<void>((resolve) => {
        release = resolve
      })
      const started = new Promise<void>((resolve) => {
        entered = resolve
      })
      const session = await stream({
        url: 'https://example.test/shape',
        live: false,
        json: true,
        fetch: async () => response(),
      })
      let finished = false
      session.subscribeJson(async () => {
        entered()
        await gate
      })
      const closed = session.closed.then(() => {
        finished = true
      })
      await started
      await Promise.resolve()
      expect(finished).toBe(false)
      release()
      await closed
      expect(finished).toBe(true)
    })

    it.each([
      new Error('callback failed'),
      new TypeError('Body processing failed'),
    ])('rejects callback failures: %s', async (error) => {
      const session = await stream({
        url: 'https://example.test/shape',
        live: false,
        json: true,
        fetch: async () => response(),
      })
      session.subscribeJson(async () => {
        throw error
      })
      await expect(session.closed).rejects.toBe(error)
    })

    it('rejects invalid JSON even when the response is already at tail', async () => {
      const session = await stream({
        url: 'https://example.test/shape',
        live: false,
        json: true,
        fetch: async () => response('[{"partial":'),
      })
      let notified = false
      session.subscribeJson(() => {
        notified = true
      })
      await expect(session.closed).rejects.toThrow('Failed to parse JSON')
      expect(notified).toBe(false)
    })

    it('rejects a body read failure without reporting successful completion', async () => {
      const error = new TypeError('Body truncated')
      const body = new ReadableStream({
        start(controller) {
          controller.error(error)
        },
      })
      const session = await stream({
        url: 'https://example.test/shape',
        live: false,
        json: true,
        fetch: async () => response(body),
      })
      session.subscribeJson(() => {
        throw new Error('must not deliver')
      })
      await expect(session.closed).rejects.toBe(error)
    })
  })
}
