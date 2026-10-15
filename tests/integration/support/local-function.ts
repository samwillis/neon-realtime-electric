import { spawn } from 'node:child_process'
import { createWriteStream } from 'node:fs'
import { mkdir } from 'node:fs/promises'
import { createServer } from 'node:net'
import { once } from 'node:events'
import type { ConfiguredTable } from '../../../packages/server/src/definition.js'

/** Credentialed integration harness; never provisions or deploys a Function. */
export async function startLocalFunction(options: {
  tables: ConfiguredTable[]
  realtimeUrl: string
  artifact: string
}) {
  for (const name of ['DATABASE_URL', 'NEON_REALTIME_SECRET'])
    if (!process.env[name])
      throw new Error(
        `Local Function integration requires ${name}; load the ignored environment file`
      )
  await mkdir('.artifacts/staging', { recursive: true })
  const output = createWriteStream(
    `.artifacts/staging/${options.artifact}-function.log`
  )
  const socket = createServer()
  socket.listen(0, '127.0.0.1')
  await once(socket, 'listening')
  const port = (socket.address() as { port: number }).port
  await new Promise<void>((resolve) => socket.close(() => resolve()))
  const origin = `http://127.0.0.1:${port}`
  const child = spawn(
    'neon',
    [
      ...(process.env.NEON_CLI_PROFILE
        ? ['--profile', process.env.NEON_CLI_PROFILE]
        : []),
      'dev',
      '--source',
      'tests/integration/support/configured-function.ts',
      '--port',
      String(port),
    ],
    {
      env: {
        ...process.env,
        NEON_REALTIME_URL: options.realtimeUrl,
        NEON_TEST_SHAPE_CONFIG: JSON.stringify(options.tables),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  )
  child.stdout!.pipe(output, { end: false })
  child.stderr!.pipe(output, { end: false })
  let startupError: Error | undefined
  child.once('error', (error) => {
    startupError = error
  })
  let stopped = false
  const stop = async () => {
    if (stopped) return
    stopped = true
    if (child.pid && child.exitCode === null && child.signalCode === null) {
      const exited = once(child, 'exit')
      child.kill('SIGTERM')
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        await Promise.race([
          exited,
          new Promise<void>((resolve) => {
            timer = setTimeout(() => {
              child.kill('SIGKILL')
              resolve()
            }, 5000)
          }),
        ])
      } finally {
        clearTimeout(timer)
      }
    }
    output.end()
  }
  try {
    const deadline = Date.now() + 25_000
    const ready = new URL('/proxy/shapes', origin)
    ready.searchParams.set('table', options.tables[0].table)
    while (Date.now() < deadline) {
      if (startupError) throw startupError
      if (child.exitCode !== null || child.signalCode !== null)
        throw new Error('neon dev exited before readiness')
      try {
        // HEAD validates the allowlist without creating a Realtime subscription.
        if (
          (
            await fetch(ready, {
              method: 'HEAD',
              signal: AbortSignal.timeout(1000),
            })
          ).status === 404
        )
          return { origin, stop }
      } catch {
        /* Server is still starting. */
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error('neon dev did not become ready')
  } catch (error) {
    await stop()
    throw error
  }
}
