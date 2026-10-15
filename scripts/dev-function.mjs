import { spawn } from 'node:child_process'

// Host overrides and credentials are inherited from ignored local configuration.
const profile = process.env.NEON_CLI_PROFILE
const child = spawn(
  'neon',
  [
    ...(profile ? ['--profile', profile] : []),
    'dev',
    '--source',
    'apps/function/src/index.ts',
    '--port',
    '4327',
  ],
  { stdio: 'inherit', env: process.env }
)
process.once('SIGINT', () => child.kill('SIGINT'))
process.once('SIGTERM', () => child.kill('SIGTERM'))
child.once('error', (error) => {
  console.error(error.message)
  process.exitCode = 1
})
child.once('exit', (code) => {
  process.exitCode = code ?? 1
})
