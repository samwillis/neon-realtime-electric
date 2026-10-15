// Build an independent frozen install and verify both committed dependency patches.
import './test-durable-reader-install.mjs'
import { writeFile } from 'node:fs/promises'
import { spawnSync } from 'node:child_process'
import { resolve, join } from 'node:path'

const target = resolve('.artifacts/durable-reader/clean-install')
const result = spawnSync(
  process.execPath,
  ['scripts/probe-realtime-sdk.mjs', target],
  { stdio: 'inherit' }
)
if (result.status !== 0)
  throw new Error('Isolated Realtime SDK runtime probe failed')
await writeFile(
  join(target, 'packages/server/sdk-types.ts'),
  `
import type {RealtimeSourceEvent, RawLiveQuerySubscription, RawLiveQueryOptions} from '@neon/realtime/client'
const options:RawLiveQueryOptions={materialize:false,rawValues:true}
function check(subscription:RawLiveQuerySubscription<unknown>) {
  subscription.onSource((event:RealtimeSourceEvent)=>{
    if(event.type==='publication') {
      const frontier:string=event.frontier.lsn
      const txids:readonly string[]=event.txids
      return [frontier,txids]
    }
    if(event.type==='baseline') return event.mvcc.xip
  })
}
void options;void check
`
)
const types = spawnSync(
  'pnpm',
  [
    'exec',
    'tsc',
    '--noEmit',
    '--strict',
    '--module',
    'esnext',
    '--moduleResolution',
    'bundler',
    '--target',
    'es2022',
    'packages/server/sdk-types.ts',
  ],
  { cwd: target, stdio: 'inherit' }
)
if (types.status !== 0)
  throw new Error('Isolated Realtime SDK declarations failed')
console.log('Clean frozen install: Realtime exported declarations verified')
