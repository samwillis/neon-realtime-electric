import { createRoot } from 'react-dom/client'
import { abort } from './shared'

const mode = new URLSearchParams(location.search).get('mode') ?? 'basic'
window.addEventListener('pagehide', () => abort.abort())
async function start() {
  const View =
    mode === 'custom'
      ? (await import('./basic/custom')).CustomBasic
      : mode === 'react'
        ? (await import('./react')).ReactShape
        : mode === 'tanstack'
          ? (await import('./tanstack')).TanStack
          : (await import('./basic')).Basic
  createRoot(document.getElementById('app')!).render(<View />)
}
void start()
