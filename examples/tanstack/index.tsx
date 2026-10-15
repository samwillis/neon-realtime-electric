import { createCollection, useLiveQuery } from '@tanstack/react-db'
import { electricCollectionOptions } from '@tanstack/electric-db-collection'
import { useState } from 'react'
import { options, type Item } from '../shared'

export const collection = createCollection(
  electricCollectionOptions<Item>({
    id: 'electric-items',
    shapeOptions: options,
    getKey: (row) => row.id,
    syncMode: 'eager',
    onUpdate: async ({ transaction, collection }) => {
      const mutation = transaction.mutations[0]
      const response = await fetch('/api/items', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ id: mutation.original.id, ...mutation.changes }),
      })
      if (!response.ok) throw new Error('Mutation failed')
      const { txid } = (await response.json()) as { txid: number }
      await collection.utils.awaitTxId(txid, 15_000)
    },
  })
)
export function TanStack() {
  const { data, isLoading } = useLiveQuery((q) => q.from({ items: collection }))
  const [state, setState] = useState('idle')
  return (
    <>
      <pre data-testid="rows" data-loading={isLoading}>
        {JSON.stringify(data)}
      </pre>
      <button
        onClick={() => {
          const requested = new URLSearchParams(location.search).get('item')
          const row = requested
            ? data.find((row) => row.id === Number(requested))
            : data[0]
          if (!row) return
          setState('pending')
          const tx = collection.update(row.id, (draft) => {
            draft.label = 'optimistic'
          })
          void tx.isPersisted.promise.then(
            () => setState('persisted'),
            () => setState('failed')
          )
        }}
      >
        Update first row
      </button>
      <output data-testid="mutation">{state}</output>
      <button
        onClick={() => {
          const id = Number(new URLSearchParams(location.search).get('item'))
          setState('pending')
          void (async () => {
            const response = await fetch('/api/items', {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({ id, noop: true }),
            })
            if (!response.ok) throw new Error('Mutation failed')
            const { txid } = (await response.json()) as { txid: number }
            await collection.utils.awaitTxId(txid, 15_000)
            setState('acknowledged')
          })().catch(() => setState('failed'))
        }}
      >
        Acknowledge unchanged row
      </button>
    </>
  )
}
