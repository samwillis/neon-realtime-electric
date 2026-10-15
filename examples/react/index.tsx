import { useShape } from '@electric-sql/react'
import { options, type Item } from '../shared'

export function ReactShape() {
  const { data, isLoading, error } = useShape<Item>(options)
  if (error) throw error
  return (
    <pre data-testid="rows" data-loading={isLoading}>
      {JSON.stringify(data)}
    </pre>
  )
}
