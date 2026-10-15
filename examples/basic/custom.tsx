import { useEffect, useState } from 'react'
import { createColumnMapper, Shape, ShapeStream } from '@electric-sql/client'
import { options } from '../shared'

type MappedItem = { itemId: number; title: string; enabled: boolean }
const stream = new ShapeStream<MappedItem>({
  ...options,
  parser: { text: (value) => value.toUpperCase() },
  columnMapper: createColumnMapper({ id: 'itemId', label: 'title' }),
})
const shape = new Shape(stream)
export function CustomBasic() {
  const [rows, setRows] = useState<MappedItem[]>([])
  useEffect(() => shape.subscribe(({ rows }) => setRows(rows)), [])
  return <pre data-testid="rows">{JSON.stringify(rows)}</pre>
}
