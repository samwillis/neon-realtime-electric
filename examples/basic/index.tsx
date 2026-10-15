import { useEffect, useState } from 'react'
import { Shape, ShapeStream } from '@electric-sql/client'
import { options, type Item } from '../shared'

const stream = new ShapeStream<Item>(options)
const shape = new Shape(stream)
export function Basic() {
  const [rows, setRows] = useState<Item[]>([])
  useEffect(() => shape.subscribe(({ rows }) => setRows(rows)), [])
  return <pre data-testid="rows">{JSON.stringify(rows)}</pre>
}
