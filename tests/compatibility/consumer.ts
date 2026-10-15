import {
  Shape,
  ShapeStream,
  FetchError,
  FetchBackoffAbortError,
  isChangeMessage,
  isControlMessage,
  createColumnMapper,
  snakeCamelMapper,
  compileExpression,
  compileOrderBy,
  ELECTRIC_PROTOCOL_QUERY_PARAMS,
  type Row,
  type Message,
  type ShapeData,
  type ShapeStreamInterface,
  type ShapeStreamOptions,
  type ExternalParamsRecord,
  type SnapshotMetadata,
  type Offset,
} from '@electric-sql/client'

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false
type Assert<T extends true> = T
type Task = { id: number; title: string; createdAt: Date }

const options: ShapeStreamOptions<Date> = {
  url: 'https://example.test/api/shape?tenant=one',
  params: { table: 'tasks', columns: ['id', 'title', 'created_at'] },
  parser: { timestamptz: (value) => new Date(value) },
  columnMapper: snakeCamelMapper(),
  fetchClient: fetch,
  subscribe: false,
  onError: async () => ({ headers: { authorization: 'refreshed' } }),
}
const stream = new ShapeStream<Task>(options)
const shape = new Shape(stream)
const structural: ShapeStreamInterface<Task> = stream
const originalOptions: ShapeStreamOptions<Date> = stream.options
const subscribers: number = shape.numSubscribers
const paused: boolean = stream.isPaused()
const offset: Offset = stream.lastOffset

type RowsAreInferred = Assert<Equal<typeof shape.rows, Promise<Task[]>>>
type ValuesAreInferred = Assert<
  Equal<typeof shape.value, Promise<ShapeData<Task>>>
>
const defaultStream = new ShapeStream({ url: options.url })
type DefaultRowsArePreserved = Assert<
  Equal<typeof defaultStream, ShapeStream<Row>>
>

stream.subscribe(async (messages) => {
  type CallbackRowsAreInferred = Assert<Equal<typeof messages, Message<Task>[]>>
  for (const message of messages) {
    if (isChangeMessage(message)) {
      const date: Date = message.value.createdAt
      // @ts-expect-error a custom parsed date must not become any or string
      const invalid: string = message.value.createdAt
      void [date, invalid]
    } else if (isControlMessage(message)) {
      const control: string = message.headers.control
      void control
    }
  }
})
shape.subscribe(({ rows, value }) => {
  const taskRows: Task[] = rows
  const taskMap: Map<string, Task> = value
  void [taskRows, taskMap]
})

const dynamicParams: ExternalParamsRecord = {
  table: 'tasks',
  tenant: async () => 'one',
}
const reserved: ExternalParamsRecord = {
  // @ts-expect-error transport parameters are reserved
  handle: 'not-an-application-param',
}
// @ts-expect-error offsets retain their numeric-pair / sentinel contract
const invalidOffset: Offset = 'arbitrary-opaque-text'

async function subsets() {
  const { metadata, data } = await stream.fetchSnapshot({
    whereExpr: {
      type: 'func',
      name: 'eq',
      args: [
        { type: 'ref', column: 'id' },
        { type: 'val', paramIndex: 1 },
      ],
    },
    params: { '1': 1 },
    limit: 10,
    method: 'POST',
  })
  const snapshot: SnapshotMetadata = metadata
  const changes: Message<Task>[] = data
  await stream.requestSnapshot({ where: 'id = $1', params: { '1': 1 } })
  return [snapshot, changes]
}

void [
  structural,
  originalOptions,
  subscribers,
  paused,
  offset,
  dynamicParams,
  reserved,
  invalidOffset,
  subsets,
  FetchError,
  FetchBackoffAbortError,
  createColumnMapper,
  compileExpression,
  compileOrderBy,
  ELECTRIC_PROTOCOL_QUERY_PARAMS,
]
