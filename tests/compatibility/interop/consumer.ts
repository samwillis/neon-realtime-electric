import * as original from 'electric-reference'
import * as candidate from 'electric-candidate'

type Task = { id: number; title: string; createdAt: Date }
type Assert<T extends true> = T
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false

// Compile-only: keep both packages installed independently, without an override.
declare const originalStream: original.ShapeStream<Task>
declare const candidateStream: candidate.ShapeStream<Task>
const originalInterface: original.ShapeStreamInterface<Task> = candidateStream
const candidateInterface: candidate.ShapeStreamInterface<Task> = originalStream
const originalShape = new original.Shape(candidateStream)
const candidateShape = new candidate.Shape(originalStream)
type OriginalInference = Assert<
  Equal<typeof originalShape.rows, Promise<Task[]>>
>
type CandidateInference = Assert<
  Equal<typeof candidateShape.rows, Promise<Task[]>>
>

type BothWays<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false
type PublicContracts = [
  Assert<
    BothWays<
      original.ShapeStreamOptions<Date>,
      candidate.ShapeStreamOptions<Date>
    >
  >,
  Assert<BothWays<original.Message<Task>, candidate.Message<Task>>>,
  Assert<BothWays<original.Schema, candidate.Schema>>,
  Assert<BothWays<original.Offset, candidate.Offset>>,
  Assert<BothWays<original.SnapshotMetadata, candidate.SnapshotMetadata>>,
  Assert<BothWays<original.ShapeData<Task>, candidate.ShapeData<Task>>>,
]

originalInterface.subscribe((messages) => {
  for (const message of messages) {
    if (original.isChangeMessage(message)) {
      const date: Date = message.value.createdAt
      // @ts-expect-error the crossed package boundary must preserve Date inference
      const invalid: string = message.value.createdAt
      void [date, invalid]
    }
  }
})
candidateShape.subscribe(({ rows }) => {
  const tasks: Task[] = rows
  // @ts-expect-error mapped rows retain the declared property names
  const invalid: Date = rows[0].created_at
  void [tasks, invalid]
})
void [originalShape, candidateShape, originalInterface, candidateInterface]
