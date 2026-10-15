import { compareSequence, parseEnvelope, ShapeProtocolError } from './index'
import type { Envelope, Sequence } from './index'

export interface Checkpoint {
  /** Durable Streams response-end offset. Not a sequence or a WAL LSN. */
  offset: string
  cursor?: string
}

export interface ConsumerState {
  appliedSeq: Sequence
  ready: boolean
  checkpoint: Checkpoint
}

/**
 * One consumer per handle. A failed batch keeps its earlier byte checkpoint,
 * while successfully applied logical events are suppressed on replay.
 */
export class EnvelopeConsumer {
  #state: ConsumerState
  #busy = false

  constructor(state?: ConsumerState) {
    this.#state = state
      ? { ...state, checkpoint: { ...state.checkpoint } }
      : { appliedSeq: '0', ready: false, checkpoint: { offset: '-1' } }
  }

  get state(): ConsumerState {
    return { ...this.#state, checkpoint: { ...this.#state.checkpoint } }
  }

  async consume(
    batch: { items: readonly unknown[]; offset: string; cursor?: string },
    apply: (envelope: Envelope) => Promise<void>
  ): Promise<void> {
    if (this.#busy)
      throw new ShapeProtocolError('Concurrent envelope consumption')
    this.#busy = true
    try {
      const envelopes = batch.items.map(parseEnvelope)
      for (const envelope of envelopes) {
        if (compareSequence(envelope.seq, this.#state.appliedSeq) <= 0) continue
        if (
          BigInt(envelope.seq) !==
          BigInt(this.#state.appliedSeq) + BigInt(1)
        ) {
          throw new ShapeProtocolError('Gap in shape envelope sequence')
        }
        if (
          envelope.kind === 'initializing' &&
          this.#state.appliedSeq !== '0'
        ) {
          throw new ShapeProtocolError(
            'Initialization inside an existing history'
          )
        }
        if (
          this.#state.appliedSeq === '0' &&
          envelope.kind !== 'initializing'
        ) {
          throw new ShapeProtocolError('History must start with initialization')
        }
        if (envelope.kind === 'snapshot-chunk' && this.#state.ready) {
          throw new ShapeProtocolError(
            'Snapshot chunk after baseline completion'
          )
        }
        if (envelope.kind === 'snapshot-complete' && this.#state.ready) {
          throw new ShapeProtocolError('Repeated baseline completion')
        }
        if (
          (envelope.kind === 'publication' || envelope.kind === 'progress') &&
          !this.#state.ready
        ) {
          throw new ShapeProtocolError(
            'Live evidence before baseline completion'
          )
        }
        if (
          'coveredSeq' in envelope &&
          envelope.coveredSeq !== this.#state.appliedSeq
        ) {
          throw new ShapeProtocolError(
            'Evidence must cover the preceding installed boundary'
          )
        }
        await apply(envelope)
        this.#state.appliedSeq = envelope.seq
        if (envelope.kind === 'snapshot-complete') this.#state.ready = true
      }
      this.#state.checkpoint = { offset: batch.offset, cursor: batch.cursor }
    } finally {
      this.#busy = false
    }
  }
}
