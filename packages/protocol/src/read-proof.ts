import { isSequence, ShapeProtocolError } from './index'
import type { Sequence } from './index'

/** Evidence for the requested byte boundary, recovered on ordinary GET reads. */
export interface ReadProof {
  version: 1
  offset: string
  afterSeq: Sequence
  baselineComplete: boolean
  nativeCursor?: string
}

export function encodeReadProof(proof: ReadProof): string {
  return `neon1.${encodeURIComponent(JSON.stringify(proof))}`
}

export function decodeReadProof(value: string | null): ReadProof {
  try {
    if (!value?.startsWith('neon1.')) throw new Error()
    const proof = JSON.parse(decodeURIComponent(value.slice(6)))
    if (
      proof.version !== 1 ||
      typeof proof.offset !== 'string' ||
      !isSequence(proof.afterSeq) ||
      typeof proof.baselineComplete !== 'boolean' ||
      (proof.nativeCursor !== undefined &&
        typeof proof.nativeCursor !== 'string') ||
      (proof.afterSeq === '0' && proof.baselineComplete)
    )
      throw new Error()
    return proof
  } catch {
    throw new ShapeProtocolError('Missing or invalid shape read-boundary proof')
  }
}
