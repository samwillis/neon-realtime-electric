/** Numeric cache-collapse cursors, independent of shape read-boundary proofs.
 * Follows the pinned reference server's 20-second intervals and 1–3600-second
 * collision jitter, without importing its Node/LMDB runtime into the Function.
 */
export function generateResponseCursor(clientCursor?: string): string {
  const epoch = Date.UTC(2024, 9, 9)
  const current = BigInt(Math.max(0, Math.floor((Date.now() - epoch) / 20_000)))
  const previous =
    clientCursor && /^\d+$/.test(clientCursor) ? BigInt(clientCursor) : -1n
  if (previous < current) return String(current)
  const jitterSeconds = 1 + Math.floor(Math.random() * 3600)
  return String(previous + BigInt(Math.ceil(jitterSeconds / 20)))
}
