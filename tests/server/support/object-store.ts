import type { LogObjectStore } from '../../../packages/server/src/object-log.js'

export class FixtureObjectStore implements LogObjectStore {
  readonly objects = new Map<string, Uint8Array>()
  readonly puts: { key: string; bytes: Uint8Array }[] = []
  readonly gets: string[] = []
  readonly deletes: string[] = []
  putError?: Error
  getError?: Error
  deleteError?: Error
  beforePut?: () => Promise<void>
  beforeGet?: (signal?: AbortSignal) => Promise<void>
  async put(
    key: string,
    bytes: Uint8Array,
    signal?: AbortSignal
  ): Promise<void> {
    signal?.throwIfAborted()
    this.puts.push({ key, bytes: Uint8Array.from(bytes) })
    await this.beforePut?.()
    signal?.throwIfAborted()
    if (this.putError) throw this.putError
    this.objects.set(key, Uint8Array.from(bytes))
  }
  async get(key: string, signal?: AbortSignal): Promise<Uint8Array> {
    signal?.throwIfAborted()
    this.gets.push(key)
    await this.beforeGet?.(signal)
    signal?.throwIfAborted()
    if (this.getError) throw this.getError
    const bytes = this.objects.get(key)
    if (!bytes) throw new Error('Missing object')
    return Uint8Array.from(bytes)
  }
  async delete(key: string): Promise<void> {
    this.deletes.push(key)
    if (this.deleteError) throw this.deleteError
    this.objects.delete(key)
  }
}
