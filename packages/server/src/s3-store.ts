import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3'
import type { LogObjectStore } from './object-log.js'

/** Private Neon bucket, standard AWS_* credentials and mandatory path-style URLs. */
export function createS3LogStore(options: {
  bucket: string
  client?: S3Client
}): LogObjectStore & {
  sweepExpired(prefix: string, now?: number): Promise<number>
  close(): void
} {
  const client = options.client ?? new S3Client({ forcePathStyle: true })
  return {
    async put(key, bytes, signal) {
      await client.send(
        new PutObjectCommand({
          Bucket: options.bucket,
          Key: key,
          Body: bytes,
          ContentType: 'application/x-ndjson',
          CacheControl: 'private, no-store',
        }),
        { abortSignal: signal }
      )
    },
    async get(key, signal) {
      const response = await client.send(
        new GetObjectCommand({ Bucket: options.bucket, Key: key }),
        { abortSignal: signal }
      )
      if (!response.Body) throw new Error('Empty S3 object body')
      return response.Body.transformToByteArray()
    },
    async delete(key) {
      await client.send(
        new DeleteObjectCommand({ Bucket: options.bucket, Key: key })
      )
    },
    /** Prefix layout: <deployment-prefix>/<expiresAt-ms>/<handle>/<range>. */
    async sweepExpired(prefix, now = Date.now()) {
      if (!prefix || prefix.endsWith('/'))
        throw new Error(
          'Sweep requires a dedicated deployment prefix without trailing slash'
        )
      let token: string | undefined
      let count = 0
      do {
        const page = await client.send(
          new ListObjectsV2Command({
            Bucket: options.bucket,
            Prefix: `${prefix}/`,
            ContinuationToken: token,
          })
        )
        for (const object of page.Contents ?? []) {
          const key = object.Key
          if (!key) continue
          const remainder = key.slice(prefix.length + 1)
          const expiry = remainder.split('/')[0]
          if (!/^[0-9]{13}$/.test(expiry) || Number(expiry) > now) continue
          await client.send(
            new DeleteObjectCommand({ Bucket: options.bucket, Key: key })
          )
          count++
        }
        token = page.IsTruncated ? page.NextContinuationToken : undefined
        if (page.IsTruncated && !token)
          throw new Error('Missing S3 pagination cursor')
      } while (token)
      return count
    },
    close() {
      if (!options.client) client.destroy()
    },
  }
}
