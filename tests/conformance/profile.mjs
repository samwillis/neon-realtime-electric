// Exact selected test names from @durable-streams/server-conformance-tests@0.3.7.
export const upstreamTests = [
  'HEAD Metadata should return 404 for non-existent stream',
  'Offset Validation and Resumability should return empty JSON array for offset=now on JSON streams',
  'SSE Mode should accept live=sse query parameter for application/json',
  'SSE Mode JSON SSE catch-up pairs every data event with a control event',
  'SSE Mode should wrap JSON data in arrays for SSE and produce valid JSON',
  'SSE Mode should handle JSON payloads with embedded newlines safely',
  'SSE Mode should NOT include Stream-SSE-Data-Encoding header for application/json streams',
  'JSON Mode should allow PUT with empty array body (creates empty stream)',
  'JSON Mode should reject POST with empty array body',
  'JSON Mode should handle content-type with charset parameter',
  'JSON Mode should wrap single JSON value in array',
  'JSON Mode should store arrays as single messages',
  'JSON Mode should concatenate multiple appends into single array',
  'JSON Mode should handle mixed single values and arrays',
  'JSON Mode should reject invalid JSON with 400',
  'JSON Mode should handle various JSON value types',
  'JSON Mode should preserve JSON structure and nesting',
  'JSON Mode should work with client json() iterator',
  'JSON Mode should reject empty JSON arrays with 400',
  'JSON Mode should store nested arrays as single messages',
  'JSON Mode should store arrays as values when double-wrapped',
  'JSON Mode should store primitive arrays as single messages',
  'JSON Mode should handle mixed batching - single values, arrays, and nested arrays',
]

// Additional JSON-only coverage for read cases expressed with text/binary fixtures upstream.
export const supplementalTests = [
  'HEAD returns the tail and expiry without reading object bytes',
  'resumes immutable JSON messages without duplicates across sealed ranges',
  'long polling returns numeric cursors with data and on 204 timeout',
  'delivers new JSON at a saved tail and supports now without historical data',
  'rejects malformed or ambiguous read parameters with security headers',
  'keeps GET, HEAD, live and error responses out of caches',
  'does not renew absolute expiry on reads or writes',
  'revalidates expiry after a long-poll wait',
  'cancels a waiting poll without poisoning the stream',
]
