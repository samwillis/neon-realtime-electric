# Local performance evidence

The eight-scenario run on 15 October 2026 passed against the Function running
under `neon dev`, real staging Postgres/Realtime and real Neon Object Storage.
Both the runner and Function used Node 22.23.2. Node 24 remains the intended
server target; these measurements do not establish its performance.

Each scenario uses a primary-keyed int4/text/bool table, full replica mode,
512-character ASCII labels and eight sequential updates. Readers share one
query-string handle. The runner verifies every baseline cell, live rows and real
transaction acknowledgements, compares final rows with independent SQL, replays
from a saved offset and measures refetch after deleting the old owner.

| Rows | Readers | Mode | Baseline ms | Live p95 ms | Warm fan-out ms | Delta replay ms | Refetch ms | Peak RSS MiB | Object GETs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | Poll | 1,476 | 116 | — | 1,187 | 1,276 | 80 | 1 |
| 1,000 | 5 | Poll | 1,273 | 119 | 1,095 | 2 | 1,264 | 104 | 8 |
| 1,000 | 1 | SSE | 1,273 | 113 | — | 151 | 1,290 | 110 | 1 |
| 1,000 | 5 | SSE | 1,120 | 112 | 2,072 | 454 | 1,114 | 123 | 9 |
| 5,000 | 1 | Poll | 1,602 | 113 | — | 138 | 1,615 | 139 | 1 |
| 5,000 | 5 | Poll | 1,449 | 112 | 2,576 | 169 | 1,609 | 172 | 53 |
| 5,000 | 1 | SSE | 1,458 | 127 | — | 133 | 1,588 | 167 | 1 |
| 5,000 | 5 | SSE | 1,584 | 111 | 2,516 | 142 | 1,461 | 181 | 53 |

Baseline means first usable snapshot. Live latency starts before the SQL update
call and ends when every reader has installed and acknowledged that write; it
includes the database round trip. With eight observations, p95 is the maximum.
Warm fan-out starts additional readers only after baseline offload; their reads
must exercise stored objects. Object GET counts include replay and refetch.

Baseline offload took another 0.4–1.5 seconds at 1,000 rows and 2.6–2.8 seconds
at 5,000 rows. Two sealed ranges left about 160 KiB of resident log at 1,000 rows;
13 ranges left about 6 KiB at 5,000 rows. The owner still retained all raw rows
for classification. Eight updates added about 10.8 KiB of log per scenario.
Successful object uploads transferred 0.69/3.45 MB per scenario, including the
replacement baseline. Bytes divided by summed upload service time ranged from
0.41 to 1.24 MB/s; this is a request-service measurement, not end-to-end capacity.

There were no unexpected storage failures. Each scenario cancelled one upload
when deleting an owner. The runner waited for all test handles and pending object
cleanups to disappear, and verified deletion of its database rows.

These are local-to-staging timings with one owner and at most five readers,
5,000 rows and eight sequential writes. Baseline, fan-out and refetch fit well
inside the 15-minute lifetime for these fixtures. They establish no larger size,
sustained write-rate, concurrency or deployed capacity limit. There is no object
cache: additional readers reread history. Process memory is sampled every 100 ms,
can miss short peaks and includes runtime/SDK overhead and GC. The process is
reused across scenarios; RSS and heap deltas are not exact retained-row memory.

## Reproduce

Load the ignored private environment files and start the Function in one terminal:

```sh
mkdir -p .artifacts/staging
NEON_SHAPE_DIAGNOSTICS=true \
NEON_SHAPE_DIAGNOSTICS_FILE="$PWD/.artifacts/staging/function-performance.ndjson" \
node --env-file=.env.local --env-file=.env.storage.local scripts/dev-function.mjs
```

After the Function emits memory samples, use a second terminal:

```sh
NEON_SHAPE_DIAGNOSTICS_FILE="$PWD/.artifacts/staging/function-performance.ndjson" \
pnpm test:local-performance
```

`SHAPE_URL` can select another local port/path. The recorded run used a separate
local dev port. Both commands must use the same diagnostics file. Keep source
files stable during the run; restart `neon dev` after changing server dependencies.
Do not force `NEON_SHAPE_SEAL_BYTES=1` for these measurements: the default is
256 KiB, and both processes must agree on any override.

`BENCHMARK_ROWS`, `BENCHMARK_READERS`, `BENCHMARK_PAYLOAD_CHARS` and
`BENCHMARK_MUTATIONS` select additional experiments. JSON results stay in ignored
`.artifacts/staging/local-performance.json`. Missing credentials, diagnostics,
local routing or object-backed replay fail the gate explicitly.
