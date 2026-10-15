# Local performance evidence

The latest eight-scenario run on 16 October 2026 passed against the Function
running under `neon dev`, real staging Postgres/Realtime and real Neon Object
Storage. Both runner and Function used Node 24.10.0; the Function startup
record verifies its runtime version. An earlier run used Node 22.23.2. These
separate local runs are not a controlled comparison between Node versions.

Each scenario uses a primary-keyed int4/text/bool table, full replica mode,
512-character ASCII labels and eight sequential updates. Readers share one
query-string handle. The runner verifies every baseline cell, live rows and real
transaction acknowledgements, compares final rows with independent SQL, replays
from a saved offset and measures refetch after deleting the old owner.

| Rows  | Readers | Mode | Baseline ms | Live p95 ms | Warm fan-out ms | Delta replay ms | Refetch ms | Peak RSS MiB | Object GETs |
| ----- | ------- | ---- | ----------- | ----------- | --------------- | --------------- | ---------- | ------------ | ----------- |
| 1,000 | 1       | Poll | 1,284       | 140         | —               | 606             | 1,421      | 98           | 1           |
| 1,000 | 5       | Poll | 1,266       | 240         | 2,728           | 381             | 2,508      | 118          | 9           |
| 1,000 | 1       | SSE  | 1,425       | 113         | —               | 158             | 1,262      | 130          | 1           |
| 1,000 | 5       | SSE  | 1,406       | 115         | 1,151           | 4               | 1,274      | 138          | 8           |
| 5,000 | 1       | Poll | 1,606       | 112         | —               | 140             | 1,748      | 185          | 1           |
| 5,000 | 5       | Poll | 1,615       | 119         | 2,645           | 184             | 1,628      | 272          | 53          |
| 5,000 | 1       | SSE  | 1,582       | 112         | —               | 147             | 1,589      | 288          | 1           |
| 5,000 | 5       | SSE  | 1,603       | 117         | 2,839           | 143             | 1,585      | 312          | 53          |

Baseline means first usable snapshot. Live latency starts before the SQL update
call and ends when every reader has installed and acknowledged that write; it
includes the database round trip. With eight observations, p95 is the maximum.
Warm fan-out starts additional readers only after baseline offload; their reads
must exercise stored objects. Object GET counts include replay and refetch.

Baseline offload took another 0.3–0.9 seconds at 1,000 rows and 3.0–3.2
seconds at 5,000 rows. Resident log bytes after sealing were about 160 KiB at
1,000 rows and 6 KiB at 5,000 rows; the owner still retained raw rows for
classification. Eight updates added about 10.8 KiB of log per scenario.
Successful object uploads transferred 0.69 MB at 1,000 rows and 3.45–3.72 MB
at 5,000 rows, including the replacement baseline. Bytes divided by summed
upload service time ranged from 0.60 to 1.26 MB/s; this is a request-service
measurement, not end-to-end capacity.

There were no unexpected storage failures. Each scenario cancelled one or two
uploads when deleting owners. The runner waited for all test handles and pending
object cleanups to disappear, and verified deletion of its database rows.

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
