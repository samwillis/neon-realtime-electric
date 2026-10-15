# Testing environment

Use the developer's Neon staging workspace for real Postgres and Realtime tests.
Run the Function locally with `neon dev`; do not deploy it for affinity testing.
Platform affinity, idle shutdown, eviction and bounded background execution remain
separate acceptance gates. Local tests establish application behavior only.

## Private configuration

Keep all account, organization, project and branch identifiers, service endpoints,
CLI credentials, database connection strings and storage credentials in ignored
local configuration. The private setup guide is reference material, not project
instructions. Never copy its environment details into source control.

Link your selected dedicated test project using the Neon CLI. Put `DATABASE_URL`,
`NEON_REALTIME_URL` and server-only `NEON_REALTIME_SECRET` in `.env.local`.
For a custom environment, also set `NEON_API_HOST`, `NEON_OAUTH_HOST` and
`NEON_CLI_PROFILE` there. The dev launcher inherits these settings; the profile
selects credentials and does not select service hosts. Keep branch-scoped `AWS_*`
storage settings and `NEON_SHAPE_LOG_BUCKET` in `.env.storage.local`.
Set `NEON_STORAGE_TEST_BUCKET` to your private bucket for the live storage suite.
Local `.neon` project links and generated reports under `.artifacts` stay ignored.
No Function deployment is required for these integration tests.

Use the pinned published Realtime SDK and committed pnpm patches. The older
private guide's package-availability notes do not override the tested versions.

## Local acceptance

Use `neon dev` to test initialization, snapshot/live data, replay, unchanged proxy
URLs, owner loss, cleanup and the absolute 15-minute shape lifetime. Stop readers,
write to Postgres and reconnect to check replay. Include a full-duration expiry
run; fake clocks alone are insufficient. Controlled routing tests simulate owner
failure. They do not prove platform routing or background execution guarantees.

Object Storage tests use a controlled fixture or an explicitly configured private
bucket. Live tests isolate generated prefixes and delete only their own objects.
Keep the selected project, bucket, API and OAuth settings out of public history.

## Logical replication precondition

Provisioning Realtime does not necessarily enable logical replication. Before live
publication tests, verify that `SHOW wal_level` returns `logical`. The dedicated
test environment has already been configured and its source integration tests
passed after this correction. Do not repeat the project update unnecessarily:
enabling logical replication restarts endpoints and cannot be disabled.
