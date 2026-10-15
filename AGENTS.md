# Project working context

- Use Sam's Neon staging workspace for real Postgres and Realtime integration tests.
- Run the Function locally with `neon dev`. Request affinity is not available on the platform yet; do not deploy the Function for shape/affinity testing at this stage.
- Test single-owner behavior locally and simulate routing, owner loss and expiry. Keep actual platform affinity, idle/eviction and `waitUntil` verification deferred until the required platform support exists. Local success does not prove these platform guarantees.
- See [testing environment](./docs/testing-environment.md) for the private configuration workflow. Never commit environment-specific hosts, account/resource identifiers or credentials, including in commit history.
- Keep credentials in ignored environment files. Use the pinned published Neon Realtime SDK, with committed pnpm patches if needed, rather than assuming the guide's older package-availability note is current.

The architecture and scope are recorded in [the implementation plan](./implementation-and-testing-plan.md) and [the Durable Streams design](./durable-streams-shape-design.md).
