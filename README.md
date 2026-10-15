# RelayLab

A recoverable execution system for coding agents -- durable task
ownership, fenced handoff, idempotent commands, and at-least-once event
delivery with durable consumer-side deduplication. Independent portfolio
project; not affiliated with Capy.

Phase 1 (durable orchestration foundation) and the Phase 1.1 correctness
audit are implemented. See [CLAUDE.md](CLAUDE.md) for the full picture:
purpose, structure, exact commands, correctness invariants, and rules for
what counts as "tested." See [docs/PROGRESS.md](docs/PROGRESS.md) for what
was actually built and verified -- including every real bug found and
fixed -- and [evidence/phase1-evidence.md](evidence/phase1-evidence.md)
for raw test output and version info.

```bash
docker compose -f infra/compose.yaml up -d
npm install
npm run migrate
npm run test:unit
npm run test:integration
npm run demo:recovery
npm run demo:crash-windows
```
