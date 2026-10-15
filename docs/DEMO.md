# A reviewer’s tour

## Start with the failure, then inspect the guarantee

1. Run `npm run demo:handoff` after the quickstart. Follow the source/result Git
   identities and the ownership epoch rather than treating exit code zero as proof.
2. Open `src/worker/handoff.ts`: inspect preparation, ownership transfer,
   independent restoration, verification, and acknowledgement.
3. Open `src/db/task-store.ts`: inspect the locked task row, receipt replay,
   fresh-write fencing, checkpoint identity, and atomic durable mutations.
4. Read `tests/integration/git-worker-handoff-crash.ts` to see the pause/kill/restart
   scenario. Run it only against the intended local disposable infrastructure.
5. Read `src/worker/docker-verify.ts` alongside `docs/EXECUTION.md`. Input fidelity,
   cleanup failure, and constrained execution are separate guarantees.
6. Follow `docs/KUBERNETES.md` to see where services actually run. The host workers
   are intentionally outside the local cluster in this version.

## What to look for

| Observation | What it supports |
| --- | --- |
| Source and restored commit/tree match | The handoff preserved the selected Git identity |
| A and B use distinct execution identities | Restoration receives independent verification |
| One acknowledgement after retry | The durable acknowledgement path deduplicates that scenario |
| Stale owner is rejected | The tested durable boundary enforces ownership fencing |
| An orphan is removed while another execution survives | Cleanup is scoped in the exercised case |

None of these observations alone proves production readiness or exactly-once
external effects. Read the test’s assertions and threat-model limitations.

## Publication evidence

[Fresh check output](../evidence/publication-checks.txt) records Node 24, build,
typecheck, unit results, and source-import comparison from this publication pass.
The publication-check path count reflects the moment it ran, before this evidence
file and tour were added. Current checks can be reproduced with
`npm run check:publication`.
