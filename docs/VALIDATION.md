# Validation ledger

## Publication checks â€” 2026-10-15

The publication copy was checked on Windows with host Node **v24.21.0**.

| Check | Result | Scope |
| --- | --- | --- |
| `npm ci --ignore-scripts --no-audit --no-fund` | Passed | Lockfile installation; 91 packages |
| `npm run typecheck` | Passed | Application and test types |
| `npm run build` | Passed | Application compilation |
| `npm run test:unit` | 16 passed, 0 failed, 0 skipped | Domain, fingerprint, health behavior |

The implementation source and SQL migrations were copied from the existing
working project. This publication pass did not rerun infrastructure integration,
process-crash, or Kubernetes scenarios, and did not disturb the working cluster.

[Fresh captured output](../evidence/publication-checks.txt) includes the commands,
exit codes, unit results, and source/test/migration import-identity comparison.
The SVG banner also passed XML parsing.

## Historical development evidence

[The imported milestone record](../evidence/phase1-evidence.md) and
[progress history](PROGRESS.md) contain earlier real-service and crash results.
They cover different snapshots, commands, and runtimes. Their counts must not
be added together or represented as a single current CI run.

Raw machine-specific log captures are not included in this publication copy.
Some historical text references their filenames; those references describe
prior local work, not files guaranteed to exist in this public repository.

## CI

The GitHub workflow runs build, typecheck, unit tests, and publication checks.
Its actual run status is available in the Actions tab. Local success does not
establish remote CI success; only a completed Actions run does that.

See [CI.md](CI.md), [QUICKSTART.md](QUICKSTART.md), and
[RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md) for reproducible commands and limits.
