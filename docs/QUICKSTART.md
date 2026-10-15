# Run RelayLab locally

## Prerequisites

- Node 24 and npm; check `node --version` in the terminal used for tests.
- Git available on PATH.
- Docker Desktop or Docker Engine running Linux containers.
- Bash for the Kubernetes scripts (Git Bash on Windows). Ordinary npm commands
  also work in PowerShell; use `npm.cmd` if script execution policy blocks npm.ps1.

## Fast checks without Docker

```bash
npm ci
npm run typecheck
npm run build
npm run test:unit
```

## Real handoff

```bash
docker compose -f infra/compose.yaml up -d --wait mysql nats
npm run migrate
npm run docker:build-verify-image
npm run demo:handoff
```

MySQL uses localhost:3306 and NATS localhost:24222. Check for occupied ports
before starting. These are development credentials, defined in Compose and
`src/config.ts`; never expose this configuration publicly.

Integration tests use `relaylab_test`, created by the MySQL init script on the
first start of a fresh volume. They truncate test tables. Do not point the test
configuration at a database containing useful data. The suite intentionally uses
`--test-concurrency=1`; do not run multiple suites against that database at once.

```bash
npm run test:integration
npm run test:git-worker-crash
npm run test:handoff-crash
```

If the test database is missing in an older existing volume, investigate its
initialization rather than deleting the volume. Missing dependencies fail loudly;
the suite does not silently replace MySQL/NATS/Docker with mocks.

## Configuration

`.env.example` documents development defaults. The application reads process
environment variables; it does **not** automatically load .env files. Set values
in your shell only when overriding the defaults. For example, in PowerShell:

```powershell
$env:RELAYLAB_MYSQL_PORT = '3306'
$env:RELAYLAB_NATS_URL = '127.0.0.1:24222'
```

See `src/config.ts` for the authoritative configuration and the Kubernetes
runbook for separate cluster ports, credentials, and context targeting.

## Observability and local Kubernetes

Follow [OBSERVABILITY.md](OBSERVABILITY.md), [METRICS.md](METRICS.md), and
[KUBERNETES.md](KUBERNETES.md). Start only the stack you intend to exercise.
Host workers and verification containers remain outside Kubernetes in this phase.

## Stop without deleting data

```bash
docker compose -f infra/compose.yaml stop
```

This stops the Compose services and retains volumes. Kubernetes has its own
explicit teardown script; read the runbook before invoking destructive teardown.
