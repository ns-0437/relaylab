# RelayLab on Kubernetes (Phase 3A Part B + Phase 3A.1 reliability patch)

A local `kind` deployment of RelayLab's two existing long-lived
coordination services (the outbox publisher, the durable NATS consumer)
plus the MySQL and NATS JetStream they depend on. **This phase does not
move verification into Kubernetes.** The one-shot Git-worker CLIs
(`src/worker-main.ts`, `src/restore-worker-main.ts`) and the Docker
verification adapter (`src/worker/docker-verify.ts`) stay on the host,
unchanged, using the host's own Docker daemon exactly as before.

## Deployment boundary

Decided by reading the actual entry points (`src/messaging/publisher-main.ts`,
`src/messaging/consumer-main.ts`, `src/worker-main.ts`,
`src/restore-worker-main.ts`, `src/config.ts`) before writing anything --
not assumed from the project's description.

```
In Kubernetes (namespace: relaylab, cluster: relaylab-local)
  relaylab-publisher   Deployment, replicas: 1   (src/messaging/publisher-main.ts)
  relaylab-consumer    Deployment, replicas: 1   (src/messaging/consumer-main.ts)
  relaylab-mysql       StatefulSet, replicas: 1, PVC   (mysql:8.4)
  relaylab-nats        StatefulSet, replicas: 1, PVC   (nats:2.10-alpine, JetStream -sd /data)

Stay on the host, unchanged
  src/worker-main.ts            one-shot CLI, worker A -- needs a local Git repo on disk
  src/restore-worker-main.ts    one-shot CLI, worker B -- needs a local Git repo on disk
  src/worker/docker-verify.ts   the Docker verification adapter -- needs the HOST's Docker daemon
                                 (never a Docker-in-Docker setup, never mounts the host socket
                                 into any pod)
  infra/verify-image/Dockerfile the pinned verification sandbox image (unchanged, unrelated
                                 to infra/app-image/Dockerfile below)

DB/broker connections
  In-cluster:  publisher/consumer -> relaylab-mysql.relaylab.svc.cluster.local:3306,
               relaylab-nats.relaylab.svc.cluster.local:4222 (ClusterIP/headless Service DNS)
  From host:   worker-main.ts/restore-worker-main.ts, demo scripts, and the migration step all
               reach the SAME cluster MySQL via an explicitly started
               `kubectl port-forward svc/relaylab-mysql-lb` on 127.0.0.1:33060 (never 3306 --
               that is infra/compose.yaml's own dev MySQL; see "Two local stacks" below)

Git repositories / handoff artifacts
  Always on the host filesystem (worker-main.ts's managed workspace root, handoff.ts's
  artifact bundles) -- Kubernetes has no involvement in Git state at all, in this phase.
```

### Why replicas stay at 1, and the rollout strategy

Both Deployments use `strategy: { type: Recreate }`: Kubernetes waits
for the OLD ReplicaSet's pod to be FULLY gone (not just `Terminating`)
before creating the new one. This is the appropriate choice given the
project's existing single-instance assumptions -- `RollingUpdate` would
briefly run old and new pods simultaneously, which is exactly what must
never happen here. A brief outage during a planned upgrade (no pod
Ready for a few seconds) is an accepted cost, per the explicit
instruction for this patch.

- **Publisher**: `publisher_leader` is a DB-backed exclusive lock
  (`src/messaging/publisher-lock.ts`); a second replica would just be a
  process permanently losing the leadership race, not real HA. Raising
  this requires implementing and proving real multi-instance
  coordination first -- not done this phase.
- **Consumer**: the durable NATS consumer name doubles as the
  `consumer_name` partition key in `task_consumer_state`
  (`src/config.ts` `eventsDurable()`); the code's own comments
  (`consumer-main.ts`, `relaylab_consumer_gauge_exporter_count`) already
  flag a second instance sharing that name as unsupported, not merely
  untested.

### Consumer exclusivity (Phase 3A.1) -- what actually prevents two consumers from conflicting

**`replicas: 1` + `Recreate` only protect a NORMAL rollout.** They do
nothing against a manual second Pod, a forced/SIGKILL-style termination
racing a replacement, or someone editing `replicas: 2` by hand. Before
this patch, NOTHING at the application layer actually enforced
single-instance execution -- it was pure operational discipline.
**Publisher leadership does NOT protect the consumer either** -- they
are two separate lock rows (see below); the publisher being
single-leader says nothing about the consumer.

Tracing `src/messaging/consumer.ts`'s actual transaction logic first
(before assuming a gap exists): `processBatch` already does
`SELECT ... FROM task_consumer_state ... FOR UPDATE` and
`SELECT ... FROM consumer_processed ... FOR UPDATE` inside one
transaction per batch. This means two consumer processes pulling
concurrently from NATS could NOT corrupt `last_applied_seq` or
double-apply an `event_id` even before this patch -- MySQL's row lock
serializes the second transaction behind the first, and the gap-buffer
logic (`task_consumer_pending`) correctly absorbs out-of-order delivery
either way. **This patch is not fixing a data-corruption bug that
doesn't exist; it is removing wasted/competing work and giving an
explicit, provable "exactly one active" guarantee**, per this patch's
explicit instruction to enforce the supported lifecycle regardless.

The fix reuses the SMALLEST existing mechanism: `src/messaging/
publisher-lock.ts`'s one function was already generic (parameterized by
`lock_name`, never assumed to mean "the publisher"). `consumer-main.ts`
now calls the same function (aliased as `tryAcquireOrRenewExclusiveLock`)
under `lock_name = "consumer-default"` (its own row, added by
`infra/migrations/0012_consumer_leader_lock.sql`) once per poll
iteration; `processBatch` is only called when the lock is held. A
`relaylab_consumer_exclusivity_state` gauge mirrors the publisher's
`relaylab_publisher_leadership_state` exactly.

**Proven directly, not just by rollout**: `kubectl scale deployment
relaylab-consumer --replicas=2` on the live cluster. Both pods reached
`Ready` (readiness never checks the lock, by design), but only ONE
pod's log ever showed `acquired the consumer exclusivity lock`; a
command seeded while both ran was processed exactly once, by the lock
holder, with the standby pod's log showing zero further lines. See
evidence/phase1-evidence.md section C for the full transcript.

### Two local stacks can coexist on this host

`infra/compose.yaml`'s dev stack (MySQL on `127.0.0.1:3306`, NATS on
`127.0.0.1:24222`) and this Kubernetes stack are **independent** and can
run at the same time. The Kubernetes port-forwards deliberately use
different host ports (`33060` for MySQL, `24322` for NATS -- see
`infra/k8s/scripts/lib/common.sh`) specifically because `kubectl
port-forward 3306:3306` was observed to bind `[::1]:3306` instead of
failing when compose's MySQL already held `127.0.0.1:3306` -- a
same-port, different-address-family coincidence that would have let a
plain `127.0.0.1:3306` connection reach the wrong database silently.
Never point the migration step or a demo script at port 3306/24222 when
working with the cluster.

### Port-forwards do NOT follow a replaced pod -- correcting Part B's original claim

Part B's first draft of this document claimed a Service-targeted
`kubectl port-forward` (`svc/relaylab-mysql-lb`) "already solves" pod
replacement. **That was wrong, and Phase 3A.1 found out the hard way**:
`kubectl port-forward svc/X` resolves the Service to ONE specific pod
the moment the tunnel is established, then behaves exactly like a
pod-direct forward from then on. When that pod is later replaced (a
rollout, a crash, or -- as exercised directly in this patch's bounded
outage test -- scaling the StatefulSet to 0 and back), the existing
tunnel does not migrate to the new pod. It errors out:

```
error forwarding port 3306 to pod <old-sandbox-id>: failed to find sandbox "<old-sandbox-id>" in store: not found
error: lost connection to pod
```

**Operational rule**: after ANY action that replaces the MySQL or NATS
pod (a dependency-outage test, a manual delete, a StatefulSet
rollout), run `stop-port-forwards.sh` then `start-port-forwards.sh`
again before any further host-side script. `04-run-demo.sh`'s
`assert_targeting_cluster_mysql` check (see below) will fail loudly and
specifically, not hang or silently misbehave, if this step is skipped.

## What's new

```
infra/
  app-image/
    Dockerfile            new: the publisher/consumer application image. Node 24.21.0-alpine3.24
                           (same pin as infra/verify-image/Dockerfile), multi-stage (build -> prod
                           deps -> runtime), non-root, no ENTRYPOINT (command supplied by the
                           Deployment manifest -- ONE image backs both services)
  k8s/
    kind-config.yaml       new: pins the kind node image by exact sha256 digest
    manifests/
      00-namespace.yaml     new
      10-mysql.yaml          new: headless + ClusterIP Services, StatefulSet+PVC, startup/
                             liveness/readiness all via `mysqladmin ping`
      20-nats.yaml           new: headless + ClusterIP Services, StatefulSet+PVC (`-js -sd /data`),
                             probes via the server's own /healthz on :8222
      30-configmap.yaml      new: non-secret settings, the SAME env var names src/config.ts
                             already reads
      40-publisher.yaml      new: Deployment (replicas: 1, Recreate strategy) + Service
      50-consumer.yaml       new: Deployment (replicas: 1, Recreate strategy) + Service
    scripts/
      lib/common.sh          new: shared constants/helpers (cluster/namespace names, dedicated
                              kubeconfig, host ports, kctl() wrapper)
      00-check-prereqs.sh    new
      01-build-image.sh      new: docker build + kind load docker-image
      02-up.sh               new: create/reuse cluster, namespace, generated Secret, ConfigMap,
                              MySQL/NATS manifests
      03-wait-ready.sh        new: bounded readiness waits + the explicit migration step (refuses
                              to apply/wait for publisher/consumer if migration fails)
      start-port-forwards.sh / stop-port-forwards.sh   new (Phase 3A.1: --address 127.0.0.1
                              explicit, preflight port-conflict detection, PID+taskkill+
                              port-based cleanup -- see "Port-forwards do NOT follow a
                              replaced pod" above for why Service-targeting alone isn't enough)
      04-run-demo.sh          new: identity evidence + the A->B handoff + messaging smoke test
      05-pod-replacement-demo.sh   new: the consumer pod-replacement proof
      teardown.sh             new: the ONLY script that deletes the cluster/data; requires typed
                              confirmation; never called automatically
  .dockerignore               new (repo root)
src/
  telemetry/health.ts          new (Part B), extended (Phase 3A.1): startup/live/ready HTTP
                                 probes + startDependencyPing (bounded/cached MySQL check) +
                                 a real 'error' listener (see "Health checks" below for the
                                 regression this closes)
  messaging/publisher-main.ts  changed: starts/stops the health server + dependency ping;
                                 readiness composed from NATS+MySQL+!stopping; health port
                                 now defaults to an EPHEMERAL port, not a fixed 9470
  messaging/consumer-main.ts   changed: same health/readiness wiring as publisher-main.ts,
                                 PLUS (Phase 3A.1) the consumer exclusivity lock -- see
                                 "Consumer exclusivity" above
  messaging/publisher-lock.ts  changed (Phase 3A.1): one new exported alias,
                                 tryAcquireOrRenewExclusiveLock, for the SAME existing
                                 function -- no new locking logic
infra/migrations/
  0012_consumer_leader_lock.sql  new (Phase 3A.1): adds the consumer's own lock_name row.
                                 Numbered 0012, not 0011 -- see "Migration failure" below
tests/integration/
  demo-k8s-messaging.ts        new: proves the REAL in-cluster publisher/consumer pods carry a
                                command end to end, without spawning its own processes
tests/unit/
  health.test.ts                new (Phase 3A.1): 9 tests -- startup/live/ready, dependency
                                 failure/recovery, shutdown, startDependencyPing's bounded
                                 timeout and stop() behavior
```

## Exact commands

```bash
# One-time per session, in order:
infra/k8s/scripts/00-check-prereqs.sh
infra/k8s/scripts/02-up.sh            # creates/reuses the kind cluster + namespace + secrets,
                                       # applies ConfigMap/MySQL/NATS, builds+loads the app image
infra/k8s/scripts/03-wait-ready.sh    # waits for MySQL/NATS, runs the migration, applies and
                                       # waits for publisher/consumer

# For any demo/validation:
infra/k8s/scripts/start-port-forwards.sh
infra/k8s/scripts/04-run-demo.sh
infra/k8s/scripts/05-pod-replacement-demo.sh
infra/k8s/scripts/stop-port-forwards.sh

# Manual inspection:
export KUBECONFIG=infra/k8s/.kubeconfig
kubectl --context kind-relaylab-local --namespace relaylab get pods

# Separate, explicit, NEVER automatic -- deletes the cluster and all PV data:
infra/k8s/scripts/teardown.sh
```

Tool/image versions used (verified against official docs/release pages
before use, not assumed):

- `kind` v0.33.0 (`winget install Kubernetes.kind`)
- kind node image: `kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`
  (pinned to match this host's already-installed `kubectl` client v1.36.1's minor version)
- `kubectl` client v1.36.1 (pre-existing on this host)
- App image base: `node:24.21.0-alpine3.24` (same pin as `infra/verify-image/Dockerfile`)
- `mysql:8.4`, `nats:2.10-alpine` (same images/tags as `infra/compose.yaml`)

## Setup re-entry and credential safety (Phase 3A.1)

### The PVC-deletion incident, explained

During Part B's ORIGINAL setup, a stuck MySQL pod (`ContainerCreating`
for several minutes -- traced to the kind node's own image pull hanging,
see "Image loading" below) was force-deleted. The pod's PVC then
remained bound while a NEW pod attempted to re-initialize into the SAME
(partially-written, from the stuck first attempt) data directory, and
MySQL's own entrypoint refused: `--initialize specified but the data
directory has files in it. Aborting.` The PVC
(`data-relaylab-mysql-0`) was then deliberately deleted (after scaling
the StatefulSet to 0 first, so nothing still referenced it) to let a
FRESH PVC be provisioned. **This was a manual recovery action taken
during live debugging, not something any setup script did on its own**,
and at that point the PVC contained ONLY a partially-initialized,
never-successfully-started MySQL data directory (zero application
data -- migrations had not even run yet). No schema, no rows, no
checkpoints existed in it. There is no remaining uncertainty about this
specific incident: it is fully reconstructable from the session's own
command transcript (preserved in this conversation), and nothing about
it was invented after the fact.

**What Phase 3A.1 actually closes** is the GENERAL risk class this
incident exposed, not a recurrence of the exact incident: nothing
previously stopped an ordinary RE-RUN of the setup scripts from
destroying data or desynchronizing credentials. That general risk is
addressed below, independent of whether the original incident recurs.

### Credentials are never blindly rotated

`infra/k8s/scripts/lib/common.sh`'s `ensure_mysql_secret` (called by
`02-up.sh`) replaces the old "always re-apply from the local file"
logic. It now:
- Reuses the local `.secrets.env` file WITHOUT touching the cluster's
  Secret when the two already match (the common, expected case on
  every re-run).
- **Fails loudly, before touching anything**, if the local file and the
  cluster's existing Secret have DIFFERENT values -- re-applying would
  silently desynchronize the Secret from what MySQL's user table was
  actually initialized with (`MYSQL_PASSWORD` only takes effect on the
  data directory's FIRST initialization; changing the Secret afterward
  changes nothing inside MySQL itself).
- If the local file is missing but the cluster's Secret already exists,
  reads the credentials BACK from the cluster into a fresh local file,
  rather than generating new, incompatible ones.
- Refuses to generate fresh credentials at all if the MySQL PVC already
  exists (likely holds initialized data) and NEITHER the Secret nor the
  local file does -- that combination can only mean the real credentials
  were lost, and guessing new ones would just produce silent auth
  failures later.

### Migration failure still gates readiness, and so does a content mismatch

`03-wait-ready.sh` runs the migration step BEFORE applying the
publisher/consumer Deployments, exactly as in Part B. Phase 3A.1 added
a NEW migration, `0012_consumer_leader_lock.sql` -- numbered 0012, not
0011, because of a genuine finding during THIS patch's own re-entry
test: the database already had an `0011_consumer_leader_lock.sql` row
recorded with a DIFFERENT checksum than the file this patch initially
wrote under that name (almost certainly from an earlier, now-unrecoverable
draft of this same change). `migrate.ts`'s own existing integrity check
caught this immediately and refused to proceed -- exactly the designed
behavior ("a previously-applied migration must never be edited"). The
correct, policy-compliant fix was to renumber the new file, which this
patch did; the original 0011 file's content could not be recovered (a
filesystem search found no surviving copy) and is not guessed at here.

### Setup re-entry, proven on the live cluster

With the cluster already running from Part B: recorded the MySQL PVC's
UID, the exact list of applied migrations, and seeded a sentinel task
via `demo:k8s-messaging`. Re-ran `02-up.sh` then `03-wait-ready.sh` in
full. Confirmed: the local secrets file matched the cluster's Secret
and was reused without change; the PVC UID was byte-for-byte identical
before and after; all 10 pre-existing migrations reported `[skip] ...
checksum matches`; the sentinel task's `event_count` was unchanged. See
evidence/phase1-evidence.md for exact output.

## Persistence -- what it does and does not mean

Both MySQL and NATS use `volumeClaimTemplates` (real PersistentVolumeClaims,
backed by kind's default `local-path-provisioner`, which stores the data
on the kind node container's own filesystem).

- **Survives**: pod deletion/replacement, `kubectl rollout restart`,
  normal pod crashes. Proven directly in this phase (`05-pod-replacement-demo.sh`).
- **Does NOT survive**: `kind delete cluster` (i.e. `teardown.sh`) --
  the node container itself is destroyed, taking its local storage with
  it. This is **not backup/HA** -- it is single-node local-dev
  persistence only. There is no snapshot/restore story in this phase.

## Health checks: why three, not one

`src/telemetry/health.ts` exposes `/healthz/startup`, `/healthz/live`,
`/healthz/ready` as three independently meaningful signals, because a
single shared check cannot express the distinction this project's own
code already relies on (see `publisher-main.ts`'s pre-existing comment:
*"a healthy /metrics scrape does NOT prove this process can reach
NATS"*):

| Probe | Reflects | Can a NATS/MySQL outage fail it? |
|---|---|---|
| `/healthz/startup` | the one-time initial connect sequence has completed, ever | No -- latches true once reached, never reverts |
| `/healthz/live` | the HTTP server itself is answering | No, by construction -- never inspects NATS/MySQL |
| `/healthz/ready` | NATS connectivity AND a bounded/cached MySQL ping AND `!stopping` | Yes -- this is the ONE probe allowed to fail here |

This is what keeps a transient broker/DB outage from ever triggering a
liveness-driven restart loop, per Part B's explicit requirement.

**A real crash-causing regression, found and fixed while validating
THIS patch**: `startHealthServer`'s `server.listen(port, ...)` had no
`'error'` handler, and a fixed default port (`9470`) meant that TWO
real `publisher-main.ts` processes spawned on the SAME host (exactly
what `tests/integration/publisher-leadership.test.ts` deliberately
does, to test leader failover) collided on that port -- Node's default
behavior for an unhandled `'error'` event is to THROW, which crashed
the second process outright with `EADDRINUSE`. The crash happened to
leave the test's OWN assertion passing for the wrong reason (the
survivor trivially "won" leadership because its rival had just
crashed), masking the bug on a first run. Fixed two ways: (1)
`publisher-main.ts`/`consumer-main.ts` now default `RELAYLAB_HEALTH_PORT`
to `0` (an OS-assigned ephemeral port) unless explicitly set -- the
Kubernetes ConfigMap always sets it to `9470` explicitly, so the real
deployment is unaffected; (2) `startHealthServer` now has a real
`'error'` listener that logs instead of crashing, as defense in depth
for any other unexpected bind failure. Confirmed fixed: the 4 directly
affected test files (`health.test.ts`, `publisher-leadership.test.ts`,
`publisher-lock-epoch.test.ts`, `shutdown.test.ts`) went from a hard
process crash + 3 cascading failures to **24/24 passing** on Node 24,
run with `--test-concurrency=1` matching this project's own
`test:integration` convention (an EARLIER attempt without that flag
produced 3 unrelated-looking failures that turned out to be ordinary
shared-test-database interference from running 4 files concurrently,
not a real regression -- re-confirmed by rerunning with the flag).

**Phase 3A.1**: readiness now ALSO reflects MySQL, via
`startDependencyPing` (`src/telemetry/health.ts`) -- a periodic
(2s), bounded (1.5s timeout), CACHED ping (`pool.query("SELECT 1")`),
never a query run synchronously inside the HTTP probe handler itself
(which would turn kubelet's own probe cadence into an unbounded query
loop). Readiness also now goes false the INSTANT shutdown begins
(`!stopping`), independent of when the health server itself actually
closes -- proven by `tests/unit/health.test.ts`'s shutdown case and by
the live bounded-outage test below. Liveness and startup are
UNCHANGED: liveness still never inspects either dependency, and startup
still only reflects the one-time initial connect.

**Live, bounded outage test performed on the real cluster** (never
deleting storage): `kubectl scale statefulset relaylab-mysql --replicas=0`,
confirmed via `/healthz/ready` returning 503 on both publisher and
consumer within seconds, `/healthz/live` staying `200` throughout, and
restart counts staying at `0` after waiting 70s (past the liveness
probe's own 60s failure window) -- then scaled back to `1`, confirmed
readiness returned to `200` automatically, the PVC's UID was unchanged,
and a freshly-seeded command was processed end to end by the real
in-cluster pods. See evidence/phase1-evidence.md for the exact command
transcript.

## Image loading, and what "pinned" actually means here (Phase 3A.1)

The `docker save` + `ctr images import` workaround for `mysql:8.4` /
`nats:2.10-alpine` (found necessary in Part B: the kind node's own pull
hangs indefinitely on this host, and `kind load docker-image`'s
`--all-platforms` import fails separately) is ALREADY encoded in
`load_vendor_image` (`infra/k8s/scripts/lib/common.sh`), called
automatically by `02-up.sh` -- no manual recovery step is required for
a normal setup run.

**Honesty about "pinned"**: `mysql:8.4` and `nats:2.10-alpine` are
ordinary, MUTABLE Docker Hub tags -- not content-pinned by digest, and
this document does not call them immutable. Only the kind NODE image
(`kindest/node:v1.36.4@sha256:...`) is pinned by exact digest. What this
phase records instead, as the honest substitute, is the ACTUAL resolved
image ID of what is running at any given moment:

```
locally built app image id:        sha256:dfe93d64a71b567350da61782852fa37e1efece2a7da5e220e6df83875ddace3
publisher/consumer pod running:    docker.io/library/import-2026-10-15@sha256:3d14a806c478034539d9c64d768b4554da8b1e07dff468bcca518d7da59728d9
```
(The pod's reported image reference differs in NAME from the locally
built tag -- `import-<date>@sha256:...` is containerd's own naming for
an image brought in via `ctr images import`, not evidence of a
different image; what matters is that `03-wait-ready.sh`'s
`stamp_and_rollout` ties the Deployment's pod template directly to the
CONTENT id above, so a rebuild with different content is guaranteed to
trigger a real rollout -- see "Why replicas stay at 1" section's
rollout-strategy note.)

**A real gap closed**: `kubectl apply` on a Deployment whose pod
template still names the SAME floating tag (`relaylab-app:local`) is a
NO-OP even when the underlying image content changed -- confirmed
directly (a rebuilt image sat unused by the running pods until this was
fixed). `stamp_and_rollout` now stamps the freshly-built image's
resolved ID onto the pod template as an annotation on every
`03-wait-ready.sh` run, so a content change always produces a real,
observable rollout (`kubectl rollout status` has something genuine to
wait on), and a re-run with an unchanged image is a true no-op.

## What this phase deliberately does not do

- Does not move verification/Docker-adapter work into the cluster --
  `docker-verify.ts` still runs against the HOST's Docker daemon; no pod
  mounts the Docker socket.
- Does not deploy an OTel Collector/Prometheus/Grafana into the cluster
  -- OTLP trace export from in-cluster pods will retry and fail
  harmlessly (the same already-documented, bounded, non-fatal behavior
  as running the publisher on the host without
  `docker compose --profile observability up`). Prometheus `/metrics`
  itself is NOT disabled (it does not share the same failure mode).
- No Kubernetes Jobs, no Helm, no registry, no cloud account, no
  external publication, no live AI, no UI -- all explicitly deferred to
  a later phase.
- No multi-replica publisher/consumer -- see "Why replicas stay at 1" above.
