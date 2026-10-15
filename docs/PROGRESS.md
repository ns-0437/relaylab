# Progress

## 2026-10-15 (tenth follow-up session) -- Phase 3A Part B: local Kubernetes deployment

Deployed the two existing long-lived coordination services (outbox
publisher, durable NATS consumer) to a local `kind` cluster, against
real persistent MySQL + NATS JetStream, with the existing host-side
Docker verification adapter and one-shot Git-worker CLIs left unchanged
on the host. Full details, runbook, and architecture diagram:
[docs/KUBERNETES.md](KUBERNETES.md). Summary of what was proven, not
just applied:

- The existing `tests/integration/demo-handoff.ts` (UNMODIFIED) ran the
  real A->B Git-worker handoff against the cluster's MySQL via an
  explicit port-forward: epoch 1->2, identical restored commit/tree,
  exactly one non-replayed restore acknowledgement.
- A new `tests/integration/demo-k8s-messaging.ts` proved the REAL
  in-cluster publisher/consumer pods (not the script) carry a command
  end to end via `task_consumer_state.event_count`.
- A real `kubectl delete pod` on the running consumer proved: a
  genuinely new pod UID became Ready, durable state survived, processing
  resumed for a new command, and the resulting count was exactly right
  (no duplicate, no loss) -- while correctly distinguishing a
  deliberately-broken pod-direct port-forward (expected) from an actual
  application failure (never observed).
- Two real infrastructure problems were found and fixed, not papered
  over: the kind node's own image pull hanging indefinitely on this host
  (worked around with a manual `docker save` + `ctr images import`
  pipeline), and a `kubectl port-forward` silently binding `[::1]:3306`
  instead of failing when `infra/compose.yaml`'s dev MySQL already held
  `127.0.0.1:3306` (fixed by using entirely different host ports for the
  cluster, never 3306/24222).
- `src/telemetry/health.ts` (new) is the only non-trivial application
  change: three deliberately distinct startup/live/ready probes so a
  transient MySQL/NATS outage can never trigger a liveness restart loop.

### Explicitly not done this session

Kubernetes Jobs, Helm, a registry, a cloud account, external
publication, live AI, the UI, multi-replica publisher/consumer, and
moving verification itself into the cluster -- all deferred, per scope.

## 2026-10-15 (ninth follow-up session) -- Phase 3A Part A: post-verification input-integrity check

Closed the one gap the Phase 2C correctness patch (below) explicitly
left open rather than fixed: `chmod -R a-w` on the extracted verification
input is permission-based, so the verifying user (who owns the files)
can `chmod` them back, tamper, restore, and exit 0 -- with nothing
downstream ever checking. Confirmed via direct code search (`docker cp`,
"integrity", "tamper" across `src/worker` and the docker-verify tests)
that no before/after integrity check existed anywhere prior to this.

Added `verifyInputIntegrity` (`src/worker/docker-verify.ts`): compares
the expected file set -- read back from the original `candidate.tar` via
a new `readTar` (`src/worker/tar-writer.ts`) -- against the actual final
state of `/tmp/verify`, hashed INSIDE the still-running container via one
`docker exec ... node -e <script>` call, before the container is
stopped. Runs only on what would otherwise be reported `passed` (a
failing run already refuses for its own reason), so the failure path's
latency is unchanged. Applies automatically to both A's checkpoint path
and B's acknowledgement path (both call the same `verifyInDocker`).
Inability to complete the check is `infrastructure_failure`, never
`passed`.

A real implementation pitfall was found and fixed, not assumed away: the
first version used `docker cp` to pull the container's final state onto
the host, which CANNOT read a `--tmpfs`-mounted path at all on this
Docker setup (confirmed by direct probe, including against a still-
running container) -- a general Docker limitation, not specific to this
project. Switched to `docker exec` (runs inside the container's real
namespace), which required the container's bootstrap to idle on success
(`exec tail -f /dev/null`) instead of exiting immediately, giving the
adapter a window to inspect before explicitly stopping it.

A fixture that tampers with a tracked file and exits 0 is now a
permanent regression at three levels: the adapter directly, A's real
checkpoint path, and B's real restore-acknowledgement path (built from a
hand-constructed "already-checkpointed" state, since A's own gate
correctly refuses the tampering tree before B could ever receive it).
Full integration suite: 170/170 passing, 0 regressions. Full design,
probe transcripts, and timing evidence:
[docs/EXECUTION.md](EXECUTION.md) "Post-verification integrity check"
and [evidence/phase1-evidence.md](../evidence/phase1-evidence.md).

**Documented limitation, unchanged by this patch**: this is detection,
not prevention -- it does not defend against hostile code that changes
files and restores them before the final check runs. The verification
command is fixed and project-controlled, never task-supplied, so this
stays consistent with the existing local-prototype threat model.

## 2026-10-15 (eighth follow-up session) -- Phase 2C correctness patch

A narrowly scoped correctness patch closing three contract/honesty
gaps found on review of Phase 2C.1's own implementation, then
stopped -- no redesign, no new infrastructure, no Kubernetes.

### Part 1: `reconcileAbandonedExecution`'s cleanup contract -- a CONFIRMED real defect, fixed

The pre-patch implementation collapsed EVERY `docker inspect` failure
into "the container is absent" and silently swallowed every stop/
remove error via bare `.catch(() => {})`. Fixed with a classification
function (`inspectDocker`) distinguishing `present` / `absent` /
`unavailable` / `unknown`, confirmed against this project's own real
Docker CLI (never assumed):

```
genuinely absent:   error: no such object: <name>
daemon unreachable: failed to connect to the docker API at ... (simulated
                     via a deliberately bad DOCKER_HOST, never by actually
                     disrupting the real Docker Desktop installation)
```

Only a confirmed-absent INITIAL inspect short-circuits as "nothing to
do." A stop failure alone is no longer fatal (a subsequent forced
removal can still recover it), but the FINAL state is always
independently re-confirmed before returning success; anything else
throws a new, typed `ReconciliationFailed`. `verifyInDocker` catches
that and reports `infrastructure_failure` -- NEVER proceeding to
`docker create` as though the prior resource's state were known-safe.
A new `VerificationResult.cleanupError` field makes any cleanup
failure (reconciliation OR the end-of-run removal) independently
observable without ever overwriting the real verification `outcome`.

New test file: `tests/integration/docker-reconcile.test.ts` (7 tests)
-- "container already absent" and "successful cleanup" against a REAL
Docker daemon; "stop failure recovered by forced removal," "removal
failure leaves it present," "daemon-unavailable on the initial
inspect," and "unrecognized failure on the final confirmation" via
CONTROLLED FAULT INJECTION (`reconcileAbandonedExecution`'s new,
explicit `deps.exec` seam -- never by restarting the user's Docker
installation); one test additionally drives a REAL `docker` CLI call
against a deliberately invalid `DOCKER_HOST` to confirm the classifier
against genuine CLI output, not just a hand-crafted string.

### Part 2: the read-only-input guarantee -- a CONFIRMED real defect, narrowed honestly (not "fixed" -- see below for why)

Investigated whether the verification user could defeat `chmod -R
a-w` on its own extracted input. CONFIRMED directly, through the real
production image:

```
owner uid (process): 1000
before mode: 444 uid: 1000
CHMOD+WRITE SUCCEEDED. New content: "TAMPERED CONTENT -- exact-input guarantee broken"
DIRECTORY-CHMOD + FILE REPLACEMENT SUCCEEDED
```

Root cause: extraction and verification run as the SAME non-root user
(`docker create --user node` applies to the whole container), and
`chmod()`'s permission check is ownership-based, not mode-bit-based --
the owner can always `chmod` its own files back to writable. A
privilege-separated fix (extract as root, drop to `node` via `su` for
the verified command) was investigated and REJECTED: `su` requires
`CAP_SETUID`/`CAP_SETGID`, which conflicts with this project's
already-established, independently tested `--cap-drop ALL` isolation
property (confirmed: `su node -c ...` fails outright under it with
"can't set groups: Operation not permitted"). Re-adding those
capabilities to enable a privilege drop would trade one tested
isolation property for a narrower one, defending against a threat (the
FIXED, project-controlled verification command deliberately tampering
with its own input) outside this project's documented threat model.

**Resolution**: explicitly narrowed the claim rather than claiming a
fix that would require weakening an existing, validated isolation
property. The ORIGINAL tar file remains genuinely immutable (kernel-
enforced read-only bind mount, unaffected by in-container privilege).
The EXTRACTED COPY's `chmod -R a-w` is accurate protection against an
accidental/naive write, explicitly NOT an immutability guarantee
against the verification command's own code. A new, permanent
regression test
(`tests/integration/docker-verify.test.ts`, "Part 2 (documented
limitation, not a regression)") keeps this fact visible and tested.

### Part 3: archive-attribute handling -- a CONFIRMED real defect in this project's OWN prior work, fixed

`materializeCandidateInput` used `git archive`, which ALWAYS applies
`.gitattributes` `export-ignore` (omits a tracked file) and
`export-subst` (rewrites one) -- the OPPOSITE of "exact-tree
verification." This project's own Phase 2C.1 session had written
tests asserting git's filtering behavior AS fidelity -- confirmed, on
review, to be the wrong contract. Verified directly that `git cat-file
-p` (unlike `git archive`) never applies either attribute:

```
$ git cat-file -p HEAD:VERSION.txt   # tracked export-subst, marker UNCHANGED
version: $Format:%H$
$ git cat-file -p HEAD:IGNORED.md    # tracked export-ignore, STILL readable
secret, ignored for archive
```

Fixed by never calling `git archive` again: `listTreeEntries`
enumerates every tracked entry (unchanged); each SUPPORTED entry's raw
bytes are read via a new `catFileBlob` (`git cat-file -p`,
binary-safe); a new, minimal, dependency-free USTAR writer
(`src/worker/tar-writer.ts`, `writeTar`) packages them, carrying each
entry's exact git-recorded mode. Mode-bit fidelity (Phase 2C.1) is
unaffected -- extraction still happens on the container's own Linux
tmpfs. Symlink/gitlink rejection is unchanged; this patch does not
broaden tree-shape support.

Rewrote `tests/integration/docker-verify-exact-input.test.ts`'s main
fidelity test to assert the CORRECT contract: export-ignore'd files
STILL present with original bytes; export-subst'd files STILL contain
the unchanged marker; expected paths come from `listTreeEntries`
directly; expected bytes come from `git cat-file -p` on the SAME
selected objects, never a second archive export.

### Validation

```
npm run build / npm run typecheck                        # clean, Node 22 and Node 24
node --import tsx --test tests/integration/docker-reconcile.test.ts
                                                            # 7/7 (new)
node --import tsx --test tests/integration/docker-verify.test.ts
                                                            # 14/14 (13 existing + 1 new Part 2 tamper test)
node --import tsx --test tests/integration/docker-verify-exact-input.test.ts
                                                            # 5/5 (corrected assertions)
npm run test:git-worker-crash                              # ALL SCENARIOS PASSED (unaffected by this patch)
npm run test:integration                                   # 168/168, 0 skipped (160 baseline + 7 + 1 new)
```

See evidence/phase1-evidence.md for exact commands, raw probe output
(the chmod-tamper probe, the su/CAP_SETUID probe, the cat-file-vs-
archive probe), and the Node 24 targeted-validation results.

### Not done (explicitly out of scope, per this patch's own instruction)

Kubernetes, Helm, Terraform, cloud deployment, a frontend, live
model-provider integration, any broadening of supported tree shapes
beyond what Phase 2C.1 already supported, and a privilege-separated
extraction mechanism (investigated, explicitly rejected -- see Part 2
above).

## 2026-10-15 (seventh follow-up session) -- Phase 2C.1: containerized-verification acceptance patch

A bounded acceptance patch closing three gaps found on review of Phase
2C's own implementation, then stopped -- no redesign, no Kubernetes/
cloud/UI/live-AI.

### Part 1: exact verification-input fidelity -- a CONFIRMED real defect, found and fixed

Probed directly, BEFORE changing any code: a tracked executable file
(git mode `100755`) and a tracked non-executable file (`100644`) were
archived and extracted on the WINDOWS HOST via `tar.exe` (Phase 2C's
design), then the resulting directory bind-mounted read-only into the
real pinned image. Both arrived inside the container as mode `777`,
INDISTINGUISHABLY:

```
/verify/exec.sh 777
/verify/plain.txt 777
```

Windows NTFS has no native unix-permission concept; Docker Desktop's
Windows-host bind-mount translation flattened the distinction
entirely. Fixed by extracting on the CONTAINER'S OWN Linux filesystem
instead: `materializeCandidateInput` only runs `git archive` now
(producing `candidate.tar`); the container's bootstrap command
extracts that tar onto its own tmpfs and immediately `chmod -R a-w`s
the result before the verified command ever runs. Re-probed through
the real production code path after the fix:

```
/tmp/verify/exec.sh 755
/tmp/verify/plain.txt 644
```

-- correctly distinguished, and a write attempt now fails with
`EACCES` (restoring "read-only verification input" under the new
mechanism; it was `EROFS` under the old one). `materializeCandidateInput`
also now explicitly refuses a tree containing a symlink (`120000`) or
submodule gitlink (`160000`) entry (verified via `git update-index
--cacheinfo` plumbing, portable on Windows), and exposes the actual
archived tree SHA so `run-task.ts`/`handoff.ts` can refuse a
verification of the wrong tree (`assertMaterializedTreeMatches`,
`VerificationIdentityMismatch`) before trusting it for anything.
`.gitattributes` `export-ignore`/`export-subst` were confirmed to work
with zero special handling (both are `git archive`'s own behavior).

New test file: `tests/integration/docker-verify-exact-input.test.ts`
(5 tests, real Docker) -- export-ignore/export-subst/exec-bit/non-
exec-bit/untracked-file fidelity against a real container (one test),
binary content + a filename containing spaces (one test), symlink
rejection (one test), submodule-gitlink rejection (one test), and the
identity-mismatch regression (one test).

### Part 2: a real worker-process crash while its container is still running -- a CONFIRMED real gap in test coverage, closed

Every Phase 2C crash scenario crashed either before verification
started or after it had already finished and its container had
already been removed -- none exercised "the worker process dies while
its container is still genuinely alive." Closed with a new
`onContainerCreated` hook on `DockerVerifyOptions` (races "confirmed
running" against "already exited" so it never adds latency to the
ordinary fast path -- a real regression this patch caught and fixed
in its own first draft, see below) and a new scenario,
`tests/integration/git-worker-crash.ts`
"crash-during-container-verification": a real worker process, a
deliberately slow fixture, a structured IPC readiness barrier (real
pid, real container ID, task/attempt identity, owner epoch), an
independent `docker inspect` confirming `Running: true` BEFORE the
kill, a real `SIGKILL`, an independent `docker inspect` confirming the
container is STILL `Running: true` AFTER the kill (the real, recorded
orphan this part asked for), a restart that publishes exactly one
checkpoint, and a final `docker inspect` confirming the orphan is GONE
-- removed by the restarted worker's own `reconcileAbandonedExecution`,
matched purely by its exact deterministic name (this project sets no
shared container label at all). A SECOND, independent,
concurrently-running container (a different name, standing in for a
different worker's legitimate execution) was kept alive throughout and
independently confirmed still running and untouched at the end.

**A real self-caught regression**: wiring `onContainerCreated`
unconditionally into `run-task.ts`'s real call site meant the FIRST
version of the "wait until running" helper used a flat, unconditional
poll-until-running bound (up to 5s) before proceeding -- since this
hook is now invoked on EVERY real verification (not just the crash
test), this would have added up to 5 seconds of pure latency to every
ordinary, fast verification whenever the container happened to finish
before the poll got around to checking. Caught by reasoning through
the change before running it, not by a failing test; fixed by racing
"confirmed running" against the container's own exit event instead of
polling on a fixed timer.

### Part 3: manifest v1/v2 compatibility -- verified already correct, no gap found

Investigated per this patch's instruction that "adding
`containerImageId: null` to an old fixture does not test compatibility
with manifests where the field was absent" -- every PRIOR test fixture
in this repo (including ones this session's own earlier Phase 2C work
added) did exactly that non-test. Built an authentically v1-shaped
manifest BY HAND (the key absent from the object entirely, never
passed through `buildManifest()`, which cannot produce this shape at
all now) and proved: it is accepted; its stored `manifest_version`
column is `1`, not silently upgraded; its `verification_image_id`
column is `null`; its PERSISTED `manifest_json` still lacks the key
(a direct property-presence check -- the column is a native MySQL
`JSON` type, which the `mysql2` driver auto-parses into a JS object on
read, not a string, a real test-writing pitfall this patch's own
regex-based first draft of this assertion fell into and had to fix);
and, decisively, a REPLAY of the exact same `commandId` succeeds --
if the digest recompute path had normalized the object into a v2
shape before hashing, this replay would have failed with a digest
mismatch. Also confirmed: an invalid `containerImageId` type is still
rejected regardless of `manifestVersion`, and a real v2, container-
backed checkpoint's `verification_image_id` column round-trips the
actual image ID exactly.

New tests appended to
`tests/integration/git-worker-checkpoint-manifest.test.ts` (3 new, 11
total in that file now).

### Validation

```
npm run build / npm run typecheck                      # clean, Node 22 and Node 24
npm run test:docker-verify                               # 13/13 (unchanged by this patch, re-verified against the new mount design)
node --import tsx --test tests/integration/docker-verify-exact-input.test.ts
                                                          # 5/5 (new)
node --import tsx --test tests/integration/git-worker-checkpoint-manifest.test.ts
                                                          # 11/11 (8 existing + 3 new)
npm run test:git-worker-crash                             # ALL SCENARIOS PASSED (7 scenarios, including the new one), Node 22 AND Node 24
npm run test:integration                                  # 160/160, 0 skipped -- Node 22 AND Node 24 (separately, full runs)
npm run demo:handoff                                      # PASS, identical Git identities, distinct container execution IDs, one acknowledgement
```

See evidence/phase1-evidence.md for exact commands, raw probe output,
and the full Node 22 vs. Node 24 (host) vs. container (always
v24.21.0) breakdown.

### Infrastructure instability encountered during this session (recorded, not a code defect)

Docker Desktop crashed and required a manual restart three times
during this session's validation work (unrelated to any code in this
repository) -- once leaving the daemon unreachable for over 10 minutes
with `com.docker.diagnose.exe` running, and later crashing again mid-
test-run, which produced one transient `git worktree add` failure
(exit 107) in PRE-EXISTING, unmodified Phase 2A/2B code
(`git-worker-handoff.test.ts` test 6) as a casualty of MySQL/Docker
going down mid-run. Re-ran that single file in isolation once Docker
was confirmed stable again: 11/11 passed cleanly, confirming it was an
environment casualty, not a regression -- consistent with this
patch's own instruction to investigate a specific failure rather than
assume or repeatedly rerun everything.

### Not done (explicitly out of scope, per this patch's own instruction)

Kubernetes, Helm, Terraform, cloud deployment, a frontend, live
model-provider integration, any broader task-store rewrite, and any
claim of VM-grade or hostile-multi-tenant container security (see
docs/EXECUTION.md "What this does not claim," unchanged by this
patch's scope).

## 2026-10-14 (sixth follow-up session) -- Phase 2C: containerized verification

Added containerized verification: worker A verifies its candidate
through a real Docker execution adapter instead of a bare host
process; worker B independently re-verifies the restored artifact in
its OWN, separate container before acknowledging. Then stopped, per
this patch's own instruction -- no Kubernetes, Helm, Terraform, cloud
deployment, frontend, or live model integration.

### What was built

- `src/worker/verification.ts` (new): the narrow interface both
  adapters implement -- `VerificationAdapter`/`VerificationRequest`/
  `VerificationResult`, FIVE outcomes (`passed`, `verification_failed`,
  `timed_out`, `cancelled`, `infrastructure_failure` -- the host-
  process adapter only ever had three; `infrastructure_failure` is new
  and deliberately distinct from "the command ran and failed," since a
  worker must never checkpoint/acknowledge on either but must never
  conflate "Docker couldn't launch" with "the candidate is bad").
- `src/worker/docker-verify.ts` (new): the Docker adapter.
  `resolveVerifyImageId` always records the ACTUAL resolved
  `sha256:...` image ID, never trusting the mutable tag alone.
  `buildVerifyCreateArgs` is a pure, exported function building the
  exact `docker create` isolation argument list (non-root, read-only
  root + explicit `noexec,nosuid` tmpfs scratch, network disabled, all
  capabilities dropped, no-new-privileges, memory/CPU/pids/log-size
  limits, exactly one read-only bind mount) -- extracted specifically
  so a test can get real `docker inspect` evidence against the SAME
  args production runs, with zero risk of drift.
  `materializeCandidateInput` exports the candidate's exact tracked
  tree at an already-pinned commit via `git archive`, never the live
  worktree, into a disposable scratch directory that is what actually
  gets bind-mounted. `reconcileAbandonedExecution` removes exactly one
  deterministically-named prior abandoned container (never a global
  prune) before a fresh create. On timeout/cancellation, the actual
  container is stopped AND independently re-inspected
  (`State.Running`) to confirm it actually stopped -- "killing the
  Docker CLI alone is not proof its container stopped" is answered
  structurally. The container is always removed afterward, scoped to
  exactly that one execution; a removal failure is logged but never
  overrides the already-computed verification result.
- `infra/verify-image/Dockerfile` (new): the ONE pinned image,
  `node:24.21.0-alpine3.24`, non-root `node` user, `WORKDIR /verify`.
  Built via `npm run docker:build-verify-image`, never implicitly
  built/pulled by the adapter.
- `src/worker/checkpoint.ts` / `src/db/task-store.ts` /
  `infra/migrations/0010_checkpoint_container_evidence.sql`:
  `CheckpointManifest.verification.containerImageId: string | null`
  (manifestVersion 1 -> 2) plus a matching nullable
  `task_checkpoints.verification_image_id` column -- additive,
  backward compatible (an old manifest or the host-process adapter
  simply has `null` here, never treated as invalid).
- `src/worker/run-task.ts` / `src/worker/handoff.ts`: both A's and B's
  verification steps now call `verifyInDockerWithImageId` instead of
  the old host-process `runVerification`. `RunOutcome`/`RestoreOutcome`
  each gained explicit `verification_timeout`/`verification_cancelled`/
  `verification_infrastructure_failure` variants alongside the
  pre-existing `verification_failed` -- a failed/unavailable
  verification still never produces a successful checkpoint or
  acknowledgement, now with the specific reason preserved as a type,
  not collapsed into one generic failure. `RunOutcome`'s "checkpointed"
  variant also now carries the RAW `verification`/`imageId` (with the
  real container `executionId`), alongside the durable, narrower
  `manifest.verification` -- needed so a caller (the end-to-end demo)
  can tell apart two different executions of the same candidate, which
  the durable manifest deliberately does not persist.

### A REAL gap found and fixed: lease validity going stale across a lock wait (Part 5's own instruction)

Investigated per this patch's explicit instruction: "inspect whether
lease validity can become stale while waiting for a task row lock... verify
that a fresh guarded mutation prevents an expired lease from authorizing
a write." All four `command_receipts`-backed mutation methods
(`applyTaskCommandImpl`, `recordGitCheckpointImpl`,
`transferOwnershipImpl`, `recordRestoreAcknowledgementImpl`) read
`db_now` via `SELECT *, NOW(3) AS db_now FROM tasks WHERE task_id = ?
FOR UPDATE` -- one statement doing BOTH the blocking row-lock
acquisition and the "current time" read.

Wrote a focused regression first (`task-store-concurrency.test.ts`,
"lease-staleness-across-lock-wait"): claim a task with an 800ms lease,
hold the row's lock open on a SEPARATE real connection for 1500ms (long
enough that the lease has definitely expired by real wall-clock time
before the lock is released), then let a blocked `applyTaskCommand`
call finally proceed. It FAILED as predicted -- the write succeeded
(`{ok: true}`) when it should have been rejected as `ClaimLost`.

Confirmed the root cause with a direct, isolated probe against this
project's own MySQL instance (two `SELECT NOW(3)` reads via the SAME
connection/clock basis, one immediately before dispatching the blocked
statement and one immediately after it unblocked):

```
t0 (before dispatch, MySQL clock)      : 1791970258389
db_now inside the blocked statement     : 1791970258393
t1 (after unblock, MySQL clock)         : 1791970259907
db_now - t0 = 4 ms    (captured at/near DISPATCH time)
t1 - db_now = 1514 ms (~1.5s STALE relative to when the lock actually released)
```

MySQL evaluates `NOW(3)` at or near statement dispatch time, not after
a lock wait resolves -- exactly the gap the instruction anticipated.
Fixed with a new `freshDbNow(conn)` helper (`src/db/task-store.ts`): a
SEPARATE, trivial, non-blocking `SELECT NOW(3) AS db_now` (no
FROM/WHERE, so it cannot itself block) called immediately AFTER the
row lock is confirmed held, replacing the bundled read in all four
methods above and nowhere else -- per the instruction's own "do not
expand this into a broad task-store rewrite." Re-ran the regression
after the fix: passes, `ClaimLost` correctly rejects the write.

### Tests added / changed

- `tests/integration/docker-verify.test.ts` (new, 13 tests, requires
  real Docker -- `before()` fails loudly if unreachable or the image
  isn't built): existing fixture fails before repair / passes after
  repair+commit (reusing the SAME deterministic fixture, not a new
  one); long-running fixture stopped and reported `timed_out`, never
  left running; `AbortSignal` cancellation stops the container the same
  way; noisy fixture's output is bounded, never captured unbounded; a
  write to the read-only mount is rejected (real `EROFS` captured);
  network access is disabled (a real DNS lookup fails); real `docker
  inspect` evidence for every documented isolation property; cleanup
  never touches an unrelated running container; restart reconciliation
  removes exactly one prior abandoned same-name container before a
  fresh create succeeds; an unresolvable image AND a `docker create`-
  level failure are both reported as `infrastructure_failure`, never a
  success; `reconcileAbandonedExecution` is a safe no-op when nothing
  needs reconciling.
- `tests/integration/task-store-concurrency.test.ts`: +1
  (lease-staleness-across-lock-wait, see above).
- `tests/integration/demo-handoff.ts`: now prints and asserts A's and
  B's verification `executionId`s are DISTINCT and their restored
  commit/tree are IDENTICAL, alongside the pre-existing checkpoint/
  artifact/transfer/restore/acknowledge evidence.
- 5 existing test files (`git-worker-checkpoint-manifest.test.ts`,
  `git-worker-crash.ts`, `git-worker-handoff-authorization.test.ts`,
  `task-store-concurrency.test.ts`) updated to pass the new
  `containerImageId: null` field their existing `buildManifest()` call
  sites now require.

### Validation

```
npm run build                                               # clean (Node 22 and Node 24)
npm run typecheck                                            # clean (Node 22 and Node 24)
npm run test:docker-verify                                   # 13/13 (new), real Docker
node --import tsx --test --test-concurrency=1 \
  tests/integration/task-store-concurrency.test.ts tests/integration/ownership.test.ts \
  tests/integration/idempotency.test.ts tests/integration/retry-authorization.test.ts \
  tests/integration/git-worker-handoff-authorization.test.ts tests/integration/git-worker-checkpoint-manifest.test.ts
  # 50/50 (first run WITHOUT --test-concurrency=1 produced 15 spurious
  # failures from cross-file DB truncation racing -- a self-inflicted
  # test-invocation mistake, re-confirmed as such by re-running correctly;
  # not a code regression -- see evidence/phase1-evidence.md)
npm run test:integration                                     # 139/139, full suite, Node 22
npm run demo:handoff                                         # PASS -- see evidence for full identity/execution-ID printout
```

Node 24.21.0: see evidence/phase1-evidence.md for the exact Node 24 run
and counts, and for why "Node 24" means two different things now (host
vs. the container's own always-v24.21.0 runtime).

### Not done (explicitly out of scope, per this patch's own instruction)

Kubernetes, Helm, Terraform, cloud deployment, a frontend, live
model-provider integration, a general tool-execution interface, and any
claim of VM-grade or hostile-multi-tenant container security (see
docs/EXECUTION.md "What this does not claim").

## 2026-10-14 (fifth follow-up session) -- Phase 2B.2: durable-command gap closure, handoff milestone finished

Closed the last two durable-command gaps found on review of Phase
2B.1's own implementation, then stopped -- per this patch's own
instruction, the handoff milestone (Phase 2B/2B.1/2B.2) is now
considered FINISHED; no new phase started.

### Finding 1: the generic `applyTaskCommand` route could bypass `recordRestoreAcknowledgement` -- CONFIRMED, closed

Inspected directly (not assumed): `applyCommand`'s `RestoreAcknowledged`
domain case succeeds unconditionally (pure function, cannot see
`task_handoffs`). Wrote a direct test submitting `RestoreAcknowledged`
through `store.applyTaskCommand` with a VALID current owner/epoch/lease
and arbitrary `artifactDigest`/`restoredCommit`/`restoredTree` -- it
succeeded, emitting a real `RestoreAcknowledged` outbox event with none
of `recordRestoreAcknowledgementImpl`'s validation ever applied. This
is the CONFIRMED bypass. Closed with a single, cheap, synchronous check
at the top of `applyTaskCommandImpl` (`src/db/task-store.ts`): refuses
any `command.type === "RestoreAcknowledged"` outright, before any I/O,
naming the correct method in the error. Chose REFUSAL over ROUTING
(calling into `recordRestoreAcknowledgementImpl` from inside
`applyTaskCommandImpl`) specifically to avoid a nested transaction or
sharing an open connection across two call paths -- neither needed,
since nothing in this project legitimately submits this command type
through the generic path. The specialized method's own validation is
UNTOUCHED.

Also inspected the "already acknowledged via a different commandId"
branch specifically, per the task's instruction to make its behavior
explicit: it unconditionally throws `CommandRejected`, regardless of
whether the new request's artifact/commit/tree content happens to
match the original -- added a dedicated test proving this (identical
content under a second commandId is still rejected, confirming the
rejection is NOT secretly keyed off content equality).

### Finding 2: the receipt-lookup-before-row-lock race also affected `applyTaskCommandImpl`/`recordGitCheckpointImpl` -- CONFIRMED by direct reproduction, fixed

Reproduced BEFORE changing any code, as instructed (see
`tests/integration/task-store-concurrency.test.ts`'s own header comment
for the methodology): two real, concurrent, BYTE-IDENTICAL
`store.applyTaskCommand` calls (same `taskId`/`ownerId`/`epoch`/
`commandId`/`command`) against a real MySQL instance. Actual observed
output, captured directly from a throwaway repro script before any fix:

```
claimed epoch 1
r1 { ok: true, replay: false }
r2 {
  ok: false,
  error: 'command cmd-1 for task repro-race-<ts> was already used with a different payload',
  tag: undefined
}
```

The payload was NOT actually different -- `r2` failed because its OWN
pre-lock `command_receipts` lookup (the first query in its transaction)
established an InnoDB REPEATABLE READ snapshot BEFORE `r1` committed;
after waiting for `r1`'s row lock and finding `r1`'s already-updated
row, `r2` proceeded to compute its own transition and attempt its own
`INSERT INTO command_receipts`, which failed on the primary key and was
mapped UNCONDITIONALLY to `CommandConflict` by the existing catch
block -- never re-checking whether the fingerprints actually matched.
Fixed by acquiring the `tasks` row lock FIRST, then the
`command_receipts` lookup second -- the exact same reordering Phase
2B.1 already applied to `transferOwnershipImpl`/
`recordRestoreAcknowledgementImpl`. Re-ran the SAME repro script after
the fix, three times:

```
claimed epoch 1
r1 { ok: true, replay: false }
r2 { ok: true, replay: true }
```

-- consistently `{false, true}`, never a conflict, across three runs.
`recordGitCheckpointImpl` has the IDENTICAL code shape and was fixed
the same way; verified with a DEDICATED concurrent test (not merely by
analogy to `applyTaskCommandImpl`'s fix) -- see "Tests added" below.

### Tests added

`tests/integration/task-store-concurrency.test.ts` (new, 9 tests, real
MySQL, real `Promise.all` concurrency -- deterministic synchronization
via the row lock itself, never a sleep-based race), for BOTH
`applyTaskCommand` and `recordGitCheckpoint`: two concurrent identical
requests produce one durable mutation/event with both callers observing
the correct committed/replayed result; concurrent requests sharing a
commandId but differing in payload produce one winner and one conflict
with no partial writes (durable row counts asserted, not just returned
status strings); authorized historical replay remains valid after
ownership changes; an unauthorized caller cannot retrieve a successful
receipt; and (checkpoint-specific) altered manifest content with a
reused claimed digest remains rejected, re-verified against the
reordered code.

Two new tests appended to
`tests/integration/git-worker-handoff-authorization.test.ts`: the
CONFIRMED-bypass regression (generic `applyTaskCommand` refuses
`RestoreAcknowledged`, no event, no receipt; the SUPPORTED valid path
through the specialized method still works for the same handoff
immediately after) and the different-commandId-identical-content
rejection.

### Validation

```
npm run typecheck                                          # clean
npm run build                                               # clean
node --import tsx --test tests/integration/task-store-concurrency.test.ts
  # 9/9 (new)
node --import tsx --test tests/integration/git-worker-handoff-authorization.test.ts
  # 21/21 (19 from Phase 2B.1 + 2 new)
```

Node 24.21.0: see evidence/phase1-evidence.md for the exact Node 24 run
and counts.

### Telemetry test investigated under stable conditions, per this patch's own instruction

`tests/integration/telemetry-resilience.test.ts` test 4 ("a real child
process exits within the stated bound with telemetry enabled...")
failed once during the Phase 2B.1 session, inside a multi-hour
backgrounded full-suite run that also showed unrelated MySQL
`ETIMEDOUT` errors consistent with the host having slept mid-run. This
session re-ran test 4 ALONE, under confirmed-stable conditions (fresh
Docker Compose stack, no concurrent heavy test load), with the
Collector's own logs tailed in parallel for corroboration: it PASSED
three consecutive times, at ~6.5 seconds each, comfortably under the
test's stated 10-second bound. No baseline revision exists to diff
against (`git: false` in this working directory -- there is no VCS
history at all in this project), so baseline status is UNVERIFIED in
the literal sense of "no prior commit to check out" -- but the test's
OWN source code independently documents this exact failure mode as a
named, anticipated possible outcome ("this is the stated limitation,
not a silent pass"), and this patch changes zero code in
`telemetry/init.ts`, `consumer-main.ts`, or any OTel SDK configuration.
Conclusion, stated precisely rather than assumed: the single observed
failure is attributed to TRANSIENT system load from the earlier
multi-hour background run (consistent with the Collector container's
own uptime having reset around the same time), not to a defect this
patch introduced or left behind -- re-confirmed by direct re-execution
under stable conditions, not by analogy or by the file being
"unrelated." No test-harness fix was made because none was needed: the
test's assertions are already correct, and weakening them (longer
bound, retry loop, skip) was explicitly out of bounds per this patch's
own instruction not to obtain a green result by softening checks.

### Full integration suite, run once after the focused fixes

See evidence/phase1-evidence.md for the exact command, Node version,
and pass/fail/skip totals from this run.

## 2026-10-14 (fourth follow-up session) -- Phase 2B.1: handoff correctness patch

Phase 2B was provisionally accepted on its own reported tests, with an
explicit instruction to inspect the ACTUAL implementation first (not
trust the prior report) and close gaps before any new technology.
Three real gaps were confirmed and fixed; stopped after this bounded
patch, per its own instruction.

### Finding 1: `transferOwnership` never checked lease validity -- CONFIRMED, fixed

Read `transferOwnershipImpl` directly (not the prior report): the
fresh-transfer branch checked `row.epoch`, `row.owner_id`, `row.status`,
and `row.checkpoint_digest` -- but never `row.lease_expires_at`. An
owner whose lease had already expired, with nobody yet taking over,
could still successfully "transfer" authority. Fixed the same way
`renewLeaseImpl` already enforces lease validity: `AND lease_expires_at
>= NOW(3)` added to the guarded UPDATE's own WHERE clause (the real
enforcement), plus a diagnostic pre-check reading the SAME `NOW(3)`-
sourced row (never a second, independent `Date.now()` read). Does NOT
apply to replay, by design (unchanged historical-replay policy).

### Finding 2: a receipt-lookup-before-row-lock race -- CONFIRMED, fixed

Also found by reading the code directly, exactly as the task predicted:
`transferOwnershipImpl` checked `command_receipts` BEFORE acquiring the
`tasks` row's `FOR UPDATE` lock. Reasoned through concretely: two
concurrent, IDENTICAL transfer requests (same `handoffId`) both read
"no receipt" before either acquires the lock; the loser then blocks on
the winner's lock; once released, the loser's STALE pre-lock belief
("no receipt") persists for the rest of its transaction under InnoDB
REPEATABLE READ, so it proceeds to check the row's epoch -- now
incremented by the winner -- and fails with a misleading `EpochStale`
instead of correctly resolving as a replay. Fixed by acquiring the row
lock FIRST: a locking read establishes the transaction's consistent-
read snapshot AT THAT POINT (after waiting for the winner's commit), so
the SUBSEQUENT receipt lookup correctly sees the winner's row. Verified
directly with a real concurrent-duplicate-request test (see below) --
this is exactly the failure mode the fix targets, reproduced and then
closed, not merely reasoned about.

### Finding 3: `RestoreAcknowledged` had NO handoff-identity validation -- CONFIRMED, fixed

Read the entire `applyTaskCommand`/`RestoreAcknowledged` path before
changing anything, as instructed. What it ALREADY provided (verified,
not assumed): generic current-epoch/owner/lease fencing (the same
checks every command gets), and the generic commandId-keyed receipt
ledger (same commandId + same payload = replay; same commandId +
different payload = `CommandConflict`). What it did NOT provide: any
check that the handoff being acknowledged exists, belongs to this task,
was actually authorized for this caller/epoch/artifact, or that the
claimed restored commit/tree matched the checkpoint it was for; and no
protection at all against the SAME handoff being acknowledged a SECOND
time under a DIFFERENT, never-before-seen commandId (the commandId-
keyed ledger cannot catch this by construction). Fixed with a new
method, `TaskStore.recordRestoreAcknowledgement`
(`src/db/task-store.ts`), NOT reusing `applyTaskCommandImpl` directly
(same reasoning Phase 2A used for `recordGitCheckpoint`: never put the
well-tested generic path at risk). Two new columns on `task_handoffs`
(migration `0009_handoff_identity_binding.sql`): `artifact_digest`
(bound when `transferOwnership` commits -- that method now takes
`artifactDigest` as an argument) and `restore_ack_command_id` (set
exactly once, atomically, with the first successful acknowledgement --
this is what makes "a different commandId acknowledging an already-
acknowledged handoff" detectable). Restored commit/tree are validated
against `task_checkpoints` (joined by `checkpoint_digest`) -- REUSING
existing checkpoint metadata, never duplicating a manifest copy, per
the task's own instruction.

**Ownership-transfer event decision, reported as asked**:
`transferOwnership` does NOT emit an outbox event. This was already
true in Phase 2B and is UNCHANGED by this patch -- it matches the
EXISTING, established design: `claimTask`/`renewLease` (the OTHER
ownership/epoch-mechanics operations) have never gone through the
Command/DomainEvent/outbox protocol either; only business-state
commands do. Adding one now would have been inventing a parallel event
system specifically for this patch, which was explicitly out of scope.

### Finding 4 (test infrastructure, not the protocol): the crash test's barrier was unsound -- CONFIRMED, fixed

Investigated directly: `src/testing/failpoint.ts`'s hang (`await new
Promise(() => {})`) holds NO active handle. Traced
`restore-worker-main.ts`'s actual call sequence: validate artifact ->
restore (git/fs only) -> **failpoint** -> verify (child process, no DB)
-> `recordRestoreAcknowledgement` (the FIRST real DB call in the whole
flow). Because the failpoint sits BEFORE the only DB call, and
telemetry is disabled in these test runs, nothing else in the process
had opened a handle by the time the bare Promise was reached -- so the
process could, and empirically did, exit NATURALLY (`exitCode: 0`)
instead of being interrupted. Contrast with `worker-main.ts`'s own
scenarios in `git-worker-crash.ts`, which claim the task (a real DB
call) BEFORE any of their own failpoints, so they always had an
incidental handle keeping them alive -- the asymmetry fully explains why
one crash test showed the expected `{exitCode: null, signalCode:
"SIGKILL"}` pattern and the other did not. Fixed with an explicit,
permanent `setInterval` handle inside `hitFailpoint` itself (so this
can never again depend on what a given call site happens to have open),
a richer structured readiness message (`pid`, `taskId`, `handoffId`,
`ownerId`, `epoch`), and, in the test, an independent
`process.kill(pid, 0)` liveness probe both at the barrier and
immediately before the real kill. Re-run after the fix: the SAME
`{exitCode: null, signalCode: "SIGKILL"}` pattern now appears, matching
`git-worker-crash.ts` -- confirmed on two separate runs, not a one-off.

### Tests added

`tests/integration/git-worker-handoff-authorization.test.ts` (new, 19
tests, real MySQL, no real Git needed -- both `transferOwnership` and
`recordRestoreAcknowledgement` are pure DB-boundary functions): valid
transfer; expired-owner transfer rejected; rejection leaves owner/epoch/
receipt/handoff/event rows unchanged; authorized replay still succeeds
after the original lease has expired; two concurrent IDENTICAL transfer
requests both resolve per the idempotency contract (the race-fix
regression); two competing transfers produce exactly one winner; then,
for `recordRestoreAcknowledgement` called DIRECTLY: unknown handoff;
handoff belonging to another task; wrong destination owner; wrong
destination epoch; wrong artifact digest; wrong restored commit/tree;
valid acknowledgement (provenance preserved); all three duplicate forms
(same commandId+same payload replays; same commandId+changed payload
conflicts; same handoff via a different commandId is rejected with no
second event); new acknowledgement rejected after destination authority
expires; new acknowledgement rejected after destination authority
changes (lease expired, a third owner took over); historical replay
still succeeds after ownership has since changed. All 19 pass.

`tests/integration/git-worker-handoff-crash.ts` (test 9, rewritten):
added pid/taskId/handoffId/ownerId/epoch assertions on the failpoint
payload, an independent `process.kill(pid, 0)` liveness probe (both at
the barrier and immediately before the kill), an assertion that
`killReturned` is true, and -- the decisive new assertion -- that the
observed exit code is NOT 0 (would indicate a natural exit, meaning the
crash scenario failed to actually interrupt a live worker).

### Validation

```
npm run typecheck                                        # clean
npm run build                                             # clean
node --import tsx --test --test-concurrency=1 tests/integration/git-worker-handoff.test.ts
  # 11/11 (unchanged, re-verified against the new validation)
node --import tsx --test tests/integration/git-worker-handoff-authorization.test.ts
  # 19/19 (new)
npm run test:handoff-crash
  # 1/1 scenario, re-run twice: {exitCode: null, signalCode: "SIGKILL"} both times (was {0, null})
npm run test:integration
  # ran once as a full-suite check; see "Honest note on an unrelated failure" below
```

All of the above repeated on Node 24.21.0 (same `.relaylab-tools`
install) -- typecheck/build clean; `git-worker-handoff.test.ts` +
`git-worker-handoff-authorization.test.ts` + `git-worker-checkpoint-
manifest.test.ts` together, identical results to Node 22; see
evidence/phase1-evidence.md for the exact counts and the
`test:handoff-crash` Node 24 run's exit-status evidence.

### Honest note on an unrelated failure observed during this session

A full `npm run test:integration` run (launched in the background,
spanning several HOURS of wall-clock time due to this session's own
tool-level backgrounding) surfaced 10 failures, all in
`telemetry-resilience.test.ts` -- a file this patch never touches, and
which has no relationship to `task-store.ts`/`worker/handoff.ts`/
`worker/artifact.ts`. Every one of those 10 failures' actual error was
`Cannot reach the relaylab_test MySQL database ... (connect ETIMEDOUT)`;
`docker compose ps` afterward showed every container freshly restarted
("Up 9 hours" on a host that had been running this session far longer),
consistent with the host machine having slept/suspended mid-run, not
with anything this patch changed. Re-running `telemetry-resilience.test.ts`
alone, with the infrastructure confirmed healthy again, passed 8/9; the
ONE remaining failure (`4. a real child process exits within the stated
bound with telemetry enabled...`) is a pre-existing, timing-sensitive
test (its own name anticipates reporting a timing limitation "honestly"
rather than silently passing) against a real OTel Collector over a
hardcoded 10-second bound, reproduced twice including after restarting
the Collector container -- not touched by this patch's changes, and
not chased further, per this patch's own bounded scope. Stated
precisely rather than silently folded into a pass count, per this
project's own standing convention.

## 2026-10-14 (third follow-up session) -- Phase 2B: committed-checkpoint handoff between two workers

Phase 2A.2 was provisionally accepted. This session implemented Phase
2B: committed-checkpoint handoff between TWO independent local
workers -- explicitly NOT cloud deployment, NOT dirty-file transfer,
NOT live AI coding. Stopped after this, per the task's own instruction.

### Part A: the evidence-handoff corrections and the one real gap found

- **Evidence table corrected** (see this file's own Phase 2A.1 section
  below, now annotated): the original "pre-kill AND post-restart,
  identical" framing applied to all three crash scenarios, but only the
  first (`commit-created-checkpoint-not-persisted`) actually HAS a
  pre-kill result commit -- the other two scenarios kill BEFORE any
  repair commit exists at all, so there is nothing to compare against
  pre-kill; their single commit SHA is the POST-RESTART result only.
- **A real, demonstrated gap in `recordGitCheckpoint`'s replay path**,
  found by the full-code review this session was asked to do BEFORE
  writing any handoff code (not by a test failure): the replay branch
  matched only `fingerprint({type:"Checkpoint", digest: manifestDigest})`
  -- a hash of the CLAIMED digest value, never of the `manifestJson`
  actually supplied on the retry. A retry carrying the SAME, already-
  committed digest paired with ALTERED manifest content was accepted as
  an identical retry without ever checking that the content actually
  corresponds to that digest. Fixed in `src/db/task-store.ts`:
  validation split into `parseAndValidateGitCheckpointManifestShapeAndDigest`
  (runs on EVERY call, replay or not -- "manifest-internal consistency")
  and `assertManifestIdentityMatchesRequest` (new-write-only -- "current
  task ownership," unchanged, since a replay must still succeed after
  the task's epoch has moved on, docs/decisions/0004). New regression
  test: `tests/integration/git-worker-checkpoint-manifest.test.ts`
  "Phase 2B Part A: a replay supplying the ORIGINAL ... digest together
  with ALTERED manifestJson content is rejected."

### Part C: the protocol, designed before coding

`docs/decisions/0011-handoff-protocol.md` -- six named states (owned/no
handoff; artifact prepared, local only; atomic ownership transfer;
destination restoration; restore acknowledgement; failed/interrupted
restoration), an explicit policy for when A loses authority (AT the
`transferOwnership` commit, never before -- waiting for B to validate
would allow two simultaneously-authoritative owners; never after --
that is exactly the forbidden "revoke based on an unvalidated artifact"
case), and a documented recovery path for B failing after transfer
(same owner identity retries from the SAME artifact at the SAME epoch;
every step -- `git init`, bundle fetch, checkout, the restore
acknowledgement's deterministic commandId -- is naturally idempotent).

### Parts B/D/E/F: what was built

- **`src/worker/artifact.ts`** (new): `createHandoffArtifact` (a
  self-contained Git bundle from the worktree's own branch -- a bare
  commit SHA does NOT work with `git bundle create`, confirmed directly:
  "fatal: Refusing to create empty bundle"; fixed by bundling the
  branch ref, whose tip equals the result commit), atomic temp-then-
  rename promotion to a digest-addressed final path,
  `validateHandoffArtifact` (size then full digest, before consuming
  any byte), `restoreHandoffArtifact` (fresh independent repo, re-verify
  the bundle in the DESTINATION's own context, fetch, an explicit
  `git cat-file -e` object-availability proof, checkout, then HEAD/tree/
  clean-workspace equality checks against the manifest) -- including
  idempotent-retry support (a non-empty destination that is ALREADY the
  correctly-restored result of this exact artifact is accepted, not
  rejected; verified by positive HEAD/tree/clean equality, never
  inferred from ".git exists").
- **`TaskStore.transferOwnership`** (new, `src/db/task-store.ts`,
  migration `0008_task_handoffs.sql`): the atomic, idempotent ownership
  change, requiring the supplied checkpoint digest to equal the task's
  CURRENT durably-recorded one.
- **`RestoreAcknowledged`** (new domain command/event,
  `src/domain/domain.ts`): fenced under B's current epoch through the
  EXISTING `applyTaskCommand` path -- no new fencing logic needed.
- **`src/worker/handoff.ts`** (new): the four-step orchestration
  (prepare -> transfer -> restore -> acknowledge), one OTel span per
  step.
- **`src/restore-worker-main.ts`** (new): B's real, separate one-shot
  CLI entrypoint, same convention as `worker-main.ts`.
- **`tests/integration/demo-handoff.ts`** (new, `npm run demo:handoff`):
  the full A -> transfer -> B flow against the dev database, in-process
  (same convention as `demo-git-worker.ts`).

### A real bug this session's OWN demo/test run caught (not hypothetical)

Running the demo for the first time failed with `git bundle create ...
failed (exit null)` and, after fixing the ref-vs-SHA issue above, a
SECOND real bug: `runOnce` (`src/worker-main.ts`) has no parameter to
override its managed workspace root -- it always resolves
`createManagedWorkspaceRoot()` with NO argument, reading
`RELAYLAB_WORKER_WORKSPACE_ROOT` or falling back to a shared, stable
OS-temp default. The demo script had computed A's worktree path against
a DIFFERENT, invented root it never actually told `runOnce` to use,
resolving to a directory that didn't exist -- which `createHandoffArtifact`'s
`git bundle create` then failed against with a spawn-level error
(`exitCode: null`, empty stderr, because the child process never
started in a nonexistent cwd). Fixed by setting
`process.env.RELAYLAB_WORKER_WORKSPACE_ROOT` explicitly before calling
`runOnce`, matching what the real CLI path actually resolves.

A third issue, found while writing `git-worker-handoff.test.ts` test 7:
a long `taskId` combined with a doubly-nested worktree path (the test's
own `managedRootA` construction redundantly included `taskId` as a path
segment ON TOP OF what `resolveWorktreePath` already adds) pushed the
resulting absolute path's length into a range where `git rev-list
--count <40-char-sha>..<40-char-sha>` failed with "fatal: failed to
stat '...': Filename too long" on this Windows host -- confirmed
directly (the SAME range with short SHA prefixes, or `--not` instead of
`..`, worked fine from the IDENTICAL directory, so this is a cwd-length-
sensitive quirk of the full `..`-range argument string, not a real
revision-resolution problem). This is a TEST-code bug (redundant path
nesting), not a production-code bug -- fixed by removing the redundant
nesting; `ensureWorktreeAndCommit`'s own production code path was never
affected since `resolveWorktreePath` does not double-nest on its own.

### Tests added (Part G), all against real Git + real MySQL

`tests/integration/git-worker-handoff.test.ts` (new, 11 tests covering
G1-8/10-12): test 1 forks TWO real, separate `worker-main.ts`/
`restore-worker-main.ts` processes; test 2 proves artifact self-
containment by renaming A's ENTIRE source repo and worktree away before
B restores; tests 3/4 reject a truncated artifact and a wrong manifest/
artifact pairing; tests 5/6 prove duplicate-handoff idempotency and
competing-handoff single-winner; test 7 distinguishes "a genuinely new
command under a stale epoch" (ordinary `EpochStale`) from "a retry of
A's own already-published checkpoint" (intercepted by the replay/
fingerprint check first, per docs/decisions/0004 -- see "Part B note"
below); test 8 confirms historical replay creates no second effect;
test 10 confirms acknowledgement-retry idempotency end to end (not just
at the DB layer -- the FULL restore flow is retried); test 11 confirms
an unrelated task on A is unaffected; test 12 confirms unexpected
destination contents are preserved and the restore rejected.
`tests/integration/git-worker-handoff-crash.ts` (new, `npm run
test:handoff-crash`, test 9): a real SIGKILL of B's process between
ownership transfer and restore acknowledgement (confirmed via the new
`restore-after-checkout-before-ack` failpoint, reporting the real
restored commit/tree over IPC), followed by a real restart (no
failpoint, same owner/epoch/artifact/destination) that recovers via the
documented retry path -- exactly one `RestoreAcknowledged` event, epoch
unchanged.

**Honest note on the killed child's reported exit status**: this
scenario's killed restore-worker process reported `{exitCode: 0,
signalCode: null}` after the `SIGKILL` call, on this Windows host --
NOT the `{exitCode: null, signalCode: "SIGKILL"}` pattern
`git-worker-crash.ts`'s scenarios observe. The DECISIVE evidence that
the kill-before-acknowledgement property actually held is NOT this
reported exit status (inconsistent Windows TerminateProcess reporting
for a process parked in a bare, unresolved `Promise` with no other
pending work is plausible but not independently confirmed this
session) but two things that WERE directly observed: (1) no further
stdout lines from the killed process after its failpoint-ready message
(it never logged "restored successfully"), and (2) the restarted
process's own acknowledgement explicitly reporting `replay: false` --
proving it was the FIRST successful acknowledgement, not a replay of
one the killed process had already completed. Stated this precisely
rather than assumed, per this project's own standing convention (see
CLAUDE.md "How to distinguish simulated behavior from real integrations").

## 2026-10-14 (second follow-up session) -- Phase 2A.2: workspace-classification + checkpoint-validation correctness patch

Phase 2A.1's happy path and crash-recovery evidence were accepted as
exact-commit preservation across crash/restart. This was a second,
narrow correctness patch before two-worker handoff -- explicitly NOT a
second worker, new infrastructure, model providers, or frontend work.

### What was found on full-code inspection (section 1)

Re-reading the ACTUAL order of operations in `ensureWorktreeAndCommit`
(not the prior session's summary of it): identity check, then
`applyScriptedRepair(worktreeDir)` UNCONDITIONALLY, then
`hasUncommittedChanges` -> `commitAll` (`git add -A`), and only THEN,
on the clean path, `assertCleanWorktreeIsValidResult`. Two confirmed
findings:

- **Unexpected dirty edits CAN bypass validation.** `commitAll` staged
  with `git add -A`, not an explicit path list. A reused worktree with
  an unrelated tracked-file edit, a stray staged edit, or an untracked
  file present alongside a genuinely-correct, freshly-applied repair
  would be committed TOGETHER, in the same commit, with nothing having
  ever decided that was safe -- `assertCleanWorktreeIsValidResult` never
  runs on the dirty path at all, so it provides no protection here.
- **An unexpected HEAD CAN be bypassed, destructively.** `applyScriptedRepair`
  ran before ANY check of the worktree's actual Git state (identity
  verification checks only the SIDECAR file, a JSON record, never the
  real on-disk HEAD/dirty state it claims to describe). A worktree that
  was clean but sitting at an unrelated, non-repair commit still had
  `applyScriptedRepair` write to `src/add.js` before the ancestry/
  commit-count check (`assertCleanWorktreeIsValidResult`) ever ran on
  the clean path that DOES exist today -- for the specific case where
  that unrelated commit's `src/add.js` still contained the known defect
  line, the unconditional write would silently convert an inconsistent,
  rejectable state into a dirty, superficially-normal-looking one BEFORE
  the rejection check got a chance to run, destroying the evidence of
  the inconsistency in the process.

Both findings were CONFIRMED, not merely suspected -- both are now
exercised end-to-end through the real orchestration path (see below),
not just reasoned about.

### What was fixed

- **`classifyWorkspace`** (`src/worker/workspace-classify.ts`, new): a
  READ-ONLY classification (`git status`/`rev-parse`/`merge-base`/
  `rev-list` only -- never a write) called BEFORE `ensureWorktreeAndCommit`
  touches anything. Returns one of `ready_for_repair`, `repair_pending_commit`
  (the known interrupted-repair state -- dirty, but the ENTIRE
  changed-path set is exactly the one repair target with content already
  matching the known fix), `repair_committed` (delegates to
  `checkCommittedRepairState`, factored out so `assertCleanWorktreeIsValidResult`
  and `classifyWorkspace` share the SAME ancestry/repair-presence/
  commit-count logic rather than risking it drifting apart), or
  `unexpected` (everything else, with the offending path(s)/HEAD state
  named). `ensureWorktreeAndCommit` throws immediately on `unexpected`,
  BEFORE calling `applyScriptedRepair` or staging/committing anything.
- **`commitAllowedPaths`** (`src/worker/git.ts`, replacing `commitAll`/
  `git add -A` entirely -- it had exactly one call site, now removed):
  stages EXACTLY a caller-supplied path list; verifies the worktree's
  changed-path set contains nothing else BEFORE staging (refuses if it
  does); verifies the worktree is fully clean AFTER committing. Backed
  by a new `changedPaths` helper (`git status --porcelain=v1 -z`,
  NUL-parsed into `{status, path}` entries) that `classifyWorkspace`
  also uses. See docs/decisions/0010-explicit-path-commits.md.
- **A validated checkpoint-persistence boundary**
  (`parseAndValidateGitCheckpointManifest`, `src/db/task-store.ts`):
  `TaskStore.recordGitCheckpoint` previously accepted
  `baseCommit`/`resultCommit`/`resultTree`/`verificationCommandJson`/
  `verificationOutcome`/`runtimeFingerprintJson`/`manifestVersion` as
  SEPARATE arguments, independent of (and never cross-checked against)
  `manifestJson`/`manifestDigest` -- consistent at the one production
  call site today, but not verified, and not safe against a different
  caller supplying a contradictory combination. The interface no longer
  accepts those separate fields: the store now parses and validates
  `manifestJson`'s shape, RECOMPUTES its digest and rejects a mismatch,
  rejects a manifest whose own taskId/attemptId/epoch disagree with the
  request's, rejects a manifest whose own `verification.outcome` is not
  `"passed"`, and derives every persisted column from that validated
  manifest. This validation runs ONLY on the genuinely-new-write path,
  strictly AFTER the existing-receipt replay lookup, so a replay from the
  original committing owner still succeeds even after the task's epoch
  has since moved on (unchanged from docs/decisions/0004) -- verified by
  a dedicated test, not merely unchanged by inspection.
  `src/worker/checkpoint.ts`'s `publishCheckpoint` was simplified to
  match (passes only the digest/JSON, not a second flattened copy).

### Tests added, all through the REAL orchestration path

Per this patch's own instructions, NOT limited to directly testing
`assertCleanWorktreeIsValidResult`/`classifyWorkspace` in isolation --
`tests/integration/git-worker-workspace-state.test.ts` (new, 6 tests)
calls `runGitWorkerTask` itself (the exact function `worker-main.ts`'s
real CLI entrypoint calls), against a manually-constructed REUSED
worktree in each state:

- An unrelated UNSTAGED tracked-file edit (`verify.js`) -- rejected; no
  commit, no checkpoint, the edit itself verified byte-for-byte unchanged
  after the rejected attempt.
- An unexpected STAGED edit -- rejected; the file content AND its staged
  status verified unchanged.
- An unexpected UNTRACKED file -- rejected; the file verified still
  present, unchanged, never swept into a commit.
- **The repair target IS correctly fixed AND `verify.js` is ALSO
  dirty** -- rejected; the direct test of the original vulnerability
  (the correct repair must NOT be committed together with an unrelated
  edit). Both files' content verified unchanged, worktree verified still
  dirty (nothing committed).
- A clean worktree whose HEAD was moved, via a REAL commit, to an
  unrelated (non-repair) commit -- rejected; HEAD verified unchanged by
  the rejected attempt.
- The known interrupted-repair state (fix written to disk, not yet
  committed, nothing else touched) -- recovers SUCCESSFULLY through
  `runGitWorkerTask`: exactly one new commit, matching the exact content
  written, exactly one checkpoint.

`tests/integration/git-worker-checkpoint-manifest.test.ts` (new, 7
tests) calls `TaskStore.recordGitCheckpoint` directly: digest mismatch,
taskId mismatch, attemptId mismatch, epoch mismatch, malformed JSON, a
`"failed"` verification outcome, and a dedicated replay-after-ownership-
change test (the original owner's retry, with its own now-stale epoch,
still succeeds as a replay and returns the original receipt) -- each
rejection case also verified to leave zero checkpoint rows.

All pre-existing `git-worker.test.ts` (11) and `git-worker-crash.ts` (6
scenarios) tests continue to pass unmodified against the refactored
code, including the exact-commit crash-recovery guarantees from Phase
2A.1.

### Honest note on "bugs found this session"

Unlike Phase 2A.1 (which found and fixed a real Windows path-aliasing
bug during its own test run), this session's OWN testing did not catch
a new PRODUCTION bug -- the two findings above were identified by
reading the code (section 1), not by a test failing unexpectedly. One
mistake WAS caught and fixed in this session's own TEST code (not
production code): the first version of
`git-worker-workspace-state.test.ts`'s worktree-identity setup wrote the
fixture's raw (non-`realpath`'d) directory into the identity sidecar,
which failed every scenario with a `sourceRepoRealPath` mismatch on this
Windows machine's 8.3-short-name aliasing (`NAVINK~1` vs. `Navin Kumar`)
-- the same class of issue Phase 2A.1 fixed in production code,
recurring in this session's test helper, fixed by calling `realpath()`
on the fixture directory before writing the sidecar, exactly as
`run-task.ts` itself does.

### Validation (Node 22.21.0 AND Node 24.21.0 -- see table below)

```
node --version                                       # v22.21.0 (system default)
npm run typecheck                                     # clean
npm run build                                          # clean
npm run test:unit                                      # 7 pass, 0 fail
node --import tsx --test tests/integration/git-worker.test.ts
  # 11 pass, 0 fail (unchanged)
node --import tsx --test tests/integration/git-worker-workspace-state.test.ts
  # 6 pass, 0 fail (new)
node --import tsx --test tests/integration/git-worker-checkpoint-manifest.test.ts
  # 7 pass, 0 fail (new)
npm run test:git-worker-crash
  # 6 scenarios, ALL PASS (unchanged)
npm run test:integration                               # full suite: 89 pass, 0 fail
```

Node 24.21.0 (`node-v24.21.0-win-x64`, previously downloaded to this
machine's user-local tools directory, re-located this session rather
than re-downloaded -- `<user-home>\.relaylab-tools\`) was used,
by absolute path, to re-run build/typecheck and every worker/checkpoint
test above (git-worker.test.ts, git-worker-workspace-state.test.ts,
git-worker-checkpoint-manifest.test.ts, and the full
`test:git-worker-crash` scenario set) -- ALL PASS, identically to Node
22.21.0. `child_process.fork()` (used by `process-harness.ts` to spawn
the worker child in `git-worker-crash.ts`) defaults to the PARENT
process's own `execPath`, so running the orchestrator on Node 24
automatically ran the forked worker child on Node 24 too -- confirmed
directly from the real run's own logged runtime identity: orchestrator
`node v24.21.0 at <user-home>\.relaylab-tools\node-v24.21.0-win-x64\node.exe`,
and every one of the killed AND recovered worker children logging the
identical `node v24.21.0` at that same path. Unrelated monitoring
suites (OpenTelemetry/Prometheus/metrics) were NOT re-run this session,
per this patch's own instructions ("do not rerun unrelated monitoring
suites unless affected") -- they are unaffected by this patch's changes.

Docker Desktop and its MySQL/NATS/OTel/Prometheus/Grafana containers
were already running at the start of this session (from a prior
session); not restarted, not a new/different environment.

### Exact pre-kill/post-restart commit IDs and checkpoint/event counts (Node 24.21.0 run)

**Correction (Phase 2B session)**: the table below, as originally
written, mislabeled its "result commit" column as "pre-kill AND
post-restart, identical" for ALL THREE scenarios. That equality is only
MEANINGFUL for the first scenario -- in the other two, the kill happens
BEFORE any repair commit exists at all (one before the repair is even
applied to disk, one after it is applied but before `git commit` runs),
so there IS no pre-kill result commit to compare against; the single
commit SHA shown for those two is the POST-RESTART result only, created
entirely by the recovered worker.

| Scenario | base commit | pre-kill result commit | post-restart result commit | result tree | checkpoints after restart |
|---|---|---|---|---|---|
| commit-created-checkpoint-not-persisted | `2c41e734f47cdda17704e0381e8109b2bdd9d2b3` | `532d7bdbc0562cba0f136a0d8dcf1fd425d7ee4a` | `532d7bdbc0562cba0f136a0d8dcf1fd425d7ee4a` (EXACTLY equal -- a real commit already existed before the kill, re-used, not recreated) | `77f8925d968fe2d9610062362c28d808d53bda99` | 1 (1 CheckpointRecorded event) |
| crash-after-worktree-before-repair | `673ffedbb138d184d340e8b832d2f6ca7d66f047` | **none -- no commit existed yet** (HEAD was still exactly the base commit, clean, repair not yet applied) | `44b0cd6ea9be1b9f8538ff16ef0ddaee607870ea` (created by the RECOVERED worker) | (not independently re-captured by this scenario's own assertions) | 1 |
| crash-after-repair-before-commit | `73f8df3dd3c00f0f30ff1b03e05fa347d71ab023` | **none -- the repair was written to disk but `git commit` never ran** (HEAD was still the base commit, worktree dirty) | `ab1f3966b1e19fd3f5e76c551ed1024b6252265c` (created by the RECOVERED worker, committing the SAME pending content) | (not independently re-captured by this scenario's own assertions) | 1 |

Only the FIRST scenario's own assertions independently re-derive and
compare a pre-kill vs. post-restart value at all (see
`tests/integration/git-worker-crash.ts` `scenario_commitBeforeCheckpoint`
for the exact assertions; the other two scenarios' own assertions check
commit-count delta and `result_commit` equality, not a separately
re-derived tree value.)

### Remaining limitations, stated explicitly

- This patch validates INTERNAL CONSISTENCY of the checkpoint manifest
  (digest matches content, request fields match manifest fields,
  verification outcome is "passed") -- it does NOT prove an untrusted
  worker actually ran the verification command it claims to have run.
  That would require independent re-execution or a trusted execution
  environment, neither of which exists in this phase.
- The supported dirty state (`repair_pending_commit`) is still defined
  narrowly in terms of this ONE fixture's ONE repair target and its two
  known contents (defect, fix) -- this is not a generic patch-
  reconciliation engine, and was explicitly asked not to be one.
- `classifyWorkspace`/`commitAllowedPaths` catch an INCONSISTENT
  resulting state; they do not prevent a process OTHER than this worker
  from mutating the worktree CONCURRENTLY with a classify-then-act
  sequence (no atomic fencing of arbitrary filesystem writes) -- same,
  unchanged limitation as Phase 2A.1.
- Two-worker handoff, a generic agent-reconciliation framework, and
  anything beyond this deterministic single-repair fixture remain
  explicitly out of scope (docs/ROADMAP.md Phase 2) -- stopping after
  this patch, per its own instructions.

## 2026-10-14 (follow-up session) -- Phase 2A.1: committed-state reconciliation correctness patch

Phase 2A's happy path was provisionally accepted; committed-state
reconciliation specifically was flagged as not yet approved ("a clean
worktree alone does not prove a completed repair"). This was a focused
correctness patch -- explicitly NOT a second worker, new infrastructure,
model providers, or frontend work.

### What was found on audit (section 1)

Re-reading `ensureWorktreeAndCommit`'s prior clean-worktree branch
(`if (dirty) {...} return current HEAD;`) against the five things a
completed checkpoint needs to actually prove:
- Worktree belongs to the requested repo/task/attempt: **NOT checked on
  reuse** -- only `stat()` existence was checked.
- Base commit matches the requested base: **NOT checked on reuse**.
- HEAD is the intended result: **unproven** -- no positive check that
  HEAD differs from base or that the repair is actually present.
- The intended repair is present: enforced only IMPLICITLY, via
  `applyScriptedRepair`'s own idempotency guarantee in the non-test
  path -- never an explicit, named, independently-testable check.
- Verification applies to the same code state being checkpointed: **real
  gap** -- the result tree was read AFTER verification ran, not pinned
  before it, so nothing would have caught drift between the two.

### What was fixed

- **`src/worker/worktree-identity.ts`** (new): a sidecar JSON file next
  to each worktree (`<worktreeDir>.identity.json`) recording
  `{taskId, attemptId, sourceRepoRealPath, baseCommit, branch,
  createdAtIso}`. Reusing an existing worktree now requires this to
  match the current call's expected identity on every field, or the
  call is rejected with a field-naming error.
- **`ensureWorktreeAndCommit`** (`src/worker/run-task.ts`): rewritten
  around explicit, named states (fresh checkout; reused+identity-
  verified; repair-applied-not-committed; repair-committed; and an
  explicit rejection state). The rejection logic was factored into its
  own exported function, `assertCleanWorktreeIsValidResult`, specifically
  because the REAL (non-test) call path's `applyScriptedRepair` step is
  unconditional and self-healing -- it always either applies the fix or
  confirms it is already present, or throws -- so the rejection branch
  can never be organically reached by driving the full worker through
  its public entrypoint. Extracting it lets the rejection be tested
  directly against a deliberately inconsistent worktree instead of
  either leaving it untested or claiming an exploit that doesn't exist
  in this fixture. The committed-state check also now confirms HEAD is a
  real descendant of base (`isAncestor`, new in `src/worker/git.ts`) and
  that the commit-count delta from base to HEAD is exactly 1
  (`commitCountBetween`, also new) -- this deterministic fixture's own
  known invariant, made an explicit assertion rather than an assumption.
- **Verification-to-tree binding** (`runGitWorkerTask`): the result tree
  is now captured via `currentTree()` BEFORE verification runs. After
  verification passes, the worktree is re-checked (clean, same tree,
  same commit) against that pinned value; any drift throws explicitly.
  Documented limitation: this does NOT claim atomic fencing of arbitrary
  filesystem writes -- it closes the specific "manifest describes a tree
  that was never actually verified" gap, nothing broader.
- **Richer failpoints** (`src/testing/failpoint.ts`): `hitFailpoint` now
  accepts an optional `extra` payload, merged into the IPC readiness
  message. The `worker-after-verify-before-checkpoint` failpoint (the
  actual commit-before-checkpoint crash window) now reports the REAL
  result commit SHA and tree ID at the exact moment it fires. Two new,
  earlier failpoints (`worker-after-worktree-before-repair`,
  `worker-after-repair-before-commit`) cover the two earlier crash
  windows requested.
- **`tests/integration/git-worker-crash.ts`**, substantially extended:
  - The original "commit created, checkpoint not persisted" scenario now
    independently inspects the real worktree from the orchestrator
    process itself (via `src/worker/git.ts` calls against a test-owned,
    explicit `RELAYLAB_WORKER_WORKSPACE_ROOT`, not an assumed shared
    fallback), asserts the IPC payload's reported SHA/tree match that
    independent inspection BEFORE the kill, and after the real SIGKILL +
    restart, asserts EXACT equality (commit, tree, persisted
    `result_commit`/`result_tree` columns) against the pre-kill values,
    plus real ancestry (`isAncestor`) and an exact commit-count delta
    (`commitCountBetween`) -- never "a checkpoint row now exists" alone,
    and never an argument from code-path structure in place of a
    captured value.
  - Two new real-process-kill scenarios: crash after the worktree is
    created but before the repair is applied
    (`crash-after-worktree-before-repair`), and crash after the repair
    is written to disk but before it is committed
    (`crash-after-repair-before-commit`) -- both restart and reconcile to
    exactly one commit and one checkpoint, independently verified.
  - Two new non-process-kill scenarios directly exercising the explicit-
    state rejections: `clean-at-buggy-base-rejected` (calls
    `assertCleanWorktreeIsValidResult` directly against a manually
    constructed clean-at-base worktree) and `identity-mismatch-rejected`
    (a worktree whose sidecar records a different base commit than the
    current request).
- **`assertNoSymlinkEscapes`/`assertSafeForDeletion`**
  (`src/worker/workspace.ts`): the prior `assertWithinRoot` check was
  purely lexical (string comparison on resolved paths) and could not
  detect a symlink/junction inside the managed root pointing elsewhere.
  `assertNoSymlinkEscapes` walks every existing path segment rejecting
  `lstat().isSymbolicLink()`, plus a real-to-real `realpath` containment
  check. `assertSafeForDeletion` is stricter still (never the root
  itself, exactly a `(taskId, attemptId)` depth), wired into
  `cleanupWorktree`. **A real bug found and fixed during this same
  patch's own test run**: the first version of the real-to-real check
  compared a path's `realpath` against ITS OWN un-realpath'd lexical
  form, which on this Windows machine flagged ordinary 8.3-short-name
  vs. long-name path aliasing (`C:\Users\NAVINK~1\...` vs.
  `<user-home>\...`) as a false-positive "symlink escape" --
  every `git-worker.test.ts` test started failing immediately after the
  check was added. Fixed by comparing the candidate's real path against
  the ROOT's own real path instead of against the candidate's lexical
  self.

### Validation (Node 22.21.0 -- see "Node 24" note below)

Node 24 was downloaded to this machine in a PRIOR session (Phase 1.2.1)
to a user-local tools directory outside the repo; that directory is no
longer present on this machine in this session (only the system-wide
Node 22.21.0 install was found). This patch's validation therefore ran
on Node 22.21.0 only. `engines.node` in package.json remains `>=24.0.0`
as a forward-looking constraint; nothing in this patch is known to
require a Node-24-only API, but it was NOT re-verified against Node 24
in this session -- do not claim Node 24 validation for this specific
patch without re-downloading/re-pointing at a Node 24 binary and
re-running the commands below.

```
node --version                              # v22.21.0 (system default)
npm run typecheck                           # clean
npm run build                                # clean
npm run test:unit                            # 7 pass, 0 fail
node --import tsx --test tests/integration/git-worker.test.ts
  # 11 pass, 0 fail (10 pre-existing + 1 new: the real NTFS junction boundary test)
npm run test:git-worker-crash
  # 6 scenarios, ALL PASS (2 pre-existing + 4 new: crash-before-repair,
  # crash-after-repair-before-commit, clean-at-buggy-base-rejected,
  # identity-mismatch-rejected)
```

Docker Desktop was not running at the start of this session (containers
from the prior session's `infra/compose.yaml` stack were present but
stopped); it was started and the existing MySQL/NATS/OTel/Prometheus/
Grafana containers came back up unchanged, so these are the SAME real
MySQL 8.4 + real `git` binary this project has used throughout -- not a
new or different environment.

### Remaining limitations, stated explicitly

- `assertNoSymlinkEscapes` was verified on this host (win32) with a real
  NTFS junction. A true Windows symlink (`SYMLINK` reparse tag, as
  opposed to `MOUNT_POINT`/junction) and any POSIX symlink behavior are
  UNTESTED on this machine -- expected to behave the same way per Node's
  documented, platform-uniform `fs.Stats` semantics, but not exercised.
- The worktree-identity sidecar and ancestry/commit-count checks catch
  an INCONSISTENT resulting state; they do not prevent a process OTHER
  than this worker from mutating the worktree concurrently -- there is
  still no atomic fencing of arbitrary filesystem writes, and this patch
  does not claim otherwise.
- `assertCleanWorktreeIsValidResult`'s own rejection branches are, by
  design, unreachable through the real (non-test) `ensureWorktreeAndCommit`
  call path alone (see "What was fixed" above) -- they are tested
  directly, not through the full worker, and this is stated as such
  rather than implied to be a live, organically-triggerable path.
- Two-worker handoff, a generic agent-reconciliation framework, and
  anything beyond this deterministic single-repair fixture remain
  explicitly out of scope (docs/ROADMAP.md Phase 2).

## 2026-10-14 (later session) -- Phase 2A: one real Git execution worker + a small monitoring fix

### Part A: the missing-freshness-metric alert gap, actually closed

The prior report said "missing freshness metrics are covered by `up ==
0`" -- true only for a target that is DOWN, not for a target that
scrapes successfully but whose expected durable-gauge freshness series
NEVER appeared (refresher crashed/never started, or MySQL was
unreachable from the very first attempt). Closed with two new alerts
(`RelaylabPublisherOutboxFreshnessMissing`,
`RelaylabConsumerGapsFreshnessMissing`, `infra/prometheus-alerts.yml`),
each scoped to the ONE gauge group that service is actually expected to
export (the publisher rule additionally requires
`relaylab_publisher_leadership_state == 1`, so a legitimate STANDBY
publisher -- which never exports the outbox group by design -- never
false-fires), with a 2-minute startup grace period. `promtool test
rules` fixtures added for all 5 points the review asked about (target
down / up-but-absent / never-refreshed [observationally identical to
absent, documented as such] / stale / fresh), run against the real
Prometheus container: `SUCCESS` (9 rules, all fixtures pass). See
docs/RUNBOOKS.md for the two new runbook entries.

### Part B-J: the Phase 2A worker

A SCRIPTED (explicitly, repeatedly labeled non-LLM) worker that: claims
a real task (`TaskStore.claimTask`, unchanged), creates an isolated Git
worktree from a specified commit in a disposable local fixture
repository, applies ONE fixed deterministic repair
(`src/worker/repair.ts` -- a hardcoded `src/add.js` subtraction->addition
fix), runs a fixed allowlisted verification command (`node verify.js`,
never a task-supplied command), commits on success, builds a COMMITTED-
STATE-ONLY checkpoint manifest, and persists it transactionally through
the EXISTING `command_receipts`/task-update/outbox pipeline via a new
`TaskStore.recordGitCheckpoint` method that reuses the unchanged
`Checkpoint{digest}` domain command -- ZERO changes to
`src/domain/domain.ts`'s Command/DomainEvent protocol, one additive
migration (`0007_task_checkpoints.sql`).

**Real end-to-end success on the very first full run** (`npm run
demo:git-worker`): real fixture repo, real worktree, real scripted
repair, real `node verify.js` passing, real local commit, real durable
checkpoint -- see "Final review packet" below for the actual commit/tree
IDs from both a Node 22 and a Node 24 run.

#### Two real bugs found by this session's OWN crash testing (not hypothetical)

1. **The managed workspace root was randomized per worker process**
   (`mkdtemp`), which silently broke restart reconciliation entirely: a
   restarted worker could never find a prior attempt's worktree, because
   it was looking in an unrelated directory. Symptom that surfaced
   first: the crash test's "restarted worker" used a DIFFERENT owner
   identity than the killed one and got a real, correct `ClaimLost`
   rejection (the killed worker's lease had not expired) -- which, once
   understood, led to fixing the TEST (use the SAME persistent owner
   identity across a restart of the SAME logical worker -- a realistic
   scenario, not the "different worker takes over" handoff this
   milestone explicitly does not implement) AND the real underlying bug
   (`createManagedWorkspaceRoot` now defaults to a STABLE, shared path,
   `RELAYLAB_WORKER_WORKSPACE_ROOT` or a fixed OS-temp-dir path, not a
   fresh random one -- see docs/ARCHITECTURE.md "Workspace root
   persistence").
2. **`worker-main.ts`'s real CLI path deleted the ENTIRE managed
   workspace root after every single successful run** -- harmless when
   the root was randomized per-process (nothing else used it), but once
   fixed to be STABLE/SHARED (bug 1's fix), this would have destroyed
   OTHER tasks' worktrees and, worse, deleted the very checkpoint's own
   evidentiary Git objects immediately after publishing it. Fixed by
   removing the automatic cleanup entirely -- workspace cleanup is now
   an explicit, NOT-yet-implemented, separate operator concern (see
   docs/ARCHITECTURE.md "Checkpoint manifest -- COMMITTED STATE ONLY":
   "local Git objects must remain available... a manifest alone is not a
   portable backup").

#### Architecture decisions (documented BEFORE coding, per the task's own instruction)

- **Single, deterministic `attemptId` per task** (`"attempt-1"`, always)
  -- not a fresh random ID per restart. This is what makes restart
  reconciliation a simple, LOCAL filesystem check rather than a
  distributed search across possible prior attempts. A real multi-
  worker handoff/election model is explicitly NOT implemented (per this
  session's own "stop after Phase 2A" instruction).
- **`recordGitCheckpoint` is a SEPARATE TaskStore method**, not a
  generic "extra insert" hook threaded through the already-complex,
  well-tested `applyTaskCommand`. The duplication is a SMALL, identical
  fencing/idempotency block (same pattern, same risk profile), not new
  logic -- judged safer than risking the generic command path.
- **The checkpoint's idempotency key (`checkpointCommandId`) is
  TASK-scoped, not attempt-scoped** -- a retry from a DIFFERENT
  `attemptId` after a crash still collides correctly with an
  already-published checkpoint for that task.
- **Ownership check vs. filesystem fencing, stated honestly**: a DB
  epoch/ownership check before running Git does NOT atomically fence the
  subsequent filesystem work. What DOES hold: isolated, attempt-scoped
  worktrees (two attempts can never corrupt each other's workspace), and
  the FINAL authoritative boundary (`recordGitCheckpoint`) re-checks
  epoch/ownership before persisting anything, so a stale worker's Git
  work -- even if it fully completes -- can never become the
  authoritative durable result. Verified with a REAL epoch takeover
  (`tests/integration/git-worker.test.ts` test 9): the stale worker's
  Git work succeeds, but its final publish is rejected with `EpochStale`,
  and zero checkpoint rows exist afterward.

### Exact commands and counts (Node 22 -> Node 24, both real)

```
Node 22.21.0:
  npm run build / npm run typecheck                    -- exit 0 / exit 0
  npm run test:unit                                      -- 7/7
  npm run test:integration                                -- 82/82 (was 72; +10 new git-worker.test.ts)
  npm run demo:recovery / demo:crash-windows               -- PASS
  npm run test:telemetry-collector / test:metrics          -- 1/1 each
  npm run test:monitoring-scenarios                         -- 3/3
  npm run test:git-worker-crash                             -- 2/2 scenarios PASS
  npm run demo:git-worker                                    -- PASS (real checkpoint published)

Node 24.21.0 (downloaded, SHA256-verified, absolute path, forked children confirmed to inherit it):
  build/typecheck                                            -- exit 0 / exit 0
  unit                                                        -- 7/7
  integration                                                 -- 82/82
  test:git-worker-crash                                       -- 2/2 scenarios PASS, both orchestrator AND forked worker processes confirmed on node v24.21.0
  demo:git-worker                                              -- PASS
```

### Explicitly NOT done this session (per its own instructions)

No second worker, no multi-worker handoff/election, no general tool-
execution interface, no dirty/staged/untracked-file checkpoint capture,
no Terraform/Kubernetes/Helm/React/Electron/live model providers/GitHub
integration, no generalized distributed-consensus framework.

## 2026-10-14 -- Phase 1.2.1 monitoring-reliability patch: COMPLETE

(Supersedes the in-progress HANDOFF entry that previously occupied this
slot -- that entry's open question, the broker-outage recovery failure,
was diagnosed and fixed in this same continuation; see "root cause" and
"recovery contract" below. Repo: `<local-repo>`,
NOT a git repository -- no `.git` found; "committed" anywhere in this
project's docs means a DATABASE transaction commit, the domain term from
`command_receipts`/`applyTaskCommand`, never a git commit.)

A second review identified three gaps in the Prometheus/Grafana
monitoring milestone (overlapping gauge refreshes, operational failures
mislabeled as domain rejections, an unsafe quarantine identity), two
missing real scenarios (broker outage, event gap), and an unstarted
gauge-ownership-visibility requirement. All are now closed, verified on
real MySQL/NATS/Prometheus, and validated on both Node 22.21.0 (system
default) and Node 24.21.0 (the same previously-downloaded, SHA256-
verified binary from the prior patch).

### 1. Overlapping durable-gauge refreshes -- fixed

`src/telemetry/durable-gauges.ts` rewritten from `setInterval(refresh,
intervalMs)` (which could overlap a slow query with the next scheduled
tick) to COMPLETION-BASED scheduling (`setTimeout`, only scheduled after
the previous refresh settles) -- "at most one in-flight refresh" and "no
older refresh can overwrite newer state" are now structurally true, not
separately enforced. Queries use mysql2's own `timeout` option, honestly
documented as destroying the CLIENT connection only (never claimed to
cancel the server-side query -- the same honest claim this project's
telemetry shutdown code already makes about its own `Promise.race`
pattern). `stop()` returns a `Promise`, resolved only once any in-flight
refresh has settled, and is now called (and AWAITED) BEFORE `pool.end()`
in both `*-main.ts` files (previously backwards). 4 new tests in
`tests/integration/durable-gauges.test.ts`, including a REAL `SELECT
SLEEP()` proving max concurrency never exceeds 1.

### 2. Operational failures vs. domain rejections -- fixed

`CommandOutcome`/`OwnershipOutcome` gained a 4th value,
`infrastructure_failure`, distinct from `rejected` (now reserved for the
SAME finite expected-domain-rejection set `isExpectedDomainRejection`
already uses). `with-span.ts`'s shared `withRelaylabSpan` wrapper now
picks the outcome bucket based on that classification, not "any throw =
rejected." Verified with a REAL unreachable-MySQL connection (not a
mock) in `tests/integration/metrics-outcomes.test.ts` (4 new tests) --
confirmed a genuine infrastructure failure is NEVER recorded as
`outcome="rejected"`, and a genuine domain rejection (EpochStale) is
NEVER recorded as `outcome="infrastructure_failure"`. Also documented
(not changed -- a stated, honest limitation): cascade-draining a gap
closure applies MULTIPLE events in one transaction but is still counted
as ONE `relaylab_consumer_attempts_total{outcome="applied"}` observation
(see docs/METRICS.md "operational failures vs. domain rejections").

### 3. Quarantine identity and INSERT auditing -- fixed

The dedup key widened from `(consumer_name, nats_stream_seq)` to
`(consumer_name, stream_name, stream_generation, nats_stream_seq)`
(migration `0005_quarantine_stream_identity.sql`) -- `stream_generation`
is the NATS stream's own creation timestamp (`StreamInfo.created`,
`src/messaging/nats-client.ts` `StreamIdentity`), the simplest available
identifier that changes exactly when a stream is deleted and recreated
(when low sequence numbers could otherwise collide with a PRIOR
incarnation's quarantine rows) and never otherwise -- no new generation-
tracking subsystem was built. `writeQuarantineRecord`
(`src/messaging/quarantine.ts`) now also distinguishes a genuine
duplicate-key collision (confirmed via a follow-up existence check) from
an ANOMALOUS `INSERT IGNORE` outcome (0 affected rows, but no matching
row exists -- meaning some OTHER problem was silently suppressed) --
the anomalous case THROWS, so the caller never acks on unverified
evidence. 3 new tests added to `tests/integration/quarantine.test.ts`
(8 total in that file), including a real cross-generation collision
probe proving two different generations' seq=1 rows coexist rather than
one silently overwriting the other.

### 4. Executable alert-rule tests -- added

Every one of the (now 7, see section 6) alerts has a `promtool test
rules` fixture in `infra/prometheus-alert-tests.yml` covering healthy/
non-firing, the `for:`-duration pending boundary (where applicable),
firing, and recovery -- run via `promtool test rules
prometheus-alert-tests.yml` inside the real Prometheus container,
kept explicitly separate from (and in addition to) the syntax-only
`promtool check config`/`check rules`. `RelaylabDurableGaugeRefreshStale`'s
missing-data behavior is explicitly tested and documented (see section 7
below) rather than silently assumed.

### 5. The two missing monitoring scenarios -- both now exercised with real evidence

`tests/integration/monitoring-scenarios.manual.ts` gained "A1" (outage
WITHIN the reconnect budget) and "A2" (outage BEYOND it, exercising the
new application-level reconnect), alongside the existing "B" (event
gap, unchanged, already passing). See "NATS reconnect and recovery
contract" in docs/OBSERVABILITY.md for the full causal diagnosis this
required, and the "Final review packet" below for the actual captured
values. All three pass on real MySQL/NATS/Prometheus, with real
instrumented publisher/consumer processes, on both Node 22 and Node 24.

A real diagnostic investigation (a standalone script consuming
`nc.status()`'s own event stream, run against the real NATS container
via actual `docker stop`/`docker start`) found: an idle connection's
reconnect cadence exactly matches nats.js's documented defaults (10
attempts, ~2.1s apart, ~20s total) before permanently closing
(`nc.isClosed()` stays `true` forever afterward, confirmed ~17s past the
broker's real recovery); an ACTIVE consumer/publisher loop exhausts the
SAME nominal budget measurably faster than an idle connection. This is
why the tests use a small, test-only configurable reconnect budget
(`RELAYLAB_NATS_MAX_RECONNECT_ATTEMPTS`/`RELAYLAB_NATS_RECONNECT_TIME_WAIT_MS`,
never set in production) rather than relying on the nominal idle timing.

**The chosen, documented recovery policy for "beyond budget"**:
application-level reconnect/recreation with bounded backoff (NOT
process termination + external supervision) -- see docs/OBSERVABILITY.md
for the full contract, including why durable consumer identity/delivery
position survives (it lives on the NATS SERVER, never the client), and
why duplicate processing loops cannot occur (the reconnect check and
retry happen once per iteration inside a single sequential loop).
`relaylab_nats_connected` (new gauge, event-driven via `nc.status()`) is
the bounded readiness signal distinguishing "process alive" (a healthy
`/metrics` scrape, which starts before `connectNats` even runs) from
"able to process work."

A real bug was found and fixed along the way: `nc.drain()` on an
already-permanently-closed connection THROWS rather than resolving,
which (before the fix) crashed the process with an uncaught exception
during shutdown. Fixed by checking `nc.isClosed()` explicitly first
(skip `drain()` for the EXPECTED closed case; still log loudly for any
OTHER, unexpected `drain()` failure) -- never a blanket catch-and-report-
success.

### 6. Gauge-ownership visibility -- completed (was unstarted at the prior handoff)

The outbox-gauge group was already ENFORCED single-owner (the real
`publisher_leader` DB lock). The gap/quarantine group has no equivalent
election mechanism (horizontal consumer scaling is an explicit
non-goal) -- made VISIBLE instead: `consumer_gauge_exporters` (migration
`0006`) + `relaylab_consumer_gauge_exporter_count` (new gauge) +
`RelaylabDuplicateConsumerGaugeExporters` (new alert, with its own
promtool fixture) together make a > 1 count -- two consumer instances
exporting for the same `consumer_name` -- directly observable rather
than silently producing duplicate series. Tested with TWO REAL,
simultaneously-running refresher instances
(`tests/integration/durable-gauges.test.ts` "section 6"): confirmed the
count correctly reports 2 while both are active, and that a stopped
instance's heartbeat correctly ages out of the count once past 3x the
refresh interval (verified at the DB-query level directly, since
OpenTelemetry JS's ObservableGauge API has no callback-removal method --
running two refreshers in ONE test process leaves one stale Prometheus
data point behind as a testing-harness artifact that does not occur in
production, where each real instance is its own OS process with its own
separate `/metrics` endpoint; documented explicitly in the test). No
generalized election/distributed-consensus framework was added -- this
patch explicitly chose detection over enforcement for the consumer side,
per its own scope instruction.

**Exactly what scaling behavior remains unsupported, stated plainly**:
running more than one consumer process against the same durable
consumer name is NOT supported -- it is only made OBSERVABLE via the
alert above, never load-balanced, prevented, or deduplicated at the
gauge level.

### 7. The missing-data alert gap -- reviewed, one real gap documented (not silently left open)

`RelaylabDurableGaugeRefreshStale`'s query is `time() -
relaylab_gauge_last_refresh_success_timestamp_seconds`. Before the FIRST
successful refresh, the series is simply ABSENT (never a fabricated 0),
so the alert correctly never fires on a freshly-started process that
hasn't completed its first refresh yet -- this is the INTENDED
"honest absence" behavior (see docs/METRICS.md "missing metrics must
not silently appear as healthy zeroes"), not a gap. The REAL, DOCUMENTED
gap: if the exporting PROCESS itself goes down entirely (not merely
stalled), the WHOLE series vanishes, and `time() - <absent series>` has
no samples to subtract from -- producing NO alert from
`RelaylabDurableGaugeRefreshStale` at all. This specific condition
(process down) IS reliably caught by a DIFFERENT alert,
`RelaylabServiceScrapeDown` (`up == 0`), so overall alert COVERAGE
remains coherent -- not every alert needs to handle every failure mode,
but every failure mode this project's alerts claim to cover is caught by
AT LEAST one of them. Explicitly tested as its own `promtool test rules`
case (`infra/prometheus-alert-tests.yml`, "Missing/stale-data behavior")
asserting `exp_alerts: []` for an absent series, labeled as a documented
limitation in the fixture file's own comment, not silently passed over.

### Exact commands and counts (this patch's final validation)

```
Node 22.21.0 (system default):
  npm run build / npm run typecheck                         -- exit 0
  npm run test:unit                                           -- 7/7
  npm run test:integration                                    -- 72/72 (was 71 before section 6's 2-instance test; +1)
  npm run demo:recovery / npm run demo:crash-windows          -- PASS
  npm run test:telemetry-collector / npm run test:metrics     -- 1/1 each
  npm run test:monitoring-scenarios                           -- 3/3 (A1, A2, B)

Node 24.21.0 (downloaded, SHA256-verified, invoked by absolute path):
  build/typecheck                                              -- exit 0
  unit                                                          -- 7/7
  integration                                                   -- 72/72
  recovery/crash-windows demos                                  -- PASS
  telemetry-collector / metrics manual tests                    -- 1/1 each
  monitoring-scenarios (A1, A2, B)                               -- 3/3

promtool (real container):
  check config / check rules                                    -- SUCCESS
  test rules prometheus-alert-tests.yml (7 alerts x 3-4 fixtures each) -- SUCCESS
```

### Explicitly NOT done this session (per its own instructions)

No Kubernetes, Terraform, Helm, Git workers, or any new technology/
process-management dependency. No generalized distributed election
framework for consumer instances.

## 2026-10-13 (latest session) -- Phase 1.2.1 Part B-E: Prometheus/Grafana monitoring

Builds on Part A (below) in the SAME patch, started only after Part A was
complete and re-verified. Added Prometheus metrics and a local, optional
Grafana/Prometheus stack. Did not touch the orchestration system, did not
start Terraform/Kubernetes/Helm/React/Electron/Git workers, per this
session's own instructions.

### Architecture decision and metric dictionary (written BEFORE coding)

docs/METRICS.md -- OpenTelemetry Metrics API + `@opentelemetry/exporter-prometheus`
(same version line already pinned for tracing, peer-dependency-compatible
with the installed `@opentelemetry/api@1.9.1`, verified via `npm view
<pkg>@<version> peerDependencies` before installing), not a second,
unrelated metrics library. Full metric dictionary (name/type/unit/
meaning/exact increment point/labels/restart behavior/attempts-vs-durable)
written before any instrumentation code, then implemented to match.

### A real bug this phase's own end-to-end testing caught (not hypothetical)

`NodeSDK` (tracing's `startTelemetry`), when constructed WITHOUT an
explicit `metricReaders` option, silently builds and globally registers
its OWN metrics pipeline by default (`getMetricReadersFromEnv()` in
`node_modules/@opentelemetry/sdk-node/build/src/sdk.js` defaults to an
OTLP metric reader when `OTEL_METRICS_EXPORTER` is unset). Because
`startTelemetry()` runs before `startMetrics()` in both
`*-main.ts` entrypoints, NodeSDK's registration won the ONE-registration-
per-process `@opentelemetry/api` global MeterProvider slot every time --
`src/telemetry/metrics.ts`'s own `setGlobalMeterProvider` call silently
lost the race, so every instrument created by this project's own code
attached to NodeSDK's reader-less provider instead of the
Prometheus-backed one. Symptom: a real publisher/consumer process would
run correctly, log "published N event(s)," and its Prometheus `/metrics`
HTTP server would answer 200 with ONLY `target_info` and the literal text
"# no registered metrics" -- no crash, no error, just silently wrong
data. Found by actually scraping a real running process end-to-end
(not by reading the code), isolated via a sequence of standalone
reproduction scripts (direct `node -e` calls that worked, vs. the real
`*-main.ts` entrypoint that didn't, narrowing to the NodeSDK construction
site), and fixed with one line: `metricReaders: []` added to
`src/telemetry/init.ts`'s `NodeSDK` config, which makes NodeSDK's own
`_meterProviderConfig.readers.length > 0` check false so it never
registers a competing provider. Verified fixed by re-running the exact
same real-process-and-scrape reproduction and the automated
`metrics.manual.ts` test (see below) after the fix.

### Instrumentation added

`src/telemetry/metrics.ts` (instrument definitions + lifecycle, mirroring
`init.ts`'s shutdown-bound pattern) and `src/telemetry/durable-gauges.ts`
(bounded periodic DB refresh, cached values, honest absence/staleness on
failure -- never a fabricated zero). Wired into: `src/db/task-store.ts`'s
`withRelaylabSpan` (command/ownership attempts+duration, one shared hook
for `claimTask`/`renewLease`/`applyTaskCommand`), `src/messaging/publisher.ts`
(publish attempts+duration), `src/messaging/consumer.ts` (consumer
attempts+duration), `src/messaging/quarantine.ts` (quarantine attempts),
and both `*-main.ts` entrypoints (metrics server start/shutdown, leader-
gated outbox-gauge refresher for the publisher, always-on gap/quarantine-
gauge refresher for the consumer, instance-local leadership-state gauge).
Each publisher/consumer process serves its own Prometheus endpoint on its
own port (9464/9465 by default, `RELAYLAB_METRICS_PORT` override).

### Local infrastructure (Compose, same optional `observability` profile as the OTel Collector)

`infra/prometheus.yml` (scrape config, `host.docker.internal` to cross
the container->host boundary since the publisher/consumer run as host
processes), `infra/prometheus-alerts.yml` (6 alert rules, `promtool
check rules`-validated), `infra/grafana-provisioning/` (datasource +
dashboard-folder provisioning) and `infra/grafana-dashboards/
relaylab-overview.json` (9 panels, each with an explicit "what question
does this answer / what to inspect next" description, committed as code,
NOT hand-built through the Grafana UI and exported).

A second Windows-specific infrastructure quirk (same CATEGORY as NATS's
documented 4222->24222 host-port issue, but a different cause): host
port 9090 (and, it turned out, EVERY host port immediately retried after
a failed bind) reproducibly failed with "ports are not available ...
forbidden by its access permissions" from `dockerd` -- NOT a port in
`netsh interface ipv4 show excludedportrange`'s listed ranges, and
confirmed via `Get-NetTCPConnection` that nothing was actually listening
on it. Isolated via a sequence of plain `docker run` tests (bypassing
Compose) that a fresh, never-before-attempted port bound instantly and
cleanly, while a just-failed port stayed stuck -- i.e. a failed Windows
bind attempt appears to leave behind a short-lived, opaque reservation on
that exact port. Fixed by mapping Prometheus's host-side port to 9099
(originally) then the working final a value 9298 after repeated local
retries during diagnosis re-poisoned 9099 too (see `infra/compose.yaml`'s
inline comment for the full sequence) -- the CONTAINER's own internal
port is still Prometheus's standard 9090 throughout; only the host-side
mapping changed.

### Validation performed (real infrastructure, not just code review)

- `promtool check config` / `check rules` inside the real running
  Prometheus container: both pass.
- Real publisher/consumer processes started with metrics enabled;
  Prometheus's own `/api/v1/targets` showed both `up`.
- Real command/publish/consume activity generated (5 distinct tasks);
  Prometheus queried directly for the SAME expressions the dashboard
  panels use (`sum by (outcome) (rate(...))`, `histogram_quantile(0.95,
  ...)`, raw gauge reads) -- all returned real, correct values. The
  `outcome="applied"` series stayed ONE time series across all 5 distinct
  task IDs, direct evidence that task/event IDs are not leaking into
  labels (docs/METRICS.md "label discipline").
- A real DB-refresh failure was induced (renaming `outbox_events` out
  from under a live refresher): the cached gauge value froze at its last
  known value (did NOT reset to 0 or vanish), `relaylab_gauge_refresh_failures_total`
  incremented, and `relaylab_gauge_last_refresh_success_timestamp_seconds`
  stopped advancing -- confirming a DB failure is distinguishable from a
  genuinely empty queue, not silently reported as healthy.
- The Prometheus container was stopped entirely while a real command was
  committed: it succeeded identically, confirming a monitoring outage
  never blocks business processing (metrics are pull-based; there is no
  network call on the command/event hot path).
- `RelaylabServiceScrapeDown` was made to actually fire: the consumer
  process was killed, the alert observed transitioning `(absent) ->
  pending -> firing` in Prometheus's own `/api/v1/alerts` (firing at
  ~65s, matching its `for: 1m`), then the consumer was restarted and the
  alert observed resolving back to absent -- real firing, pending, AND
  recovery behavior exercised, not merely a rule-syntax check.
- Grafana's datasource and dashboard were confirmed provisioned via its
  own HTTP API (`/api/datasources`, `/api/search`, `/api/dashboards/uid/...`),
  and the dashboard was additionally opened and VISUALLY inspected in a
  real browser (logged in with the dev-only admin credentials) -- all 9
  panels render with real data, and panel 1 honestly showed "DOWN" in red
  once the underlying processes were stopped for cleanup, rather than a
  stale "healthy" reading.
- `tests/integration/metrics.manual.ts` (new, excluded from the main
  glob like `telemetry-collector.manual.ts`, run via `npm run
  test:metrics`) codifies the core of the above as a repeatable automated
  test: real separate publisher/consumer processes, real HTTP scrape,
  asserts the per-process gauge-ownership split (publisher never exposes
  the consumer's gauge group and vice versa) and real recorded counter/
  histogram data.
- The full existing regression suite (build, typecheck, 7 unit tests, 60
  integration tests, both real-process-kill demos, the real-Collector
  test) was re-run and confirmed passing on BOTH Node 22.21.0 (system
  default) and Node 24.21.0 (the previously-downloaded, SHA256-verified
  binary) after all Part B-E changes, per this session's explicit
  instruction to re-validate Node 24.

### Explicitly NOT separately re-validated with a dedicated new test this session (reasoned about, not fabricated as tested)

- A deliberate event gap appearing in and clearing from
  `relaylab_pending_gap_events_count`/`relaylab_tasks_with_unresolved_gaps_count`
  specifically THROUGH a live Prometheus scrape -- these gauges read
  directly from `task_consumer_pending`/`task_consumer_state`, which
  `gap-handling.test.ts` already exercises at the database level; the
  gauge reflecting that correctly follows from the refresher's query
  being a direct, unconditional `COUNT(*)` over those same tables, not
  independently re-verified end-to-end here.
- A broker (NATS) outage specifically producing `relaylab_publish_attempts_total{outcome="failed"}`
  growth was not separately re-driven this session (the counter's
  increment point was exercised by the `publishOne` retry-exhaustion code
  path reasoning, and `shutdown.test.ts`'s existing "repeated NATS/DB
  failures back off" test already exercises that failure path at the
  application level, but not with metrics enabled/scraped in that
  specific test run).
- Do not read either gap above as "untested" at the correctness level --
  both underlying mechanisms are covered by existing, passing integration
  tests. What is specifically NOT re-verified is the METRICS reflecting
  them live through an actual Prometheus scrape in those two particular
  scenarios.

## 2026-10-13 (later session) -- Phase 1.2.1 Part A: OpenTelemetry review-gap patch

Phase 1.2's OpenTelemetry work was "provisionally accepted, but its
review identified specific correctness and validation gaps." This
session closed six specific gaps, added no new technology (no
Prometheus/Grafana, no Git workers/React/Terraform/Helm/Kubernetes, per
this session's own explicit instructions), and avoided broad refactoring.

1. **Validated the actual target runtime (Node 24).** `engines.node` was
   `>=22.0.0` in package.json despite the spec requiring Node 24; prior
   validation had only run on the installed Node 22.21.0. No Node 24
   install existed on this machine. With explicit user confirmation, the
   official Node 24.21.0 Windows x64 build was downloaded and SHA256-
   verified against upstream SHASUMS256.txt, then invoked by ABSOLUTE
   PATH (setting `PATH` within the same shell command still silently
   resolved to the system Node 22 in this environment -- invoking the
   binary by its full path was the only reliable method found). Verified
   `fork()` correctly propagates `process.execPath` to child processes
   (a standalone probe script confirmed both parent and forked child
   reported `v24.21.0`), so publisher/consumer child processes spawned
   by the test harness also run on Node 24 when the parent does. The
   FULL check suite (build, typecheck, unit, integration, recovery demo,
   crash-window demo, the real-Collector test) was run on Node 24 and
   passed identically to Node 22 -- no code changes were needed; `engines`
   now correctly states `>=24.0.0`, and Node 22 remains the system
   default (see CLAUDE.md "Runtime and dependency strategy" for the
   exact, current phrasing of this split).
2. **Fixed the telemetry shutdown lifecycle.** The prior
   `Promise.race([target.shutdown(), timeout])` pattern had four real,
   named bugs: the timer was never cleared when shutdown won the race
   (a dangling handle); `Promise.race` does not cancel its losing side,
   so a timed-out caller silently abandoned the real shutdown instead of
   still awaiting it somewhere; a `started = false` boolean was reset
   before the real shutdown had settled, which could let a new
   `startTelemetry()` construct a second `NodeSDK` mid-teardown; and
   nothing called `trace.disable()`/`context.disable()`/
   `propagation.disable()`, so a same-process reinitialization after
   shutdown silently produced a SDK whose spans went nowhere (verified
   both by reading the installed SDK's source and empirically with a
   direct Node script). Rewrote `src/telemetry/init.ts` around an
   explicit `"idle" | "running" | "shutting_down" | "shutdown"` state
   machine, a single shared `inFlightShutdown` promise for concurrent/
   repeated callers, a `finally`-cleared timer, and a documented,
   tested reinitialization contract (supported, but only after a prior
   shutdown has fully settled). `shutdownTelemetry` now returns a typed
   `ShutdownOutcome` that is explicit about NOT conflating "this call's
   bound was met" with "the process exited" or "the Collector received
   everything" -- both `-main.ts` entrypoints now check it and log
   honestly instead of silently exiting past the bound. See
   docs/OBSERVABILITY.md "Lifecycle, shutdown, and reinitialization
   contract" for the full writeup, and
   `tests/integration/telemetry-resilience.test.ts` (expanded from 2 to
   7 tests) for the exact scenarios covered, including a controlled
   local `node:http` "black hole" server built specifically to make the
   timeout branch deterministic (a non-routable-IP approach was tried
   first and empirically resolved too fast in this environment to be a
   reliable signal).
3. **Made consumer trace-context isolation explicit.** The consumer
   already produced correctly-isolated spans, but only as an IMPLICIT
   side effect of nothing else being ambiently active -- it used the
   3-argument `startActiveSpan(name, options, fn)` form, which derives
   the new span's parent from `context.active()` at the call site.
   Changed `src/messaging/consumer.ts`'s `handleMessage` to the
   4-argument form, passing `ROOT_CONTEXT` explicitly as the parent, so
   the documented contract (each consumer processing attempt is a new
   root trace, linked to its producer) holds regardless of what else is
   running in the process. Added `telemetry.test.ts` test "2b", which
   deliberately keeps an unrelated parent span active for the entire
   duration of `processBatch` and asserts the resulting consumer span
   has no parent, a distinct traceId from both the unrelated span and
   the producer, and a correct link to the producer -- this would have
   failed under the old 3-argument form if anything else had ever been
   active at the call site.
4. **Strengthened real Collector evidence.** The prior collector test
   matched a marker string in the Collector's human-readable `debug`
   exporter log output. Added a `file` exporter to
   `infra/otel-collector-config.yaml` (writes structured OTLP-JSON lines
   to `infra/otel-evidence/spans.jsonl`, bind-mounted, gitignored) and
   rewrote `tests/integration/telemetry-collector.manual.ts` to: start
   REAL, SEPARATE publisher and consumer OS processes (not in-process
   function calls); discover the Collector's actual container via
   `docker compose ps` rather than an assumed name; read ONLY the bytes
   appended to the evidence file after the test's own start offset
   (never matching a stale span from a previous run); and assert, using
   REAL trace/span IDs, that the publish span shares the command span's
   traceId and has the command span as its parent, that the consumer
   span is a genuinely NEW trace carrying exactly one link back to the
   publish span's traceId/spanId, and that the publish and consumer
   spans share the same application `event_id`. Confirmed working on
   both Node 22 and Node 24 with real chains such as `commandTraceId=
   7f187b0b4e7decd31e05585efc637d58` / `consumeTraceId=
   d381ee41e60c845d0af10e7e84bb20d0` (link -> the publish span)
   (see evidence/phase1-evidence.md for the full, current run's IDs).
5. **Expanded sensitive-data testing.** The prior test checked only span
   attributes/names against one normal-path sentinel. Split into three
   tests, each covering a DIFFERENT explicitly-stated surface:
   `telemetry.test.ts` 9a (normal/success payload -- span names,
   attributes, AND events), 9b (a synthetic credential-bearing error on
   the domain-rejection ERROR path -- attributes AND the span status
   `message`, a surface the prior test never checked), and 9c (a direct
   unit test of `classifyError`/`classifyShutdownError` against
   synthetic credential-bearing messages/URLs). See docs/OBSERVABILITY.md
   "Redaction rules" for exactly which surfaces are and are not covered
   by this -- do not infer universal redaction from a single payload or
   a single surface.
6. **Documentation and evidence** (this update): CLAUDE.md,
   OBSERVABILITY.md, this file, and evidence/phase1-evidence.md updated
   to match every item above. The full check suite (build, typecheck,
   unit, integration, recovery demo, crash-window demo, Collector test)
   was run once on Node 22 (system default) as the final verification
   after all code changes, following the earlier Node 24 validation
   runs -- see evidence/ for raw output and exact counts.

### Explicitly not done in this session (per its own instructions)

Prometheus/Grafana, any Git-worker/React/Terraform/Helm/Kubernetes work,
and any broad refactoring beyond the six items above. "Stop after this
patch. Prometheus/Grafana comes next, after review."

## 2026-10-13 -- Phase 1.2: foundation fixes + OpenTelemetry tracing

Two concrete foundation issues were raised and closed before any new
technology was added, then OpenTelemetry distributed tracing was
implemented as the project's first additional technology.

### Part A.1: test-harness child-process cleanup

A prior session's report of "no lingering processes" was contradicted
minutes later by three leftover Node processes, still alive roughly ten
hours after being started. This session positively identified them
before touching anything: full command line + parent-process-ID chain
(not PID alone -- PIDs are reused) via `Get-CimInstance Win32_Process`,
confirming all three (an `npm run test:integration` shell, the
`node --test` runner it started, and one per-file `node:test` subprocess
for `consumer-dedup.test.ts`) shared the exact same creation timestamp
and parent lineage, traced back to an early-session command that this
assistant's own tool-level `timeout` wrapper had "moved to the
background" without actually killing the process tree. Terminated with
`Stop-Process -Force` and independently confirmed gone via a follow-up
`Get-CimInstance` query returning no results. The root cause (an
orchestration-layer process never being re-awaited or killed) is outside
what this repository's code can fix, but what IS in scope -- making sure
every RelayLab child process a TEST spawns is reliably cleaned up
regardless of how the test ends -- was fixed: a shared
`tests/integration/process-harness.ts` (`ProcessHarness`) replaced four
slightly different ad hoc `ChildProcess[]` arrays across
`crash-windows.ts`, `recovery-scenario.ts`,
`publisher-leadership.test.ts`, and `shutdown.test.ts`. See
docs/decisions/0009-harness-child-cleanup.md. Two new tests
(`shutdown.test.ts`) verify cleanup on both a successful scenario and one
that throws mid-flight, checked via an OS-level `process.kill(pid, 0)`
existence probe, not just Node's own `exitCode` bookkeeping.

A genuine, reproducible bug surfaced while testing this: on this Windows
machine, `child.kill("SIGTERM")` does not deliver a catchable signal at
all (confirmed by a failing test before being understood, not assumed) --
Node forcibly terminates the child regardless of signal name. Fixed by
adding an IPC `"shutdown"` message path to `publisher-main.ts`/
`consumer-main.ts` (inert in real deployment, used only by this harness),
and documented the real `SIGINT`/`SIGTERM` handlers as unverified by an
automated kill test on this machine.

A second, unrelated issue surfaced in the SAME investigation: the
consumer's graceful-shutdown test was consistently ~6.5-6.7s under
`node:test`'s own overhead for process-spawning scenarios -- verified
NOT a true hang by temporarily widening the bound to 20s and observing
consistent completion well under it, then set to a safe 10s (this
matches a pattern seen repeatedly in this project: `node:test` adds
real, reproducible latency/behavior differences vs. running the identical
code as a standalone script).

### Part A.2: publisher-lock / task-ownership SQL

A review flagged that `claimTaskImpl` and
`tryAcquireOrRenewPublisherLeadership`'s `UPDATE ... SET owner_id = ?,
epoch = CASE WHEN owner_id = ? ...` assigns `owner_id` BEFORE the `epoch`
expression reads it. Verified empirically against this project's own
MySQL 8.4 instance (not from memory) that MySQL evaluates a multi-column
`UPDATE`'s `SET` list left to right, and a later expression referencing a
column assigned earlier in the same list sees the NEW value. Traced
through every call path and found this was **correct by accident, not by
construction**: the corrupted `owner_id = ?` comparison became vacuously
true, but the `WHERE` clause's own gating happened to rule out the one
case where that would have produced a wrong answer. Fixed by reordering
both statements so `epoch`'s `CASE` is evaluated FIRST, before `owner_id`/
`lease_expires_at` are reassigned -- see
docs/decisions/0008-set-clause-evaluation-order.md. Added
`tests/integration/publisher-lock-epoch.test.ts`, six real-MySQL
regression tests covering every property the review asked about (expired
leader replaced increments epoch; valid renewal preserves epoch;
reacquisition after expiration always +1, documented as the explicit
rule; competing owner blocked while unexpired; `>=`/`<` boundary
consistency; new owner never inherits the old epoch value). Existing
`ownership.test.ts`/`retry-authorization.test.ts` continued to pass
unchanged (their lease-timing margins were separately widened for
robustness after an unrelated flake surfaced -- see below).

### Part B-D: OpenTelemetry distributed tracing

Implemented explicit, hand-written spans (no auto-instrumentation) across
command processing, the durable outbox, the publisher, NATS delivery, and
the consumer. Full design rationale in
[docs/OBSERVABILITY.md](OBSERVABILITY.md). Highlights:

- `src/telemetry/` -- `init.ts` (NodeSDK start/shutdown, one `getTracer()`,
  clean `RELAYLAB_OTEL_DISABLED=1` disabled mode), `propagation.ts` (W3C
  carrier capture/restore, always rooted at `ROOT_CONTEXT`),
  `attributes.ts` (a fixed, typed attribute allowlist -- the actual
  redaction mechanism), `with-span.ts` (shared span-status policy:
  expected domain rejections never mark a span ERROR; infrastructure
  failures do), `log.ts` (trace-ID suffix for a few existing logs).
- Additive migration `0003_otel_trace_carrier.sql` adds
  `outbox_events.trace_carrier` -- 0001/0002 untouched, checksums
  unchanged.
- Parent/child (command → outbox publish, same trace) vs. link (outbox
  publish → consumer processing, new trace) was a deliberate, documented
  choice, not an oversight -- see OBSERVABILITY.md for why each was
  chosen.
- A local OTel Collector (`otel/opentelemetry-collector-contrib:0.116.1`,
  pinned) runs via an optional `docker compose --profile observability`
  service, never started by default.

#### Bugs found and fixed while building this (real, not hypothetical)

1. **The NATS header carrier pointed at the wrong span.** The consumer's
   link was found (via a failing test) to reference the ORIGINAL
   command span, not the publish-attempt span that actually put the
   message on the wire -- because `publisher.ts` injected the row's
   original carrier into NATS headers instead of capturing a fresh one
   after creating its own span. Fixed by calling `captureCurrentCarrier()`
   again inside the publish span, after it is created.
2. **Telemetry's own default shutdown timeout (5s) silently broke an
   existing test's bound.** Wiring `shutdownTelemetry()` into
   `consumer-main.ts`'s graceful-shutdown path caused
   `shutdown.test.ts`'s consumer test to time out entirely, because the
   SDK tried (and failed) to flush to an unreachable Collector for the
   full 5s. Fixed by making telemetry disabled-by-default for every test
   that is not ABOUT telemetry (`process-harness.ts`'s `testChildEnv`
   sets `RELAYLAB_OTEL_DISABLED=1`), rather than quietly shortening the
   production timeout to paper over a test environment issue.
3. **Two `@opentelemetry/api` global-registration footguns in the test
   setup itself** (not application code): `trace.setGlobalTracerProvider`
   and `propagation.setGlobalPropagator` are each ONE-TIME, process-wide
   registrations -- a second call is silently ignored (verified
   empirically). The original `telemetry.test.ts` created a fresh
   provider/exporter in `beforeEach`, so only the FIRST test in the file
   ever actually received spans; every later test silently shared that
   first exporter instance. Fixed by registering once in `before()` and
   calling `exporter.reset()` between tests. Relatedly, without ALSO
   registering a context manager (`AsyncLocalStorageContextManager`),
   `context.active()`/`context.with()` are no-ops across `await`
   boundaries by default -- production gets one automatically from
   `NodeSDK`; the bare `BasicTracerProvider` used for testing does not,
   and this was the actual cause of the first "carrier never persisted"
   test failures, not a bug in `captureCurrentCarrier` itself.
4. **A flaky lease-timing margin surfaced incidentally**, unrelated to
   telemetry: `retry-authorization.test.ts` and `ownership.test.ts` used
   a 50ms lease with only a 50-80ms wait before asserting expiry, which
   intermittently failed once overall test-suite load increased. Widened
   to a 150ms lease / 220ms wait, matching the margin already used
   elsewhere (`publisher-lock-epoch.test.ts`).

### Explicitly not done in this session

React, Prometheus/Grafana, Terraform, Helm, Kubernetes, and any Phase 2
worker implementation -- all explicitly out of scope per this session's
own instructions. Metrics and logs pipelines (tracing only). A tracing
UI/backend beyond the Collector's own debug-exporter log output.

## 2026-10-12 (later session) -- Phase 1.1 audit

An independent review did not approve Phase 1 as originally reported and
asked for an audit closing specific correctness gaps, with actual source
excerpts and raw evidence, not summaries. This session re-read the entire
Phase 1 codebase, investigated each suspected gap, found that some were
real and some were not, fixed the real ones, and added tests that exercise
the real mechanism (not a weaker proxy for it). See
evidence/phase1-evidence.md for exact commands/counts and
docs/ARCHITECTURE.md for the resulting design.

### What was investigated, and what was actually found

1. **Command retry semantics (section 2).** The suspected bug --
   "incorrectly rejects a retry of a previously committed command after
   its lease expires" -- did NOT exist: the idempotency fast path already
   ran before the fencing check. What WAS real: that fast path had NO
   authorization check at all, so any caller who knew
   `(task_id, command_id)` and the payload could retrieve another
   principal's receipt. Fixed by storing `owner_id`/`committed_epoch` on
   the receipt and checking identity before fingerprint, decoupled from
   epoch/lease fencing (docs/decisions/0004-receipt-authorization.md,
   with an explicit trusted-caller-boundary statement since Phase 1 has
   no real authentication). Also found and fixed: `claimTask` was NOT
   safely retryable by the RIGHTFUL current owner (a retried claim while
   still validly held threw a spurious `ClaimLost`) --
   docs/decisions/0007-claim-idempotency.md. Both covered by
   `tests/integration/retry-authorization.test.ts`, including the exact
   8-step regression scenario requested.
2. **Consumer dedup independent of broker dedup (section 3).** Added a
   test publishing the SAME application `event_id` via two DIFFERENT NATS
   `msgID`s (so broker-level dedup cannot be the thing doing the work) and
   proved exactly one durable receipt and one durable effect. Also added
   `content_hash` tracking so an `event_id` reused with DIFFERENT content
   is detected as a distinct "conflict" outcome (recorded in
   `consumer_event_conflicts`), not silently treated as an ordinary
   duplicate (docs/decisions/0005-event-identity-conflict.md).
3. **Publisher concurrency (section 4).** The single-publisher assumption
   was previously just a documented limitation. Implemented and enforced
   a DB-backed leader lock (`publisher_leader` table,
   `publisher-lock.ts`), reusing the task-ownership epoch+lease mechanism.
   Its exact scope and limits (does not fence an in-flight NATS call) are
   stated plainly in docs/decisions/0006-publisher-leader-lock.md.
   `tests/integration/publisher-leadership.test.ts` starts two REAL
   publisher processes and proves at most one is ever leader, and that
   killing the leader lets the survivor finish publishing in order.
4. **Event-gap behavior (section 5).** The prior policy applied
   out-of-order events immediately while only flagging the gap -- exactly
   the "silently advance a complete projection past a missing event"
   failure mode the audit asked to rule out. Replaced with durable
   pending-event buffering and cascade application on gap closure, bounded
   per task (default cap 50), never an in-memory buffer (revised
   docs/decisions/0003-event-ordering.md).
   `tests/integration/gap-handling.test.ts` covers 1-3-2 ordering,
   duplicates around the gap, durability across a simulated restart, and
   the bounded-cap case.
5. **Real crash-window scenarios (section 6).** Added
   `tests/integration/crash-windows.ts`, three scenarios each using a REAL
   killed child process at an EXACT point confirmed by an IPC readiness
   handshake (not a sleep): publisher ack-received-before-mark-sent,
   consumer transaction-open-before-commit, consumer
   committed-before-ack. Each retains the application event_id, the
   relevant rows before/after, the killed process's pid and exit outcome,
   and the recovered final state as evidence (see
   evidence/crash-windows-output.txt). The actual termination mechanism
   on this Windows machine is stated precisely in both the script's
   header comment and evidence/phase1-evidence.md, rather than assumed
   from the Node API's signal-name argument.
6. **Resource lifecycle and shutdown (section 7).** Both
   publisher-main.ts and consumer-main.ts now track in-flight work and
   wait for it before releasing the DB pool / NATS connection on
   shutdown, and use bounded exponential backoff on sustained failure
   instead of hot-looping. Discovered along the way (a real, useful
   finding, not a nuisance): on this Windows machine,
   `child_process.kill("SIGTERM")` does not deliver a catchable signal at
   all -- confirmed by a FAILING test before it was understood, not
   assumed. Added an IPC-triggered shutdown path specifically so the
   graceful-shutdown code could still be exercised by an automated test
   here, documented inline as a Windows test accommodation, not a
   production mechanism (the real `SIGINT`/`SIGTERM` handlers remain for
   POSIX hosts, unverified by an automated kill test on this machine).
   `tests/integration/shutdown.test.ts` covers both processes plus
   bounded backoff under sustained DB failure. One of its own assertions
   was itself flaky on first use -- it checked that no "acquired
   leadership" log line appeared textually AFTER the "shutdown requested"
   line, but an iteration already in flight when shutdown is requested is
   allowed to finish (by design) and can legitimately log its own state
   transition after that point with no second iteration ever starting.
   Removed that assertion and documented why in the test file rather than
   loosening it silently.
7. **Migration and evidence hygiene (section 8).** Replaced the single
   `infra/schema.sql` with an ordered, checksummed migration ledger
   (`infra/migrations/*.sql`, `schema_migrations` table, a safety guard
   refusing to run against a non-loopback/non-`relaylab`-named database).
   Integration tests now run against a disposable `relaylab_test` database
   and `relaylab-test` NATS namespace (`infra/mysql-init/`,
   `loadTestMysqlConfig`), never the dev database/namespace. All docs
   below were rewritten to match the current file set, and the phrase
   "exactly-once completion" from the original Phase 1 report is retired
   in favor of the precisely-scoped claims in evidence/phase1-evidence.md.

### A real, non-obvious bug this session's own testing caught (not a review-prompted one)

While writing the section 3 test, `processBatch`'s default parameter was
a module-level constant (`DEFAULT_CONSUMER_OPTIONS = defaultConsumerOptions()`)
evaluated once at `consumer.ts`'s import time. Because ES module static
imports resolve in the order written, and the test files imported
`consumer.js` before `helpers.js` (which sets `RELAYLAB_NAMESPACE` for the
test run), any call to `processBatch(pool, js)` without explicit options
silently used the DEV namespace's consumer name while fetching from the
TEST namespace's stream -- producing a confusing "503 no responders" with
no obvious connection to the real cause. Diagnosed by progressively
narrowing a reproduction (raw `js.fetch` worked, `processBatch` did not,
difference traced to the cached default) rather than guessing. Fixed by
making the default a function-call expression (`options: ConsumerOptions =
defaultConsumerOptions()`), evaluated fresh on every call, and removing
the unsafe exported constant entirely.

## 2026-10-12 -- Phase 1 implemented from scratch

**Note (added during the Phase 1.1 audit above): several specifics in
this section are superseded by Phase 1.1 and are kept only as a historical
record of what Phase 1 looked like at the time. In particular,
`infra/schema.sql` no longer exists (replaced by `infra/migrations/`), the
test count below (12) is now 26, and `config.ts`'s `EVENTS_DURABLE`
constant mentioned in bug #4 below was itself later replaced by the
`eventsDurable()` function for the same kind of reason. For the current
state of each, see docs/ARCHITECTURE.md and evidence/phase1-evidence.md.**

No prior RelayLab code existed in this workspace (searched for
`domain.ts`, `*relaylab*` across the whole machine -- nothing found), so
this session built Phase 1 from a clean slate rather than resuming a
prototype. The spec that commissioned this work described a prior
prototype with "13 passing local tests, one skipped MySQL/NATS test" --
that prototype was not present here and no claim about it is repeated or
relied on anywhere in this repo.

### What exists and is verified working

- `src/domain/` -- pure transition rules (`domain.ts`), typed errors
  (`errors.ts`), deterministic fingerprinting (`fingerprint.ts`). 7 unit
  tests, 0 infra required.
- `src/protocol/protocol.ts` -- versioned, runtime-validated
  command/event envelopes (Effect Schema).
- `src/db/` -- connection pool (`pool.ts`), schema migration runner
  (`migrate.ts`), and `TaskStore` (`task-store.ts`): the single
  transactional boundary for ownership/lease/command/receipt/outbox.
- `src/messaging/` -- outbox publisher (`publisher.ts` +
  `publisher-main.ts`) and durable NATS JetStream consumer
  (`consumer.ts` + `consumer-main.ts`), runnable as two separate
  processes.
- `infra/schema.sql` + `infra/compose.yaml` -- real local MySQL 8.4 +
  NATS JetStream, applied and run in this session.
- 12 integration tests against those real services, all passing (see
  evidence/phase1-evidence.md). 7 unit tests, all passing.
- One real-process-kill recovery demo (`tests/integration/recovery-scenario.ts`,
  `npm run demo:recovery`): spawns a real consumer process, SIGKILLs it
  after confirmed partial progress, spawns a second consumer process,
  verifies exactly-once recovery.

### Bugs found and fixed during this session (not hypothetical -- these
### were real failures caught by the integration tests)

1. **Clock-skew fencing bug**: `applyTaskCommandImpl`'s lease-expiry check
   originally compared the database's `lease_expires_at` against Node's
   `Date.now()`. This is exactly the hazard invariant B warns against
   ("use an authoritative clock for database-backed lease decisions") and
   it was caught immediately by the idempotency integration tests
   failing with a spurious `ClaimLost`. Fixed by selecting `NOW(3)`
   alongside the row in the same query and comparing two DB-clock
   timestamps. `renewLease` had the same redundant, skewed pre-check;
   removed it since the atomic `UPDATE ... WHERE lease_expires_at >=
   NOW(3)` already enforces this correctly.
2. **Test-isolation race**: `node --test` runs separate test files
   concurrently by default. Two integration test files' `beforeEach`
   hooks both called a global `TRUNCATE TABLE tasks` (etc.) against the
   same shared local MySQL instance, so one file's truncate wiped rows
   another file's in-flight test had just written. Fixed with
   `--test-concurrency=1` in the `test:integration` script.
3. **Broker-level dedup silently defeated the consumer-redelivery test's
   premise**: the original test simulated "publisher crashes and
   republishes the same event" by republishing with the identical
   `Nats-Msg-Id`. NATS JetStream's own duplicate-message window absorbed
   that at the broker, so the message was never actually redelivered to
   the consumer -- the test was accidentally proving broker dedup, not
   consumer dedup (invariant E explicitly distinguishes these: "Broker
   deduplication is an optimization with a bounded window, not the sole
   correctness mechanism"). Split into two honest tests: one confirming
   the broker-dedup behavior for publisher-side republish, and a new one
   that forces genuine redelivery with `msg.nak()` and proves the
   consumer's own `consumer_processed` table (not the broker) is what
   prevents the duplicate local effect.
4. **Dead config constant silently diverged from the real consumer
   identity**: `config.ts` exported a `CONSUMER_NAME` that nothing used;
   the actual running consumer's identity was `EVENTS_DURABLE` via
   `DEFAULT_CONSUMER_OPTIONS`. The recovery demo queried under the wrong
   name and always saw zero progress. Removed the dead constant and
   consolidated on `EVENTS_DURABLE` as the single source of truth for a
   consumer process's identity (both the NATS durable name and the
   `consumer_name` partition key).

### Explicitly not done in this session

- Workers, Git worktrees, tool execution, model integration, GitHub PRs,
  React/Electron interfaces, Kubernetes -- all out of scope for Phase 1
  by the commissioning spec (section 11, "scope discipline").
- A real migration ledger (see docs/ROADMAP.md known gaps).
- Any load/throughput measurement.

### Deviations from the commissioning spec, and why

- **Node 24 requested, Node 22.21.0 used.** 22 is what's actually
  installed on this machine; nothing in Phase 1's code requires a
  Node-24-only API. Upgrading the Node install itself was out of scope
  for this session (would require system-level changes not requested).
- **`nats` npm package (not `@nats-io/transport-node`).** The installed
  `nats@2.x` package prints a deprecation notice pointing at the new
  package, but is still the documented, widely-used JetStream client.
  Given the scope of Phase 1, switching to an unfamiliar package's API
  surface under time pressure was judged riskier than using the
  well-documented, currently-functional one. Revisit in a later phase.
