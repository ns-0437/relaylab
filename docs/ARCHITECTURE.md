# RelayLab Architecture (Phase 1 + Phase 1.1 audit)

## Logical boundaries

```
src/
  domain/        pure transition rules, no I/O (domain.ts, errors.ts, fingerprint.ts)
  protocol/      runtime-validated envelopes for command/event boundaries (protocol.ts)
  db/            MySQL persistence: pool, migration ledger, TaskStore (pool.ts, migrate.ts, migrate-cli.ts, task-store.ts)
  messaging/     outbox publisher, publisher leader lock, durable consumer, NATS client
                 (publisher.ts, publisher-lock.ts, consumer.ts, nats-client.ts, *-main.ts)
  testing/       opt-in, disabled-by-default crash-window failpoints (failpoint.ts)
  config.ts      env reading, NATS namespacing, no secret logging
tests/
  unit/          pure domain + fingerprint tests, no infrastructure required
  integration/   real MySQL + NATS tests against a DISPOSABLE relaylab_test
                 database and relaylab-test NATS namespace (helpers.ts fails
                 loudly if unreachable, never falls back to mocks)
infra/
  migrations/    ordered, checksummed *.sql files (0001_init.sql, 0002_phase1_1_audit.sql)
  mysql-init/    docker-entrypoint-initdb.d script creating the disposable test database
  compose.yaml   local MySQL 8.4 + NATS JetStream
```

There is no `workers/` or `protocol`-as-package split yet beyond the single
`protocol.ts` file -- Phase 1 only needs command/event envelopes, not a
worker runtime. `interfaces/` (React/Electron) does not exist; it is Phase 4.

## Why a single TaskStore service, not a repository-per-table split

Every Phase-1 correctness invariant (A-J) is about *one transaction
spanning task state + command receipt + outbox event*. Splitting that into
`TaskRepository`, `ReceiptRepository`, `OutboxRepository` objects would
either (a) force the atomicity back together via a shared-connection
parameter threaded through all three, which is the same coupling with more
ceremony, or (b) tempt someone into calling them independently and losing
atomicity. `TaskStore.applyTaskCommand` is one method because it is one
transaction; that is deliberate, not an oversight.

## Ownership and lease fencing

`tasks.epoch` increases by exactly 1 on a genuine takeover (claiming while
unclaimed, or after the current lease expires). It does NOT increment when
the CURRENT rightful owner retries its own still-valid claim -- see
docs/decisions/0007-claim-idempotency.md; this was a real bug found and
fixed during the Phase 1.1 audit. Every command carries the epoch the
caller believes is current; `applyTaskCommand` re-reads the authoritative
row (`SELECT ... FOR UPDATE`, alongside the database's own `NOW(3)`) and
rejects with `EpochStale` if it does not match -- but ONLY for a
genuinely new command. A REPLAY of an already-committed command (same
`task_id`+`command_id`+payload) bypasses fencing entirely and returns the
original result regardless of current epoch, gated instead by a separate
identity check against who originally committed it -- see
docs/decisions/0004-receipt-authorization.md. See also
[decisions/0001-ownership-fencing.md](decisions/0001-ownership-fencing.md).

## Idempotency / deduplication identity

- **Command dedup** (producer side, invariant C): `(task_id, command_id)` is
  the unique key. `fingerprint(payload)` (stable-stringified SHA-256) detects
  "same ID, different payload" so retries are safe but ID reuse is not.
  Replay is additionally gated by an `owner_id` match against who
  originally committed the receipt (0004) -- this is distinct from, and
  checked before, epoch/lease fencing.
- **Event dedup** (consumer side, invariant E/F): `event_id` (a UUID minted
  when the outbox row is written) is the unique key in `consumer_processed`,
  scoped per `consumer_name` so multiple independent consumers could each
  track their own durable progress. A `content_hash` stored alongside it
  detects `event_id` REUSE with DIFFERENT content as a distinct "conflict"
  outcome, recorded in `consumer_event_conflicts` -- never silently treated
  as an ordinary duplicate. See 0005.

See [decisions/0002-idempotency-keys.md](decisions/0002-idempotency-keys.md).

## Event ordering: durable pending events with cascade application

Each outbox row carries a per-task `seq`. The consumer tracks
`last_applied_seq` per `(consumer_name, task_id)` -- the point up to which
the projection is CONTIGUOUSLY complete. An event that arrives exactly at
`last_applied_seq + 1` is applied immediately, and then any CONTIGUOUS
pending rows are drained and applied too (cascade), all in the same
transaction. An event that arrives early (`seq > last_applied_seq + 1`) is
durably buffered in `task_consumer_pending` -- NOT applied, and
`gap_detected = 1` is set so the projection visibly shows itself as
incomplete, rather than silently advancing past the missing event. This
buffering is bounded per task (`maxPendingPerTask`, default 50): past that
cap, further early events are acked (so they don't jam redelivery) but
logged loudly and left unapplied, rather than growing the pending table
without limit. This replaced an earlier Phase-1 design that applied
out-of-order events immediately while merely flagging the gap -- a real
correctness gap closed during the Phase 1.1 audit. See
docs/decisions/0003-event-ordering.md (revised) for the full rationale and
rejected alternatives.

## Publisher concurrency: a leader lock, not an unenforced assumption

Exactly one running publisher process is enforced (not merely documented)
via a single-row epoch+lease lock in `publisher_leader`
(`publisher-lock.ts`), claimed with the identical mechanism as task
ownership. A process that is not the current leader skips publishing that
iteration rather than racing another leader. The lock does NOT fence an
in-flight NATS call that was already underway when leadership changed
hands -- see docs/decisions/0006-publisher-leader-lock.md for exactly what
it does and does not guarantee, and why that gap is acceptable given the
consumer's own duplicate/reorder-tolerant design (sections 3 and 5 of the
Phase 1.1 audit).

## Publisher and consumer crash behavior

- **Publisher**: marks a row `published_at = NOW()` only after the broker
  publish call resolves. A crash between broker-ack and mark-sent causes a
  duplicate publish on restart (same `event_id`/`msgID`); this is accepted
  because the consumer's dedup is the actual correctness mechanism, not the
  publisher's exactly-once intent. Verified with a real killed child
  process at this exact point -- see `tests/integration/crash-windows.ts`
  scenario A.
- **Consumer**: never holds a DB transaction open across the network ACK.
  The sequence is dedup-lookup + local-effect in one transaction, COMMIT,
  then `msg.ack()`. A crash before commit is invisible to the dedup table
  (as if never attempted) and the message simply redelivers after
  `ack_wait`. A crash after commit but before ack also redelivers, but the
  content-hash lookup now finds the existing record and returns
  `"deduped"`, so the local effect is not re-applied. Both windows are
  verified with real killed child processes, each paused at the exact
  point via an IPC failpoint handshake (`src/testing/failpoint.ts`) rather
  than a sleep -- see crash-windows.ts scenarios B and C.
- **Graceful shutdown**: both processes track the current in-flight
  iteration and, on a shutdown request, wait for it to finish before
  draining NATS and ending the MySQL pool, then exit. On this Windows dev
  machine, `child.kill("SIGTERM")` does not deliver a catchable signal at
  all (Node forcibly terminates the process regardless of signal name);
  the real `SIGINT`/`SIGTERM` handlers remain the production mechanism for
  POSIX hosts, and are additionally reachable via an IPC message so the
  graceful-shutdown code path is still exercised by this project's own
  test suite here. See `tests/integration/shutdown.test.ts`.

## Effect usage

`Effect` is used for:
- Typed failures (`TaskStore`'s methods return `Effect<A, SpecificError>`,
  not `Promise<A>` with throw-anything).
- Service composition (`TaskStore` as a `Context.Tag`, `Layer.succeed` to
  provide the live MySQL-backed implementation; tests construct it directly
  with `makeTaskStoreLive(pool)` without going through a `Layer` because
  there is nothing else to compose with yet in Phase 1).

It is deliberately NOT used inside `publisher.ts` / `consumer.ts` / their
`-main.ts` entrypoints, which are plain async loops (now with explicit
in-flight-work tracking and bounded backoff, added during the Phase 1.1
audit -- see "Publisher and consumer crash behavior" above). Wrapping a
sequential poll-sleep-repeat loop in Effect's `Schedule`/fiber machinery
would be a decorative abstraction at this stage; Phase 2's worker runtime
(bounded concurrency, structured cancellation across a real
tool-execution boundary) is where Effect's resource/fiber model actually
earns its keep, and that is where it will be introduced for the workers
layer.

## Migration ledger

`infra/migrations/*.sql` are applied in filename order, exactly once each,
tracked in a `schema_migrations(id, checksum, applied_at)` table
(`src/db/migrate.ts` → `applyMigrations`, invoked by `src/db/migrate-cli.ts`
for `npm run migrate` and directly by `tests/integration/helpers.ts` for
the disposable test database -- the SAME code path, not a parallel ad hoc
schema loader). Re-running is safe (already-applied migrations are
skipped after a checksum match); editing an already-applied migration file
is refused loudly rather than silently re-applied or silently ignored. A
safety guard (`assertSafeTarget`) refuses to run against a
non-loopback-host or non-`relaylab`-prefixed database name unless
`RELAYLAB_ALLOW_UNSAFE_MIGRATE=1` is explicitly set. See the file-level
comment in `migrate.ts` for what this does and does not guarantee (MySQL
DDL is not transactional, so a multi-statement migration file that fails
partway CAN leave partial state -- the runner detects that case and
refuses to mark the migration applied, rather than hiding it).

## OpenTelemetry tracing (Phase 1.2)

Full detail in [docs/OBSERVABILITY.md](OBSERVABILITY.md). Summary of the
module boundary: `src/telemetry/` is self-contained -- `init.ts`
(start/shutdown, the one `getTracer()`), `propagation.ts` (the W3C
carrier captured with the outbox row and restored from NATS headers --
rooted at `ROOT_CONTEXT`, never `context.active()`, so concurrent tasks
cannot cross-contaminate), `attributes.ts` (the fixed, typed attribute
allowlist -- the actual redaction mechanism), `with-span.ts` (the shared
span-status policy for ownership/command operations), `log.ts` (a trace-ID
suffix for a few existing log lines, not a new logging system). No other
module reaches into OTel internals directly; `task-store.ts`,
`publisher.ts`, `publisher-lock.ts`, and `consumer.ts` only ever go
through these five files. The additive migration
`0003_otel_trace_carrier.sql` is the only schema change this introduced.

## Shared test/demo process harness (Phase 1.2)

`tests/integration/process-harness.ts`'s `ProcessHarness` is used by
every test/demo that spawns a real `publisher-main.ts`/`consumer-main.ts`
child process (`crash-windows.ts`, `recovery-scenario.ts`,
`publisher-leadership.test.ts`, `shutdown.test.ts`). It registers a child
the instant `spawn()` is called (covering a child that crashes before
reporting readiness), attempts a graceful IPC shutdown first, bounded-waits,
force-kills only if still alive, and always awaits the real `exit` event --
see docs/decisions/0009-harness-child-cleanup.md for why this replaced four
slightly different ad hoc `ChildProcess[]` arrays.

## Phase 2A worker: one real Git execution worker

A SCRIPTED (no LLM, no task-supplied arbitrary command) worker that
claims a durable task, creates an isolated Git worktree from a specified
commit, applies ONE fixed deterministic repair, runs a fixed allowlisted
verification command, commits on success, and publishes a durable
checkpoint manifest through the existing task/receipt/outbox
infrastructure (see `docs/METRICS.md`-style "not a redesign" discipline
-- this reuses task authority, epochs, receipts, outbox, and tracing
unchanged, adds exactly ONE table). Source: `src/worker/*.ts`,
`src/worker-main.ts`.

### Worker lifecycle

A **one-shot CLI** (`src/worker-main.ts`), not a polling loop like
`publisher-main.ts`/`consumer-main.ts` -- this milestone has no task
QUEUE for a worker to poll; a single task and starting commit are
specified explicitly (env vars for the CLI, direct call args for
tests/demo). `runOnce()`: claims the task (`TaskStore.claimTask`, the
SAME ownership/epoch/lease mechanism every other part of this project
uses) -> `runGitWorkerTask` (`src/worker/run-task.ts`) resolves the
starting commit, creates/reuses a worktree, applies the repair, verifies,
commits, and publishes a checkpoint -> process exits (code 0 on a
published checkpoint, 1 otherwise). A continuously-running, task-
discovering worker process is explicitly Phase 2's LATER scope
(docs/ROADMAP.md), not this milestone's.

### Task, attempt, ownership epoch, and worktree -- the relationship

- **Task**: the SAME `tasks` row and epoch/lease fencing every other
  command in this project uses -- a Git checkpoint is published via
  `TaskStore.recordGitCheckpoint`, which re-checks epoch/owner at the
  DB boundary exactly like `applyTaskCommand` does (see "Ownership and
  lease fencing" above).
- **Attempt**: this milestone uses a SINGLE, DETERMINISTIC `attemptId`
  per task (`defaultAttemptId`, currently always `"attempt-1"`) --
  deliberately NOT a fresh random ID per process restart. This is a
  SCOPE CHOICE, not an oversight: a deterministic attempt identity is
  what makes restart reconciliation a simple, local filesystem check
  (does THIS exact worktree path already have work on it?) rather than a
  distributed search across possible prior attempts. A real multi-
  attempt/handoff model (a DIFFERENT worker instance picking up after the
  first is presumed dead) is Phase 2's later, NOT-yet-implemented scope.
- **Worktree**: `resolveWorktreePath(managedRoot, taskId, attemptId)` --
  one worktree directory per (task, attempt), inside a STABLE, shared
  managed workspace root (see "Workspace root persistence" below). The
  epoch is NOT part of the worktree path -- the worktree is attempt-
  scoped, the epoch is checked only at the final DB-publish boundary.

### How the worker receives/selects work

Explicitly, not via discovery: a task ID, a source repository path, and
a starting commit ref are all supplied by the caller (CLI env vars, or
direct function arguments from a test/demo). There is no queue, no
polling, no "pick the next available task" logic in this milestone.

### Which operations are idempotent

- `TaskStore.claimTask`: idempotent for the RIGHTFUL current owner
  (unchanged project-wide behavior, docs/decisions/0007).
- `TaskStore.recordGitCheckpoint`: idempotent via the SAME
  `command_receipts` ledger `applyTaskCommand` uses -- a retried publish
  with the same `checkpointCommandId(taskId)` returns `replay: true`,
  never a duplicate row (verified: `tests/integration/git-worker-crash.ts`
  "checkpoint-retry-no-duplicate").
- `applyScriptedRepair` (`src/worker/repair.ts`): idempotent by
  construction -- re-running it against an already-repaired file reads
  current content, sees the defect is already gone, makes NO write.
- `ensureWorktreeAndCommit` (`src/worker/run-task.ts`): idempotent as a
  WHOLE -- reuses an existing worktree, re-applies the (idempotent)
  repair, and reuses an existing clean-worktree commit instead of
  creating a new one. NOT a single atomic operation -- it is a sequence
  of individually-idempotent steps, which is what makes it safe to
  re-enter after a crash at ANY point within it (see "restart
  reconciliation" below).
- `git worktree add` itself is NOT idempotent (fails if the worktree
  already exists) -- `ensureWorktreeAndCommit` checks for existence
  FIRST and only calls it when the worktree does not yet exist.

### What happens if the worker fails between Git commit and database checkpoint persistence

Exactly the scenario `tests/integration/git-worker-crash.ts`'s
"commit-created-checkpoint-not-persisted" test exercises with a real
`SIGKILL`, confirmed via an IPC failpoint handshake
(`RELAYLAB_FAILPOINT=worker-after-verify-before-checkpoint`): the local
Git commit and the verification result both already exist on disk/in the
worktree; NO row exists yet in `task_checkpoints`. A restarted worker
(same persistent owner identity, same task) finds the SAME worktree
(stable workspace root), sees it is clean with HEAD already past the
base commit, REUSES that commit as `resultCommit` (creates NO new
commit), re-runs verification (cheap, deterministic, and the actual
proof the checkpoint needs), and publishes -- exactly once, confirmed by
both a real process kill and a direct count of `task_checkpoints`/
`outbox_events` rows afterward.

### Phase 2A.1: committed-state reconciliation hardening

Phase 2A's happy path was accepted; a follow-up review found that a
CLEAN worktree alone does not prove a completed repair, nor did the
original path-boundary check resolve real filesystem identity. This
patch (`src/worker/run-task.ts`, `src/worker/workspace.ts`,
`src/worker/worktree-identity.ts`, `src/worker/repair.ts`,
`src/worker/git.ts`) closes those specific gaps without adding a second
worker, new infrastructure, or a generic agent-reconciliation framework:

- **Named worktree identity** (`src/worker/worktree-identity.ts`): a
  sidecar file next to each worktree (`<worktreeDir>.identity.json`,
  never inside the worktree itself, so `git add -A` never touches it)
  records `{taskId, attemptId, sourceRepoRealPath, baseCommit, branch,
  createdAtIso}` at creation. Reusing an existing worktree directory now
  requires this recorded identity to match the CURRENT call's expected
  identity on every field, or the call is rejected with a named error --
  a worktree belonging to a different repo/task/attempt/base can no
  longer be silently adopted (verified:
  `tests/integration/git-worker-crash.ts` `identity-mismatch-rejected`).
- **Explicit workspace states** (`ensureWorktreeAndCommit` in
  `src/worker/run-task.ts`): fresh checkout; reused-and-identity-
  verified; repair applied but not committed; repair committed (clean,
  HEAD past base, repair positively confirmed present via
  `isRepairPresent`, AND HEAD proven a real descendant of base via
  `isAncestor`, AND an exact 1-commit delta via `commitCountBetween`);
  and an explicit rejection state covering both "clean at the
  unmodified buggy base" and "HEAD moved to an unrelated commit that
  still lacks the repair" -- extracted into its own function
  (`assertCleanWorktreeIsValidResult`) specifically so it can be tested
  directly against a deliberately inconsistent worktree, since the real
  (non-test) call path's own repair step is unconditional and
  self-healing and can never organically reach that state (documented
  on the function itself -- an honest statement of what is and is not
  reachable through the public API, not a claim of a live exploit).
- **Verification bound to the checkpointed tree** (`runGitWorkerTask`):
  the result tree is now pinned via `currentTree()` BEFORE verification
  runs, not re-read after. After verification passes, the worktree is
  re-checked (clean, same tree, same commit) against that pinned value;
  any drift throws explicitly rather than publishing a manifest
  describing a tree that was never actually what was verified. This
  project's own `verify.js` fixture is read-only, so this is not
  expected to ever trip in practice -- stated as a closed gap, not as
  "proven to be impossible for any future verification command."
- **Richer, real-value failpoints** (`src/testing/failpoint.ts`'s
  `hitFailpoint` now accepts an optional `extra` payload): the
  `worker-after-commit-before-verify` and
  `worker-after-verify-before-checkpoint` failpoints now report the
  ACTUAL result commit SHA and tree ID over IPC at the moment they fire,
  plus two NEW, earlier failpoints
  (`worker-after-worktree-before-repair`,
  `worker-after-repair-before-commit`) covering the two earlier crash
  windows. `tests/integration/git-worker-crash.ts` independently
  re-derives the same values via its OWN direct Git calls against the
  worktree and asserts exact equality against the IPC-reported values,
  and again, exactly, after a real SIGKILL and restart -- never
  "a checkpoint row exists" alone, and never an argument from code-path
  structure in place of a captured value.
- **Deletion held to a stricter standard than inspection**
  (`assertSafeForDeletion`, `src/worker/workspace.ts`): a cleanup
  operation must target EXACTLY a `(taskId, attemptId)` worktree path
  (never the managed root itself, never a shallower/deeper path), and
  additionally passes through the symlink/junction check below --
  wired into `cleanupWorktree` in `src/worker/run-task.ts`.
- **Real filesystem boundary checks, not lexical-only**
  (`assertNoSymlinkEscapes`, `src/worker/workspace.ts`): walks every
  path segment that exists on disk between the managed root and a
  candidate path, rejecting any symlink/junction/reparse point
  (`lstat().isSymbolicLink()`), plus a real-to-real `realpath`
  containment check (comparing the candidate's resolved real path
  against the ROOT's own real path -- not against the candidate's own
  un-realpath'd lexical form, which on this Windows host would falsely
  flag ordinary 8.3-short-name/long-name path aliasing, e.g.
  `NAVINK~1` vs. `Navin Kumar`, as an escape; a real bug this patch's
  own test run caught and fixed before landing). Verified on this host
  with a REAL NTFS junction (`fs.symlink(..., "junction")` -- junctions
  do not require elevated privileges, unlike Windows symlinks, which is
  why this is the reparse-point type actually exercised here) placed
  inside the managed root and pointing outside it
  (`tests/integration/git-worker.test.ts` test 11). **Untested on this
  host**: a true Windows symlink (`SYMLINK` reparse tag, as opposed to a
  `MOUNT_POINT`/junction reparse tag) and any POSIX symlink behavior --
  the `lstat().isSymbolicLink()` check is expected to behave the same
  way per Node's own documented, platform-uniform `fs.Stats` semantics,
  but this has not been exercised against either on this machine.
- **Trusted-local-execution assumptions, stated explicitly**: this
  patch does NOT claim atomic fencing of arbitrary filesystem writes --
  nothing prevents a process OTHER than this worker from mutating the
  worktree between this worker's own steps (the identity sidecar and
  ancestry/commit-count checks catch an INCONSISTENT resulting state,
  they do not prevent one from being created by something else). There
  is still no sandboxing and no defense against a hostile repository --
  see "Supported input/path boundaries" below, unchanged from Phase 2A
  on that point.

### Phase 2A.2: workspace classification before mutation, and a validated checkpoint-persistence boundary

Phase 2A.1 proved a CLEAN result worktree's HEAD genuinely contained the
repair. It left one gap open: `ensureWorktreeAndCommit` still called
`applyScriptedRepair` unconditionally, and committed with `git add -A`,
BEFORE ever inspecting whether a REUSED worktree's actual dirty/staged/
untracked state was consistent with that one supported repair. This
patch (`src/worker/workspace-classify.ts` (new), `src/worker/run-task.ts`,
`src/worker/git.ts`, `src/db/task-store.ts`, `src/worker/checkpoint.ts`)
closes that gap plus a second, independent one at the checkpoint-
persistence boundary -- again without a second worker or new
infrastructure:

- **Classify before touching anything**
  (`classifyWorkspace`, `src/worker/workspace-classify.ts`): a READ-ONLY
  function (`git status`/`rev-parse`/`merge-base`/`rev-list` only, never
  a write) called BEFORE `ensureWorktreeAndCommit` applies or commits
  anything. Returns exactly one of: `ready_for_repair` (clean, at base,
  defect present -- the normal starting point); `repair_pending_commit`
  (dirty, but the ENTIRE changed-path set is exactly the one supported
  repair target with content already matching the known fix -- the
  "repair applied, worker interrupted before commit" crash window, and
  the ONLY dirty state ever permitted through); `repair_committed`
  (clean, HEAD a real descendant of base, repair present, exact 1-commit
  delta -- delegates to `checkCommittedRepairState`, the SAME helper
  `assertCleanWorktreeIsValidResult` uses, so the two can never silently
  drift apart); or `unexpected` (everything else -- an unrelated
  tracked-file edit, a stray staged edit, an untracked file, a dirty
  worktree whose HEAD is not the base commit, or a clean worktree that
  fails `checkCommittedRepairState` -- with the offending path(s)/HEAD
  state named in the rejection reason). "Checkpoint already persisted"
  (a fifth state named in the originating task) is NOT separately
  distinguishable at the Git level from `repair_committed` -- both look
  identical in the worktree; that distinction is handled entirely
  downstream by `TaskStore.recordGitCheckpoint`'s own idempotency ledger.
- **`unexpected` throws before any mutation, staging, or deletion**:
  `ensureWorktreeAndCommit` throws immediately on an `unexpected`
  classification, naming the offending path(s)/HEAD state, WITHOUT
  calling `applyScriptedRepair` or committing anything -- verified
  through the REAL orchestration path (`runGitWorkerTask`, not a direct
  call into the classification function) by
  `tests/integration/git-worker-workspace-state.test.ts`: an unrelated
  unstaged edit, an unexpected staged edit, an unexpected untracked file,
  a dirty worktree where the repair target IS correctly fixed BUT an
  unrelated file is ALSO dirty (the direct test of the original
  vulnerability -- the correct repair must NOT be swept into a commit
  together with an unrelated edit), and a clean worktree whose HEAD
  moved to an unrelated, non-repair commit are all rejected, with the
  tampered path's content, staged status, and the worktree's HEAD
  verified UNCHANGED after the rejected attempt.
- **`commitAllowedPaths`, never `commitAll`/`git add -A`**
  (`src/worker/git.ts`, replacing `commitAll` entirely -- it had exactly
  one call site, now removed): stages EXACTLY a caller-supplied path
  list, verifies the worktree's changed-path set contains nothing else
  BEFORE staging (refuses to commit if it does), and verifies the
  worktree is fully clean AFTER committing. Backed by a new
  `changedPaths` helper (`git status --porcelain=v1 -z`, NUL-parsed, not
  a raw string a caller might substring-match) that returns the exact
  status code and path of every change, which `classifyWorkspace` uses
  for its own "is the changed-path set exactly the one allowed target"
  check. See docs/decisions/0010-explicit-path-commits.md.
- **A validated checkpoint-persistence boundary**
  (`parseAndValidateGitCheckpointManifest`, `src/db/task-store.ts`):
  `TaskStore.recordGitCheckpoint` previously accepted
  `baseCommit`/`resultCommit`/`resultTree`/`verificationCommandJson`/
  `verificationOutcome`/`runtimeFingerprintJson`/`manifestVersion` as
  SEPARATE arguments alongside `manifestJson`/`manifestDigest` -- fields
  that were, at the one production call site, always derived from the
  SAME manifest object, but that the store itself never verified were
  consistent, and that a different caller (a test, or a future caller)
  could supply contradictorily. The interface no longer accepts those
  separate fields at all: `recordGitCheckpointImpl` now (1) parses
  `manifestJson` and validates its shape field-by-field, (2) RECOMPUTES
  `fingerprint(manifest)` (the SAME function
  `computeManifestDigest`/`src/worker/checkpoint.ts` uses) and rejects a
  supplied `manifestDigest` that does not match, (3) rejects a manifest
  whose own `taskId`/`attemptId`/`epoch` disagree with the request's,
  and (4) rejects a manifest whose own `verification.outcome` is not
  `"passed"` -- then derives every persisted `task_checkpoints` column
  from THAT validated manifest, never from a second, independently-
  contradictable representation. This validation deliberately runs ONLY
  on the "genuinely new write" path, AFTER the existing-receipt replay
  lookup -- a REPLAY from the original committing owner must still
  succeed even after the task's epoch has since moved on
  (docs/decisions/0004-receipt-authorization.md), so cross-checking the
  manifest's epoch against the caller's current, possibly-stale epoch
  would otherwise break that guarantee. `src/worker/checkpoint.ts`'s
  `publishCheckpoint` was simplified to match: it now passes only
  `manifestDigest`/`manifestJson` (plus the request's own identity
  fields), not a second, flattened copy of the manifest's own fields.
  **What this validates, and what it does not**: this proves the
  manifest is internally consistent and that its digest/task/attempt/
  epoch genuinely correspond to the request committing it -- it does NOT
  prove an untrusted worker actually ran the verification command it
  claims to have run; that would require independent re-execution or a
  trusted execution environment, neither of which exists in this phase.

### Supported input/path boundaries

- `requestedCommitRef` MUST resolve to a real, existing commit object
  (`git rev-parse --verify <ref>^{commit}`) -- anything else is rejected
  explicitly, never silently falling back to `HEAD` or any default.
- Every worktree path is validated to be WITHIN the managed workspace
  root both LEXICALLY (`assertWithinRoot`) and, since Phase 2A.1,
  against REAL filesystem identity (`assertNoSymlinkEscapes`, rejecting
  any symlink/junction/reparse point along the way -- see "Phase 2A.1"
  above) -- `taskId`/`attemptId` are restricted to a conservative
  `[A-Za-z0-9_-]` allowlist and rejected outright (not sanitized) if
  they contain anything else, including `..` or path separators.
- The worker NEVER touches the SOURCE repository's own working tree --
  all file changes happen in a SEPARATE worktree directory created by
  `git worktree add`; verified directly
  (`tests/integration/git-worker.test.ts` test 2: the source repo's
  `src/add.js` is byte-for-byte unchanged after a full worker run).
- `makeIsolatedGitEnv` (`src/worker/git.ts`) points `HOME`/
  `XDG_CONFIG_HOME` at a throwaway directory and sets
  `GIT_CONFIG_NOSYSTEM=1`/`GIT_TERMINAL_PROMPT=0`/a failing
  `GIT_ASKPASS`/`GIT_SSH_COMMAND` -- this project's own `~/.gitconfig`
  (hooks, signing, credential helpers) is never read, and an operation
  that would otherwise prompt for credentials fails immediately instead
  of hanging. **This does NOT make running an arbitrary repository
  safe** -- it only prevents THIS project's own local environment from
  leaking into a controlled, disposable fixture repository the test
  harness itself creates. There is no sandboxing, no filesystem
  containment beyond the managed-workspace-root path check, and no
  claim of safety against a hostile/untrusted repository.

### Workspace root persistence (a real bug this milestone's own crash test caught)

`createManagedWorkspaceRoot` (`src/worker/workspace.ts`) is a STABLE,
shared directory by default (`RELAYLAB_WORKER_WORKSPACE_ROOT`, or a
fixed path under the OS temp directory) across worker process restarts
-- an EARLIER version used `mkdtemp` (a fresh random directory every
call), which silently broke restart reconciliation entirely: a
restarted process could never find a prior attempt's worktree, because
it was looking in a completely different, unrelated directory. Found by
`tests/integration/git-worker-crash.ts`'s real kill-and-restart test
failing with an ownership error (the symptom that surfaced first), not
a theoretical review. `src/worker-main.ts`'s real CLI path deliberately
does NOT delete this shared root after a successful run (deleting it
would destroy OTHER tasks' worktrees sharing the same root, and would
remove the checkpoint's own evidentiary Git objects right after
publishing them) -- cleanup is an explicit, separate, NOT-yet-
implemented operator concern, not an automatic side effect.
Tests use an explicitly isolated, randomized root instead (no shared
state between test runs).

### Ownership check vs. filesystem fencing -- an honest limitation

A database ownership/epoch check before running Git does **NOT**
atomically fence the entire subsequent filesystem operation -- there is
a real window between "the DB says this worker's epoch is current" and
"the Git commit this worker creates actually gets durably checkpointed"
during which a DIFFERENT, newer attempt could theoretically also be
running. What DOES hold: isolated, attempt-scoped worktrees mean two
attempts can never corrupt EACH OTHER's workspace (different paths
entirely), and the FINAL authoritative boundary -- `recordGitCheckpoint`
-- re-checks epoch/ownership against CURRENT state before persisting
anything, so a stale worker's Git work (even if it fully completes) can
never become the authoritative durable result (verified:
`tests/integration/git-worker.test.ts` test 9, a real epoch takeover
followed by a rejected stale-epoch publish attempt). This is the SAME
property this project's publisher-leader-lock already has and states
honestly (docs/decisions/0006) -- not a new or weaker guarantee.

### Checkpoint manifest -- COMMITTED STATE ONLY

Explicitly does NOT capture dirty working-tree edits, staged changes, or
untracked files -- only the state of a real, local Git commit (see
`src/worker/checkpoint.ts`'s file header comment). Three different kinds
of "ID" in the manifest are deliberately kept distinct: Git object IDs
(verifiable independently against the real repository), the manifest's
OWN content digest (`fingerprint()` over the manifest's stable-sorted
JSON encoding -- the SAME helper this project's command-idempotency
fingerprints already use), and verification EVIDENCE (the actual
command/exit-code/output record, not an identity at all). Local Git
objects must remain available for a checkpoint to be usable -- the
manifest alone is NOT a portable backup (see `src/worker/checkpoint.ts`
and the "workspace root persistence" note above for why the worktree is
deliberately not deleted on success). **Phase 2B's handoff artifact
(below) is precisely what makes a checkpoint's Git state portable** --
the manifest format above never changed to support this; a SEPARATE
artifact (a self-contained Git bundle) is built from the checkpoint's
already-committed result when, and only when, a handoff is requested.

## Phase 2B: committed-checkpoint handoff between two workers

Full protocol/state-machine design: docs/decisions/0011-handoff-protocol.md
(written BEFORE this section's code). Scope, stated precisely: NOT cloud
deployment, NOT dirty-file transfer, NOT live AI coding -- a second,
independent LOCAL worker process restores an already-validated,
committed checkpoint from a self-contained artifact and re-verifies it.

### The three new pieces

- **`src/worker/artifact.ts`**: `createHandoffArtifact` builds a
  self-contained Git bundle (`git bundle create <path> <branchRef>` --
  a NAMED ref, never a bare commit SHA; `git bundle create` fails with
  "fatal: Refusing to create empty bundle" for a bare SHA, confirmed
  directly against this host's real git binary, because a bundle's
  header stores ref-name -> object-id pairs and a bare SHA has no name
  to record) from the worktree's own branch, verifies it (`git bundle
  verify`) BEFORE promoting it, and writes both the bundle and its
  manifest ATOMICALLY (temp file, then `rename` to a final,
  digest-addressed, immutable path -- never a partially-written file
  visible at a path a caller could mistake for a finished one).
  `validateHandoffArtifact` checks size then full content digest against
  the manifest's OWN claimed values, BEFORE anything else ever touches
  the artifact's bytes. `restoreHandoffArtifact` restores into a FRESH,
  independent repository (`git init` + `git bundle verify` again, in the
  destination's own context + `git fetch <bundle> <ref>` + `git cat-file
  -e` as an explicit object-availability proof + `git checkout`) --
  never A's worktree, no Git alternates, no shared-object clone option.
  Rejects (untouched) a destination that already exists with unexpected
  contents, UNLESS that destination is already the exact, correctly-
  restored result of a PRIOR call for this same artifact (idempotent
  retry support -- positively confirmed via real HEAD/tree equality
  against the manifest, AND clean, never inferred from ".git exists").
- **`TaskStore.transferOwnership`** (`src/db/task-store.ts`): the one
  atomic, ownership-changing operation -- modeled alongside `claimTask`
  (NOT routed through `applyTaskCommand`, since it mutates `owner_id`/
  `epoch` themselves, which no other command does), requiring the
  supplied `checkpointDigest` to equal the task's CURRENT, already-
  durably-recorded `checkpoint_digest` (the "do not revoke A based on an
  incomplete or unvalidated artifact" guard -- see docs/decisions/0011
  "When A loses authority"). Idempotent via the SAME `command_receipts`
  ledger `recordGitCheckpoint` already uses; two competing, genuinely
  new transfer requests for the same `fromEpoch` serialize on the
  `tasks` row's own lock, the SAME mechanism `claimTask` relies on for
  takeover races.
- **`RestoreAcknowledged`** (`src/domain/domain.ts`): a genuine domain
  command/event, needing NO special-cased ownership logic -- fenced
  under B's CURRENT epoch through the EXISTING `applyTaskCommand` path,
  exactly like `SubmitCandidate`/`RecordVerification`. Deliberately does
  NOT touch `checkpointDigest`/`baseCommit`/`resultCommit`/`resultTree`
  -- those keep A's original provenance (the task's own instruction:
  "do not rewrite its epoch to pretend B created it").

### `src/worker/handoff.ts` -- the four orchestration steps

`prepareHandoffArtifact` (A-side, local only, no DB write) ->
`requestOwnershipTransfer` (the one atomic DB transaction) ->
`restoreAndAcknowledge` (B-side: validate artifact -> restore into an
independent repo -> rerun the FIXED verification command against B's
OWN freshly-restored workspace, NEVER trusting A's prior verification
result as proof B's environment works -> publish the idempotent
acknowledgement). Each step is its own OpenTelemetry span (reusing the
SAME `getTracer()`/span-per-step convention `run-task.ts` already uses
-- no new dashboard, no task ID in a metric label).

### Part A: a real, demonstrated gap found and fixed in `recordGitCheckpoint`'s replay path

Found by a full-code review this session was explicitly asked to do
BEFORE implementing the handoff protocol (not by a test failure): the
REPLAY branch of `recordGitCheckpointImpl` matched only on
`fingerprint({type:"Checkpoint", digest: manifestDigest})` -- a hash of
the CLAIMED digest VALUE, never of the `manifestJson` actually supplied
alongside it on the retry. A retry supplying the SAME, already-
committed `manifestDigest` together with a DIFFERENT, altered
`manifestJson` was silently accepted as an identical retry, returning
the original stored result, without ever checking that the freshly-
supplied content actually corresponds to that digest. Fixed by splitting
validation into two functions
(`parseAndValidateGitCheckpointManifestShapeAndDigest`, run
UNCONDITIONALLY on every call, replay or not; `assertManifestIdentityMatchesRequest`,
kept new-write-only, since a replay must still succeed after the task's
epoch has moved on -- docs/decisions/0004). Verified by a dedicated
regression test (`tests/integration/git-worker-checkpoint-manifest.test.ts`,
"a replay supplying the ORIGINAL ... digest together with ALTERED
manifestJson content is rejected") and by the pre-existing replay-after-
ownership-change test still passing unmodified.

### Separately: a subtle interaction between checkpoint-commandId reuse and epoch fencing (observed while writing Part B's own tests, documented, not "fixed" because nothing is broken)

`checkpointCommandId(taskId)` is deterministic and TASK-scoped (not
attempt- or epoch-scoped -- by design, see `run-task.ts`'s own comment).
This means a SECOND `recordGitCheckpoint` call for a task that already
has a published checkpoint ALWAYS hits the existing-receipt lookup
FIRST, before the row/epoch check is ever reached -- so a stale A
retrying its OWN already-published checkpoint is rejected via
`CommandConflict` (if the new attempt's manifest digest differs, e.g.
because `verification.durationMs` differs between runs) or silently
no-ops as a harmless REPLAY (if the digest happens to match exactly,
e.g. an exact byte-for-byte resubmission) -- in NEITHER case does A
ever get to introduce a new, different authoritative checkpoint after a
handoff. This is NOT literally "epoch fencing," but it is NOT a gap
either: ordinary epoch fencing is still the mechanism for any GENUINELY
NEW command (a fresh, never-used `commandId`) -- see
`tests/integration/git-worker-handoff.test.ts` test 7, which tests both
cases explicitly and distinguishes them in its own comments.

### Remaining limitations, stated explicitly

- `transferOwnership`'s own precondition (checkpointDigest must match)
  proves the HANDED-OFF checkpoint was already durably validated -- it
  does NOT re-validate the ARTIFACT FILE itself; that is the coordinator's
  responsibility (build + validate the artifact BEFORE requesting
  transfer). This is a documented trust boundary, not an oversight (see
  docs/decisions/0011 "When A loses authority").
- Matching worker identity strings (`fromOwnerId`/`toOwnerId`) is NOT
  cryptographic authentication -- unchanged from docs/decisions/0004. A
  stale A process can still modify its OWN isolated files after
  transfer; the guarantee is narrower: it cannot publish a NEW
  authoritative result.
- The "transfer" in this milestone is a local filesystem handoff of a
  bundle file between two directories a test/demo controls directly --
  there is no network transport, no real multi-machine deployment.
- A THIRD worker, N-way election, or a generic multi-attempt
  reconciliation framework remain explicitly out of scope
  (docs/ROADMAP.md Phase 2) -- stopping after this two-worker handoff,
  per this milestone's own instructions.
