# RelayLab Roadmap

Phase 1, the Phase 1.1 audit, Phase 1.2 (foundation fixes + OpenTelemetry
tracing), Phase 1.2.1 (OpenTelemetry/Prometheus/Grafana monitoring +
reliability patch), and Phase 2A (one real Git execution worker, below)
are implemented (see docs/PROGRESS.md for exactly what that means).
The rest of Phase 2 and Phases 3-5 below are the full plan; none of
their code exists yet.

## Phase 2A -- one real Git execution worker (DONE, see docs/PROGRESS.md and docs/ARCHITECTURE.md "Phase 2A worker")

Exactly the "smallest recommended next milestone" described at the
bottom of this file, implemented: a worker claims a real task, checks
out a real Git worktree at a given commit, applies ONE scripted
(non-LLM) deterministic repair, runs a fixed verification command,
commits, and publishes a real durable checkpoint manifest (committed-
state only) through the existing task/receipt/outbox infrastructure --
no new protocol, no new technology, no generic agent framework. Restart
reconciliation after a real process kill (verified) ensures no duplicate
commit or checkpoint. See docs/ARCHITECTURE.md "Phase 2A worker" for the
full design and docs/PROGRESS.md for what was built/verified and its
stated limitations (single-attempt only, no multi-worker handoff, no
dirty-working-tree capture, DB-ownership-check-does-not-atomically-fence-
filesystem-work).

## Phase 1.2 -- Foundation fixes + OpenTelemetry tracing (DONE, see docs/PROGRESS.md and docs/OBSERVABILITY.md)

Closed a test-harness child-process cleanup gap (docs/decisions/0009) and
a MySQL `UPDATE SET`-clause evaluation-order bug in ownership/publisher-
lock queries (docs/decisions/0008), both found by an independent review.
Added the project's first additional technology, OpenTelemetry
distributed tracing, with explicit spans only (no auto-instrumentation),
W3C context propagation across the durable outbox and NATS, a real local
Collector, and both in-memory-exporter tests and one real
OTLP-to-Collector verification test.

## Phase 1 -- Durable orchestration foundation (DONE, see PROGRESS.md)

Transactional MySQL state, command receipts, and outbox. NATS delivery and
a durable idempotent consumer. Ownership/lease correctness. Real service
integration tests. Reproducible crash-window tests, including real
process-kill scenarios for all three crash windows (publisher ack/mark-sent,
consumer before-commit, consumer after-commit-before-ack).

## Phase 1.1 -- Correctness audit (DONE, see docs/PROGRESS.md "Phase 1.1 audit")

Closed real gaps found by an independent review: receipt replay
authorization (decoupled from fencing), claim idempotency for the
rightful owner, event-identity-conflict detection, a durable (not
in-memory) bounded pending-event buffer replacing "flag but still apply"
gap handling, an enforced publisher leader lock, graceful shutdown with
bounded backoff, a real migration ledger, and a disposable test
database/NATS namespace so integration tests never touch dev data.

### Known gaps carried out of Phase 1.1 (fix opportunistically, not blocking)

- **Command receipts grow without bound.** No retention/archival policy
  exists for `command_receipts`, `consumer_processed`, `task_consumer_state`,
  `task_consumer_pending`, or `consumer_event_conflicts`. Fine at demo
  scale; needs a policy (TTL? move to cold storage after N days?) before
  any real usage.
- **No Vitess/PlanetScale validation.** Tests run against stock MySQL 8.4
  only.
- **No real authentication.** `ownerId` is a caller-supplied string; the
  `NotAuthorized` check (docs/decisions/0004-receipt-authorization.md)
  defends against accidental ID collisions between workers, not a caller
  willing to lie about its own identity. Real authentication (e.g. mTLS
  client certs, signed tokens) is unimplemented and unsimulated.
- **Real POSIX SIGTERM/SIGINT delivery to publisher/consumer processes is
  unverified on this dev machine.** Windows does not let
  `child_process.kill("SIGTERM")` deliver a catchable signal, so the
  graceful-shutdown test exercises the same code path via an IPC message
  instead (see docs/ARCHITECTURE.md "Publisher and consumer crash
  behavior"). The handler code is correct for POSIX, but an automated
  process-kill test of it has only run (and could only run) on a
  non-Windows machine or CI runner, which this session did not have.
- **The publisher leader lock does not fence an in-flight NATS call.** See
  docs/decisions/0006-publisher-leader-lock.md "Scope and failure
  behavior" -- a lease expiring does not reach into another process and
  cancel a network call already underway. Accepted because the consumer's
  own dedup is the actual correctness mechanism, not the lock.
- **The bounded pending-event cap (`maxPendingPerTask`, default 50) has no
  automated alerting** -- a permanently stuck gap is queryable
  (`task_consumer_state WHERE gap_detected = 1`) but nothing pages anyone
  yet. Fine for Phase 1; a real operator-facing surface is Phase 4/5 work.

## Phase 2 -- Real execution and checkpoint handoff

**Phase 2A/2A.1/2A.2 (DONE, see above)**: ONE worker, a real worktree at
a real commit, a real scripted (non-LLM) repair, a real fixed
verification command, a real local commit, a real COMMITTED-STATE-ONLY
durable checkpoint, restart reconciliation, workspace classification
before mutation, and a validated checkpoint-persistence boundary.

**Phase 2B (DONE, see above and docs/decisions/0011)**: TWO independent
worker processes (A and B); a self-contained Git bundle artifact
(committed state only, same COMMITTED-STATE-ONLY boundary as the
checkpoint manifest itself); an atomic, idempotent ownership transfer
(`TaskStore.transferOwnership`); restore into a SEPARATE, independent
repository with real integrity checks (size+digest BEFORE consuming the
artifact, Git object availability AND checkout-result equality AFTER);
task-scoped handoff that does not block an unrelated task (verified
directly, test 11); a documented recovery path for B failing after
ownership transfer but before acknowledgement (verified with a real
process kill, test 9). The rest of Phase 2 below remains NOT
implemented:

- A THIRD worker, N-way election, or any general multi-worker
  handoff/reconciliation topology -- Phase 2B is exactly two workers,
  one handoff, by its own explicit instruction to stop there.
- A controlled tool-execution interface (what a worker is allowed to do
  to a filesystem/repo, with an explicit boundary) -- A can only do the
  ONE hardcoded scripted repair; B can only restore/verify, never
  repair; there is no general tool-execution interface yet.
- Real Git worktree snapshots BEYOND committed state: working-tree
  changes, index state, and explicitly allowlisted untracked files --
  both the checkpoint manifest AND the Phase 2B handoff artifact are
  explicitly COMMITTED STATE ONLY (see docs/ARCHITECTURE.md "Checkpoint
  manifest" and "Phase 2B").
- A REAL network/cloud transfer mechanism for the handoff artifact --
  Phase 2B's "transfer" is a local filesystem handoff between two
  directories a test/demo controls directly.
- Fencing enforced at side-effect boundaries BEYOND ownership/epoch --
  this is the hard part Phase 1 explicitly does NOT solve (see invariant
  J: "Control-plane protection alone does not fence filesystem or remote
  API effects") and Phase 2B does not newly solve either (it adds
  artifact integrity checks and atomic ownership transfer, which is
  real progress on THIS specific handoff, but not a general answer).
  Needs a design doc before implementation beyond this milestone;
  likely candidates are single-writer worktree locks plus idempotent
  external API calls, but this has not been designed yet.

## Phase 3 -- Coding and verification

- One live model provider (no mocked-provider claims of live support).
- Typed, validated tool calls.
- Token/cost budgets, deadlines, cancellation, bounded retries.
- Real test execution and independent review (today's "review" is
  deterministic fixture logic in the pre-RelayLab prototype history; this
  phase makes it real).
- GitHub draft PR creation, reconciled after ambiguous timeouts.
- CI/review results tied to (repository, head SHA, environment, attempt)
  -- Phase 1's `verification_status`/`verification_attempt` columns exist
  but are only bound to a content-digest `candidate_id`, not a real head
  SHA; this phase closes that gap (see invariant I in the Phase 1 spec).
- No automatic merging, ever, in this phase.

## Phase 4 -- Product interface

React fleet view; task detail with event timeline, diff, checkpoint
status, verification matrix; keyboard navigation/command palette; clear
recovery/needs-attention states; reconnect/replay without gaps or dupes;
thin Electron client with a narrowly scoped native bridge.

## Phase 5 -- Operations and evaluation

Kubernetes deployment; health/readiness checks; graceful termination;
resource limits; pod-interruption and broker/DB-outage experiments;
reproducible evaluation on real repository tasks; raw traces, failure
counts, costs, latency distributions; a short recorded end-to-end demo
and a limitations document.

## Smallest recommended next milestone (DONE -- implemented as Phase 2A, see above)

A single worker process that: claims a task, checks out a real Git
worktree at a given commit, makes one trivial deterministic change (no
model involved yet), commits it, and writes a real checkpoint digest
derived from that worktree's actual state (not a content hash of
arbitrary JSON, as Phase 1's demo does). This exercises the Phase 2
Git-snapshot boundary in isolation, before adding a second worker, before
adding handoff, and long before adding a model.
