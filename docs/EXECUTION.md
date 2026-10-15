# RelayLab Execution Design (Phase 2C: containerized verification; Phase 2C.1 and the Phase 2C correctness patch: acceptance/correctness patches)

Short, focused design doc for the one thing Phase 2C added: WHERE the
fixed verification command runs, and how its lifecycle is controlled.
It does not restate Phase 2A/2B's repair/checkpoint/handoff design --
see docs/ARCHITECTURE.md "Phase 2A worker", "Phase 2B" for that.

**Phase 2C.1** closed three gaps found on review of Phase 2C: a
CONFIRMED real defect in exact-input fidelity (the executable bit was
silently discarded -- see "Exact-input fidelity" below), a CONFIRMED
real test-coverage gap (no scenario exercised a worker process crash
while its container was still running -- see "Cleanup ownership and
abandonment rules"), and a verified-already-correct check (manifest
v1/v2 compatibility -- see "Manifest compatibility").

**The Phase 2C correctness patch** (later, on review of Phase 2C.1
itself) closed three further gaps, all CONFIRMED by direct empirical
probe before any code changed: `reconcileAbandonedExecution` could
silently treat an UNKNOWN resource state as "safe to proceed" (see
"Cleanup ownership and abandonment rules" -- "Classifying failure,
honestly"); Phase 2C.1's `chmod -R a-w` read-only-input mechanism is
PERMISSION-based, not ownership-based, and does not actually prevent
the verification command's own code from reasserting write access to
files it owns (see "Actual input-protection guarantee" -- a genuinely
new section); and `materializeCandidateInput` used `git archive`,
which applies `.gitattributes` `export-ignore`/`export-subst` and
thereby verified a FILTERED view of the tree rather than the exact one
(see "Archive-attribute handling" -- which REPLACES this document's
previous, INCORRECT "no special handling needed" claim).

## Module layout

```
src/worker/verification.ts   the narrow interface both adapters implement:
                              VerificationAdapter { name, verify(request) }
                              VerificationRequest { inputDir, taskId, attemptId,
                                                     timeoutMs?, signal? }
                              VerificationResult  { outcome, exitCode, durationMs,
                                                     stdout/stderr (+ truncated flags),
                                                     executionId, adapterName }
                              VerificationOutcome = "passed" | "verification_failed"
                                                   | "timed_out" | "cancelled"
                                                   | "infrastructure_failure"
                              + boundedCollector(), deterministicExecutionName()

src/worker/verify.ts         UNCHANGED (Phase 2A) host-process adapter -- the fixed
                              allowlisted command identity (VERIFICATION_COMMAND_IDENTITY:
                              `node verify.js`) lives HERE and is imported by the
                              Docker adapter, never redefined. Still used directly by
                              tests that don't need Docker.

src/worker/docker-verify.ts  the Docker adapter: resolveVerifyImageId,
                              buildVerifyCreateArgs, materializeCandidateInput
                              (Phase 2C.1: rejects unsupported tree entries,
                              reports the real archived `treeSha`; Phase 2C
                              correctness patch: no longer uses `git archive`
                              -- reads each blob via `catFileBlob` and packages
                              via `writeTar`), assertMaterializedTreeMatches /
                              VerificationIdentityMismatch (Phase 2C.1, new),
                              reconcileAbandonedExecution (Phase 2C correctness
                              patch: now classifies `docker inspect` outcomes
                              and throws a typed `ReconciliationFailed` instead
                              of ever silently treating an unknown state as
                              absence), inspectDocker / DockerInspectOutcome /
                              DockerExec (Phase 2C correctness patch, new --
                              the classification helper and its injectable-exec
                              type, the ONLY fault-injection seam tests use),
                              verifyInDocker / verifyInDockerWithImageId,
                              dockerVerificationAdapter

src/worker/git.ts            gained listTreeEntries (Phase 2C.1, new: a thin
                              `git ls-tree -r -z --full-tree` wrapper, used by
                              materializeCandidateInput to detect unsupported
                              entries) and catFileBlob (Phase 2C correctness
                              patch, new: binary-safe `git cat-file -p` --
                              reads a blob's RAW stored bytes, unaffected by
                              `.gitattributes` export directives)

src/worker/tar-writer.ts     Phase 2C correctness patch, new: a minimal,
                              dependency-free USTAR writer (writeTar) --
                              packages `{path, mode, content}` entries into a
                              tar file directly, so materialization never
                              needs `git archive`'s attribute-filtering
                              behavior at all

src/worker/verification.ts   VerificationResult gained `cleanupError: string | null`
                              (Phase 2C correctness patch, new -- a cleanup
                              failure observable SEPARATELY from `outcome`,
                              never confused with the actual verification
                              result)

infra/verify-image/Dockerfile  the ONE pinned image: `FROM node:24.21.0-alpine3.24`,
                                `USER node`, `WORKDIR /verify`. Built explicitly via
                                `npm run docker:build-verify-image` -- never implicitly
                                built or pulled by the adapter.
```

Two concrete adapters, one shared result shape. Deliberately not a
plugin registry/factory -- there is no third adapter to justify one,
and a task can never select or influence which adapter runs; the
caller (`run-task.ts`, `handoff.ts`) picks it, same as before.

## Why `infrastructure_failure` is a fifth outcome, not folded into "failed"

The pre-existing host-process adapter only had three outcomes
(`passed`/`failed`/`timeout`). Containerizing adds a NEW failure mode
that has no analogue on the host: the execution environment itself can
fail to launch (Docker daemon unreachable, image not resolvable,
`docker create`/`docker start` erroring) *before the verified command
ever ran*. Conflating that with "the command ran and returned nonzero"
would be actively misleading -- a worker must never read
`infrastructure_failure` as "the candidate is bad" and must never
checkpoint/acknowledge on it, exactly like a real failure. Splitting it
into its own outcome makes that distinction a type-level fact `run-task.ts`/
`handoff.ts` must handle explicitly (`RunOutcome`/`RestoreOutcome` each
have a dedicated `verification_infrastructure_failure` variant), not
something a caller could accidentally paper over with `outcome !==
"passed"`.

## Execution lifecycle (one container, start to finish)

1. **Reconcile.** `reconcileAbandonedExecution(name)` looks up a
   deterministic name (`relaylab-verify-<role>-<taskId>-<attemptId>`,
   `deterministicExecutionName` in verification.ts) and, if it already
   exists (a prior worker process crashed mid-verification and never
   cleaned up), stops and removes EXACTLY that one container before
   proceeding. Never a global prune; never touches a different
   name/task/role. **Phase 2C correctness patch**: if this cannot
   CONFIRM the final state (daemon unreachable, a permission error, an
   unexpected CLI failure, or the container simply still being present
   after a stop-then-force-remove attempt), it throws
   `ReconciliationFailed` instead of proceeding -- `verifyInDocker`
   catches that and returns `infrastructure_failure` immediately,
   NEVER reaching `docker create` as though reconciliation succeeded.
   See "Cleanup ownership and abandonment rules" below for the full
   classification contract.
2. **Resolve the image.** `resolveVerifyImageId(tag)` runs `docker
   image inspect` and returns the real `sha256:...` ID. Throws (mapped
   to `infrastructure_failure`) if the tag isn't already built locally
   -- this adapter never implicitly builds or pulls it.
3. **Create.** `docker create` with the full isolation argument list
   (see "Isolation actually applied" below), via `buildVerifyCreateArgs`
   -- the SAME function a test can call directly to get real `docker
   inspect` evidence against, with no risk of a test's copy of the args
   drifting from what production runs.
4. **Start + attach, bounded.** `docker start --attach` streams
   stdout/stderr through the SAME bounded collectors
   (`MAX_CAPTURED_OUTPUT_BYTES`, 64 KiB/stream) the host-process adapter
   already used.
4b. **Optional readiness hook (Phase 2C.1, Part 2).**
   `DockerVerifyOptions.onContainerCreated(containerId)`, if supplied,
   is awaited once the container is independently CONFIRMED running
   (polled via `docker inspect`, racing against the container's own
   exit event so a fast-finishing container never pays this wait) --
   `run-task.ts` wires this, unconditionally, to `hitFailpoint`, which
   is a no-op in every normal run and every existing test. It exists
   SOLELY so a crash test can report a structured readiness barrier
   (real pid, real container ID, task/attempt identity, owner epoch)
   over IPC at the exact moment a real container is confirmed running,
   then have the orchestrator SIGKILL the worker process -- see
   `tests/integration/git-worker-crash.ts`
   "crash-during-container-verification".
5. **Race:** real exit vs. `timeoutMs` vs. an external `AbortSignal`.
   Whichever resolves first decides the outcome category
   (`passed`/`verification_failed` from a real exit code;
   `timed_out`/`cancelled` otherwise).
6. **On timeout/cancellation: stop the ACTUAL container and verify it
   stopped.** `docker stop`, then `docker inspect
   --format={{.State.Running}}` independently confirms it is no longer
   running (escalating to `docker kill` if it somehow still is).
   Killing the `docker` CLI child process is NOT treated as proof the
   container itself stopped -- that would only prove the CLI's own
   process exited, which says nothing about the container it was
   attached to.
7. **Always remove.** `docker rm -f`, unconditionally, scoped to this
   one container, regardless of outcome. A removal failure is logged
   (visibly, to stderr) AND recorded in the returned
   `VerificationResult.cleanupError` (Phase 2C correctness patch, new
   field) -- never overriding or masking the verification result
   already computed in step 5/6 (`outcome` stays whatever it already
   was), but now independently OBSERVABLE (by a test, or any future
   caller) rather than only visible in a log line.
8. **Return.** A `VerificationResult` with the real `executionId`
   (container ID -- distinct per invocation, even though the container
   NAME is deterministic and reused across retries of the same logical
   execution) and `adapterName: "docker"`.

Cleanup ownership is scoped PER EXECUTION, never globally: step 1 only
ever touches the one name this specific (role, taskId, attemptId)
would use; step 7 only ever removes the one container this specific
call created. A concurrently-running unrelated container (a different
task, a different role) is never inspected, stopped, or removed by any
step above -- verified directly in `docker-verify.test.ts` ("cleanup is
scoped to this execution only").

## Isolation actually applied (and how each claim was checked)

| Property | Flag(s) | How verified |
|---|---|---|
| Non-root | `--user node` | `docker inspect {{.Config.User}}` == `node`; empirically also confirmed via a script reading its own `process.getuid()` during earlier manual probing |
| Read-only root filesystem | `--read-only` | `docker inspect {{.HostConfig.ReadonlyRootfs}}` == `true` |
| Read-only verification INPUT -- the ORIGINAL tar (Phase 2C.1) | `--mount type=bind,...,readonly` -- a real, kernel-enforced read-only bind mount | `docker inspect {{json .Mounts}}` shows `"RW":false`; genuinely immutable, including against root inside the container -- a kernel-level VFS property, not a permission bit |
| Read-only verification INPUT -- the EXTRACTED COPY (Phase 2C.1, NARROWED by the Phase 2C correctness patch) | the extracted copy is `chmod -R a-w`'d immediately after extraction, before the verified command runs | behaviorally -- a NAIVE write attempt gets a real `EACCES`, captured in test output. **NOT an immutability guarantee**: the SAME user that extracted the files OWNS them and can `chmod` them back to writable and alter them -- CONFIRMED directly (see "Actual input-protection guarantee" below). Permission-based protection against an accidental/non-adversarial write only. |
| Explicit writable scratch | `--tmpfs /tmp:rw,noexec,nosuid,size=...` | behaviorally -- this is now where extraction itself happens (`/tmp/verify`), so it IS exercised by every single run, not just configured |
| Network disabled | `--network none` | `docker inspect {{.HostConfig.NetworkMode}}` == `none`; AND behaviorally -- a script attempting a real DNS lookup observes a real failure, captured in test output |
| Capabilities dropped | `--cap-drop ALL` | `docker inspect {{.HostConfig.CapDrop}}` == `[ALL]` |
| No privilege escalation | `--security-opt no-new-privileges` | `docker inspect {{.HostConfig.SecurityOpt}}` contains `no-new-privileges` |
| Memory/CPU/process-count bounds | `--memory`, `--cpus`, `--pids-limit` | `docker inspect` on `{{.HostConfig.Memory}}` / `{{.HostConfig.NanoCpus}}` / `{{.HostConfig.PidsLimit}}`, each nonzero/set |
| Docker's own log retention bounded | `--log-opt max-size=1m --log-opt max-file=1` | configuration only |
| Only the intended input exposed | one `--mount type=bind,source=<candidate.tar>,target=/verify-input.tar,readonly` (Phase 2C.1: the TAR FILE itself, never a directory), nothing else | `docker inspect {{json .Mounts}}` shows exactly one mount, `"RW":false` |
| No shell injection surface | `docker` invoked via `execFile`/`spawn` with argument arrays throughout; the container's OWN bootstrap command is a fixed, non-task-influenced string (never built from task-supplied input) | static -- grep `docker-verify.ts` for any string-concatenated shell call built from caller input (there is none) |

All of the above is exercised against a REAL Docker daemon in
`tests/integration/docker-verify.test.ts` -- that file's `before()`
hook fails loudly (not silently) if Docker is unreachable or the image
hasn't been built, so a green run here is never a mocked stand-in.

## Git identity invariants (unchanged from Phase 2A/2B, now realized through a container)

- A's and B's Git identity pinning happens EXACTLY where it always did
  -- `run-task.ts` pins `(resultCommit, resultTree)` before calling
  verification; `handoff.ts` binds B's verification to the restored
  `(restoredCommit, restoredTree)`. Containerization did not move this
  pinning earlier, later, or into the container.
- The existing post-verification drift check in `run-task.ts`
  (re-reading commit/tree after verification and comparing against the
  pinned pre-verification values) is UNCHANGED and still runs.
- Evidence recorded per verification: task/attempt id + owner epoch
  (already carried by the surrounding checkpoint/acknowledgement
  commands, unchanged), candidate tree or restored commit/tree (same),
  the verification command identity (same `VERIFICATION_COMMAND_IDENTITY`,
  unchanged), the RESOLVED image ID (`CheckpointManifest.verification.containerImageId`,
  new in Phase 2C, `sha256:...`, never a mutable tag), and the outcome
  (same field, same five-way type now instead of three).

## Exact-input fidelity (Phase 2C.1, Part 1)

### A CONFIRMED real defect: the executable bit was silently discarded

Phase 2C's design extracted the archived tar ON THE WINDOWS HOST (via
`tar.exe`) and bind-mounted the resulting DIRECTORY, read-only, into
the container. This was probed directly before Phase 2C.1 changed any
code: a tracked executable file (git mode `100755`) and a tracked
non-executable file (`100644`) were committed, archived, extracted on
the host, and the resulting directory bind-mounted into the real
pinned image. Both files arrived inside the container as mode `777`,
INDISTINGUISHABLY -- Windows NTFS has no native unix-permission
concept, and Docker Desktop's Windows-host bind-mount translation
flattened the distinction entirely. This is a genuine fidelity defect:
a candidate's executable/non-executable semantics were NOT faithfully
represented to the verification command.

### The fix: extract on the container's own Linux filesystem, not the host

`materializeCandidateInput` no longer extracts anything on the host --
it only validates the tree (see below) and packages
`<exportDir>/candidate.tar` (originally via `git archive`; since the
Phase 2C correctness patch, via its own `catFileBlob`+`writeTar`
pipeline -- see "Archive-attribute handling" below for why that
changed again). `buildVerifyCreateArgs` bind-mounts that TAR FILE
itself, read-only, at a fixed in-container path (`/verify-input.tar`);
the container's actual command becomes a small, fixed bootstrap
(`/bin/sh -c "mkdir -p /tmp/verify && tar -xf /verify-input.tar -C
/tmp/verify && chmod -R a-w /tmp/verify && cd /tmp/verify && exec node
verify.js"`) that extracts onto its OWN tmpfs, where a real Linux
`tar` honors mode bits natively. Re-probed after the fix, through the
exact production code path: the executable file arrives at mode 755,
the non-executable one at 644 -- correctly distinguished. `chmod -R
a-w` immediately afterward, BEFORE the verified command ever runs,
restores a PARTIAL "retain a read-only verification input" (a NAIVE
write attempt now fails with `EACCES`) -- **this is permission-based,
not an immutability guarantee; see "Actual input-protection
guarantee" below for what it does and does not actually prevent.**
`/tmp` OUTSIDE `/tmp/verify` remains genuinely writable scratch space.

This is a SMALL, narrowly scoped fix -- one bootstrap string, one
`chmod`, no Dockerfile change, no new dependency. Only WHERE and HOW
the archived content gets onto a filesystem the verified command can
see changed.

### Explicitly refused, never silently incomplete

`materializeCandidateInput` calls `listTreeEntries` (`src/worker/git.ts`,
new: a thin `git ls-tree -r -z --full-tree` wrapper) and throws if the
tree contains a `120000` (symlink) or `160000` (gitlink/submodule)
entry -- `git archive` cannot faithfully materialize a submodule's
content at all (only a useless gitlink marker), and a symlink entry
could reference an absolute path or escape outside the intended export
once followed. Refusing outright, rather than silently materializing
an incomplete input, was verified directly: both entry kinds were
staged via `git update-index --cacheinfo` plumbing (portable on
Windows, which cannot reliably create real symlinks without elevated
privileges) and confirmed to be rejected with a clear error naming the
offending path and mode.

### Archive-attribute handling (Phase 2C correctness patch -- CORRECTS this document's own previous claim)

**This document previously claimed** `.gitattributes`
`export-ignore`/`export-subst` needed "no special handling" because
`git archive` honors them itself -- and treated a file disappearing
(export-ignore) or being rewritten (export-subst) as CONFIRMED
fidelity. **That was the wrong contract.** This project's entire
purpose for `materializeCandidateInput` is "verify the EXACT candidate
tree" -- a checkpoint's claim "this tree was verified" is FALSE if
`git archive` silently dropped or rewrote a tracked file along the
way. Export-ignore/export-subst are themselves just more TRACKED
content (set via a tracked `.gitattributes` file); applying them means
verifying a FILTERED/TRANSFORMED view of the tree, not the tree
itself.

**The required (and now actual) semantics**: a tracked file marked
`export-ignore` STILL exists in the verification input, with its
original git blob bytes. A tracked file marked `export-subst` STILL
contains its original git blob bytes, INCLUDING the unchanged
`$Format:...$` marker -- never expanded. Confirmed directly, through
the real production code path:

```
$ git cat-file -p HEAD:VERSION.txt   # tracked export-subst, marker UNCHANGED
version: $Format:%H$
$ git cat-file -p HEAD:IGNORED.md    # tracked export-ignore, STILL readable
secret, ignored for archive
```

**How**: `materializeCandidateInput` no longer calls `git archive` at
all. `listTreeEntries` (`git ls-tree`) enumerates every tracked entry
(unchanged); for each SUPPORTED one (not a rejected symlink/gitlink),
`catFileBlob` (`git cat-file -p <object>`, `src/worker/git.ts`) reads
its RAW, STORED bytes directly -- `.gitattributes` export directives
are `git archive`-time-only behaviors that `cat-file -p` never applies
at all, confirmed empirically above. Those entries are packaged into
`candidate.tar` by this module's own minimal `writeTar`
(`src/worker/tar-writer.ts`, a small, dependency-free USTAR writer --
see that file's own header comment), carrying each entry's exact
git-recorded mode. Expected tracked PATHS are whatever
`listTreeEntries` reports (directly comparable in tests); expected
BYTES come from the SAME git objects `materializeCandidateInput`
itself reads -- never a second, independent archive export that could
drift from what was actually selected.

Mode-bit fidelity (the Phase 2C.1 fix, above) is unaffected by this
change -- extraction still happens on the container's own Linux tmpfs,
never the Windows host.

### Identity-mismatch guard

`MaterializedInput.treeSha` (the ACTUAL `git rev-parse <ref>^{tree}`
result for what was just archived) lets `run-task.ts` (A) and
`handoff.ts` (B) call `assertMaterializedTreeMatches(materialized,
<already-pinned tree>, context)` immediately after materializing,
BEFORE trusting the verification result for anything. A mismatch
throws `VerificationIdentityMismatch` -- a verification of a different
tree than the one about to be checkpointed or acknowledged must never
authorize either. Tested directly against the shared guard function
(materializing two genuinely different trees and confirming a swapped
expected-tree is rejected) rather than by trying to force an organic
mismatch through the public worker entrypoints, which cannot produce
one by construction (same reasoning `git-worker-crash.ts`'s own
`clean-at-buggy-base-rejected` scenario documents for a different
guard).

## Actual input-protection guarantee (Phase 2C correctness patch, Part 2)

### A CONFIRMED real defect: `chmod -R a-w` is permission-based, not ownership-based

The bootstrap extracts the tar AND runs the verified command as the
SAME user (`docker create --user node` applies to the whole
container, including the bootstrap's own `tar -xf`/`chmod` steps).
`chmod()`'s permission check is "you own the file, or you hold
`CAP_FOWNER`" -- NEVER "the file's current mode bits allow it." Since
the extracting user and the verifying user are the SAME owning user
here, that user can always `chmod` its own files back to writable,
REGARDLESS of what `chmod -R a-w` set them to. Probed directly, through
the real production image:

```
owner uid (process): 1000
before mode: 444 uid: 1000
CHMOD+WRITE SUCCEEDED. New content: "TAMPERED CONTENT -- exact-input guarantee broken"
DIRECTORY-CHMOD + FILE REPLACEMENT SUCCEEDED
```

Both a `chmod`-back-then-rewrite of a single file, AND a `chmod`-the-
directory-then-unlink-then-replace, succeeded. `chmod -R a-w` is real
protection against a NAIVE/accidental write (the EACCES test) -- it is
NOT an immutability guarantee against the verification command's own
code deliberately reasserting write access.

### A privilege-separated alternative was investigated and REJECTED

If extraction ran as ROOT and the verified command ran as `node` via a
privilege drop (`su node -c ...`), `node` would not own the extracted
files and (with `CAP_FOWNER` dropped) could not `chmod` them back.
Probed directly: extracting as root with `tar --no-same-owner`
correctly produces root-owned, `chmod -R a-w`'d files --
but the privilege-drop step itself fails outright under this
project's OWN `--cap-drop ALL`:

```
$ docker run ... --cap-drop ALL ... --user root ... \
    sh -c "... && su node -c '...'"
su: can't set groups: Operation not permitted
```

`su` requires `CAP_SETUID`/`CAP_SETGID`. Re-adding just those two
capabilities would trade this project's already-established,
independently tested "every capability dropped" isolation property for
a narrower one, in service of defending against a threat (the FIXED,
project-controlled verification command deliberately tampering with
its own input) that is already outside this project's documented
threat model. **Rejected** -- not adopted.

### The guarantee, stated honestly

- The ORIGINAL tar file -- the true, authoritative verification input
  -- remains genuinely immutable the ENTIRE time, via the
  kernel-enforced read-only bind mount (`"RW":false`, confirmed via
  `docker inspect`, unaffected by ANY in-container user's privileges,
  root included).
- The EXTRACTED COPY the verified command actually reads from is
  protected against accidental/naive writes (permission bits, real
  `EACCES`), but NOT against that same command's own code deliberately
  reasserting write access to files it owns.
- This does NOT defend against hostile code that modifies and restores
  its own input, and never claimed to -- the verification command is
  FIXED and project-controlled, never task-supplied (see CLAUDE.md
  "Non-goals" and "What this does not claim" below). A permanent
  regression test
  (`tests/integration/docker-verify.test.ts`, "Part 2 (documented
  limitation, not a regression)") keeps this fact visible and tested,
  rather than letting a future reader assume `chmod -R a-w` means
  immutable.
- **Phase 3A, Part A, below, adds a DETECTION mechanism on top of this
  -- it does not change any of the above.** The permission bypass
  described here is still real and still possible; what changed is
  that the orchestrator now independently notices afterward and
  refuses to accept the result, rather than the tamper going
  completely unnoticed. See "Post-verification integrity check" below.

## Post-verification integrity check (Phase 3A, Part A)

### What this closes

The section above describes a CONFIRMED gap: `chmod -R a-w` does not
stop the verifying user from tampering with its own extracted input
and still exiting 0. This was accepted as a stated limitation for the
fixed-command prototype -- but nothing previously DETECTED it either.
Confirmed absent by direct inspection (no `docker cp`/integrity-style
check existed anywhere in this codebase) before adding one.

### Why `docker exec`, not `docker cp`

The first implementation tried `docker cp` to pull the container's
final `/tmp/verify` state onto the host for hashing. Probed directly,
including against a container still RUNNING:

```
$ docker exec tmpfs-probe cat /tmp/test.txt
hello
$ docker cp tmpfs-probe:/tmp/test.txt -
Error response from daemon: Could not find the file /tmp/test.txt in container tmpfs-probe
```

`docker cp` cannot read a `--tmpfs`-mounted path AT ALL on this Docker
setup -- a general Docker limitation (tmpfs is in-memory, never part
of the graph-driver layers `docker cp` reads from), confirmed
regardless of whether the container is running or stopped. `docker
exec` runs INSIDE the container's actual namespace and reads the same
path correctly. Since `docker exec` only works against a RUNNING
container, and the bootstrap previously `exec`'d directly into the
verified command (so the container died the instant it finished), the
bootstrap changed: on a SUCCESSFUL exit only, it now prints its real
exit code as a marker line on stdout and then idles
(`exec tail -f /dev/null`) instead of exiting -- giving
`verifyInDocker` a window to run the integrity check before explicitly
stopping the container itself (the SAME `stopContainerAndWait` already
used for timeout/cancellation). A FAILING exit still terminates the
container immediately, exactly as before this patch -- the added
latency (a `docker exec` round-trip plus the explicit stop's grace
period, ~5-6s measured) is paid ONLY on the path that would otherwise
report `passed`.

### What it checks, and what it deliberately does not

`verifyInputIntegrity` reads the EXPECTED `{path, mode, content}` set
back from the ORIGINAL `candidate.tar` (`readTar`, never a second,
independent re-materialization), and the ACTUAL final state via one
`docker exec <id> node -e <script>` call (a small inline walker,
reusing the image's own `node`). It detects:

- **Changed bytes** -- any tracked file whose content no longer
  matches its original git blob.
- **Added paths** -- any file present in the final input that was not
  part of the materialized candidate.
- **Deleted paths** -- any originally-materialized file missing from
  the final input.
- **Changed executable semantics** -- the exec bit specifically (not
  the full mode, since `chmod -R a-w` legitimately strips write bits
  from everything as part of normal operation).

`/tmp/scratch` (a new, explicitly sanctioned writable directory,
`VERIFY_SCRATCH_DIR` env var) is never inspected -- legitimate output
the verified command writes there can never be mistaken for tampering
with the input. Runs ONLY when the command would otherwise be reported
`passed`; a failing/timed-out/cancelled run already refuses to
checkpoint for its own reason, so running this additionally there
would add latency for no protective benefit.

### Applies to both A and B automatically

`run-task.ts` (A) and `handoff.ts` (B) both call `verifyInDocker`
(via `verifyInDockerWithImageId`) -- this check lives INSIDE that one
shared function, so both paths are covered with zero changes to
either caller. Proven end-to-end: a tracked fixture whose `verify.js`
tampers with a different tracked file and exits 0 prevents BOTH a
successful checkpoint (`tests/integration/git-worker.test.ts`) and a
successful restoration acknowledgement
(`tests/integration/git-worker-handoff.test.ts` -- built from a
hand-constructed "checkpoint" bypassing A's own real run, since A's
own gate would correctly refuse this tree before B could ever receive
it; this specifically proves B's check is an independent second line
of defense, not merely inherited from A's).

### Stated precisely: detection, not prevention

This check inspects the FINAL state only, after the verified command
has exited. It does NOT defend against a hostile command that tampers
with a file and restores it (or never lets the final state reflect the
tamper) BEFORE its own process exits -- a sufficiently well-timed
tamper-then-restore within the verified command's own execution would
not be caught by this check. This remains consistent with this
project's documented local-prototype threat model: the verification
command is fixed and project-controlled, never task-supplied; this is
not a claim of defending against adversarial code that this check
cannot see.

## Cleanup ownership and abandonment rules (Phase 2C.1, Part 2)

### Classifying failure, honestly (Phase 2C correctness patch, Part 1)

The pre-patch `reconcileAbandonedExecution` collapsed EVERY `docker
inspect` failure into "the container is absent" and silently swallowed
every stop/remove error -- a caller could reach `docker create`
believing reconciliation succeeded when the true resource state was
simply unknown. `inspectDocker` now classifies every inspect attempt
into one of four outcomes, confirmed against this project's own real
Docker CLI (never a mock):

```
genuinely absent:     error: no such object: <name>
daemon unreachable:   failed to connect to the docker API at ...
                       (simulated via a deliberately invalid DOCKER_HOST,
                        never by disrupting the real daemon)
anything else:        classified "unknown" -- e.g. a permission error
```

Only a confirmed-absent INITIAL inspect short-circuits as "nothing to
do." If the container genuinely exists, a stop is attempted
(best-effort -- a stop FAILURE alone is never fatal, since a
subsequent FORCED removal can still succeed against a still-running
container) and then a forced removal is attempted (also best-effort).
The FINAL state is THEN independently re-confirmed via the SAME
classification, regardless of what the individual stop/remove calls
reported -- only a confirmed-absent final state returns normally.
Anything else (still present, or still unclassifiable) throws a typed
`ReconciliationFailed`, which `verifyInDocker` maps to
`infrastructure_failure` -- NEVER proceeding to create a replacement
execution as though the prior one's state were known-safe.

### What determines "abandoned," precisely

`reconcileAbandonedExecution(name)` treats a container as abandoned
IFF a container already exists with the EXACT deterministic name
`relaylab-verify-<role>-<taskId>-<attemptId>`
(`deterministicExecutionName`, `src/worker/verification.ts`). Nothing
else -- `buildVerifyCreateArgs` never sets a `--label` on any
container, so there is no shared "owner" tag that could (now or in a
future change) cause a loose match across unrelated executions. This
is sound specifically because this project enforces, by construction,
ONE deterministic `attemptId` per task (`defaultAttemptId`,
`src/worker/run-task.ts`) and relies on the pre-existing epoch/lease
fencing (see "Core correctness invariants" in CLAUDE.md) to ensure only
one legitimate owner acts on a given task at a time -- a pre-existing
container under that EXACT name can only mean "a prior run of this
same logical slot," never "a different, currently-legitimate
concurrent execution." This project does not add, and this
reconciliation mechanism does not attempt, any distributed scheduling
across multiple processes sharing one worker identity; if a future
phase ever introduces that, this exact-name assumption would need
re-examination.

### Proven with a real process crash, not simulated

`tests/integration/git-worker-crash.ts`
"crash-during-container-verification": a real worker process is
started against a deliberately slow fixture (so its container stays
observably alive for several seconds); a structured readiness barrier
(real pid, real container ID, task/attempt identity, owner epoch) is
reported over IPC once the container is independently confirmed
`Running`; the container's state is independently `docker inspect`-ed
(`true`); the worker process is sent a real `SIGKILL` and its exit is
confirmed via Node's own exit event AND independently via
`process.kill(pid, 0)` throwing `ESRCH`; the container is re-inspected
and recorded STILL RUNNING (a real, observed orphan -- killing the
worker process does not, by itself, stop the container it created,
and nothing in this codebase tells Docker to do so). A restarted
worker against the SAME task then publishes exactly one checkpoint
(the interrupted attempt contributed none), and the orphan is
independently confirmed GONE afterward -- removed by that restarted
worker's own `reconcileAbandonedExecution`. Throughout the entire
scenario, a SECOND, independent, concurrently-running container
(a completely different deterministic name, standing in for a
different worker's legitimate execution) is independently confirmed
to remain running, proving the exact-name matching above never
widens its scope.

### Ordinary timeout/cancellation cleanup uses the SAME ownership rules

No separate code path exists for "stop because of a timeout" versus
"stop because of reconciling an abandoned execution" versus "stop
because of an explicit remove" -- `stopContainerAndWait`/
`removeContainer` are the same two functions every path above calls,
each always scoped to one container ID/name passed in by its caller.
There is no `docker system prune`, `docker container prune`, or any
other global cleanup call anywhere in this adapter.

## Manifest compatibility (Phase 2C.1, Part 3 -- verified already correct)

`parseAndValidateGitCheckpointManifestShapeAndDigest`
(`src/db/task-store.ts`) recomputes the digest by calling `fingerprint()`
directly on the PARSED JSON object (`JSON.parse(manifestJson)`), with
no intermediate reconstruction or defaulting step -- so an
authentically v1-shaped manifest (the `verification.containerImageId`
key ABSENT from the object entirely, not merely `null`) recomputes to
the SAME digest it was originally stored under. This was PROVEN, not
just read from the code: a hand-built, genuinely v1-shaped manifest
object (never passed through `buildManifest()`, which cannot produce
this shape at all now that `containerImageId` is a required argument)
was recorded, read back, and successfully REPLAYED under its original
`commandId` -- a normalizing recompute would have produced a digest
mismatch and failed this replay. The schema's own
`containerImageId !== undefined && containerImageId !== null &&
typeof containerImageId !== "string"` check still rejects an invalid
type regardless of `manifestVersion`. A real, v2, container-backed
checkpoint's `verification_image_id` column was confirmed to round-trip
the actual recorded image ID exactly. See
`tests/integration/git-worker-checkpoint-manifest.test.ts` "manifest
compatibility" for the three tests proving this.

## What this does not claim

- **Not VM-grade isolation.** This is Docker container isolation
  (shared host kernel) for a LOCAL PROTOTYPE, explicitly not a claim of
  hypervisor-grade separation between the verified candidate and the
  host.
- **Not hostile-multi-tenant-safe.** The isolation above raises the bar
  against an ACCIDENTALLY misbehaving verification command (writes,
  network calls, runaway resource use) -- it is not represented as
  sufficient to safely run an adversarial, actively-escaping candidate
  from an untrusted third party. No container-escape, kernel-exploit,
  or side-channel analysis was performed.
- **Executable-bit fidelity is now verified (Phase 2C.1) -- the
  Windows-host bind-mount path that used to lose it is no longer
  used.** See "Exact-input fidelity" above for the confirmed defect and
  fix. Not re-verified beyond this project's own fixture repository
  (an arbitrary, much larger or differently-structured tree is
  untested).
- **The read-only verification INPUT is NOT an immutability guarantee
  against the verification command's own code (Phase 2C correctness
  patch).** The extracted copy's `chmod -R a-w` is permission-based,
  and the verifying user owns those files -- see "Actual
  input-protection guarantee" above for the confirmed tamper and the
  rejected privilege-separated alternative. Only the ORIGINAL tar
  (via the kernel-enforced read-only bind mount) is genuinely
  immutable regardless of in-container privilege.
- **`.gitattributes` export-ignore/export-subst no longer affect the
  verification input at all (Phase 2C correctness patch)** -- see
  "Archive-attribute handling" above. Every tracked path/byte is
  delivered as git itself stores it, which is the correct contract for
  "exact-tree verification"; this document's earlier claim that
  applying those attributes was "fidelity" was wrong and has been
  corrected, not merely superseded.
- **Not verified: non-regular-file content types beyond symlinks and
  gitlinks.** Those two are explicitly detected and refused (see
  "Exact-input fidelity"); other unusual tree-entry shapes (e.g. a
  sparse-checkout cone, a `.gitmodules` file whose referenced
  submodule is absent) have not been constructed or tested.
- **Not a general execution sandbox.** The adapter runs exactly ONE
  fixed, hardcoded command (`VERIFICATION_COMMAND_IDENTITY` in
  `verify.ts`) -- a task cannot supply or influence what runs inside
  the container, only which candidate tree is mounted in. This is
  controlled execution, not arbitrary/live command execution.
- **Not orchestration.** One `docker create`/`start`/`stop`/`rm` per
  execution, on the local Docker Desktop daemon. No Kubernetes, no
  Swarm, no multi-host scheduling, no image registry/pull in the
  verification path itself (the image must already exist locally).
