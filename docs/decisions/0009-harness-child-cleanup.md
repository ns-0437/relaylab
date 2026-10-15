# 0009: A shared, registered, bounded-wait child-process harness

## What actually happened

A 2026-10-12 session's integration test run (`npm run test:integration`,
wrapped by this assistant's own tool-level `timeout` command) exceeded
that wrapper's foreground wait and was reported as "moved to the
background." The command was never explicitly re-awaited or killed
afterward. Roughly ten hours later, three Node processes from that exact
invocation were still alive: the `npm run test:integration` shell, the
`node --test` runner it started, and one per-file `node:test` subprocess
for `consumer-dedup.test.ts`. All three were positively identified before
termination via full command line and parent-process-ID chain (not PID
alone, since PIDs can be reused), confirmed to share the same creation
timestamp, and terminated with `Stop-Process -Force`, then independently
confirmed gone via a follow-up `Get-CimInstance` query returning no
results for those PIDs.

## Root cause, and what this fix does and does not address

The actual failure was at the ORCHESTRATION layer (this assistant's own
tool wrapper gave up waiting without killing the process tree it had
started) -- no amount of in-process `finally`/`after()` discipline inside
the test files themselves can fix a problem that happens entirely OUTSIDE
those files' own process lifetime. That class of issue is out of scope
for this repository's code to solve.

What IS in scope, and what this decision fixes: within one run of a
test/demo that spawns its OWN RelayLab child processes
(publisher-main.ts/consumer-main.ts instances started FROM a test, not
the test process itself), make sure every one of THOSE children is
reliably cleaned up regardless of how the test ends. Before this fix,
four files (`crash-windows.ts`, `recovery-scenario.ts`,
`publisher-leadership.test.ts`, `shutdown.test.ts`) each maintained their
own slightly different `spawned: ChildProcess[]` array and manual
SIGKILL-everything-in-`after()` logic -- which has gaps: a child spawned
mid-scenario but never pushed to the array before an exception is thrown
would never be tracked at all, and `after()` alone does not run for a
standalone script like `crash-windows.ts` (it is not a `node:test` file).

## Decision

A single shared module, `tests/integration/process-harness.ts`
(`ProcessHarness`), used by all four call sites:

- **Register on spawn, not on readiness.** `spawn()` adds the child to a
  tracked `Set` before returning, so a child that crashes or exits before
  ever reporting readiness is still in scope for cleanup.
- **Graceful first.** `stopAll()` sends an IPC `"shutdown"` message (the
  real mechanism `publisher-main.ts`/`consumer-main.ts` listen for --
  added in the Phase 1.1 audit specifically because `child.kill("SIGTERM")`
  does not deliver a catchable signal on this Windows machine).
- **Bounded wait, then force.** Waits up to `gracefulTimeoutMs` (default
  3000ms) for the child to exit on its own; only sends `SIGKILL` if it is
  still alive after that.
- **Always await real exit.** Never resolves `stopOne` until the
  child's own `exit` event has fired, whether that was via graceful
  shutdown or `SIGKILL`.
- **Listeners cleaned up on exit**, not left attached: `child.stdout`/
  `child.stderr` data listeners are removed in the same `exit` handler
  that resolves the tracking promise.
- **Idempotent, never throws.** Safe to call from a `finally` without a
  second `try/catch`, and safe to call on an empty set (a scenario that
  threw before spawning anything).
- **Every call site now wraps its scenario/test body in `try { ... }
  finally { await harness.stopAll(); }`**, so cleanup runs on success,
  on an assertion failure, on a startup failure (the child never sent
  readiness), and on a timeout (the `waitForFailpointReady`/`waitFor`
  helpers' own timeout rejections are caught by the same `finally`).

## Alternatives rejected

- **A single process-wide `process.on("exit", ...)` handler that kills
  everything**: `process.exit` runs synchronously and cannot `await` a
  child's real exit, so it cannot fulfill "await actual exit" -- it can
  only fire-and-hope a kill signal lands before the parent process itself
  disappears. Explicit, awaited cleanup in `finally` is strictly stronger.
- **`tree-kill` or an OS-level process-group kill**: unnecessary extra
  dependency for a known, bounded set of direct children this code itself
  spawned; `fork()` already gives a direct handle to each one.
