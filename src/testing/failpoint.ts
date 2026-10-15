// Explicit, opt-in failpoints for crash-window tests (section 6 of the
// Phase 1.1 audit). Disabled by default: `hitFailpoint` is a no-op unless
// the specific RELAYLAB_FAILPOINT env var names this exact point, so
// production code paths are completely unaffected. When it DOES match,
// the process sends a readiness message over IPC (requires the child to
// have been spawned with an 'ipc' stdio channel) and then hangs forever
// -- the orchestrating test is the one that actually terminates the
// process (a real SIGKILL on a real OS process), at the EXACT moment it
// confirmed the process reached this point, instead of guessing with a
// sleep.
/**
 * `extra` (Phase 2A.1): an optional, JSON-serializable payload merged
 * into the IPC readiness message -- used by the Git worker's crash test
 * to carry the ACTUAL result commit SHA/tree ID the worker observed at
 * the exact failpoint moment, so the orchestrator can assert the
 * post-restart state against a real, captured value instead of just
 * "the process reported ready." Every existing caller (publisher.ts,
 * consumer.ts) omits it and is unaffected.
 */
export async function hitFailpoint(name: string, extra?: Record<string, unknown>): Promise<void> {
  if (process.env.RELAYLAB_FAILPOINT !== name) return;
  // `pid` (Phase 2B.1): included unconditionally so an orchestrator can
  // independently verify the EXACT OS process it is about to terminate
  // is the one that reported readiness, rather than inferring it only
  // from which `ManagedChild` object the IPC message arrived on.
  if (typeof process.send === "function") {
    process.send({ type: "failpoint-ready", name, pid: process.pid, ...extra });
  } else {
    console.error(`[failpoint] RELAYLAB_FAILPOINT=${name} set but process has no IPC channel to report readiness on`);
  }
  // Phase 2B.1: a real bug this patch found and fixed -- a bare
  // `await new Promise(() => {})` holds NO active libuv handle of its
  // own. If nothing ELSE in this process happens to have an open
  // handle at this exact moment (no live DB connection, no pending
  // timer, no open socket), Node's event loop correctly concludes there
  // is nothing left to do and exits NATURALLY -- with an ordinary (0)
  // exit code -- despite this function never "returning" in any JS
  // sense. Confirmed directly: `src/restore-worker-main.ts`'s call path
  // never opens a real MySQL connection before THIS failpoint
  // (`recordRestoreAcknowledgement`, its only DB call, runs AFTER
  // verification, which runs AFTER this failpoint) -- unlike
  // `worker-main.ts`'s flow, which claims the task over a real DB
  // connection BEFORE any of its own failpoints, so it always had an
  // incidental handle keeping it alive. Relying on an INCIDENTAL handle
  // from unrelated code is exactly the fragility that produced the
  // misleading `{exitCode: 0, signalCode: null}` observed on this
  // Windows host for the restore-worker crash scenario. Fixed with an
  // EXPLICIT, permanent active handle, independent of whatever else
  // this process does or does not have open -- intentionally never
  // cleared, since this function is never meant to return.
  setInterval(() => {}, 60_000);
  // Hang here. The orchestrator SIGKILLs this process once it receives
  // the readiness message above; this promise is never meant to resolve.
  await new Promise(() => {});
}
