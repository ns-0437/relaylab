import { test } from "node:test";
import assert from "node:assert/strict";
import { applyCommand, newTask, DomainRejection } from "../../src/domain/domain.js";

test("Checkpoint records digest and bumps seq", () => {
  const t0 = newTask("t1");
  const { state, events } = applyCommand(t0, { type: "Checkpoint", digest: "abc123" });
  assert.equal(state.checkpointDigest, "abc123");
  assert.equal(state.seq, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0]?.type, "CheckpointRecorded");
});

test("RecordVerification for a stale candidate is rejected", () => {
  let t = newTask("t1");
  t = applyCommand(t, { type: "SubmitCandidate", candidateId: "c1" }).state;
  // simulate a newer candidate superseding c1
  t = applyCommand(t, { type: "SubmitCandidate", candidateId: "c2" }).state;
  assert.throws(
    () => applyCommand(t, { type: "RecordVerification", candidateId: "c1", passed: true }),
    DomainRejection,
  );
});

test("RecordVerification for the current candidate succeeds and increments attempt", () => {
  let t = newTask("t1");
  t = applyCommand(t, { type: "SubmitCandidate", candidateId: "c1" }).state;
  const r1 = applyCommand(t, { type: "RecordVerification", candidateId: "c1", passed: false });
  assert.equal(r1.state.verificationStatus, "failed");
  assert.equal(r1.state.verificationAttempt, 1);
  const r2 = applyCommand(r1.state, { type: "RecordVerification", candidateId: "c1", passed: true });
  assert.equal(r2.state.verificationStatus, "passed");
  assert.equal(r2.state.verificationAttempt, 2);
});

test("Complete requires a passed verification", () => {
  let t = newTask("t1");
  t = applyCommand(t, { type: "SubmitCandidate", candidateId: "c1" }).state;
  assert.throws(() => applyCommand(t, { type: "Complete" }), DomainRejection);
  t = applyCommand(t, { type: "RecordVerification", candidateId: "c1", passed: true }).state;
  const { state } = applyCommand(t, { type: "Complete" });
  assert.equal(state.status, "completed");
});
