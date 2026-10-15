// Phase 2A.2, section 3: targeted tests for the checkpoint-persistence
// boundary's runtime manifest validation
// (`parseAndValidateGitCheckpointManifest`, src/db/task-store.ts). These
// call `TaskStore.recordGitCheckpoint` directly (real MySQL, real
// transactions) with deliberately malformed/mismatched
// manifestJson/manifestDigest/taskId/attemptId/epoch combinations --
// never through the worker's own happy path, which never produces one of
// these by construction. The point is the STORE's own independent
// defense, not a path the real worker can organically reach.
import { test, before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";
import { Effect } from "effect";
import type mysql from "mysql2/promise";
import type { RowDataPacket } from "mysql2/promise";
import { makeTaskStoreLive } from "../../src/db/task-store.js";
import { applySchema, requireMysqlPool, truncateAll, uniqueId } from "./helpers.js";
import { buildManifest, buildRuntimeFingerprint, computeManifestDigest, manifestToStableJson, type CheckpointManifest } from "../../src/worker/checkpoint.js";
import { makeIsolatedGitEnv } from "../../src/worker/git.js";
import { fingerprint, stableStringify } from "../../src/domain/fingerprint.js";

let pool: mysql.Pool;

before(async () => {
  pool = await requireMysqlPool();
  await applySchema(pool);
});

beforeEach(async () => {
  await truncateAll(pool);
});

after(async () => {
  await pool.end();
});

async function checkpointCount(taskId: string): Promise<number> {
  const [rows] = await pool.query<(RowDataPacket & { c: number })[]>(`SELECT COUNT(*) AS c FROM task_checkpoints WHERE task_id = ?`, [taskId]);
  return Number(rows[0]?.c ?? 0);
}

async function buildValidManifest(taskId: string, epoch: number, attemptId: string): Promise<CheckpointManifest> {
  const env = makeIsolatedGitEnv(process.env.TEMP ?? process.env.TMP ?? "/tmp");
  const runtimeFingerprint = await buildRuntimeFingerprint(env);
  return buildManifest({
    taskId,
    attemptId,
    epoch,
    baseCommit: "a".repeat(40),
    resultCommit: "b".repeat(40),
    resultTree: "c".repeat(40),
    verification: { exitCode: 0, outcome: "passed", stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });
}

test("recordGitCheckpoint rejects a manifestDigest that does not match manifestJson's own recomputed digest", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-digest-mismatch");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  const manifest = await buildValidManifest(taskId, claimResult.epoch, "attempt-1");
  const wrongDigest = "0".repeat(64);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-digest-mismatch",
          attemptId: "attempt-1",
          manifestDigest: wrongDigest,
          manifestJson: manifestToStableJson(manifest),
        }),
      ),
    /does not match the digest recomputed/i,
  );
  assert.equal(await checkpointCount(taskId), 0, "a digest/content mismatch must never be persisted");
});

test("recordGitCheckpoint rejects a manifest whose own taskId disagrees with the request's taskId", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-taskid-mismatch");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  // Build a manifest claiming a DIFFERENT taskId than the one this
  // request is about to be fenced against.
  const manifest = await buildValidManifest("a-completely-different-task", claimResult.epoch, "attempt-1");
  const digest = computeManifestDigest(manifest);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-taskid-mismatch",
          attemptId: "attempt-1",
          manifestDigest: digest,
          manifestJson: manifestToStableJson(manifest),
        }),
      ),
    /manifest\.taskId .* does not match the request's taskId/i,
  );
  assert.equal(await checkpointCount(taskId), 0);
});

test("recordGitCheckpoint rejects a manifest whose own attemptId disagrees with the request's attemptId", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-attemptid-mismatch");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  const manifest = await buildValidManifest(taskId, claimResult.epoch, "attempt-OTHER");
  const digest = computeManifestDigest(manifest);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-attemptid-mismatch",
          attemptId: "attempt-1", // disagrees with the manifest's own "attempt-OTHER"
          manifestDigest: digest,
          manifestJson: manifestToStableJson(manifest),
        }),
      ),
    /manifest\.attemptId .* does not match the request's attemptId/i,
  );
  assert.equal(await checkpointCount(taskId), 0);
});

test("recordGitCheckpoint rejects a manifest whose own epoch disagrees with the request's (genuinely new, non-replay) epoch", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-epoch-mismatch");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  // Manifest claims an epoch one higher than what this (first-ever,
  // non-replay) request is actually fenced against.
  const manifest = await buildValidManifest(taskId, claimResult.epoch + 1, "attempt-1");
  const digest = computeManifestDigest(manifest);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-epoch-mismatch",
          attemptId: "attempt-1",
          manifestDigest: digest,
          manifestJson: manifestToStableJson(manifest),
        }),
      ),
    /manifest\.epoch .* does not match the request's epoch/i,
  );
  assert.equal(await checkpointCount(taskId), 0);
});

test("recordGitCheckpoint rejects malformed manifestJson outright", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-malformed-json");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-malformed-json",
          attemptId: "attempt-1",
          manifestDigest: "0".repeat(64),
          manifestJson: "{ this is not valid JSON",
        }),
      ),
    /not valid JSON/i,
  );
  assert.equal(await checkpointCount(taskId), 0);
});

test("recordGitCheckpoint rejects a manifest whose own verification.outcome is \"failed\" -- a checkpoint may never be recorded for a non-passing verification", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-failed-verification");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  const env = makeIsolatedGitEnv(process.env.TEMP ?? process.env.TMP ?? "/tmp");
  const runtimeFingerprint = await buildRuntimeFingerprint(env);
  const manifest = buildManifest({
    taskId,
    attemptId: "attempt-1",
    epoch: claimResult.epoch,
    baseCommit: "a".repeat(40),
    resultCommit: "b".repeat(40),
    resultTree: "c".repeat(40),
    verification: { exitCode: 1, outcome: "failed", stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 },
    runtimeFingerprint,
    containerImageId: null,
  });
  const digest = computeManifestDigest(manifest);

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({
          taskId,
          ownerId: "worker-a",
          epoch: claimResult.epoch,
          commandId: "ckpt-failed-verification",
          attemptId: "attempt-1",
          manifestDigest: digest,
          manifestJson: manifestToStableJson(manifest),
        }),
      ),
    /must be "passed"/i,
  );
  assert.equal(await checkpointCount(taskId), 0);
});

test("Phase 2B Part A: a replay supplying the ORIGINAL (already-committed) manifestDigest together with ALTERED manifestJson content is rejected, not silently accepted as an identical retry", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-replay-altered-content");
  const claimResult = await Effect.runPromise(store.claimTask(taskId, "worker-a", 120_000));
  const manifest = await buildValidManifest(taskId, claimResult.epoch, "attempt-1");
  const digest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);

  const first = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-a", epoch: claimResult.epoch, commandId: "ckpt-altered-replay", attemptId: "attempt-1", manifestDigest: digest, manifestJson }),
  );
  assert.equal(first.replay, false);

  // Retry with the SAME commandId and the SAME (already-committed, and
  // therefore fingerprint-matching) manifestDigest, but a manifestJson
  // whose content has been tampered with -- e.g. claiming a different
  // resultTree -- so it no longer actually hashes to that digest. The
  // `fp` fingerprint check alone (over `{type:"Checkpoint", digest}`)
  // cannot catch this, because it never looks at manifestJson's content
  // at all; this is exactly the Part A gap this test exists to close.
  const tamperedManifest: CheckpointManifest = { ...manifest, resultTree: "f".repeat(40) };
  const tamperedJson = manifestToStableJson(tamperedManifest);
  assert.notEqual(tamperedJson, manifestJson, "test setup check: the tampered JSON must actually differ from the original");

  await assert.rejects(
    () =>
      Effect.runPromise(
        store.recordGitCheckpoint({ taskId, ownerId: "worker-a", epoch: claimResult.epoch, commandId: "ckpt-altered-replay", attemptId: "attempt-1", manifestDigest: digest, manifestJson: tamperedJson }),
      ),
    /does not match the digest recomputed/i,
  );
  assert.equal(await checkpointCount(taskId), 1, "still exactly the ORIGINAL checkpoint row -- the tampered retry must not have been accepted OR persisted");
});

test("recordGitCheckpoint replay semantics are preserved after ownership changes: the ORIGINAL owner's retry succeeds and returns the original receipt, even with a now-stale epoch", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-checkpoint-replay-after-ownership-change");

  // 1. Worker A owns the task and publishes a checkpoint.
  const claimA = await Effect.runPromise(store.claimTask(taskId, "worker-a", 150));
  const manifest = await buildValidManifest(taskId, claimA.epoch, "attempt-1");
  const digest = computeManifestDigest(manifest);
  const manifestJson = manifestToStableJson(manifest);
  const first = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-a", epoch: claimA.epoch, commandId: "ckpt-replay", attemptId: "attempt-1", manifestDigest: digest, manifestJson }),
  );
  assert.equal(first.replay, false);

  // 2. Ownership transfers to B (A's lease expires, B takes over --
  //    epoch increments).
  await new Promise((r) => setTimeout(r, 220));
  const claimB = await Effect.runPromise(store.claimTask(taskId, "worker-b", 10_000));
  assert.ok(claimB.epoch > claimA.epoch, "takeover must increment epoch");

  // 3. A retries the EXACT original publish, still believing its own
  //    (now stale) epoch -- this is the precise case the manifest's own
  //    epoch cross-check must NOT break: the replay lookup must return
  //    the original receipt BEFORE that cross-check ever runs.
  const second = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-a", epoch: claimA.epoch, commandId: "ckpt-replay", attemptId: "attempt-1", manifestDigest: digest, manifestJson }),
  );
  assert.equal(second.replay, true, "the original owner's retry after an ownership change must succeed as a replay, not fail on stale epoch/manifest cross-checks");
  assert.deepEqual(second.result, first.result);
  assert.equal(await checkpointCount(taskId), 1, "exactly one checkpoint row, despite the retry");
});

// Phase 2C.1, Part 3: manifest v1/v2 compatibility. A manifest built
// under MANIFEST_VERSION 1 (before `verification.containerImageId`
// existed) has that key ABSENT from its JSON object entirely -- NOT
// "present and null." Every OTHER test file in this repo that needed a
// `buildManifest()` call site to keep working after Phase 2C simply
// added `containerImageId: null` to an otherwise-current call -- which
// produces a "v2, host-process-adapter" shape, a materially DIFFERENT
// JSON object than an authentic v1 artifact. These tests construct the
// genuinely-absent shape BY HAND (never through `buildManifest()`,
// which always produces the CURRENT, v2 shape) to actually exercise
// legacy compatibility, per this patch's own instruction that adding
// `containerImageId: null` to an old fixture does not test this.

/**
 * A hand-built, AUTHENTICALLY v1-shaped manifest object -- the
 * `verification` sub-object has no `containerImageId` key at all,
 * exactly as a real manifest built before Phase 2C's code existed
 * would look. `fingerprint`/`stableStringify` (the exact functions
 * `computeManifestDigest`/`manifestToStableJson` delegate to) are
 * called directly on this PLAIN OBJECT -- never through
 * `buildManifest()`, which cannot produce this shape at all now that
 * `containerImageId` is a required argument.
 */
function buildLegacyV1Manifest(args: { taskId: string; attemptId: string; epoch: number; resultCommit: string; resultTree: string }) {
  return {
    manifestVersion: 1,
    taskId: args.taskId,
    attemptId: args.attemptId,
    epoch: args.epoch,
    baseCommit: "a".repeat(40),
    resultCommit: args.resultCommit,
    resultTree: args.resultTree,
    verification: {
      command: { executable: "node", args: ["verify.js"], version: 1 },
      outcome: "passed" as const,
      exitCode: 0,
      durationMs: 1,
      // Deliberately NO `containerImageId` key -- this IS the point.
    },
    runtimeFingerprint: { nodeVersion: "v22.0.0", execPath: "/usr/bin/node", platform: "linux", gitVersion: "git version 2.40.0" },
  };
}

test("manifest compatibility: an authentically v1-shaped manifest (containerImageId key ABSENT, not merely null) is accepted, and its digest is NOT recomputed via a normalizing v2 representation", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-legacy-v1-manifest");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-legacy", 120_000));
  const legacy = buildLegacyV1Manifest({ taskId, attemptId: "attempt-1", epoch: claim.epoch, resultCommit: "b".repeat(40), resultTree: "c".repeat(40) });

  // Computed the SAME way a real pre-Phase-2C `computeManifestDigest`/
  // `manifestToStableJson` call would have, directly against the
  // authentic v1 object -- no normalization, no added field.
  const legacyDigest = fingerprint(legacy);
  const legacyJson = stableStringify(legacy);
  assert.doesNotMatch(legacyJson, /containerImageId/, "test setup check: the legacy manifest JSON must not mention containerImageId AT ALL -- present-but-null is a different bug than absent");

  const first = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-legacy", epoch: claim.epoch, commandId: "ckpt-legacy-v1", attemptId: "attempt-1", manifestDigest: legacyDigest, manifestJson: legacyJson }),
  );
  assert.equal(first.replay, false, "a genuinely new, authentically v1-shaped manifest must be accepted, not rejected merely for lacking a field that did not exist at manifestVersion 1");
  assert.equal(await checkpointCount(taskId), 1);

  // `manifest_json` is a native MySQL `JSON` column
  // (infra/migrations/0007_task_checkpoints.sql) -- the mysql2 driver
  // auto-parses it into a plain JS object/array on read, NEVER a raw
  // JSON string, so the shape is checked by property presence below,
  // not by matching a regex against (what would otherwise silently be)
  // `String(someObject)` == `"[object Object]"`.
  const [rows] = await pool.query<(RowDataPacket & { manifest_json: Record<string, unknown>; verification_image_id: string | null; manifest_version: number })[]>(
    `SELECT manifest_json, verification_image_id, manifest_version FROM task_checkpoints WHERE task_id = ?`,
    [taskId],
  );
  assert.equal(rows[0]!.manifest_version, 1, "the PERSISTED manifest_version column must reflect the authentic v1 value, not be silently upgraded to 2");
  assert.equal(rows[0]!.verification_image_id, null, "a v1 manifest has no image evidence at all -- the flattened column must be null, never defaulted to a misleading value");
  const persistedVerification = rows[0]!.manifest_json.verification as Record<string, unknown>;
  assert.equal(
    "containerImageId" in persistedVerification,
    false,
    "the PERSISTED manifest_json must still be the authentic v1 shape -- the key must not have been silently added anywhere in the write path",
  );

  // The decisive proof that the digest was NOT recomputed via some
  // normalizing v2 representation: a REPLAY of this exact commandId
  // must succeed (re-validating shape+digest unconditionally, replay
  // or not, per Phase 2B Part A) -- if the recompute path normalized
  // the object before hashing, this would fail with a digest mismatch.
  const replay = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-legacy", epoch: claim.epoch, commandId: "ckpt-legacy-v1", attemptId: "attempt-1", manifestDigest: legacyDigest, manifestJson: legacyJson }),
  );
  assert.equal(replay.replay, true, "replaying the authentic v1 manifest must succeed -- a normalizing recompute would have produced a digest mismatch here");
  assert.equal(await checkpointCount(taskId), 1, "still exactly one row after the replay");
});

test("manifest compatibility: a v1-shaped manifest with an INVALID containerImageId type is still rejected per the documented schema", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-legacy-v1-bad-image-id");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-legacy", 120_000));
  const legacy = buildLegacyV1Manifest({ taskId, attemptId: "attempt-1", epoch: claim.epoch, resultCommit: "b".repeat(40), resultTree: "c".repeat(40) });
  // A malformed value of the WRONG TYPE (a number, not a string/null/
  // omitted) -- the documented schema
  // (`parseAndValidateGitCheckpointManifestShapeAndDigest`,
  // src/db/task-store.ts) must still reject this, regardless of
  // manifestVersion.
  const malformed = { ...legacy, verification: { ...legacy.verification, containerImageId: 12345 } };
  const digest = fingerprint(malformed);
  const json = stableStringify(malformed);

  await assert.rejects(
    () => Effect.runPromise(store.recordGitCheckpoint({ taskId, ownerId: "worker-legacy", epoch: claim.epoch, commandId: "ckpt-legacy-bad-image", attemptId: "attempt-1", manifestDigest: digest, manifestJson: json })),
    /containerImageId.*must be a string, null, or omitted/i,
  );
  assert.equal(await checkpointCount(taskId), 0, "a manifest with an invalid containerImageId type must never be persisted, v1 or not");
});

test("manifest compatibility: a newly recorded, container-backed (v2) checkpoint correctly identifies the ACTUAL image used, read back from the flattened column", async () => {
  const store = makeTaskStoreLive(pool);
  const taskId = uniqueId("task-v2-real-image");
  const claim = await Effect.runPromise(store.claimTask(taskId, "worker-v2", 120_000));
  const realLookingImageId = "sha256:" + "d5b6f0ab8be6a548ca1628b8bc6ea823b283b399e078f34f25e2ad2d1a8211f".padEnd(64, "0");
  const manifest = await buildValidManifest(taskId, claim.epoch, "attempt-1");
  const v2Manifest: CheckpointManifest = { ...manifest, verification: { ...manifest.verification, containerImageId: realLookingImageId } };
  const digest = computeManifestDigest(v2Manifest);
  const json = manifestToStableJson(v2Manifest);

  const result = await Effect.runPromise(
    store.recordGitCheckpoint({ taskId, ownerId: "worker-v2", epoch: claim.epoch, commandId: "ckpt-v2-real-image", attemptId: "attempt-1", manifestDigest: digest, manifestJson: json }),
  );
  assert.equal(result.replay, false);

  const [rows] = await pool.query<(RowDataPacket & { verification_image_id: string | null; manifest_version: number })[]>(
    `SELECT verification_image_id, manifest_version FROM task_checkpoints WHERE task_id = ?`,
    [taskId],
  );
  assert.equal(rows[0]!.manifest_version, 2);
  assert.equal(rows[0]!.verification_image_id, realLookingImageId, "the flattened verification_image_id column must identify the ACTUAL image the manifest recorded, read back exactly");
});
