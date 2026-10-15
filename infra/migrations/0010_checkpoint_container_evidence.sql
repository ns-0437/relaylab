-- Phase 2C: records the RESOLVED Docker image ID (never a mutable tag)
-- the verification command actually ran under, alongside the existing
-- flattened checkpoint evidence columns (result_commit, result_tree,
-- manifest_digest, ...) `task_checkpoints` already carries from the
-- SAME manifest -- this is "reuse existing persistence structures"
-- (the full value is already inside `manifest_json` too; this column
-- exists purely for queryability, same reasoning as the other already-
-- flattened columns). Nullable and additive: a manifest built by the
-- host-process adapter (no Docker involved -- still used by tests that
-- don't need a container) has no image at all, and an OLD manifest
-- (Phase 2A-2B.2, before `verification.containerImageId` existed as a
-- manifest field) simply has nothing to flatten here either. Neither
-- case is treated as invalid -- see
-- `parseAndValidateGitCheckpointManifestShapeAndDigest` in
-- src/db/task-store.ts, which accepts the field as optional/nullable.
ALTER TABLE task_checkpoints
  ADD COLUMN verification_image_id VARCHAR(191) NULL AFTER runtime_fingerprint;
