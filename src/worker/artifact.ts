// Phase 2B, Part D: a portable, SELF-CONTAINED Git bundle artifact --
// committed Git state ONLY (the bundled revision plus every ancestor
// reachable from it; never staged/dirty/untracked files, which do not
// exist as Git objects to bundle in the first place). `git bundle
// create <path> <singleRevision>` (no negative/exclude ref) is git's
// own documented way to produce a bundle with NO prerequisites -- the
// destination never needs ANY object A already has, no Git alternates,
// no shared-object clone option. Chosen over a tarball/zip of a
// worktree specifically because a bundle's integrity is independently
// checkable by `git bundle verify` (object graph completeness,
// corruption) BEFORE any object is ever extracted from it, which a
// plain archive of files cannot offer.
import { createHash } from "node:crypto";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { git, currentCommit, currentTree, hasUncommittedChanges } from "./git.js";
import { VERIFICATION_COMMAND_IDENTITY } from "./verify.js";

export const ARTIFACT_FORMAT_VERSION = 1;

export interface HandoffArtifactManifest {
  formatVersion: number;
  taskId: string;
  handoffId: string;
  sourceCheckpoint: {
    commandId: string;
    manifestDigest: string;
  };
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  resultRef: string;
  artifactDigest: string;
  artifactSizeBytes: number;
  verification: { executable: string; args: string[]; version: number };
  createdAtIso: string;
}

function sha256File(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * Writes `bytes` ATOMICALLY: a temp file in the same directory as
 * `finalPath` (so the rename below is same-filesystem and therefore
 * atomic), fsync-free rename to the final, immutable, digest-addressed
 * path. Never makes a partially-written file visible at `finalPath` --
 * a reader either sees nothing there yet, or the COMPLETE file.
 */
async function writeFileAtomic(finalPath: string, bytes: Buffer | string): Promise<void> {
  const dir = path.dirname(finalPath);
  await mkdir(dir, { recursive: true });
  const tmpPath = path.join(dir, `.tmp-${randomUUID()}`);
  await writeFile(tmpPath, bytes);
  await rename(tmpPath, finalPath);
}

/**
 * Phase 2B, Part D: builds the one artifact this milestone transfers --
 * a self-contained bundle of `resultCommit` (and its full ancestry) from
 * `worktreeDir`, plus a manifest describing it. Both are written to a
 * TEMP path first, the bundle is validated (`git bundle verify`) against
 * that temp path, and ONLY THEN are both promoted (atomic rename) to
 * their final, digest-addressed, immutable identity -- an incomplete or
 * invalid artifact is never made visible at a path a caller could
 * mistake for a finished one.
 */
export async function createHandoffArtifact(args: {
  taskId: string;
  handoffId: string;
  worktreeDir: string;
  env: NodeJS.ProcessEnv;
  /**
   * The worktree's own branch name (e.g. `branchNameFor(taskId)`) whose
   * tip equals `resultCommit`. `git bundle create` needs a NAMED ref,
   * not a bare commit SHA -- passing a raw SHA fails with git's own
   * "fatal: Refusing to create empty bundle" (confirmed directly against
   * this host's real git binary; a bundle's header stores ref-name ->
   * object-id pairs, and a bare SHA has no name for git to record).
   */
  resultRef: string;
  sourceCheckpointCommandId: string;
  sourceManifestDigest: string;
  baseCommit: string;
  resultCommit: string;
  resultTree: string;
  outDir: string;
}): Promise<{ artifactPath: string; manifestPath: string; manifest: HandoffArtifactManifest }> {
  await mkdir(args.outDir, { recursive: true });
  const tmpBundlePath = path.join(args.outDir, `.tmp-${randomUUID()}.bundle`);

  await git(["bundle", "create", tmpBundlePath, args.resultRef], args.worktreeDir, args.env);
  // Validate BEFORE this is trusted as a finished artifact -- a
  // malformed/incomplete bundle must never reach the "promoted" state.
  await git(["bundle", "verify", tmpBundlePath], args.worktreeDir, args.env);

  const bytes = await readFile(tmpBundlePath);
  const artifactDigest = sha256File(bytes);
  const artifactSizeBytes = bytes.length;

  const manifest: HandoffArtifactManifest = {
    formatVersion: ARTIFACT_FORMAT_VERSION,
    taskId: args.taskId,
    handoffId: args.handoffId,
    sourceCheckpoint: { commandId: args.sourceCheckpointCommandId, manifestDigest: args.sourceManifestDigest },
    baseCommit: args.baseCommit,
    resultCommit: args.resultCommit,
    resultTree: args.resultTree,
    resultRef: args.resultRef,
    artifactDigest,
    artifactSizeBytes,
    verification: { executable: VERIFICATION_COMMAND_IDENTITY.executable, args: [...VERIFICATION_COMMAND_IDENTITY.argsTemplate], version: VERIFICATION_COMMAND_IDENTITY.version },
    createdAtIso: new Date().toISOString(),
  };

  const artifactPath = path.join(args.outDir, `${artifactDigest}.bundle`);
  const manifestPath = path.join(args.outDir, `${artifactDigest}.manifest.json`);
  await writeFileAtomic(artifactPath, bytes);
  await writeFileAtomic(manifestPath, JSON.stringify(manifest, null, 2));
  await rm(tmpBundlePath, { force: true });

  return { artifactPath, manifestPath, manifest };
}

const SHA_RE = /^[0-9a-f]{40}$/;
const DIGEST_RE = /^[0-9a-f]{64}$/;

function assertManifestShape(value: unknown): HandoffArtifactManifest {
  if (typeof value !== "object" || value === null) throw new Error("handoff artifact manifest must be a JSON object");
  const m = value as Record<string, unknown>;
  const fail = (field: string, detail: string): never => {
    throw new Error(`handoff artifact manifest field "${field}" ${detail}`);
  };
  if (m.formatVersion !== ARTIFACT_FORMAT_VERSION) fail("formatVersion", `must be ${ARTIFACT_FORMAT_VERSION}`);
  if (typeof m.taskId !== "string" || m.taskId.length === 0) fail("taskId", "must be a non-empty string");
  if (typeof m.handoffId !== "string" || m.handoffId.length === 0) fail("handoffId", "must be a non-empty string");
  const sc = m.sourceCheckpoint as Record<string, unknown> | undefined;
  if (typeof sc !== "object" || sc === null || typeof sc.commandId !== "string" || typeof sc.manifestDigest !== "string") {
    fail("sourceCheckpoint", "must be an object with string commandId/manifestDigest");
  }
  if (typeof m.baseCommit !== "string" || !SHA_RE.test(m.baseCommit)) fail("baseCommit", "must be a 40-character commit SHA");
  if (typeof m.resultCommit !== "string" || !SHA_RE.test(m.resultCommit)) fail("resultCommit", "must be a 40-character commit SHA");
  if (typeof m.resultTree !== "string" || !SHA_RE.test(m.resultTree)) fail("resultTree", "must be a 40-character tree SHA");
  if (typeof m.resultRef !== "string" || m.resultRef.length === 0) fail("resultRef", "must be a non-empty string");
  if (typeof m.artifactDigest !== "string" || !DIGEST_RE.test(m.artifactDigest)) fail("artifactDigest", "must be a 64-character sha256 hex digest");
  if (typeof m.artifactSizeBytes !== "number" || m.artifactSizeBytes <= 0) fail("artifactSizeBytes", "must be a positive number");
  return m as unknown as HandoffArtifactManifest;
}

/**
 * Phase 2B, Part D: validates BEFORE consuming the artifact's bytes for
 * anything else -- size first (cheap, catches gross truncation
 * instantly), then the full content digest (catches any corruption size
 * alone would miss), both checked against the manifest's OWN claimed
 * values -- never against a value supplied separately by whoever is
 * asking to restore it. A mismatch on EITHER throws before any `git`
 * command ever touches the artifact file.
 */
export async function validateHandoffArtifact(artifactPath: string, manifestPath: string): Promise<HandoffArtifactManifest> {
  let manifestRaw: unknown;
  try {
    manifestRaw = JSON.parse(await readFile(manifestPath, "utf8"));
  } catch (err) {
    throw new Error(`handoff artifact manifest at "${manifestPath}" is not valid JSON: ${(err as Error).message}`);
  }
  const manifest = assertManifestShape(manifestRaw);

  const st = await stat(artifactPath);
  if (st.size !== manifest.artifactSizeBytes) {
    throw new Error(
      `handoff artifact at "${artifactPath}" has size ${st.size} bytes, but its manifest claims ${manifest.artifactSizeBytes} bytes -- refusing to consume a size-mismatched (truncated or corrupt) artifact`,
    );
  }
  const bytes = await readFile(artifactPath);
  const actualDigest = sha256File(bytes);
  if (actualDigest !== manifest.artifactDigest) {
    throw new Error(
      `handoff artifact at "${artifactPath}" has content digest "${actualDigest}", but its manifest claims "${manifest.artifactDigest}" -- refusing to consume a corrupt, truncated, or mismatched artifact`,
    );
  }
  return manifest;
}

export interface RestoreResult {
  restoredCommit: string;
  restoredTree: string;
}

/**
 * Phase 2B, Part D/E: restores an ALREADY-VALIDATED artifact into a
 * fresh, independent Git repository at `destRepoDir` -- never A's
 * worktree, never A's object store, no Git alternates, no
 * shared-object clone option. `destRepoDir` must not already exist
 * with unexpected contents (Part E: "do not overwrite an unexpected
 * destination directory") -- checked and rejected, untouched, before
 * anything is created.
 */
export async function restoreHandoffArtifact(args: { artifactPath: string; manifest: HandoffArtifactManifest; destRepoDir: string; env: NodeJS.ProcessEnv }): Promise<RestoreResult> {
  const { artifactPath, manifest, destRepoDir, env } = args;

  const existing = await stat(destRepoDir).catch(() => null);
  if (existing) {
    if (!existing.isDirectory()) {
      throw new Error(`restoreHandoffArtifact: destination "${destRepoDir}" exists and is not a directory -- refusing to overwrite unexpected contents`);
    }
    const { readdir } = await import("node:fs/promises");
    const entries = await readdir(destRepoDir);
    if (entries.length > 0) {
      // Idempotent-retry support (Part C: "support at least one
      // documented retry/recovery path when B fails after ownership
      // transfer"): a non-empty destination is NOT automatically
      // rejected if it is ALREADY the correctly-restored result of a
      // PRIOR call for this exact artifact -- positively confirmed (real
      // HEAD/tree equality against the manifest, AND clean), never
      // inferred from "a .git directory exists." Anything else -- any
      // mismatch, any error reading it, any dirt -- falls through to the
      // same explicit rejection as a genuinely unexpected directory.
      if (entries.includes(".git")) {
        const alreadyRestored = await (async () => {
          try {
            const head = await currentCommit(destRepoDir, env);
            const tree = await currentTree(destRepoDir, env);
            const dirty = await hasUncommittedChanges(destRepoDir, env);
            return !dirty && head === manifest.resultCommit && tree === manifest.resultTree;
          } catch {
            return false;
          }
        })();
        if (alreadyRestored) {
          return { restoredCommit: manifest.resultCommit, restoredTree: manifest.resultTree };
        }
      }
      throw new Error(
        `restoreHandoffArtifact: destination "${destRepoDir}" already exists and is NOT empty (contains: ${entries.join(", ")}), and is not already a correct restoration of this exact artifact -- refusing to overwrite unexpected destination contents`,
      );
    }
  } else {
    await mkdir(destRepoDir, { recursive: true });
  }

  await git(["init", "--initial-branch=restored"], destRepoDir, env);
  // Re-verify in the DESTINATION's own context too -- defense in depth,
  // independent of whatever validation already ran at the source side.
  await git(["bundle", "verify", artifactPath], destRepoDir, env);
  await git(["fetch", artifactPath, manifest.resultRef], destRepoDir, env);

  const fetchedCommit = (await git(["rev-parse", "FETCH_HEAD"], destRepoDir, env)).stdout.trim();
  if (fetchedCommit !== manifest.resultCommit) {
    throw new Error(`restoreHandoffArtifact: fetched commit "${fetchedCommit}" does not match the manifest's claimed resultCommit "${manifest.resultCommit}"`);
  }
  // Explicit Git-object-availability proof, independent of (and before)
  // the checkout below: `cat-file -e` confirms the object exists and is
  // reachable in THIS repo's own, freshly-populated object store.
  await git(["cat-file", "-e", `${manifest.resultCommit}^{commit}`], destRepoDir, env);

  await git(["checkout", manifest.resultCommit], destRepoDir, env);

  const restoredCommit = await currentCommit(destRepoDir, env);
  const restoredTree = await currentTree(destRepoDir, env);
  if (restoredCommit !== manifest.resultCommit) {
    throw new Error(`restoreHandoffArtifact: restored HEAD "${restoredCommit}" does not equal the manifest's resultCommit "${manifest.resultCommit}"`);
  }
  if (restoredTree !== manifest.resultTree) {
    throw new Error(`restoreHandoffArtifact: restored tree "${restoredTree}" does not equal the manifest's resultTree "${manifest.resultTree}"`);
  }
  if (await hasUncommittedChanges(destRepoDir, env)) {
    throw new Error(`restoreHandoffArtifact: restored workspace at "${destRepoDir}" is unexpectedly dirty immediately after checkout`);
  }

  return { restoredCommit, restoredTree };
}
