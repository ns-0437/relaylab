// Phase 2C.1, Part 1: dedicated coverage proving the Docker
// verification adapter's container-visible input is a FAITHFUL,
// complete representation of the selected Git tree -- not merely "a
// tar was produced." Builds a real, throwaway Git repository with one
// of each tracked-entry kind this adapter is expected to handle (plus
// two kinds it must explicitly REFUSE), materializes it through the
// REAL `materializeCandidateInput` -> `verifyInDocker` path, and
// compares the container-visible file set/bytes/modes against
// independent host-side `git` inspection of the SAME tree -- never
// trusting the archive step's own claims about itself.
//
// Requires real Docker (same `before()` convention as
// docker-verify.test.ts); does not require MySQL/NATS.
import { test, before, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { git, listTreeEntries, makeIsolatedGitEnv } from "../../src/worker/git.js";
import { VERIFY_IMAGE_TAG, assertMaterializedTreeMatches, materializeCandidateInput, resolveVerifyImageId, verifyInDocker, VerificationIdentityMismatch } from "../../src/worker/docker-verify.js";
import { uniqueId } from "./helpers.js";

const execFileAsync = promisify(execFile);

let scratchRoot: string;
let repoDir: string;
let env: NodeJS.ProcessEnv;

before(async () => {
  try {
    await execFileAsync("docker", ["version", "--format", "{{.Server.Version}}"]);
  } catch (err) {
    throw new Error(
      `docker-verify-exact-input.test.ts requires a real, reachable Docker daemon -- none found (${err instanceof Error ? err.message : String(err)}). ` +
        `This suite fails loudly rather than substituting a mock for Docker -- start Docker Desktop and retry.`,
    );
  }
  try {
    await resolveVerifyImageId(VERIFY_IMAGE_TAG);
  } catch (err) {
    throw new Error(
      `docker-verify-exact-input.test.ts requires the pinned verification image "${VERIFY_IMAGE_TAG}" to already be built -- run "npm run docker:build-verify-image" first. (${err instanceof Error ? err.message : String(err)})`,
    );
  }
});

beforeEach(async () => {
  scratchRoot = await mkdtemp(path.join(tmpdir(), "relaylab-exact-input-test-"));
  repoDir = path.join(scratchRoot, "repo");
  await mkdir(repoDir, { recursive: true });
  env = makeIsolatedGitEnv(path.join(scratchRoot, ".home"));
  await git(["init", "--initial-branch=main"], repoDir, env);
});

afterEach(async () => {
  await rm(scratchRoot, { recursive: true, force: true });
});

async function commitAll(message: string): Promise<string> {
  await git(["add", "-A"], repoDir, env);
  return commitStagedOnly(message);
}

/**
 * Commits whatever is ALREADY in the index, WITHOUT running `git add
 * -A` first -- required for the symlink/gitlink tests below, which
 * stage their fake tree entry directly via `git update-index
 * --cacheinfo` (no corresponding file ever exists in the working
 * tree). `git add -A` would otherwise see that entry as "present in
 * the index but absent from the working tree" and stage its REMOVAL,
 * silently undoing the manual staging before this function ever runs.
 */
async function commitStagedOnly(message: string): Promise<string> {
  await git(["-c", "user.name=t", "-c", "user.email=t@localhost", "-c", "commit.gpgsign=false", "commit", "--no-verify", "-m", message], repoDir, env);
  const { stdout } = await git(["rev-parse", "HEAD"], repoDir, env);
  return stdout.trim();
}

/** The manifest-walker `verify.js` this suite's own fixture repo carries -- NOT the real worker's fixed verification script. It is itself a tracked file in the fixture so it travels through the SAME `materializeCandidateInput` -> container path as everything else being tested; its job is to report, from INSIDE the Linux container, exactly what it can see (path/size/sha256/exec-bit) so this test can compare that report against independent host-side `git` inspection. */
const MANIFEST_WALKER_VERIFY_JS = `
const fs = require("node:fs");
const path = require("node:path");
const crypto = require("node:crypto");
function walk(dir, base) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    const rel = base ? base + "/" + entry.name : entry.name;
    if (entry.isDirectory()) {
      out.push(...walk(full, rel));
    } else if (entry.isFile()) {
      const content = fs.readFileSync(full);
      const mode = fs.statSync(full).mode & 0o777;
      out.push({ path: rel, size: content.length, sha256: crypto.createHash("sha256").update(content).digest("hex"), execBit: (mode & 0o111) !== 0 });
    }
  }
  return out;
}
const manifest = walk(".", "");
manifest.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
console.log("MANIFEST_JSON_START");
console.log(JSON.stringify(manifest));
console.log("MANIFEST_JSON_END");
process.exit(0);
`;

interface ManifestEntry {
  path: string;
  size: number;
  sha256: string;
  execBit: boolean;
}

function parseManifest(stdout: string): ManifestEntry[] {
  const start = stdout.indexOf("MANIFEST_JSON_START");
  const end = stdout.indexOf("MANIFEST_JSON_END");
  assert.ok(start >= 0 && end > start, `expected a MANIFEST_JSON_START/END block in stdout, got:\n${stdout}`);
  const jsonLine = stdout.slice(start + "MANIFEST_JSON_START".length, end).trim();
  return JSON.parse(jsonLine) as ManifestEntry[];
}

function sha256Hex(buf: Buffer): string {
  return createHash("sha256").update(buf).digest("hex");
}

/** Independent, host-side content hash for a tracked blob at `relPath` in `ref` -- via `git cat-file -p -- <ref>:<relPath>` rather than reading the working tree file (which could differ from what was actually committed). Returns raw bytes so a binary file's hash is computed correctly. */
async function hostBlobSha256(ref: string, relPath: string): Promise<string> {
  const { stdout: catOut } = await git(["cat-file", "-p", `${ref}:${relPath}`], repoDir, env);
  // `git()` captures stdout as a string (utf8-decoded by execFile) -- for
  // the binary-content test below this is intentionally NOT relied upon;
  // see that test's own comment for why it reads the blob a different way.
  return sha256Hex(Buffer.from(catOut, "utf8"));
}

test("exact-input fidelity: a tracked export-ignore'd file STILL exists with its original bytes, a tracked export-subst'd file keeps its UNCHANGED marker, exec/non-exec bits are correct, and untracked files are absent", async () => {
  // Phase 2C correctness patch, Part 3: the REQUIRED contract is the
  // OPPOSITE of what this test originally asserted. "Exact-tree
  // verification" means every TRACKED path and its EXACT git-object
  // bytes are delivered -- `.gitattributes` `export-ignore` (an
  // archive-time OMISSION) and `export-subst` (an archive-time
  // REWRITE) are themselves just more tracked content; applying them
  // would mean verifying a FILTERED/TRANSFORMED tree, not the exact
  // one a checkpoint claims to represent. `materializeCandidateInput`
  // no longer uses `git archive` at all (see its own doc comment and
  // docs/EXECUTION.md "Archive-attribute handling") specifically so
  // these two attributes have no effect on the verification input.
  await writeFile(path.join(repoDir, "verify.js"), MANIFEST_WALKER_VERIFY_JS, "utf8");
  await writeFile(
    path.join(repoDir, ".gitattributes"),
    "IGNORED.md export-ignore\nVERSION.txt export-subst\n",
    "utf8",
  );
  await writeFile(path.join(repoDir, "IGNORED.md"), "this must NEVER appear in the verification input\n", "utf8");
  await writeFile(path.join(repoDir, "VERSION.txt"), "version: $Format:%H$\n", "utf8");
  await mkdir(path.join(repoDir, "bin"), { recursive: true });
  await writeFile(path.join(repoDir, "bin", "run.sh"), "#!/bin/sh\necho hi\n", "utf8");
  await mkdir(path.join(repoDir, "src"), { recursive: true });
  await writeFile(path.join(repoDir, "src", "plain.txt"), "just data, not executable\n", "utf8");

  const commit = await commitAll("initial");
  // The executable bit is recorded in git's index via `--chmod`, NOT
  // via the host filesystem's own permission bits (irrelevant on
  // Windows, and `core.fileMode` is commonly `false` there anyway) --
  // this is the SAME mechanism a real contributor's `chmod +x && git
  // add` would ultimately record in the tree, applied directly via
  // plumbing for a deterministic, host-independent test.
  await git(["update-index", "--chmod=+x", "bin/run.sh"], repoDir, env);
  const finalCommit = await commitAll("mark bin/run.sh executable");
  void commit;

  const entries = await listTreeEntries(repoDir, finalCommit, env);
  const execEntry = entries.find((e) => e.path === "bin/run.sh");
  const plainEntry = entries.find((e) => e.path === "src/plain.txt");
  assert.equal(execEntry?.mode, "100755", "test setup check: bin/run.sh must be recorded as executable (100755) in the tree");
  assert.equal(plainEntry?.mode, "100644", "test setup check: src/plain.txt must be recorded as a regular, non-executable file (100644) in the tree");

  // An UNTRACKED host file, created AFTER the commit above, sitting
  // right next to the tracked ones -- must never appear in the
  // verification input (`git archive` only ever reads from git's
  // object store, never the live working tree's untracked files).
  await writeFile(path.join(repoDir, "untracked-secret.txt"), "must never leave this host\n", "utf8");

  const materialized = await materializeCandidateInput(repoDir, finalCommit, env, path.join(scratchRoot, ".verify-scratch"));
  assert.equal(materialized.treeSha, (await git(["rev-parse", `${finalCommit}^{tree}`], repoDir, env)).stdout.trim());
  try {
    const result = await verifyInDocker({ inputDir: materialized.exportDir, taskId: uniqueId("task-exact-input"), attemptId: "attempt-1" });
    assert.equal(result.outcome, "passed", `expected the manifest-walker script to exit 0; stderr: ${result.stderr}`);
    const manifest = parseManifest(result.stdout).filter((e) => e.path !== "verify.js");
    const byPath = new Map(manifest.map((e) => [e.path, e]));

    // export-ignore: IGNORED.md must STILL be PRESENT, with its
    // original tracked bytes -- the REQUIRED contract (Phase 2C
    // correctness patch, Part 3), expected bytes read directly from
    // the SELECTED git object (`hostBlobSha256`), never re-derived
    // from a second, independent archive export.
    const ignoredEntry = byPath.get("IGNORED.md");
    assert.ok(ignoredEntry, "an export-ignore'd tracked file must STILL exist in the verification input -- export-ignore must have NO effect on exact-tree materialization");
    assert.equal(ignoredEntry!.sha256, await hostBlobSha256(finalCommit, "IGNORED.md"), "IGNORED.md's bytes must exactly match its tracked git blob, unmodified");

    // export-subst: VERSION.txt must be PRESENT, with its
    // `$Format:...$` marker UNCHANGED -- not expanded, not dropped.
    const versionEntry = byPath.get("VERSION.txt");
    assert.ok(versionEntry, "VERSION.txt (export-subst) must be present in the verification input");
    assert.equal(versionEntry!.sha256, await hostBlobSha256(finalCommit, "VERSION.txt"), "VERSION.txt's bytes must exactly match its tracked git blob -- the export-subst marker must remain UNCHANGED, never expanded");
    assert.equal(versionEntry!.sha256, sha256Hex(Buffer.from("version: $Format:%H$\n", "utf8")), "VERSION.txt's content must be the literal, un-expanded placeholder text");

    // Untracked file must be completely absent.
    assert.equal(byPath.has("untracked-secret.txt"), false, "an untracked host file must never appear in the verification input");
    assert.equal(manifest.some((e) => e.path.includes("untracked")), false, "no path containing 'untracked' may appear anywhere in the verification input");

    // Executable / non-executable semantics, checked INSIDE Linux, not
    // merely inferred from the Windows host or from git's own recorded
    // tree mode.
    const execResult = byPath.get("bin/run.sh");
    const plainResult = byPath.get("src/plain.txt");
    assert.ok(execResult, "bin/run.sh must be present");
    assert.ok(plainResult, "src/plain.txt must be present");
    assert.equal(execResult!.execBit, true, "bin/run.sh (tracked as 100755) must be exec-bit SET inside the Linux container");
    assert.equal(plainResult!.execBit, false, "src/plain.txt (tracked as 100644) must be exec-bit CLEAR inside the Linux container");

    // Byte-for-byte content fidelity for the two mode-bit files,
    // against an INDEPENDENT host-side read of the SAME tracked blob
    // (`git cat-file -p`, via `hostBlobSha256`) -- never a second,
    // independent archive export that could itself drift.
    assert.equal(execResult!.sha256, await hostBlobSha256(finalCommit, "bin/run.sh"));
    assert.equal(plainResult!.sha256, await hostBlobSha256(finalCommit, "src/plain.txt"));

    // Expected tracked paths (from `listTreeEntries` -- the SAME
    // source of truth `materializeCandidateInput` itself enumerates
    // from) compared against the materialized/container-visible set.
    // EVERY tracked path is expected now, INCLUDING the export-ignore'd
    // one -- exact-tree verification means the full tracked set, not a
    // filtered view of it.
    const expectedPaths = (await listTreeEntries(repoDir, finalCommit, env))
      .map((e) => e.path)
      .filter((p) => p !== "verify.js") // the test's own introspection script -- excluded from the manifest above too, see that filter's comment
      .sort();
    assert.deepEqual(manifest.map((e) => e.path).sort(), expectedPaths, "the container-visible file set must be EXACTLY the full tracked set reported by listTreeEntries -- no more, no less, and export-ignore must not remove anything from it");
  } finally {
    await materialized.cleanup();
  }
});

test("exact-input fidelity: binary content and a filename containing spaces are preserved exactly", async () => {
  await writeFile(path.join(repoDir, "verify.js"), MANIFEST_WALKER_VERIFY_JS, "utf8");
  // Explicitly marked binary so no CRLF/text normalization is ever
  // applied to it on this (or any) host -- the content below
  // deliberately includes NUL and high-bit bytes, which would make
  // naive text handling corrupt it if attempted.
  await writeFile(path.join(repoDir, ".gitattributes"), "assets/binary.bin -text binary\n", "utf8");
  await mkdir(path.join(repoDir, "assets"), { recursive: true });
  const binaryContent = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0x0a, 0x0d, 0x80, 0x90, 0xaa, 0x00, 0x7f]);
  await writeFile(path.join(repoDir, "assets", "binary.bin"), binaryContent);
  await writeFile(path.join(repoDir, "file with spaces.txt"), "content of a file whose NAME has spaces\n", "utf8");

  const commit = await commitAll("binary + spaced filename");
  const materialized = await materializeCandidateInput(repoDir, commit, env, path.join(scratchRoot, ".verify-scratch"));
  try {
    const result = await verifyInDocker({ inputDir: materialized.exportDir, taskId: uniqueId("task-exact-input-binary"), attemptId: "attempt-1" });
    assert.equal(result.outcome, "passed", `expected the manifest-walker script to exit 0; stderr: ${result.stderr}`);
    const manifest = parseManifest(result.stdout).filter((e) => e.path !== "verify.js");
    const byPath = new Map(manifest.map((e) => [e.path, e]));

    const binEntry = byPath.get("assets/binary.bin");
    assert.ok(binEntry, "assets/binary.bin must be present");
    assert.equal(binEntry!.size, binaryContent.length, "binary file size must be preserved exactly");
    assert.equal(binEntry!.sha256, sha256Hex(binaryContent), "binary file content must be preserved byte-for-byte (NUL and high-bit bytes included)");

    const spacedEntry = byPath.get("file with spaces.txt");
    assert.ok(spacedEntry, "a tracked file whose NAME contains spaces must be present under its exact name");
    assert.equal(spacedEntry!.sha256, sha256Hex(Buffer.from("content of a file whose NAME has spaces\n", "utf8")));
  } finally {
    await materialized.cleanup();
  }
});

test("exact-input fidelity: a tracked symlink entry is explicitly rejected, never silently materialized", async () => {
  await writeFile(path.join(repoDir, "verify.js"), MANIFEST_WALKER_VERIFY_JS, "utf8");
  await commitAll("baseline");
  // Stage a tree entry with mode 120000 (symlink) via plumbing --
  // portable on Windows, which cannot reliably create a real OS-level
  // symlink without elevated privileges. The blob's content is the
  // symlink's TARGET path text, exactly as git itself stores a real
  // symlink's target; `git hash-object -w <file>` (not `--stdin`, which
  // `git()`'s argument-array wrapper has no stdin-piping support for)
  // writes that text as a real blob and reports its SHA.
  const symlinkTargetFile = path.join(scratchRoot, "symlink-target-content.txt");
  await writeFile(symlinkTargetFile, "../outside-target.txt", "utf8");
  const { stdout: blobSha } = await git(["hash-object", "-w", symlinkTargetFile], repoDir, env);
  await git(["update-index", "--add", "--cacheinfo", `120000,${blobSha.trim()},evil-link`], repoDir, env);
  const commitWithSymlink = await commitStagedOnly("add a tracked symlink entry");

  const entries = await listTreeEntries(repoDir, commitWithSymlink, env);
  assert.ok(entries.some((e) => e.path === "evil-link" && e.mode === "120000"), "test setup check: the tree must actually contain a 120000 (symlink) entry");

  await assert.rejects(
    () => materializeCandidateInput(repoDir, commitWithSymlink, env, path.join(scratchRoot, ".verify-scratch")),
    /unsupported tree entry|symlink/i,
    "a tree containing a tracked symlink entry must be explicitly refused, never silently materialized as an incomplete/unsafe input",
  );
});

test("exact-input fidelity: a tracked submodule (gitlink) entry is explicitly rejected, never silently materialized", async () => {
  await writeFile(path.join(repoDir, "verify.js"), MANIFEST_WALKER_VERIFY_JS, "utf8");
  await commitAll("baseline");
  // Stage a tree entry with mode 160000 (gitlink/submodule) via
  // plumbing -- a gitlink's "object" is a commit SHA in a DIFFERENT
  // repository, never an object this repository actually stores, so
  // an arbitrary well-formed SHA is sufficient to construct one without
  // a real nested repository.
  await git(["update-index", "--add", "--cacheinfo", "160000,1111111111111111111111111111111111111111,vendor/some-submodule"], repoDir, env);
  const commitWithGitlink = await commitStagedOnly("add a tracked submodule gitlink entry");

  const entries = await listTreeEntries(repoDir, commitWithGitlink, env);
  assert.ok(
    entries.some((e) => e.path === "vendor/some-submodule" && e.mode === "160000"),
    "test setup check: the tree must actually contain a 160000 (gitlink/submodule) entry",
  );

  await assert.rejects(
    () => materializeCandidateInput(repoDir, commitWithGitlink, env, path.join(scratchRoot, ".verify-scratch")),
    /unsupported tree entry|submodule|gitlink/i,
    "a tree containing a submodule gitlink entry must be explicitly refused, never silently materialized as an incomplete input",
  );
});

test("identity-mismatch regression: assertMaterializedTreeMatches rejects verification of a DIFFERENT tree than intended", async () => {
  await writeFile(path.join(repoDir, "verify.js"), MANIFEST_WALKER_VERIFY_JS, "utf8");
  await writeFile(path.join(repoDir, "a.txt"), "version A\n", "utf8");
  const commitA = await commitAll("version A");
  await writeFile(path.join(repoDir, "a.txt"), "version B -- a DIFFERENT tree\n", "utf8");
  const commitB = await commitAll("version B");

  const materializedA = await materializeCandidateInput(repoDir, commitA, env, path.join(scratchRoot, ".verify-scratch"));
  const materializedB = await materializeCandidateInput(repoDir, commitB, env, path.join(scratchRoot, ".verify-scratch"));
  try {
    assert.notEqual(materializedA.treeSha, materializedB.treeSha, "test setup check: the two commits must have genuinely different trees");

    // Correct usage: a materialization matched against its OWN tree
    // must never be rejected.
    assert.doesNotThrow(() => assertMaterializedTreeMatches(materializedA, materializedA.treeSha, "self-match-A"));
    assert.doesNotThrow(() => assertMaterializedTreeMatches(materializedB, materializedB.treeSha, "self-match-B"));

    // The regression itself: materialization B is NOT a verification
    // of A's tree -- this must never be silently accepted as if it
    // were, which is exactly what would let a verification of another
    // tree authorize the intended checkpoint/acknowledgement.
    assert.throws(
      () => assertMaterializedTreeMatches(materializedB, materializedA.treeSha, "cross-check-B-against-A"),
      VerificationIdentityMismatch,
      "verifying tree B's materialization against tree A's expected identity must be rejected",
    );
    assert.throws(
      () => assertMaterializedTreeMatches(materializedA, materializedB.treeSha, "cross-check-A-against-B"),
      VerificationIdentityMismatch,
    );
  } finally {
    await materializedA.cleanup();
    await materializedB.cleanup();
  }
});
